#pragma once

/* internal_tls.h - server TLS layer and backend contract
 *
 * The TLS layer never touches the socket. Ciphertext received by the event
 * loop is handed over with grpc_tls_feed() and the ciphertext to send is
 * collected with grpc_tls_drain(), so every TLS library is driven the
 * same way. Exactly one backend (src/grpc_tls_<backend>.c) is built.
 *
 * The grpc_tls_* functions (src/grpc_tls.c) are the per-session API used by the session code;
 * they validate the session and forward to the tls_backend_* contract each backend implements.
 * The grpc_tls_io helpers serve backends that move bytes through I/O callbacks. All of it runs on
 * the server loop thread.
 */

#include "grpc_tls.h"
#include "internal_helpers.h"

/* The TLS backend's server context. */
typedef struct grpc_tls_ctx grpc_tls_ctx;

/* Per-connection TLS layer, used by the session code. These return:
 *   grpc_tls_session_init, _feed, _drain: 0 on success
 *   grpc_tls_handshake: 1 when complete, 0 while in progress, -1 on error
 *   grpc_tls_read: bytes read, 0 on close_notify, -2 when more input is needed, -1 on error
 *   grpc_tls_write: bytes written (all of them), -1 on error */
/* Create the TLS session for a new connection of a TLS server (no-op on a plaintext server). */
int grpc_tls_session_init(grpc_session *session);
/* Hand ciphertext received from the socket to the backend. */
int grpc_tls_feed(grpc_session *session, const void *buf, size_t len);
/* Append the ciphertext ready to send to `out`. */
int grpc_tls_drain(grpc_session *session, grpc_buf *out);
/* Advance the server handshake with the ciphertext fed so far. */
int grpc_tls_handshake(grpc_session *session);
/* Decrypt up to `len` bytes of application data into `buf`. */
ssize_t grpc_tls_read(grpc_session *session, void *buf, size_t len);
/* Encrypt all of `buf`; collect the ciphertext with grpc_tls_drain(). */
ssize_t grpc_tls_write(grpc_session *session, const void *buf, size_t len);
/* Queue a close_notify alert (best effort); collect it with grpc_tls_drain(). */
void grpc_tls_close_notify(grpc_session *session);
/* Free the session's TLS state, if any, and reset its TLS flags. */
void grpc_tls_session_free(grpc_session *session);
/* Write the peer certificate subject into `buf` (NUL-terminated); 0 on success, -1 if there is
 * none or the backend cannot provide it (rustls). */
int grpc_tls_peer_subject(grpc_session *session, char *buf, size_t len);

/* The backend contract. Same return conventions as above. */
typedef struct grpc_tls_session grpc_tls_session;

/* The backend name reported by grpc_tls_backend() and the TLS library's version string. */
const char *tls_backend_name(void);
const char *tls_backend_version(void);
/* Build a server context (certificate, key, optional client CA, h2 ALPN) from `cfg`, whose PEM
 * lengths are already filled in; nullptr on any error. */
grpc_tls_ctx *tls_backend_ctx_new(const grpc_tls_config *cfg);
void tls_backend_ctx_free(grpc_tls_ctx *ctx);
/* The library's own context, returned by grpc_server_tls_native(). */
void *tls_backend_ctx_native(grpc_tls_ctx *ctx);
/* Create / free a per-connection session from `ctx`; new returns nullptr on failure. */
grpc_tls_session *tls_backend_session_new(grpc_tls_ctx *ctx);
void tls_backend_session_free(grpc_tls_session *tls);
int tls_backend_feed(grpc_tls_session *tls, const void *buf, size_t len);
int tls_backend_drain(grpc_tls_session *tls, grpc_buf *out);
int tls_backend_handshake(grpc_tls_session *tls);
ssize_t tls_backend_read(grpc_tls_session *tls, void *buf, size_t len);
ssize_t tls_backend_write(grpc_tls_session *tls, const void *buf, size_t len);
void tls_backend_close_notify(grpc_tls_session *tls);
int tls_backend_peer_subject(grpc_tls_session *tls, char *buf, size_t len);

/* Ciphertext queues for backends that move bytes through I/O callbacks
 * (wolfSSL, GnuTLS, rustls): the callbacks pull from `in` and push to `out`. */
typedef struct grpc_tls_io grpc_tls_io;
struct grpc_tls_io {
  /* Received ciphertext; bytes before in_seek were already consumed by the library. */
  grpc_buf in;
  size_t in_seek;
  /* Ciphertext produced by the library, waiting for tls_io_drain(). */
  grpc_buf out;
};

/* Append received ciphertext to `in`, compacting consumed bytes first. 0 on success, 1 when out
 * of memory. */
static inline int tls_io_feed(grpc_tls_io *io, const void *data, size_t len) {
  /* Drop the bytes already consumed by the library. */
  if (io->in_seek > 0) {
    memmove(io->in.data, io->in.data + io->in_seek, io->in.len - io->in_seek);
    io->in.len -= io->in_seek;
    io->in_seek = 0;
  }
  return buf_append(&io->in, data, len);
}

/* Bytes of received ciphertext not yet consumed. */
static inline size_t tls_io_pending(const grpc_tls_io *io) {
  return io->in.len - io->in_seek;
}

/* Read-callback helper: copy up to `len` unconsumed bytes to `dst`; returns the count (0 when
 * nothing is pending). */
static inline size_t tls_io_pull(grpc_tls_io *io, void *dst, size_t len) {
  size_t n = tls_io_pending(io);
  if (n > len)
    n = len;
  memcpy(dst, io->in.data + io->in_seek, n);
  io->in_seek += n;
  return n;
}

/* Write-callback helper: queue ciphertext produced by the library. 0 on success, 1 if out of
 * memory. */
static inline int tls_io_push(grpc_tls_io *io, const void *data, size_t len) {
  return buf_append(&io->out, data, len);
}

/* Move all queued output ciphertext to the end of `dst`. 0 on success (also when empty), 1 if
 * out of memory (the output stays queued). */
static inline int tls_io_drain(grpc_tls_io *io, grpc_buf *dst) {
  if (io->out.len == 0)
    return 0;
  if (buf_append(dst, io->out.data, io->out.len))
    return 1;
  io->out.len = 0;
  return 0;
}

/* Free both queues. */
static inline void tls_io_free(grpc_tls_io *io) {
  buf_free(&io->in);
  buf_free(&io->out);
}
