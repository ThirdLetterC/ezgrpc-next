/* grpc_tls.c - TLS backend independent layer: public TLS API and per-session plumbing.
 *
 * This file implements the public TLS functions (grpc_tls_backend(), grpc_server_reload_tls(),
 * grpc_server_tls_native()), the server context lifetime (tls_enable(), tls_free()) and the
 * grpc_tls_* per-session wrappers used by the session code. The wrappers validate the session
 * and forward to the tls_backend_* contract from internal_tls.h, which exactly one
 * src/grpc_tls_<backend>.c implements.
 *
 * The TLS layer never touches the socket: the event loop feeds received ciphertext in with
 * grpc_tls_feed() and collects ciphertext to send with grpc_tls_drain(). When the library is
 * built without TLS, stubs report the "none" backend and fail every session operation.
 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "core.h"
#include "internal_tls.h"
#include "internal_http3.h"

#if defined(GRPC_ENABLE_TLS)

/* Returns the name of the TLS backend compiled in (e.g. "openssl"). */
GRPC_API const char *grpc_tls_backend(void) {
  return tls_backend_name();
}

/* Returns the version string reported by the TLS library at run time. */
GRPC_API const char *grpc_tls_backend_version(void) {
  return tls_backend_version();
}

/* Returns the backend's native context of the active server context (e.g. SSL_CTX *), or nullptr
 * when the server is nullptr or has no TLS context. */
GRPC_API void *grpc_server_tls_native(grpc_server *server) {
  return server != nullptr && server->tls_pool.active.ctx != nullptr
             ? tls_backend_ctx_native(server->tls_pool.active.ctx)
             : nullptr;
}

static void free_tcp_ctx(void *ctx) {
  tls_backend_ctx_free(ctx);
}

/* Builds the server's first TLS context from `cfg` and makes it active. Returns GRPC_OK, or
 * GRPC_ERR_INVALID when the credentials cannot be loaded. Expects the PEM lengths in `cfg` to be
 * already filled in. */
grpc_err tls_enable(grpc_server *server, const grpc_tls_config *cfg) {
  grpc_tls_ctx *ctx = tls_backend_ctx_new(cfg);
  if (ctx == nullptr) {
    LOG_ERROR(server, "cannot load the TLS certificate/key configuration\n");
    return GRPC_ERR_INVALID;
  }
  if (tls_ctx_pool_reserve(&server->tls_pool) != GRPC_OK) {
    tls_backend_ctx_free(ctx);
    return GRPC_ERR_NOMEM;
  }
  tls_ctx_pool_install(&server->tls_pool, ctx, free_tcp_ctx);
  server->is_tls = 1;
  return GRPC_OK;
}

/* Validates `tls` (exactly one of the file pair or the PEM pair, each complete) and fills in the
 * lengths of NUL-terminated PEM strings given with a zero length. All or nothing: the new TCP
 * and HTTP/3 contexts are both built (and room to retire the old ones reserved) before either is
 * switched, so on any failure both keep the previous configuration. Existing connections keep
 * the context they were created with; a replaced context is freed with its last connection. A
 * server started without TLS is refused with GRPC_ERR_STATE: a "reload" must not switch its new
 * connections from plaintext to TLS under existing clients. */
GRPC_API grpc_err grpc_server_reload_tls(grpc_server *server, const grpc_tls_config *tls) {
  if (server == nullptr || tls == nullptr)
    return GRPC_ERR_INVALID;
  bool has_files = tls->cert_file != nullptr || tls->key_file != nullptr;
  bool has_pems = tls->cert_pem != nullptr || tls->key_pem != nullptr;
  if ((has_files && has_pems) || (!has_files && !has_pems))
    return GRPC_ERR_INVALID;
  if (has_files && (tls->cert_file == nullptr || tls->key_file == nullptr))
    return GRPC_ERR_INVALID;
  if (has_pems && (tls->cert_pem == nullptr || tls->key_pem == nullptr))
    return GRPC_ERR_INVALID;
  if (!server->is_tls)
    return GRPC_ERR_STATE;
  grpc_tls_config normalized = *tls;
  if (normalized.cert_pem != nullptr && normalized.cert_pem_len == 0)
    normalized.cert_pem_len = strlen(normalized.cert_pem);
  if (normalized.key_pem != nullptr && normalized.key_pem_len == 0)
    normalized.key_pem_len = strlen(normalized.key_pem);
  if (normalized.ca_pem != nullptr && normalized.ca_pem_len == 0)
    normalized.ca_pem_len = strlen(normalized.ca_pem);

  grpc_tls_ctx *ctx = tls_backend_ctx_new(&normalized);
  if (ctx == nullptr) {
    LOG_ERROR(server, "cannot load the TLS certificate/key configuration\n");
    return GRPC_ERR_INVALID;
  }
  /* HTTP/3 connections use their own QUIC TLS context, built from the same credentials */
  void *quic_ctx = nullptr;
  grpc_err err = http3_server_tls_prepare(server, &normalized, &quic_ctx);
  if (err == GRPC_OK && tls_ctx_pool_reserve(&server->tls_pool) != GRPC_OK) {
    http3_server_tls_discard(server, quic_ctx);
    err = GRPC_ERR_NOMEM;
  }
  if (err != GRPC_OK) {
    tls_backend_ctx_free(ctx);
    return err;
  }
  /* nothing can fail from here on */
  tls_ctx_pool_install(&server->tls_pool, ctx, free_tcp_ctx);
  http3_server_tls_install(server, quic_ctx);
  return GRPC_OK;
}

/* Frees the active server context and every retired one. Called when the server is destroyed,
 * after its connections. */
void tls_free(grpc_server *server) {
  tls_ctx_pool_free(&server->tls_pool);
}

/* Creates the backend session for a new connection from the server's active context. Returns 0
 * on success, and also when the server does not use TLS (nothing to do); 1 if the backend session
 * cannot be created. */
int grpc_tls_session_init(grpc_session *session) {
  if (session == nullptr || session->server == nullptr || !session->server->is_tls ||
      session->server->tls_pool.active.ctx == nullptr) {
    return 0;
  }

  void *ctx = tls_ctx_pool_acquire(&session->server->tls_pool);
  grpc_tls_session *tls = tls_backend_session_new(ctx);
  if (tls == nullptr) {
    tls_ctx_pool_release(&session->server->tls_pool, ctx);
    return 1;
  }

  session->tls = tls;
  session->tls_ctx_used = ctx;
  session->is_tls = 1;
  session->tls_handshake_done = 0;
  return 0;
}

/* Hands ciphertext received from the socket to the backend. Returns 0 on success, nonzero on
 * error or when the session has no TLS state. */
int grpc_tls_feed(grpc_session *session, const void *buf, size_t len) {
  if (session == nullptr || session->tls == nullptr) {
    return 1;
  }
  return tls_backend_feed(session->tls, buf, len);
}

/* Appends the ciphertext the backend has ready to send to `out`. Returns TLS_DRAIN_OK,
 * TLS_DRAIN_NOMEM (nothing lost, retry later) or TLS_DRAIN_ERROR, also when the session has no
 * TLS state. */
int grpc_tls_drain(grpc_session *session, grpc_buf *out) {
  if (session == nullptr || session->tls == nullptr) {
    return 1;
  }
  return tls_backend_drain(session->tls, out);
}

/* Advances the server handshake with the ciphertext fed so far. Returns 1 when complete (and on
 * every later call, without calling the backend), 0 while more input is needed, -1 on error. */
int grpc_tls_handshake(grpc_session *session) {
  if (session == nullptr || session->tls == nullptr) {
    return -1;
  }
  if (session->tls_handshake_done) {
    return 1;
  }

  int res = tls_backend_handshake(session->tls);
  if (res == 1) {
    session->tls_handshake_done = 1;
  }
  return res;
}

/* Decrypts up to `len` bytes of application data into `buf`. Returns the byte count, 0 on the
 * peer's close_notify, -2 when more ciphertext is needed, -1 on error. */
ssize_t grpc_tls_read(grpc_session *session, void *buf, size_t len) {
  if (session == nullptr || session->tls == nullptr) {
    return -1;
  }
  return tls_backend_read(session->tls, buf, len);
}

/* Encrypts all `len` bytes of `buf`; the ciphertext is collected with grpc_tls_drain(). Returns
 * `len`, or -1 on error. */
ssize_t grpc_tls_write(grpc_session *session, const void *buf, size_t len) {
  if (session == nullptr || session->tls == nullptr) {
    return -1;
  }
  return tls_backend_write(session->tls, buf, len);
}

/* Queues a close_notify alert, best effort; the caller drains it with grpc_tls_drain(). */
void grpc_tls_close_notify(grpc_session *session) {
  if (session != nullptr && session->tls != nullptr) {
    tls_backend_close_notify(session->tls);
  }
}

/* Frees the backend session, if any, and resets the session's TLS state. */
void grpc_tls_session_free(grpc_session *session) {
  if (session != nullptr && session->tls != nullptr) {
    tls_backend_session_free(session->tls);
    tls_ctx_pool_release(&session->server->tls_pool, session->tls_ctx_used);
    session->tls_ctx_used = nullptr;
    session->tls = nullptr;
    session->is_tls = 0;
    session->tls_handshake_done = 0;
  }
}

/* Writes the subject name of the peer's certificate into `buf` as a NUL-terminated string.
 * Returns 0 on success, -1 if there is no peer certificate, the arguments are invalid, or the
 * backend cannot provide it (the rustls backend never can). */
int grpc_tls_peer_subject(grpc_session *session, char *buf, size_t len) {
  if (session == nullptr || session->tls == nullptr || buf == nullptr || len == 0)
    return -1;
  return tls_backend_peer_subject(session->tls, buf, len);
}

#else /* built without TLS */

/* Without TLS: reports the "none" backend. */
GRPC_API const char *grpc_tls_backend(void) {
  return "none";
}

/* Without TLS: returns an empty version string. */
GRPC_API const char *grpc_tls_backend_version(void) {
  return "";
}

/* Without TLS: there is no native context. */
GRPC_API void *grpc_server_tls_native([[maybe_unused]] grpc_server *server) {
  return nullptr;
}

/* Without TLS: logs that TLS was requested and fails with GRPC_ERR_UNSUPPORTED. */
grpc_err tls_enable(grpc_server *server, [[maybe_unused]] const grpc_tls_config *cfg) {
  LOG_ERROR(server, "TLS is configured, but grpc is built without TLS (TLS=none)\n");
  return GRPC_ERR_UNSUPPORTED;
}

/* Without TLS: GRPC_ERR_INVALID for nullptr arguments, GRPC_ERR_UNSUPPORTED otherwise. */
GRPC_API grpc_err grpc_server_reload_tls(grpc_server *server, const grpc_tls_config *tls) {
  if (server == nullptr || tls == nullptr)
    return GRPC_ERR_INVALID;
  return GRPC_ERR_UNSUPPORTED;
}

/* Without TLS: there is nothing to free. */
void tls_free([[maybe_unused]] grpc_server *server) {}

/* Without TLS: succeeds without doing anything, as for a plaintext server. */
int grpc_tls_session_init([[maybe_unused]] grpc_session *session) {
  return 0;
}

/* Without TLS: always fails. */
int grpc_tls_feed([[maybe_unused]] grpc_session *session, [[maybe_unused]] const void *buf,
                  [[maybe_unused]] size_t len) {
  return 1;
}

/* Without TLS: always fails. */
int grpc_tls_drain([[maybe_unused]] grpc_session *session, [[maybe_unused]] grpc_buf *out) {
  return 1;
}

/* Without TLS: always fails. */
int grpc_tls_handshake([[maybe_unused]] grpc_session *session) {
  return -1;
}

/* Without TLS: always fails. */
ssize_t grpc_tls_read([[maybe_unused]] grpc_session *session, [[maybe_unused]] void *buf,
                      [[maybe_unused]] size_t len) {
  return -1;
}

/* Without TLS: always fails. */
ssize_t grpc_tls_write([[maybe_unused]] grpc_session *session, [[maybe_unused]] const void *buf,
                       [[maybe_unused]] size_t len) {
  return -1;
}

/* Without TLS: does nothing. */
void grpc_tls_close_notify([[maybe_unused]] grpc_session *session) {}

/* Without TLS: does nothing. */
void grpc_tls_session_free([[maybe_unused]] grpc_session *session) {}

/* Without TLS: there is never a peer certificate. */
int grpc_tls_peer_subject([[maybe_unused]] grpc_session *session, [[maybe_unused]] char *buf,
                          [[maybe_unused]] size_t len) {
  return -1;
}

#endif
