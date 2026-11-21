/* grpc_tls_rustls.c - TLS backend for rustls, through its C API (rustls-ffi 0.15).
 *
 * Implements the tls_backend_* contract of internal_tls.h. The rustls_server_config is used
 * directly as the opaque grpc_tls_ctx; each session wraps a rustls_connection and its grpc_tls_io
 * ciphertext queues. rustls only speaks TLS 1.2 and 1.3.
 *
 * Unlike the other backends, rustls does not pull or push ciphertext on its own: this file moves
 * it explicitly with rustls_connection_read_tls() (from the input queue, followed by
 * rustls_connection_process_new_packets()) and rustls_connection_write_tls() (into the output
 * queue), using callbacks over the grpc_tls_io queues that the libuv event loop fills and empties.
 * This backend does not extract the peer certificate subject: tls_backend_peer_subject() always
 * fails.
 */
#include <errno.h>
#include "internal/grpc_threads.h"
#include <stdio.h>
#include <rustls.h>
#include "core.h"
#include "internal_tls.h"

/* Per-connection state: the rustls connection and its ciphertext queues. */
struct grpc_tls_session {
  struct rustls_connection *conn;
  grpc_tls_io io;
};

/* Converts the opaque context handle back to the rustls_server_config it is. */
static inline const struct rustls_server_config *rustls_config(grpc_tls_ctx *ctx) {
  return (const struct rustls_server_config *)ctx;
}

/* Returns "rustls". */
const char *tls_backend_name(void) {
  return "rustls";
}

/* rustls_version() is not NUL-terminated. */
static char version[128];

/* Copies rustls_version() into `version` as a NUL-terminated string (truncated to fit). */
static void version_init(void) {
  struct rustls_str v = rustls_version();
  snprintf(version, sizeof(version), "%.*s", (int)v.len, v.data);
}

/* Returns the run-time library version string, built once (thread-safely) on first use. */
const char *tls_backend_version(void) {
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  pthread_once(&once, version_init);
  return version;
}

/* Appends the whole content of the file at `path` to `out`; the rustls-ffi calls used here take
 * PEM data from memory, so files are read in first. Returns 0 on success, 1 if the file cannot be
 * opened or read, or on allocation failure; `out` may then hold partial data, which the caller
 * frees. */
static int read_file(const char *path, grpc_buf *out) {
  FILE *fp = fopen(path, "rb");
  if (fp == nullptr)
    return 1;
  int res = 0;
  for (;;) {
    if (buf_reserve(out, 4096)) {
      res = 1;
      break;
    }
    size_t n = fread(out->data + out->len, 1, out->cap - out->len, fp);
    out->len += n;
    if (n == 0) {
      res = ferror(fp) ? 1 : 0;
      break;
    }
  }
  fclose(fp);
  return res;
}

/* Creates a server config from `cfg`: a certified key from PEM buffers (preferred when both are
 * set) or from files read into memory, and, with a CA file or PEM, a WebPKI client certificate
 * verifier that also accepts clients without a certificate unless cfg->require_client_cert is
 * set. Sets h2 as the only ALPN protocol. All error paths go through `out`, which frees the
 * intermediate objects and wipes the key file buffer. Returns nullptr on any error. */
grpc_tls_ctx *tls_backend_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr)
    return nullptr;
  grpc_buf cert = {0}, key = {0}, ca = {0};
  const struct rustls_certified_key *certified_key = nullptr;
  const struct rustls_server_config *config = nullptr;
  const struct rustls_root_cert_store *root_store = nullptr;
  struct rustls_client_cert_verifier *client_verifier = nullptr;

  const uint8_t *cert_data = nullptr;
  size_t cert_len = 0;
  const uint8_t *key_data = nullptr;
  size_t key_len = 0;
  if (cfg->cert_pem != nullptr && cfg->key_pem != nullptr) {
    cert_data = (const uint8_t *)cfg->cert_pem;
    cert_len = cfg->cert_pem_len;
    key_data = (const uint8_t *)cfg->key_pem;
    key_len = cfg->key_pem_len;
  } else if (cfg->cert_file != nullptr && cfg->key_file != nullptr) {
    if (read_file(cfg->cert_file, &cert) || read_file(cfg->key_file, &key))
      goto out;
    cert_data = cert.data;
    cert_len = cert.len;
    key_data = key.data;
    key_len = key.len;
  } else {
    goto out;
  }

  /* also checks that the key belongs to the certificate */
  if (rustls_certified_key_build(cert_data, cert_len, key_data, key_len, &certified_key) !=
      RUSTLS_RESULT_OK)
    goto out;

  struct rustls_server_config_builder *builder = rustls_server_config_builder_new();
  if (builder == nullptr)
    goto out;

  if (cfg->ca_file != nullptr || cfg->ca_pem != nullptr) {
    const uint8_t *ca_data = nullptr;
    size_t ca_len = 0;
    if (cfg->ca_pem != nullptr) {
      ca_data = (const uint8_t *)cfg->ca_pem;
      ca_len = cfg->ca_pem_len;
    } else {
      if (read_file(cfg->ca_file, &ca)) {
        rustls_server_config_builder_free(builder);
        goto out;
      }
      ca_data = ca.data;
      ca_len = ca.len;
    }
    struct rustls_root_cert_store_builder *store_builder = rustls_root_cert_store_builder_new();
    if (store_builder == nullptr ||
        rustls_root_cert_store_builder_add_pem(store_builder, ca_data, ca_len, true) !=
            RUSTLS_RESULT_OK ||
        rustls_root_cert_store_builder_build(store_builder, &root_store) != RUSTLS_RESULT_OK) {
      rustls_root_cert_store_builder_free(store_builder);
      rustls_server_config_builder_free(builder);
      goto out;
    }
    rustls_root_cert_store_builder_free(store_builder);
    struct rustls_web_pki_client_cert_verifier_builder *vbuilder =
        rustls_web_pki_client_cert_verifier_builder_new(root_store);
    if (vbuilder == nullptr) {
      rustls_server_config_builder_free(builder);
      goto out;
    }
    if (!cfg->require_client_cert)
      (void)rustls_web_pki_client_cert_verifier_builder_allow_unauthenticated(vbuilder);
    if (rustls_web_pki_client_cert_verifier_builder_build(vbuilder, &client_verifier) !=
        RUSTLS_RESULT_OK) {
      rustls_web_pki_client_cert_verifier_builder_free(vbuilder);
      rustls_server_config_builder_free(builder);
      goto out;
    }
    rustls_web_pki_client_cert_verifier_builder_free(vbuilder);
    rustls_server_config_builder_set_client_verifier(builder, client_verifier);
  }

  /* rustls rejects clients that offer ALPN without h2. clients that offer
   * no ALPN at all still connect */
  struct rustls_slice_bytes alpn = {.data = (const uint8_t *)"h2", .len = 2};
  if (rustls_server_config_builder_set_alpn_protocols(builder, &alpn, 1) != RUSTLS_RESULT_OK ||
      rustls_server_config_builder_set_certified_keys(builder, &certified_key, 1) !=
          RUSTLS_RESULT_OK) {
    rustls_server_config_builder_free(builder);
    goto out;
  }
  /* consumes the builder, even on failure */
  if (rustls_server_config_builder_build(builder, &config) != RUSTLS_RESULT_OK)
    config = nullptr;

out:
  /* the config keeps its own reference to the key and verifier */
  rustls_client_cert_verifier_free(client_verifier);
  rustls_root_cert_store_free(root_store);
  rustls_certified_key_free(certified_key);
  buf_free(&ca);
  buf_free(&cert);
  /* do not leave the private key behind in freed memory */
  if (key.data != nullptr)
    memset(key.data, 0, key.cap);
  buf_free(&key);
  return (grpc_tls_ctx *)config;
}

/* Releases the config. Connections hold their own reference, so live sessions keep it alive. */
void tls_backend_ctx_free(grpc_tls_ctx *ctx) {
  rustls_server_config_free(rustls_config(ctx));
}

/* Returns the rustls_server_config itself (const in rustls, cast away for the generic API). */
void *tls_backend_ctx_native(grpc_tls_ctx *ctx) {
  return (void *)rustls_config(ctx);
}

/* Creates a session with a new server connection from the config. Returns nullptr on failure. */
grpc_tls_session *tls_backend_session_new(grpc_tls_ctx *ctx) {
  grpc_tls_session *tls = grpc_calloc(1, sizeof(*tls));
  if (tls == nullptr) {
    return nullptr;
  }
  if (rustls_server_connection_new(rustls_config(ctx), &tls->conn) != RUSTLS_RESULT_OK) {
    grpc_free(tls);
    return nullptr;
  }
  return tls;
}

/* Frees the rustls connection, its ciphertext queues and the wrapper. */
void tls_backend_session_free(grpc_tls_session *tls) {
  rustls_connection_free(tls->conn);
  tls_io_free(&tls->io);
  grpc_free(tls);
}

/* Appends received ciphertext to the input queue; rustls reads it later, in pump(). Returns 0 on
 * success, 1 on allocation failure. */
int tls_backend_feed(grpc_tls_session *tls, const void *buf, size_t len) {
  return tls_io_feed(&tls->io, buf, len);
}

/* Read callback for rustls_connection_read_tls(): copies up to `n` buffered ciphertext bytes
 * into `buf`. Returns 0 with the count in `out_n`, or EAGAIN when nothing is buffered. */
static rustls_io_result io_read(void *userdata, uint8_t *buf, size_t n, size_t *out_n) {
  grpc_tls_session *tls = userdata;
  *out_n = tls_io_pull(&tls->io, buf, n);
  /* returning 0 bytes would mean EOF to rustls */
  return *out_n == 0 ? EAGAIN : 0;
}

/* Write callback for rustls_connection_write_tls(): appends all of `buf` to the output queue.
 * Returns 0 with `out_n` set to `n`, or ENOMEM if the queue cannot grow. */
static rustls_io_result io_write(void *userdata, const uint8_t *buf, size_t n, size_t *out_n) {
  grpc_tls_session *tls = userdata;
  if (tls_io_push(&tls->io, buf, n))
    return ENOMEM;
  *out_n = n;
  return 0;
}

/* moves the ciphertext rustls has ready into io.out. returns 0 on success, 1 on error */
static int flush_tls(grpc_tls_session *tls) {
  while (rustls_connection_wants_write(tls->conn)) {
    size_t n = 0;
    if (rustls_connection_write_tls(tls->conn, io_write, tls, &n) != 0)
      return 1;
  }
  return 0;
}

/* Flushes the ciphertext rustls has ready into the output queue, then moves the queue to the end
 * of `out`. rustls keeps its ciphertext until rustls_connection_write_tls() is called, so the
 * flush must come first. Returns 0 on success, 1 on error. */
int tls_backend_drain(grpc_tls_session *tls, grpc_buf *out) {
  if (flush_tls(tls))
    return 1;
  return tls_io_drain(&tls->io, out);
}

/* hands one batch of buffered ciphertext to rustls and decrypts it.
 * returns 1 when something was consumed, 0 when nothing was, -1 on error */
static int pump(grpc_tls_session *tls) {
  if (tls_io_pending(&tls->io) == 0)
    return 0;
  size_t n = 0;
  rustls_io_result res = rustls_connection_read_tls(tls->conn, io_read, tls, &n);
  if (res != 0 && res != EAGAIN)
    return -1;
  /* on failure rustls may have queued an alert; a later drain still flushes it to the peer */
  if (rustls_connection_process_new_packets(tls->conn) != RUSTLS_RESULT_OK)
    return -1;
  return n > 0 ? 1 : 0;
}

/* Feeds buffered ciphertext to rustls until the handshake completes. Returns 1 when complete, 0
 * when more input is needed, -1 on error. */
int tls_backend_handshake(grpc_tls_session *tls) {
  /* stop once the handshake is done: what follows is application data
   * and is decrypted by tls_backend_read() as it is consumed */
  while (rustls_connection_is_handshaking(tls->conn)) {
    int res = pump(tls);
    if (res < 0)
      return -1;
    if (res == 0)
      return 0;
  }
  return 1;
}

/* Reads up to `len` bytes of decrypted data into `buf`, pumping more buffered ciphertext into
 * rustls whenever its plaintext buffer is empty. Returns the byte count, -2 when the input queue
 * is exhausted (more ciphertext is needed), 0 on the peer's close_notify, -1 on error. */
ssize_t tls_backend_read(grpc_tls_session *tls, void *buf, size_t len) {
  for (;;) {
    size_t n = 0;
    rustls_result res = rustls_connection_read(tls->conn, buf, len, &n);
    if (res == RUSTLS_RESULT_OK) {
      /* 0 is the peer's close_notify */
      return (ssize_t)n;
    }
    if (res != RUSTLS_RESULT_PLAINTEXT_EMPTY)
      return -1;

    int pumped = pump(tls);
    if (pumped < 0)
      return -1;
    if (pumped == 0)
      return -2;
  }
}

/* Encrypts all of `buf`. When rustls' internal output buffer is full (it accepts 0 bytes), its
 * pending ciphertext is flushed into the output queue to make room. Returns `len`, or -1 on
 * error. */
ssize_t tls_backend_write(grpc_tls_session *tls, const void *buf, size_t len) {
  size_t written = 0;
  while (written < len) {
    size_t n = 0;
    if (rustls_connection_write(tls->conn, (const u8 *)buf + written, len - written, &n) !=
        RUSTLS_RESULT_OK)
      return -1;
    if (n == 0) {
      /* rustls buffers a limited amount of output. take it and retry */
      if (!rustls_connection_wants_write(tls->conn) || flush_tls(tls))
        return -1;
      continue;
    }
    written += n;
  }
  return (ssize_t)written;
}

/* Queues a close_notify alert inside rustls (best effort); the caller's next drain flushes it. */
void tls_backend_close_notify(grpc_tls_session *tls) {
  rustls_connection_send_close_notify(tls->conn);
}

/* Not supported with rustls: always returns -1 and leaves `buf` untouched. */
int tls_backend_peer_subject([[maybe_unused]] grpc_tls_session *tls, [[maybe_unused]] char *buf,
                             [[maybe_unused]] size_t len) {
  return -1;
}
