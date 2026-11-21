/* grpc_tls_wolfssl.c - TLS backend for wolfSSL, using its native API.
 *
 * Implements the tls_backend_* contract of internal_tls.h without the OpenSSL compatibility
 * layer. The WOLFSSL_CTX is used directly as the opaque grpc_tls_ctx; each session wraps a WOLFSSL
 * object and its grpc_tls_io ciphertext queues.
 *
 * wolfSSL never touches the socket: custom I/O callbacks, installed on the context, move
 * ciphertext through the grpc_tls_io queues that the libuv event loop fills (feed) and empties
 * (drain). wolfSSL must be built with ALPN support (--enable-alpn), since HTTP/2 negotiates h2
 * through ALPN.
 */
#include <limits.h>
#include "internal/grpc_threads.h"
#include <wolfssl/options.h>
#include <wolfssl/ssl.h>
#include "core.h"
#include "internal_tls.h"

#if !defined(HAVE_ALPN)
#error "wolfSSL is built without ALPN, which HTTP/2 requires. Rebuild it with --enable-alpn."
#endif

/* Per-connection state: the wolfSSL object and its ciphertext queues. */
struct grpc_tls_session {
  WOLFSSL *ssl;
  grpc_tls_io io;
};

/* Converts the opaque context handle back to the WOLFSSL_CTX it is. */
static inline WOLFSSL_CTX *wolfssl_ctx(grpc_tls_ctx *ctx) {
  return (WOLFSSL_CTX *)ctx;
}

/* Returns "wolfssl". */
const char *tls_backend_name(void) {
  return "wolfssl";
}

/* Returns the run-time library version string. */
const char *tls_backend_version(void) {
  return wolfSSL_lib_version();
}

/* Initializes the wolfSSL library; run once through pthread_once() by tls_backend_ctx_new(). */
static void wolfssl_init_once(void) {
  (void)wolfSSL_Init();
}

/* I/O receive callback: copies up to `sz` buffered ciphertext bytes into `buf`. Returns the byte
 * count, or WOLFSSL_CBIO_ERR_WANT_READ when nothing is buffered, so wolfSSL reports want-read
 * instead of blocking or treating it as EOF. */
static int io_recv([[maybe_unused]] WOLFSSL *ssl, char *buf, int sz, void *ctx) {
  grpc_tls_session *tls = ctx;
  size_t n = tls_io_pull(&tls->io, buf, (size_t)sz);
  return n == 0 ? WOLFSSL_CBIO_ERR_WANT_READ : (int)n;
}

/* I/O send callback: appends all of `buf` to the output queue. Returns `sz`, or
 * WOLFSSL_CBIO_ERR_GENERAL if the queue cannot grow. */
static int io_send([[maybe_unused]] WOLFSSL *ssl, char *buf, int sz, void *ctx) {
  grpc_tls_session *tls = ctx;
  return tls_io_push(&tls->io, buf, (size_t)sz) ? WOLFSSL_CBIO_ERR_GENERAL : sz;
}

/* Creates a server WOLFSSL_CTX from `cfg`: TLS 1.2 minimum, the certificate chain and key from PEM
 * buffers (preferred when both are set) or from files, checked to match each other unless wolfSSL
 * is built with NO_CHECK_PRIVATE_KEY. With a CA file or PEM, client certificates are verified, and
 * required when cfg->require_client_cert is set. Installs the queue I/O callbacks. Returns nullptr
 * on any error. */
grpc_tls_ctx *tls_backend_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr)
    return nullptr;
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  pthread_once(&once, wolfssl_init_once);

  WOLFSSL_CTX *ctx = wolfSSL_CTX_new(wolfSSLv23_server_method());
  if (ctx == nullptr) {
    return nullptr;
  }

  if (wolfSSL_CTX_SetMinVersion(ctx, WOLFSSL_TLSV1_2) != WOLFSSL_SUCCESS) {
    wolfSSL_CTX_free(ctx);
    return nullptr;
  }

  bool ok = false;
  if (cfg->cert_pem != nullptr && cfg->key_pem != nullptr) {
    ok = wolfSSL_CTX_use_certificate_chain_buffer(ctx, (const unsigned char *)cfg->cert_pem,
                                                  (long)cfg->cert_pem_len) == WOLFSSL_SUCCESS &&
         wolfSSL_CTX_use_PrivateKey_buffer(ctx, (const unsigned char *)cfg->key_pem,
                                           (long)cfg->key_pem_len,
                                           WOLFSSL_FILETYPE_PEM) == WOLFSSL_SUCCESS;
  } else if (cfg->cert_file != nullptr && cfg->key_file != nullptr) {
    ok = wolfSSL_CTX_use_certificate_chain_file(ctx, cfg->cert_file) == WOLFSSL_SUCCESS &&
         wolfSSL_CTX_use_PrivateKey_file(ctx, cfg->key_file, WOLFSSL_FILETYPE_PEM) ==
             WOLFSSL_SUCCESS;
  }
  if (!ok
#if !defined(NO_CHECK_PRIVATE_KEY)
      || wolfSSL_CTX_check_private_key(ctx) != WOLFSSL_SUCCESS
#endif
  ) {
    wolfSSL_CTX_free(ctx);
    return nullptr;
  }

  if (cfg->ca_file != nullptr || cfg->ca_pem != nullptr) {
    bool ca_ok =
        cfg->ca_file != nullptr
            ? wolfSSL_CTX_load_verify_locations(ctx, cfg->ca_file, nullptr) == WOLFSSL_SUCCESS
            : wolfSSL_CTX_load_verify_buffer(ctx, (const unsigned char *)cfg->ca_pem,
                                             (long)cfg->ca_pem_len,
                                             WOLFSSL_FILETYPE_PEM) == WOLFSSL_SUCCESS;
    if (!ca_ok) {
      wolfSSL_CTX_free(ctx);
      return nullptr;
    }
    int mode =
        WOLFSSL_VERIFY_PEER | (cfg->require_client_cert ? WOLFSSL_VERIFY_FAIL_IF_NO_PEER_CERT : 0);
    wolfSSL_CTX_set_verify(ctx, mode, nullptr);
  }

  wolfSSL_CTX_SetIORecv(ctx, io_recv);
  wolfSSL_CTX_SetIOSend(ctx, io_send);

  return (grpc_tls_ctx *)ctx;
}

/* Frees the WOLFSSL_CTX. */
void tls_backend_ctx_free(grpc_tls_ctx *ctx) {
  wolfSSL_CTX_free(wolfssl_ctx(ctx));
}

/* Returns the WOLFSSL_CTX itself. */
void *tls_backend_ctx_native(grpc_tls_ctx *ctx) {
  return wolfssl_ctx(ctx);
}

/* Creates a session: a WOLFSSL object whose I/O callbacks get the session as their context, and
 * which offers h2 through ALPN. Returns nullptr on failure. */
grpc_tls_session *tls_backend_session_new(grpc_tls_ctx *ctx) {
  grpc_tls_session *tls = grpc_calloc(1, sizeof(*tls));
  if (tls == nullptr) {
    return nullptr;
  }
  tls->ssl = wolfSSL_new(wolfssl_ctx(ctx));
  if (tls->ssl == nullptr) {
    grpc_free(tls);
    return nullptr;
  }
  wolfSSL_SetIOReadCtx(tls->ssl, tls);
  wolfSSL_SetIOWriteCtx(tls->ssl, tls);

  /* clients that do not offer h2 still connect, as with the other backends */
  char alpn[] = "h2";
  if (wolfSSL_UseALPN(tls->ssl, alpn, sizeof(alpn) - 1, WOLFSSL_ALPN_CONTINUE_ON_MISMATCH) !=
      WOLFSSL_SUCCESS) {
    tls_backend_session_free(tls);
    return nullptr;
  }
  return tls;
}

/* Frees the WOLFSSL object, its ciphertext queues and the wrapper. */
void tls_backend_session_free(grpc_tls_session *tls) {
  wolfSSL_free(tls->ssl);
  tls_io_free(&tls->io);
  grpc_free(tls);
}

/* Appends received ciphertext to the input queue. Returns 0 on success, 1 on allocation
 * failure. */
int tls_backend_feed(grpc_tls_session *tls, const void *buf, size_t len) {
  return tls_io_feed(&tls->io, buf, len);
}

/* Moves the queued output ciphertext to the end of `out`. Returns 0 on success (also when nothing
 * is queued), 1 on allocation failure. */
int tls_backend_drain(grpc_tls_session *tls, grpc_buf *out) {
  return tls_io_drain(&tls->io, out);
}

/* Runs wolfSSL_accept() on the ciphertext fed so far. Returns 1 when complete, 0 when it needs
 * more input (WANT_READ/WANT_WRITE), -1 on error. */
int tls_backend_handshake(grpc_tls_session *tls) {
  int ret = wolfSSL_accept(tls->ssl);
  if (ret == WOLFSSL_SUCCESS) {
    return 1;
  }
  int err = wolfSSL_get_error(tls->ssl, ret);
  return err == WOLFSSL_ERROR_WANT_READ || err == WOLFSSL_ERROR_WANT_WRITE ? 0 : -1;
}

/* Decrypts up to `len` bytes (capped at INT_MAX) into `buf`. Returns the byte count, -2 on
 * WANT_READ/WANT_WRITE (more ciphertext is needed), 0 on the peer's close_notify, -1 on error. */
ssize_t tls_backend_read(grpc_tls_session *tls, void *buf, size_t len) {
  int ret = wolfSSL_read(tls->ssl, buf, len > INT_MAX ? INT_MAX : (int)len);
  if (ret > 0) {
    return ret;
  }
  switch (wolfSSL_get_error(tls->ssl, ret)) {
  case WOLFSSL_ERROR_WANT_READ:
  case WOLFSSL_ERROR_WANT_WRITE:
    return -2;
  case WOLFSSL_ERROR_ZERO_RETURN:
    return 0;
  default:
    return -1;
  }
}

/* Encrypts all of `buf` into the output queue, looping in chunks of at most INT_MAX bytes.
 * Returns `len`, or -1 on error. */
ssize_t tls_backend_write(grpc_tls_session *tls, const void *buf, size_t len) {
  /* io_send never blocks, so wolfSSL_write() only fails on errors */
  size_t written = 0;
  while (written < len) {
    size_t n = len - written;
    int ret = wolfSSL_write(tls->ssl, (const u8 *)buf + written, n > INT_MAX ? INT_MAX : (int)n);
    if (ret <= 0) {
      return -1;
    }
    written += (size_t)ret;
  }
  return (ssize_t)written;
}

/* Queues a close_notify alert with wolfSSL_shutdown(); the result is ignored (best effort). The
 * caller drains the alert. */
void tls_backend_close_notify(grpc_tls_session *tls) {
  (void)wolfSSL_shutdown(tls->ssl);
}

/* Writes the peer certificate's subject in one-line form into `buf` via
 * wolfSSL_X509_NAME_oneline(). Returns 0 on success, -1 if there is no peer certificate or on
 * error. */
int tls_backend_peer_subject(grpc_tls_session *tls, char *buf, size_t len) {
  WOLFSSL_X509 *cert = wolfSSL_get_peer_certificate(tls->ssl);
  if (cert == nullptr)
    return -1;
  WOLFSSL_X509_NAME *name = wolfSSL_X509_get_subject_name(cert);
  char *res = name != nullptr ? wolfSSL_X509_NAME_oneline(name, buf, (int)len) : nullptr;
  wolfSSL_X509_free(cert);
  return res != nullptr ? 0 : -1;
}
