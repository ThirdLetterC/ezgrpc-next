/* grpc_quic_tls_openssl.c - QUIC TLS for the OpenSSL family (HTTP/3)
 *
 * Implements internal_quic_tls.h with ngtcp2's crypto helper for the library the build uses:
 *   - OpenSSL 3.5+: ngtcp2_crypto_ossl, which drives OpenSSL's native QUIC TLS API. The native
 *     handle given to ngtcp2 is an ngtcp2_crypto_ossl_ctx wrapping the SSL.
 *   - BoringSSL and AWS-LC: ngtcp2_crypto_boringssl, configured on the SSL_CTX
 *     (SSL_CTX_set_quic_method). The native handle is the SSL itself.
 * LibreSSL has no ngtcp2 helper, so the build never selects HTTP/3 with it.
 *
 * Certificates load through tls_openssl_load_server_credentials() / tls_openssl_load_ca(), shared
 * with the HTTP/2 backend in grpc_tls_openssl.c. Contexts are TLS 1.3 only with ALPN "h3". Each SSL
 * holds a reference on its SSL_CTX, so freeing a context never invalidates live sessions.
 */
#include <limits.h>
#include <stdbool.h>
#include <string.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#if defined(GRPC_TLS_BORINGSSL) || defined(GRPC_TLS_AWSLC)
#include <ngtcp2/ngtcp2_crypto_boringssl.h>
#define QUIC_CRYPTO_NAME "boringssl"
#else
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#define QUIC_CRYPTO_NAME "ossl"
#define GRPC_QUIC_OSSL   1
#endif

#include "core.h"
#include "internal_quic_tls.h"
#include "internal_tls_openssl.h"

/* The per-connection TLS state: the SSL and, for OpenSSL, the helper context wrapping it. */
struct grpc_quic_tls_session {
  SSL *ssl;
#if defined(GRPC_QUIC_OSSL)
  ngtcp2_crypto_ossl_ctx *octx;
#endif
};

/* ALPN wire form of "h3". */
static const unsigned char H3_ALPN[] = {2, 'h', '3'};

static uv_once_t init_once = UV_ONCE_INIT;
static int init_result = 0;

/* Runs the helper's global initialisation once (ngtcp2_crypto_ossl_init loads the QUIC ciphers). */
static void do_init(void) {
#if defined(GRPC_QUIC_OSSL)
  init_result = ngtcp2_crypto_ossl_init();
#else
  init_result = 0;
#endif
}

int quic_tls_init(void) {
  uv_once(&init_once, do_init);
  return init_result;
}

/* OpenSSL and BoringSSL clean up per-thread state themselves. */
void quic_tls_thread_cleanup(void) {}

const char *quic_tls_crypto_name(void) {
  return QUIC_CRYPTO_NAME;
}

/* Server ALPN callback: selects "h3" from the client's list, or fails the handshake. Unlike the
 * HTTP/2 backend, QUIC requires an application protocol, so a client without h3 is refused. */
static int alpn_select_h3(SSL *ssl, const unsigned char **out, unsigned char *outlen,
                          const unsigned char *in, unsigned int inlen, void *arg) {
  (void)ssl;
  (void)arg;
  for (unsigned int i = 0; i < inlen;) {
    unsigned int len = in[i];
    if (i + 1 + len > inlen)
      break;
    if (len == 2 && memcmp(in + i + 1, "h3", 2) == 0) {
      *out = in + i + 1;
      *outlen = 2;
      return SSL_TLSEXT_ERR_OK;
    }
    i += 1 + len;
  }
  return SSL_TLSEXT_ERR_ALERT_FATAL;
}

/* BoringSSL / AWS-LC configure QUIC on the context (SSL_CTX_set_quic_method); OpenSSL 3.5 does it
 * per session in quic_tls_session_new(). Returns 0 on success. */
static int configure_quic_ctx(SSL_CTX *ctx, bool server) {
#if defined(GRPC_QUIC_OSSL)
  (void)ctx;
  (void)server;
  return 0;
#else
  return server ? ngtcp2_crypto_boringssl_configure_server_context(ctx)
                : ngtcp2_crypto_boringssl_configure_client_context(ctx);
#endif
}

/* Creates an SSL_CTX restricted to TLS 1.3, as QUIC requires. */
static SSL_CTX *new_tls13_ctx(const SSL_METHOD *method) {
  SSL_CTX *ctx = SSL_CTX_new(method);
  if (ctx == nullptr)
    return nullptr;
  if (!SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) ||
      !SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION)) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  return ctx;
}

grpc_quic_tls_ctx *quic_tls_server_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr || quic_tls_init() != 0)
    return nullptr;
  SSL_CTX *ctx = new_tls13_ctx(TLS_server_method());
  if (ctx == nullptr)
    return nullptr;
  if (configure_quic_ctx(ctx, true) != 0 || !tls_openssl_load_server_credentials(ctx, cfg)) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  SSL_CTX_set_alpn_select_cb(ctx, alpn_select_h3, nullptr);
  return (grpc_quic_tls_ctx *)ctx;
}

grpc_quic_tls_ctx *quic_tls_client_ctx_new(const grpc_quic_tls_client_config *cfg) {
  if (cfg == nullptr || quic_tls_init() != 0)
    return nullptr;
  SSL_CTX *ctx = new_tls13_ctx(TLS_client_method());
  if (ctx == nullptr)
    return nullptr;
  if (configure_quic_ctx(ctx, false) != 0) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  if (cfg->insecure_skip_verify) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    bool ca_ok = cfg->ca_file != nullptr || cfg->ca_pem != nullptr
                     ? tls_openssl_load_ca(ctx, cfg->ca_file, cfg->ca_pem, cfg->ca_pem_len)
                     : SSL_CTX_set_default_verify_paths(ctx) == 1;
    if (!ca_ok) {
      SSL_CTX_free(ctx);
      return nullptr;
    }
  }
  if (cfg->cert_file != nullptr || cfg->key_file != nullptr) {
    if (cfg->cert_file == nullptr || cfg->key_file == nullptr ||
        SSL_CTX_use_certificate_chain_file(ctx, cfg->cert_file) <= 0 ||
        SSL_CTX_use_PrivateKey_file(ctx, cfg->key_file, SSL_FILETYPE_PEM) <= 0 ||
        !SSL_CTX_check_private_key(ctx)) {
      SSL_CTX_free(ctx);
      return nullptr;
    }
  }
  return (grpc_quic_tls_ctx *)ctx;
}

void quic_tls_ctx_free(grpc_quic_tls_ctx *ctx) {
  SSL_CTX_free((SSL_CTX *)ctx);
}

grpc_quic_tls_session *quic_tls_session_new(grpc_quic_tls_ctx *ctx,
                                            ngtcp2_crypto_conn_ref *conn_ref, bool server,
                                            const char *sni) {
  grpc_quic_tls_session *session = grpc_calloc(1, sizeof(*session));
  if (session == nullptr)
    return nullptr;
  session->ssl = SSL_new((SSL_CTX *)ctx);
  if (session->ssl == nullptr)
    goto fail;
#if defined(GRPC_QUIC_OSSL)
  if (ngtcp2_crypto_ossl_ctx_new(&session->octx, session->ssl) != 0)
    goto fail;
  int rv = server ? ngtcp2_crypto_ossl_configure_server_session(session->ssl)
                  : ngtcp2_crypto_ossl_configure_client_session(session->ssl);
  if (rv != 0)
    goto fail;
#endif
  /* BoringSSL / AWS-LC: QUIC was configured on the SSL_CTX (configure_quic_ctx()) */
  SSL_set_app_data(session->ssl, conn_ref);
  if (server) {
    SSL_set_accept_state(session->ssl);
  } else {
    SSL_set_connect_state(session->ssl);
    if (SSL_set_alpn_protos(session->ssl, H3_ALPN, sizeof(H3_ALPN)) != 0)
      goto fail;
    if (sni != nullptr && sni[0] != '\0') {
      (void)SSL_set_tlsext_host_name(session->ssl, sni);
      /* verify the certificate against the name too (ignored when verification is off) */
      if (SSL_get_verify_mode(session->ssl) != SSL_VERIFY_NONE && !SSL_set1_host(session->ssl, sni))
        goto fail;
    }
  }
  return session;

fail:
  quic_tls_session_free(session);
  return nullptr;
}

void *quic_tls_session_native(grpc_quic_tls_session *session) {
#if defined(GRPC_QUIC_OSSL)
  return session->octx;
#else
  return session->ssl;
#endif
}

void quic_tls_session_free(grpc_quic_tls_session *session) {
  if (session == nullptr)
    return;
  if (session->ssl != nullptr) {
    /* no callbacks into a connection that is going away */
    SSL_set_app_data(session->ssl, nullptr);
    SSL_free(session->ssl);
  }
#if defined(GRPC_QUIC_OSSL)
  if (session->octx != nullptr)
    ngtcp2_crypto_ossl_ctx_del(session->octx);
#endif
  grpc_free(session);
}

int quic_tls_peer_subject(grpc_quic_tls_session *session, char *buf, size_t len) {
  if (session == nullptr || session->ssl == nullptr || len == 0)
    return -1;
  X509 *cert = SSL_get_peer_certificate(session->ssl);
  if (cert == nullptr)
    return -1;
  X509_NAME *subject = X509_get_subject_name(cert);
  char *res = subject != nullptr
                  ? X509_NAME_oneline(subject, buf, (int)(len > INT_MAX ? INT_MAX : len))
                  : nullptr;
  X509_free(cert);
  return res != nullptr ? 0 : -1;
}
