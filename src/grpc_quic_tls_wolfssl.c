/* grpc_quic_tls_wolfssl.c - QUIC TLS for wolfSSL (HTTP/3)
 *
 * Implements internal_quic_tls.h with ngtcp2_crypto_wolfssl. wolfSSL must be built with QUIC
 * support (WOLFSSL_QUIC, e.g. ./configure --enable-quic); distribution packages usually are. QUIC
 * is configured on the context (ngtcp2_crypto_wolfssl_configure_{server,client}_context), and the
 * native handle given to ngtcp2 is the WOLFSSL object.
 *
 * Server certificates load through tls_wolfssl_load_server_credentials(), shared with the HTTP/2
 * backend in grpc_tls_wolfssl.c. Contexts are TLS 1.3 only with ALPN "h3". wolfSSL objects do not
 * hold a reference on their context, so the server keeps retired contexts until it is freed
 * (http3_server_reload_tls()) and a client frees its context only after its connections.
 */
#include <limits.h>
#include <stdbool.h>
#include <string.h>
#include <wolfssl/options.h>
#include <wolfssl/ssl.h>
#include <wolfssl/wolfcrypt/ecc.h>
#include <ngtcp2/ngtcp2_crypto_wolfssl.h>

#include "core.h"
#include "internal_quic_tls.h"
#include "internal_tls_wolfssl.h"

#if !defined(WOLFSSL_QUIC)
#error "HTTP/3 needs wolfSSL built with QUIC support (--enable-quic)"
#endif

struct grpc_quic_tls_session {
  WOLFSSL *ssl;
};

static const unsigned char H3_ALPN[] = {2, 'h', '3'};

static uv_once_t init_once = UV_ONCE_INIT;
static int init_result = 0;

/* wolfSSL_Init() is reference counted, but calling it once is enough for the process. */
static void do_init(void) {
  init_result = wolfSSL_Init() == WOLFSSL_SUCCESS ? 0 : -1;
}

int quic_tls_init(void) {
  uv_once(&init_once, do_init);
  return init_result;
}

/* wolfSSL builds with FP_ECC keep a per-thread fixed-point cache that must be freed explicitly. */
void quic_tls_thread_cleanup(void) {
#if defined(FP_ECC)
  wc_ecc_fp_free();
#endif
}

const char *quic_tls_crypto_name(void) {
  return "wolfssl";
}

/* Server ALPN callback: selects "h3" or fails the handshake (QUIC needs an application
 * protocol). */
static int alpn_select_h3(WOLFSSL *ssl, const unsigned char **out, unsigned char *outlen,
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

grpc_quic_tls_ctx *quic_tls_server_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr || quic_tls_init() != 0)
    return nullptr;
  WOLFSSL_CTX *ctx = wolfSSL_CTX_new(wolfTLSv1_3_server_method());
  if (ctx == nullptr)
    return nullptr;
  if (ngtcp2_crypto_wolfssl_configure_server_context(ctx) != 0 ||
      !tls_wolfssl_load_server_credentials(ctx, cfg)) {
    wolfSSL_CTX_free(ctx);
    return nullptr;
  }
  wolfSSL_CTX_set_alpn_select_cb(ctx, alpn_select_h3, nullptr);
  return (grpc_quic_tls_ctx *)ctx;
}

grpc_quic_tls_ctx *quic_tls_client_ctx_new(const grpc_quic_tls_client_config *cfg) {
  if (cfg == nullptr || quic_tls_init() != 0)
    return nullptr;
  WOLFSSL_CTX *ctx = wolfSSL_CTX_new(wolfTLSv1_3_client_method());
  if (ctx == nullptr)
    return nullptr;
  if (ngtcp2_crypto_wolfssl_configure_client_context(ctx) != 0)
    goto fail;
  if (cfg->insecure_skip_verify) {
    wolfSSL_CTX_set_verify(ctx, WOLFSSL_VERIFY_NONE, nullptr);
  } else {
    wolfSSL_CTX_set_verify(ctx, WOLFSSL_VERIFY_PEER, nullptr);
    bool ca_ok;
    if (cfg->ca_file != nullptr) {
      ca_ok = wolfSSL_CTX_load_verify_locations(ctx, cfg->ca_file, nullptr) == WOLFSSL_SUCCESS;
    } else if (cfg->ca_pem != nullptr) {
      size_t len = cfg->ca_pem_len ? cfg->ca_pem_len : strlen(cfg->ca_pem);
      ca_ok = len <= LONG_MAX &&
              wolfSSL_CTX_load_verify_buffer(ctx, (const unsigned char *)cfg->ca_pem, (long)len,
                                             WOLFSSL_FILETYPE_PEM) == WOLFSSL_SUCCESS;
    } else {
      ca_ok = wolfSSL_CTX_load_system_CA_certs(ctx) == WOLFSSL_SUCCESS;
    }
    if (!ca_ok)
      goto fail;
  }
  if (cfg->cert_file != nullptr || cfg->key_file != nullptr) {
    if (cfg->cert_file == nullptr || cfg->key_file == nullptr ||
        wolfSSL_CTX_use_certificate_chain_file(ctx, cfg->cert_file) != WOLFSSL_SUCCESS ||
        wolfSSL_CTX_use_PrivateKey_file(ctx, cfg->key_file, WOLFSSL_FILETYPE_PEM) !=
            WOLFSSL_SUCCESS)
      goto fail;
  }
  return (grpc_quic_tls_ctx *)ctx;

fail:
  wolfSSL_CTX_free(ctx);
  return nullptr;
}

void quic_tls_ctx_free(grpc_quic_tls_ctx *ctx) {
  wolfSSL_CTX_free((WOLFSSL_CTX *)ctx);
}

grpc_quic_tls_session *quic_tls_session_new(grpc_quic_tls_ctx *ctx,
                                            ngtcp2_crypto_conn_ref *conn_ref, bool server,
                                            const char *sni) {
  grpc_quic_tls_session *session = grpc_calloc(1, sizeof(*session));
  if (session == nullptr)
    return nullptr;
  session->ssl = wolfSSL_new((WOLFSSL_CTX *)ctx);
  if (session->ssl == nullptr)
    goto fail;
  wolfSSL_set_app_data(session->ssl, conn_ref);
  /* QUIC v1 transport parameters extension */
  wolfSSL_set_quic_transport_version(session->ssl, 0x39);
  if (server) {
    wolfSSL_set_accept_state(session->ssl);
  } else {
    wolfSSL_set_connect_state(session->ssl);
    if (wolfSSL_set_alpn_protos(session->ssl, H3_ALPN, sizeof(H3_ALPN)) != 0)
      goto fail;
    if (sni != nullptr && sni[0] != '\0') {
      size_t len = strlen(sni);
      if (len > UINT16_MAX || wolfSSL_UseSNI(session->ssl, WOLFSSL_SNI_HOST_NAME, sni,
                                             (uint16_t)len) != WOLFSSL_SUCCESS)
        goto fail;
      /* verify the certificate against the name too (no-op when verification is off) */
      if (wolfSSL_check_domain_name(session->ssl, sni) != WOLFSSL_SUCCESS)
        goto fail;
    }
  }
  return session;

fail:
  quic_tls_session_free(session);
  return nullptr;
}

void *quic_tls_session_native(grpc_quic_tls_session *session) {
  return session->ssl;
}

void quic_tls_session_free(grpc_quic_tls_session *session) {
  if (session == nullptr)
    return;
  if (session->ssl != nullptr) {
    wolfSSL_set_app_data(session->ssl, nullptr);
    wolfSSL_free(session->ssl);
  }
  grpc_free(session);
}

int quic_tls_peer_subject(grpc_quic_tls_session *session, char *buf, size_t len) {
  if (session == nullptr || session->ssl == nullptr || len == 0 || len > INT_MAX)
    return -1;
  WOLFSSL_X509 *cert = wolfSSL_get_peer_certificate(session->ssl);
  if (cert == nullptr)
    return -1;
  WOLFSSL_X509_NAME *name = wolfSSL_X509_get_subject_name(cert);
  char *res = name != nullptr ? wolfSSL_X509_NAME_oneline(name, buf, (int)len) : nullptr;
  wolfSSL_X509_free(cert);
  return res != nullptr ? 0 : -1;
}
