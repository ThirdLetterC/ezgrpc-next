/* grpc_quic_tls_gnutls.c - QUIC TLS for GnuTLS (HTTP/3)
 *
 * Implements internal_quic_tls.h with ngtcp2_crypto_gnutls (GnuTLS 3.7.5 or newer). QUIC is set up
 * per session (ngtcp2_crypto_gnutls_configure_{server,client}_session); the native handle given
 * to ngtcp2 is the gnutls_session_t, whose pointer holds the ngtcp2_crypto_conn_ref.
 *
 * A context is certificate credentials plus a TLS 1.3-only priority and the verification flags.
 * Server certificates load through tls_gnutls_load_server_credentials(), shared with the HTTP/2
 * backend in grpc_tls_gnutls.c. GnuTLS credentials are not reference counted, so sessions must be
 * gone before their context is freed: the server keeps retired contexts until it is freed, and a
 * client frees its context after its connections.
 */
#include <limits.h>
#include <stdbool.h>
#include <string.h>
#include <gnutls/gnutls.h>
#include <gnutls/x509.h>
#include <ngtcp2/ngtcp2_crypto_gnutls.h>

#include "core.h"
#include "internal_quic_tls.h"
#include "internal_tls_gnutls.h"

/* QUIC requires TLS 1.3; middlebox compatibility mode must be off for QUIC. */
static const char PRIORITY[] = "NORMAL:-VERS-ALL:+VERS-TLS1.3:%DISABLE_TLS13_COMPAT_MODE";

struct grpc_quic_tls_ctx {
  gnutls_certificate_credentials_t cred;
  gnutls_priority_t priority;
  /* server: request client certificates (a CA is configured) / require them */
  bool verify_client;
  bool require_client_cert;
  /* client: verify the server certificate */
  bool verify_server;
};

struct grpc_quic_tls_session {
  gnutls_session_t session;
};

static const gnutls_datum_t H3_ALPN = {.data = (unsigned char *)"h3", .size = 2};

int quic_tls_init(void) {
  return 0; /* gnutls initialises itself when the library loads */
}

void quic_tls_thread_cleanup(void) {}

const char *quic_tls_crypto_name(void) {
  return "gnutls";
}

/* Allocates a context with credentials and the QUIC priority; nullptr on failure. */
static grpc_quic_tls_ctx *ctx_alloc(void) {
  grpc_quic_tls_ctx *ctx = grpc_calloc(1, sizeof(*ctx));
  if (ctx == nullptr)
    return nullptr;
  if (gnutls_certificate_allocate_credentials(&ctx->cred) < 0 ||
      gnutls_priority_init(&ctx->priority, PRIORITY, nullptr) < 0) {
    quic_tls_ctx_free(ctx);
    return nullptr;
  }
  return ctx;
}

grpc_quic_tls_ctx *quic_tls_server_ctx_new(const grpc_tls_config *cfg) {
  if (cfg == nullptr)
    return nullptr;
  grpc_quic_tls_ctx *ctx = ctx_alloc();
  if (ctx == nullptr)
    return nullptr;
  if (!tls_gnutls_load_server_credentials(ctx->cred, cfg)) {
    quic_tls_ctx_free(ctx);
    return nullptr;
  }
  ctx->verify_client = cfg->ca_file != nullptr || cfg->ca_pem != nullptr;
  ctx->require_client_cert = cfg->require_client_cert;
  return ctx;
}

grpc_quic_tls_ctx *quic_tls_client_ctx_new(const grpc_quic_tls_client_config *cfg) {
  if (cfg == nullptr)
    return nullptr;
  grpc_quic_tls_ctx *ctx = ctx_alloc();
  if (ctx == nullptr)
    return nullptr;
  if (!cfg->insecure_skip_verify) {
    int rv;
    if (cfg->ca_file != nullptr) {
      rv = gnutls_certificate_set_x509_trust_file(ctx->cred, cfg->ca_file, GNUTLS_X509_FMT_PEM);
    } else if (cfg->ca_pem != nullptr) {
      size_t len = cfg->ca_pem_len ? cfg->ca_pem_len : strlen(cfg->ca_pem);
      gnutls_datum_t ca = {.data = (unsigned char *)cfg->ca_pem, .size = (unsigned int)len};
      rv = len <= UINT_MAX
               ? gnutls_certificate_set_x509_trust_mem(ctx->cred, &ca, GNUTLS_X509_FMT_PEM)
               : -1;
    } else {
      rv = gnutls_certificate_set_x509_system_trust(ctx->cred);
    }
    if (rv <= 0) {
      quic_tls_ctx_free(ctx);
      return nullptr;
    }
    ctx->verify_server = true;
  }
  if (cfg->cert_file != nullptr || cfg->key_file != nullptr) {
    if (cfg->cert_file == nullptr || cfg->key_file == nullptr ||
        gnutls_certificate_set_x509_key_file(ctx->cred, cfg->cert_file, cfg->key_file,
                                             GNUTLS_X509_FMT_PEM) < 0) {
      quic_tls_ctx_free(ctx);
      return nullptr;
    }
  }
  return ctx;
}

void quic_tls_ctx_free(grpc_quic_tls_ctx *ctx) {
  if (ctx == nullptr)
    return;
  if (ctx->priority != nullptr)
    gnutls_priority_deinit(ctx->priority);
  if (ctx->cred != nullptr)
    gnutls_certificate_free_credentials(ctx->cred);
  grpc_free(ctx);
}

/* Server hook after the ClientHello: QUIC needs an application protocol, so a client that did
 * not negotiate h3 is refused. */
static int client_hello_cb(gnutls_session_t session, unsigned int htype, unsigned when,
                           unsigned int incoming, const gnutls_datum_t *msg) {
  (void)htype;
  (void)when;
  (void)incoming;
  (void)msg;
  gnutls_datum_t alpn;
  if (gnutls_alpn_get_selected_protocol(session, &alpn) != 0 || alpn.size != 2 ||
      memcmp(alpn.data, "h3", 2) != 0)
    return -1;
  return 0;
}

grpc_quic_tls_session *quic_tls_session_new(grpc_quic_tls_ctx *ctx,
                                            ngtcp2_crypto_conn_ref *conn_ref, bool server,
                                            const char *sni) {
  grpc_quic_tls_session *s = grpc_calloc(1, sizeof(*s));
  if (s == nullptr)
    return nullptr;
  unsigned int flags = (server ? GNUTLS_SERVER : GNUTLS_CLIENT) | GNUTLS_NO_END_OF_EARLY_DATA;
  if (gnutls_init(&s->session, flags) != 0) {
    s->session = nullptr;
    goto fail;
  }
  if (gnutls_priority_set(s->session, ctx->priority) != 0 ||
      gnutls_credentials_set(s->session, GNUTLS_CRD_CERTIFICATE, ctx->cred) != 0)
    goto fail;
  if (server) {
    if (ngtcp2_crypto_gnutls_configure_server_session(s->session) != 0)
      goto fail;
    gnutls_handshake_set_hook_function(s->session, GNUTLS_HANDSHAKE_CLIENT_HELLO, GNUTLS_HOOK_POST,
                                       client_hello_cb);
    if (ctx->verify_client)
      gnutls_certificate_server_set_request(
          s->session, ctx->require_client_cert ? GNUTLS_CERT_REQUIRE : GNUTLS_CERT_REQUEST);
  } else {
    if (ngtcp2_crypto_gnutls_configure_client_session(s->session) != 0)
      goto fail;
    if (sni != nullptr && sni[0] != '\0') {
      if (gnutls_server_name_set(s->session, GNUTLS_NAME_DNS, sni, strlen(sni)) != 0)
        goto fail;
      /* verify the chain and the host name during the handshake */
      if (ctx->verify_server)
        gnutls_session_set_verify_cert(s->session, sni, 0);
    } else if (ctx->verify_server) {
      gnutls_session_set_verify_cert(s->session, nullptr, 0);
    }
  }
  gnutls_session_set_ptr(s->session, conn_ref);
  if (gnutls_alpn_set_protocols(s->session, &H3_ALPN, 1,
                                GNUTLS_ALPN_MANDATORY | GNUTLS_ALPN_SERVER_PRECEDENCE) != 0)
    goto fail;
  return s;

fail:
  quic_tls_session_free(s);
  return nullptr;
}

void *quic_tls_session_native(grpc_quic_tls_session *session) {
  return session->session;
}

void quic_tls_session_free(grpc_quic_tls_session *session) {
  if (session == nullptr)
    return;
  if (session->session != nullptr) {
    gnutls_session_set_ptr(session->session, nullptr);
    gnutls_deinit(session->session);
  }
  grpc_free(session);
}

int quic_tls_peer_subject(grpc_quic_tls_session *session, char *buf, size_t len) {
  if (session == nullptr || session->session == nullptr || len == 0)
    return -1;
  unsigned int list_size = 0;
  const gnutls_datum_t *certs = gnutls_certificate_get_peers(session->session, &list_size);
  if (certs == nullptr || list_size == 0)
    return -1;
  gnutls_x509_crt_t crt;
  if (gnutls_x509_crt_init(&crt) < 0)
    return -1;
  int res = -1;
  if (gnutls_x509_crt_import(crt, &certs[0], GNUTLS_X509_FMT_DER) >= 0) {
    size_t sz = len;
    if (gnutls_x509_crt_get_dn(crt, buf, &sz) == GNUTLS_E_SUCCESS)
      res = 0;
  }
  gnutls_x509_crt_deinit(crt);
  return res;
}
