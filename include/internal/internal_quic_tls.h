#pragma once

/* internal_quic_tls.h - the TLS contract of the HTTP/3 (QUIC) transport
 *
 * QUIC does not run TLS over a byte stream: ngtcp2's crypto helper drives the TLS library's QUIC
 * API directly on a native per-connection object (SSL, gnutls_session_t, WOLFSSL), so the
 * byte-stream contract of internal_tls.h cannot be reused. Each QUIC-capable backend implements
 * this one in src/grpc_quic_tls_<family>.c:
 *   - openssl.c: OpenSSL 3.5+ (ngtcp2_crypto_ossl) and BoringSSL / AWS-LC (ngtcp2_crypto_boringssl)
 *   - gnutls.c:  GnuTLS (ngtcp2_crypto_gnutls)
 *   - wolfssl.c: wolfSSL (ngtcp2_crypto_wolfssl)
 * Contexts are always TLS 1.3 only with ALPN "h3". Loop thread only, except quic_tls_init().
 */
#include <stdbool.h>
#include <stddef.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "grpc_tls.h"

/* A server or client TLS context (certificates, verification, ALPN). */
typedef struct grpc_quic_tls_ctx grpc_quic_tls_ctx;
/* The TLS state of one QUIC connection. */
typedef struct grpc_quic_tls_session grpc_quic_tls_session;

/* Client-side TLS settings, taken from grpc_client_config.tls. */
typedef struct grpc_quic_tls_client_config {
  const char *ca_file;
  const char *ca_pem;
  size_t ca_pem_len;
  const char *cert_file;
  const char *key_file;
  bool insecure_skip_verify;
} grpc_quic_tls_client_config;

/* One-time global initialisation of the crypto helper. Returns 0 on success. Thread-safe. */
int quic_tls_init(void);

/* Releases per-thread caches the TLS library keeps for the calling thread (wolfSSL's FP_ECC
 * fixed-point cache, which otherwise leaks when the thread exits). Called on the loop thread when
 * grpc_server_run() returns and from grpc_server_free() / grpc_client_free(). Safe to call any
 * time; the cache is rebuilt on demand. */
void quic_tls_thread_cleanup(void);

/* The ngtcp2 crypto helper in use, e.g. "ossl", "boringssl", "gnutls", "wolfssl". */
const char *quic_tls_crypto_name(void);

/* Builds a server context from `cfg` (PEM lengths already filled in): certificate chain and key,
 * optional CA for client certificates (require_client_cert makes them mandatory), TLS 1.3 only,
 * ALPN "h3". Returns nullptr if the configuration does not load. */
grpc_quic_tls_ctx *quic_tls_server_ctx_new(const grpc_tls_config *cfg);

/* Builds a client context: CA bundle (or the system default) unless insecure_skip_verify, optional
 * client certificate for mTLS, TLS 1.3 only, ALPN "h3". Returns nullptr on failure. */
grpc_quic_tls_ctx *quic_tls_client_ctx_new(const grpc_quic_tls_client_config *cfg);

/* Releases a context. Sessions created from it stay valid (they keep their own reference, or the
 * caller keeps the context until they are gone; see each backend). */
void quic_tls_ctx_free(grpc_quic_tls_ctx *ctx);

/* Creates the TLS state of one connection and configures it for ngtcp2: `conn_ref` is attached as
 * the TLS object's application data and must outlive the session. `sni` is the server name a
 * client sends and verifies (nullptr: none). Returns nullptr on failure. */
grpc_quic_tls_session *quic_tls_session_new(grpc_quic_tls_ctx *ctx,
                                            ngtcp2_crypto_conn_ref *conn_ref, bool server,
                                            const char *sni);

/* The object to pass to ngtcp2_conn_set_tls_native_handle(). */
void *quic_tls_session_native(grpc_quic_tls_session *session);

/* Frees the session (detaching conn_ref first, so the TLS library cannot call back into a freed
 * connection). Accepts nullptr. */
void quic_tls_session_free(grpc_quic_tls_session *session);

/* Writes the verified peer certificate subject (e.g. "/CN=client") into buf. Returns 0, or -1 when
 * there is no peer certificate or the backend cannot report it. */
int quic_tls_peer_subject(grpc_quic_tls_session *session, char *buf, size_t len);
