#pragma once

#include "grpc_defs.h"
#include "grpc_error.h"

typedef struct grpc_server grpc_server;

/**
 * TLS settings: provide either cert_file+key_file or cert_pem+key_pem.
 * Optionally configure ca_file/ca_pem and require_client_cert for mTLS.
 */
typedef struct grpc_tls_config {
  const char *cert_file;
  const char *key_file;
  const char *cert_pem;
  size_t cert_pem_len;
  const char *key_pem;
  size_t key_pem_len;
  const char *ca_file;
  const char *ca_pem;
  size_t ca_pem_len;
  bool require_client_cert;
} grpc_tls_config;

/**
 * Hot-reloads the server's TLS certificate/key/CA configuration without
 * dropping existing connections. New connections use the updated context.
 * On failure, the previous TLS configuration remains active.
 */
GRPC_API grpc_err grpc_server_reload_tls(grpc_server *server, const grpc_tls_config *tls);

/**
 * TLS is enabled with grpc_config.tls. The library is built with one of
 * OpenSSL, LibreSSL, BoringSSL, AWS-LC, wolfSSL, GnuTLS or rustls
 * (``-DGRPC_TLS_BACKEND=...`` in CMake). For settings grpc does not expose,
 * grpc_server_tls_native() returns the library's own context:
 *
 *   * OpenSSL, LibreSSL, BoringSSL, AWS-LC: ``SSL_CTX *``
 *   * wolfSSL: ``WOLFSSL_CTX *``
 *   * GnuTLS: ``gnutls_certificate_credentials_t``
 *   * rustls: ``const struct rustls_server_config *``
 */

/**
 * Returns the name of the TLS backend the library was built with: ``"openssl"``,
 * ``"libressl"``, ``"boringssl"``, ``"awslc"``, ``"wolfssl"``, ``"gnutls"``,
 * ``"rustls"``, or ``"none"``.
 */
GRPC_API const char *grpc_tls_backend(void);

/**
 * Returns the version string reported by the TLS library in use, or ``""``.
 */
GRPC_API const char *grpc_tls_backend_version(void);
