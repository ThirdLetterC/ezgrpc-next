#pragma once

/* grpc_tls.h - server TLS configuration
 *
 * The grpc_tls_config struct embedded in grpc_config.tls, hot reloading of server certificates,
 * and queries about the TLS backend compiled into the library. Client TLS is configured
 * separately in grpc_client_config (grpc_client.h). See docs/tls.md.
 */

#include "grpc_defs.h"
#include "grpc_error.h"

typedef struct grpc_server grpc_server;

/**
 * TLS settings: provide either cert_file+key_file or cert_pem+key_pem.
 * Optionally configure ca_file/ca_pem and require_client_cert for mTLS.
 */
typedef struct grpc_tls_config {
  /** Server certificate chain file (PEM). */
  const char *cert_file;
  /** Server private key file (PEM). */
  const char *key_file;
  /** Server certificate chain as PEM text. */
  const char *cert_pem;
  /** Length of ``cert_pem`` (0 = NUL-terminated). */
  size_t cert_pem_len;
  /** Server private key as PEM text. */
  const char *key_pem;
  /** Length of ``key_pem`` (0 = NUL-terminated). */
  size_t key_pem_len;
  /** CA file used to verify client certificates (mTLS). */
  const char *ca_file;
  /** CA certificates as PEM text, used when ``ca_file`` is nullptr. */
  const char *ca_pem;
  /** Length of ``ca_pem`` (0 = NUL-terminated). */
  size_t ca_pem_len;
  /** Reject clients that do not present a valid certificate. */
  bool require_client_cert;
} grpc_tls_config;

/**
 * Hot-reloads the server's TLS certificate/key/CA configuration without
 * dropping existing connections. New connections use the updated context.
 * On failure, the previous TLS configuration remains active.
 * ``tls`` must name exactly one of the file pair or the PEM pair (else
 * GRPC_ERR_INVALID) and is not kept after the call. Call it from the server
 * thread. Returns GRPC_ERR_STATE on a server started without TLS (a reload
 * never switches a plaintext server to TLS), GRPC_ERR_NOMEM if the new context
 * cannot be recorded, and GRPC_ERR_UNSUPPORTED when built without TLS.
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
