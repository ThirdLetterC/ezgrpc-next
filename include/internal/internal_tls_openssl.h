#pragma once

/* internal_tls_openssl.h - credential loading shared by the OpenSSL-family TLS code
 *
 * Implemented in src/grpc_tls_openssl.c and used by both the HTTP/2 TLS backend there and the
 * HTTP/3 QUIC TLS code in src/grpc_quic_tls_openssl.c, so certificates load the same way on both
 * transports. OpenSSL, LibreSSL, BoringSSL and AWS-LC.
 */
#include <stdbool.h>
#include <stddef.h>
#include <openssl/ssl.h>

#include "grpc_tls.h"

/* Loads the server chain and key from cfg and sets up client-certificate verification when a CA
 * is configured. Returns false if anything fails to load. */
bool tls_openssl_load_server_credentials(SSL_CTX *ctx, const grpc_tls_config *cfg);

/* Loads trusted CAs from ca_file, or from ca_pem (pem_len 0 = NUL-terminated). Returns false on
 * failure. */
bool tls_openssl_load_ca(SSL_CTX *ctx, const char *ca_file, const char *ca_pem, size_t pem_len);
