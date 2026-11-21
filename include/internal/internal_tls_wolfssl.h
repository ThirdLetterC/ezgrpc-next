#pragma once

/* internal_tls_wolfssl.h - credential loading shared by the wolfSSL TLS code
 *
 * Implemented in src/grpc_tls_wolfssl.c; used by the HTTP/2 backend there and by the HTTP/3 QUIC
 * TLS code in src/grpc_quic_tls_wolfssl.c, so certificates load the same way on both transports.
 */
#include <stdbool.h>
#include <wolfssl/options.h>
#include <wolfssl/ssl.h>

#include "grpc_tls.h"

/* Loads the server chain and key from cfg and sets up client-certificate verification when a CA
 * is configured. Returns false if anything fails to load. */
bool tls_wolfssl_load_server_credentials(WOLFSSL_CTX *ctx, const grpc_tls_config *cfg);
