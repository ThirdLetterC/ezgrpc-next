#pragma once

/* internal_tls_gnutls.h - credential loading shared by the GnuTLS TLS code
 *
 * Implemented in src/grpc_tls_gnutls.c; used by the HTTP/2 backend there and by the HTTP/3 QUIC
 * TLS code in src/grpc_quic_tls_gnutls.c, so certificates load the same way on both transports.
 */
#include <stdbool.h>
#include <gnutls/gnutls.h>

#include "grpc_tls.h"

/* Loads the server chain and key from cfg into cred, plus the CA for client certificates when one
 * is configured. Returns false if anything fails to load. */
bool tls_gnutls_load_server_credentials(gnutls_certificate_credentials_t cred,
                                        const grpc_tls_config *cfg);
