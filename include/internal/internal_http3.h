#pragma once

/* internal_http3.h - hooks between the server/client core and the HTTP/3 transport
 *
 * The server side lives in src/grpc_http3_server*.c and the client side in src/grpc_http3_client*.c,
 * both compiled only with GRPC_ENABLE_HTTP3. src/grpc_http3.c provides the public feature queries
 * and, in builds without HTTP/3, stubs of the functions below that report GRPC_ERR_UNSUPPORTED.
 */
#include "core.h"

#if defined(GRPC_ENABLE_HTTP3)
#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>

/* Memory tables routing ngtcp2 and nghttp3 allocations through the grpc allocator, shared by the
 * server and client transports. Both libraries keep a pointer to them, so they live for the
 * program (defined in grpc_http3.c). */
extern const ngtcp2_mem grpc_quic_mem;
extern const nghttp3_mem grpc_h3_mem;

/* Largest batch of QUIC packets sent with one generic segmentation offload (GSO) call: whole
 * packets of at most 1452 bytes, within a UDP datagram's 64 KiB and the kernel's 64 segments. */
enum { GRPC_H3_GSO_BUF = 44 * 1452 };

/* Sends `len` bytes as UDP datagrams of `gsolen` bytes each (the last may be shorter) with one
 * sendmsg() and UDP_SEGMENT, to `to` (nullptr on a connected socket). Linux only. Returns 0, a
 * negative libuv error (UV_EAGAIN / UV_ENOBUFS when the socket buffer is full), or UV_ENOTSUP when
 * the platform or the route cannot segment, so that the caller sends datagram by datagram. */
int grpc_h3_send_gso(uv_udp_t *udp, const struct sockaddr *to, const uint8_t *data, size_t len,
                     size_t gsolen);
#endif

/* Opens the UDP listener(s) and builds the QUIC TLS context from `tls` (the normalized server TLS
 * config, still valid during grpc_server_new_ex()) and installs the HTTP/3 server state in
 * server->h3. Called by grpc_server_new_ex() after the loop, TCP listeners and session slots
 * exist. GRPC_ERR_INVALID without TLS, GRPC_ERR_IO if a socket cannot be bound,
 * GRPC_ERR_UNSUPPORTED without HTTP/3 support. */
grpc_err http3_server_start(grpc_server *server, const grpc_tls_config *tls);

/* Withdraws the server's connection IDs from the registry shared with servers on other loops
 * (config.reuse_port), so that nothing is forwarded to it any more. First step of
 * grpc_server_free(): datagrams forwarded before then are run by its last run_posted(). */
void http3_server_leave_cluster(grpc_server *server);

/* Stops accepting new QUIC connections (grpc_server_drain); existing ones keep the socket. */
void http3_server_stop_accepting(grpc_server *server);

/* Closes the UDP handles; part of grpc_server_free() before the loop is drained. Sessions must be
 * closed already. */
void http3_server_close_handles(grpc_server *server);

/* Frees the HTTP/3 server state after the loop has been drained. Accepts a server without it. */
void http3_server_free(grpc_server *server);

/* Builds a QUIC TLS context from `cfg` and makes it the one new connections use (the old one is
 * retired until the server is freed). GRPC_ERR_INVALID if it does not load; GRPC_OK when HTTP/3
 * is not enabled. */
/* grpc_server_reload_tls() for HTTP/3, in two steps so the reload is all or nothing: prepare
 * builds the new QUIC TLS context into *out (nullptr without HTTP/3) and reserves what install
 * needs, returning GRPC_ERR_INVALID or GRPC_ERR_NOMEM on failure; install then switches to it and
 * cannot fail; discard drops a prepared context that will not be installed. */
grpc_err http3_server_tls_prepare(grpc_server *server, const grpc_tls_config *cfg, void **out);
void http3_server_tls_install(grpc_server *server, void *ctx);
void http3_server_tls_discard(grpc_server *server, void *ctx);

/* The bound UDP port, or 0. */
uint16_t http3_server_port(const grpc_server *server);

/* Frees the calling thread's TLS-library caches if HTTP/3 is built in (quic_tls_thread_cleanup);
 * a no-op otherwise. */
void http3_thread_cleanup(void);
