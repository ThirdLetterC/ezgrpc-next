#pragma once

/* grpc_http3.h - gRPC over HTTP/3 (QUIC)
 *
 * The server can listen for HTTP/3 on UDP next to its HTTP/2 listeners (grpc_config.http3), and
 * the client can connect over HTTP/3 (grpc_client_config.transport). HTTP/3 always uses TLS 1.3
 * with ALPN "h3", so the server needs grpc_config.tls and the client tls.enable.
 *
 * HTTP/3 is an optional build feature (-DGRPC_HTTP3=AUTO|ON|OFF) built on ngtcp2 and nghttp3 with
 * the ngtcp2 crypto helper for the TLS backend: OpenSSL 3.5+, BoringSSL, AWS-LC, GnuTLS or
 * wolfSSL. LibreSSL and rustls have no QUIC support. Without it, enabling HTTP/3 fails with
 * GRPC_ERR_UNSUPPORTED. See docs/http3.md.
 */

#include "grpc_defs.h"

/** The wire protocol a client uses. */
typedef enum grpc_transport {
  /** gRPC over HTTP/2 on TCP or a Unix socket (the default). */
  GRPC_TRANSPORT_HTTP2 = 0,
  /** gRPC over HTTP/3 on QUIC (UDP). Requires TLS and a build with HTTP/3. */
  GRPC_TRANSPORT_HTTP3 = 1,
} grpc_transport;

/**
 * HTTP/3 settings of a server (grpc_config.http3). A zeroed struct leaves HTTP/3 off.
 * The QUIC listener binds the same addresses as the TCP listeners (ipv4_addr / ipv6_addr, or
 * all interfaces), never a Unix socket.
 */
typedef struct grpc_http3_config {
  /** Also accept gRPC over HTTP/3. Requires grpc_config.tls. */
  bool enable;
  /** UDP port. 0 uses the TCP port (grpc_config.port), or an OS-assigned port when that is 0
   * too; query it with grpc_server_http3_port(). */
  uint16_t port;
  /** QUIC idle timeout (ms) after which a silent connection is dropped. 0 uses
   * grpc_config.max_connection_idle_ms when set, otherwise 30000. */
  uint32_t max_idle_timeout_ms;
  /** Add ``alt-svc: h3=":<port>"`` to HTTP/2 responses so HTTP/3-capable clients can switch. */
  bool advertise_alt_svc;
  /** Validate client addresses with a QUIC Retry before creating connection state (costs one
   * round trip; protects against address spoofing). */
  bool require_retry;
} grpc_http3_config;

/** True when the library was built with HTTP/3 support. */
GRPC_API bool grpc_http3_available(void);

/**
 * The QUIC/HTTP/3 libraries in use, e.g. ``"ngtcp2/1.26.0 nghttp3/1.19.0 crypto=ossl"``, or
 * ``"none"`` when built without HTTP/3. The string is static.
 */
GRPC_API const char *grpc_http3_versions(void);
