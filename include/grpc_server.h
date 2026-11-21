#pragma once

/* grpc_server.h - the gRPC server
 *
 * Creating, configuring, running and stopping a server, and registering methods. A method is
 * served by a handler (grpc_server_handle(), grpc_server_handle_stream()) or reported as events
 * (grpc_server_register_path(), grpc_event.h). Calls themselves are answered through grpc_call.h.
 *
 * Threads: a server runs on one thread, the one calling grpc_server_run() or grpc_server_poll();
 * handlers and callbacks run there. Unless documented otherwise, functions here must be called
 * from that thread (or before it starts serving). grpc_server_stop(), grpc_server_post(),
 * grpc_server_get_stats() and grpc_server_set_log_level() are safe from any thread.
 */

#include <stdio.h>
#include "grpc_call.h"
#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_event.h"
#include "grpc_tls.h"

/** A gRPC server. Opaque; created with grpc_server_new(). */
typedef struct grpc_server grpc_server;

/** Server log thresholds, from silent to most verbose. */
typedef enum grpc_log_level {
  /** Use the default level, GRPC_LOG_INFO. */
  GRPC_LOG_DEFAULT = 0,
  /** No logging. */
  GRPC_LOG_OFF,
  /** Errors only. */
  GRPC_LOG_ERROR,
  /** Errors and warnings. */
  GRPC_LOG_WARNING,
  /** Startup and shutdown messages. */
  GRPC_LOG_INFO,
  /** Connection messages. */
  GRPC_LOG_DEBUG,
  /** Every HTTP/2 frame, when built with T=1. */
  GRPC_LOG_TRACE,
} grpc_log_level;

/**
 * Server configuration. Zero fields take their default, so a designated
 * initializer only names what differs:
 *
 *   grpc_server *srv = grpc_server_new(&(grpc_config){.port = 50051});
 *
 * grpc_server_new() copies the configuration, including the address and TLS
 * strings, so it need not outlive the call. ``log_file`` must stay open while
 * the server lives.
 */
typedef struct grpc_config {
  /** Port for both address families. 0 requests an OS-assigned ephemeral port
   * (query it with grpc_server_port()). Ignored when only unix_path is set. */
  uint16_t port;
  /** IPv4 address to listen on. When ipv4_addr, ipv6_addr and unix_path are all
   * nullptr, the server listens on "0.0.0.0" and "::". */
  const char *ipv4_addr;
  /** IPv6 address to listen on; see ipv4_addr. */
  const char *ipv6_addr;
  /** Optional Unix Domain Socket path (or Windows named pipe) to listen on. */
  const char *unix_path;
  /** listen() backlog. Default 1024. */
  int backlog;
  /** Concurrent connections. Default 1024. */
  size_t max_connections;
  /** Concurrent calls per connection. Default 1024. */
  uint32_t max_concurrent_streams;
  /** Largest request message, in bytes. Default 1 MiB. */
  uint32_t max_message_size;
  /** Largest HTTP/2 frame, in bytes. Default 16 KiB. */
  uint32_t max_frame_size;
  /** Maximum queued outbound bytes per stream for streaming backpressure. 0 = unlimited. */
  size_t max_send_queue_bytes;
  /** HTTP/2 keepalive PING interval on idle connections (ms). 0 = disabled. */
  uint32_t keepalive_time_ms;
  /** Time to wait for a keepalive PING ACK before closing (ms). Default 20000 when keepalive
   * enabled. */
  uint32_t keepalive_timeout_ms;
  /** Close idle connections with no active streams after this many ms. 0 = unlimited. */
  uint32_t max_connection_idle_ms;
  /** Send GOAWAY after a connection has lived this many ms. 0 = unlimited. */
  uint32_t max_connection_age_ms;
  /** Grace period after max_connection_age GOAWAY before closing (ms). Default 5000. */
  uint32_t max_connection_age_grace_ms;
  /** Max unsolicited client PINGs faster than min_ping_interval_ms before ENHANCE_YOUR_CALM.
   * 0 = unlimited. */
  uint32_t max_ping_strikes;
  /** Minimum interval between client PINGs (ms). Default 5000 when max_ping_strikes > 0. */
  uint32_t min_ping_interval_ms;
  /** Default response compression when accepted by the client. Default GRPC_COMPRESS_IDENTITY. */
  grpc_compression_algorithm default_compression;
  /** Default response compression level (0 or GRPC_COMPRESSION_LEVEL_DEFAULT for codec default). */
  int default_compression_level;
  /** TLS settings: provide either cert_file+key_file or cert_pem+key_pem.
   * Optionally configure ca_file/ca_pem and require_client_cert for mTLS. */
  grpc_tls_config tls;
  /** Initial log threshold. Defaults to GRPC_LOG_INFO. */
  grpc_log_level log_level;
  /** Log output stream. Defaults to stderr. */
  FILE *log_file;
} grpc_config;

/**
 * Snapshot of server-wide runtime metrics. Each counter is read atomically,
 * but the snapshot as a whole is not.
 */
typedef struct grpc_server_stats {
  /** Connections open now. */
  uint64_t active_connections;
  /** Connections accepted since the server started. */
  uint64_t total_connections;
  /** HTTP/2 streams open now. */
  uint64_t active_streams;
  /** Calls started. */
  uint64_t total_calls;
  /** Calls that ended with GRPC_STATUS_OK. */
  uint64_t calls_ok;
  /** Calls that ended with another status, including cancellations. */
  uint64_t calls_error;
  /** Bytes read from connections (as received on the socket, so TLS included). */
  uint64_t bytes_received;
  /** Bytes written to connections. */
  uint64_t bytes_sent;
  /** Request messages received. */
  uint64_t messages_received;
  /** Response messages queued for sending. */
  uint64_t messages_sent;
} grpc_server_stats;

/**
 * Handles a method whose client sends a single request: unary and
 * server-streaming methods. Called once the request is complete. Answer with
 * grpc_call_send() (any number of times) and grpc_call_finish(), now or
 * later.
 */
typedef void (*grpc_handler_fn)(grpc_call *call, const grpc_message *request, void *userdata);

/**
 * Handles a method whose client streams requests: client-streaming and
 * bidirectional methods. Messages are valid until the callback returns.
 */
typedef struct grpc_stream_handler {
  /** Called once when the call starts (headers received, after interceptors). Optional. */
  void (*on_start)(grpc_call *call, void *userdata);
  /** A request message arrived. Required unless ``on_start`` is set. */
  void (*on_message)(grpc_call *call, const grpc_message *message, void *userdata);
  /** The client finished sending. Optional. */
  void (*on_end)(grpc_call *call, void *userdata);
  /** The call was cancelled. Optional. */
  void (*on_cancel)(grpc_call *call, void *userdata);
} grpc_stream_handler;

/**
 * Creates a server and starts listening. Returns nullptr on failure, after logging why.
 * Use grpc_server_new_ex() to also obtain a grpc_err code on failure.
 * Nothing is served until grpc_server_run() or grpc_server_poll() is called.
 * Release with grpc_server_free().
 */
GRPC_API GRPC_NODISCARD grpc_server *grpc_server_new(const grpc_config *config);

/**
 * Like grpc_server_new(), but writes the error code to ``*out_err`` (if non-null).
 */
GRPC_API GRPC_NODISCARD grpc_server *grpc_server_new_ex(const grpc_config *config,
                                                        grpc_err *out_err);

/**
 * Returns the bound TCP port, or 0 if ``server`` is nullptr.
 * Useful when configured with ``config.port = 0`` (ephemeral port).
 */
GRPC_API uint16_t grpc_server_port(const grpc_server *server);

/**
 * Closes every connection and frees the server. Release the references taken
 * on its calls first. Queued responses are flushed first, and tasks posted
 * with grpc_server_post() still run (on closed calls). Must be called from the
 * server thread, not from inside a poll. Accepts nullptr.
 */
GRPC_API void grpc_server_free(grpc_server *server);

/**
 * Changes the server's log threshold. Safe to call from any thread while the
 * server is alive. GRPC_LOG_DEFAULT selects GRPC_LOG_INFO.
 */
GRPC_API grpc_err grpc_server_set_log_level(grpc_server *server, grpc_log_level level);

/**
 * Registers a handler for a single-request method, e.g.
 * ``"/helloworld.Greeter/SayHello"``. ``path`` must start with ``/`` and is
 * copied. Returns GRPC_ERR_EXISTS if the path is already registered, or
 * GRPC_ERR_INVALID for a nullptr ``handler`` or malformed ``path``.
 */
GRPC_API grpc_err grpc_server_handle(grpc_server *server, const char *path, grpc_handler_fn handler,
                                     void *userdata);

/**
 * Registers handlers for a method whose client streams requests. ``handler``
 * is copied and needs ``on_message`` or ``on_start``. Errors as for
 * grpc_server_handle().
 */
GRPC_API grpc_err grpc_server_handle_stream(grpc_server *server, const char *path,
                                            const grpc_stream_handler *handler, void *userdata);

/**
 * Registers a method whose calls are reported as events instead of
 * dispatched to a handler; see grpc_server_poll(). ``userdata`` is copied into
 * each grpc_event. Errors as for grpc_server_handle().
 */
GRPC_API grpc_err grpc_server_register_path(grpc_server *server, const char *path, void *userdata);

/**
 * Stops accepting new calls to a method. Calls in progress are unaffected.
 * New requests for ``path`` go to the fallback handler, or get UNIMPLEMENTED.
 * The path can be registered again later. Returns GRPC_ERR_NOT_FOUND if it is
 * not registered.
 */
GRPC_API grpc_err grpc_server_unregister(grpc_server *server, const char *path);

/**
 * Sets a fallback / wildcard stream handler for any unregistered method path
 * (enabling proxies, gateways, and dynamic routing). Pass nullptr to clear it
 * and restore automatic UNIMPLEMENTED responses. ``handler`` is copied and
 * needs ``on_message`` or ``on_start``. Paths removed with
 * grpc_server_unregister() also reach it.
 */
GRPC_API grpc_err grpc_server_set_fallback(grpc_server *server, const grpc_stream_handler *handler,
                                           void *userdata);

/**
 * Copies a snapshot of runtime metrics into ``out``. Thread-safe.
 */
GRPC_API grpc_err grpc_server_get_stats(const grpc_server *server, grpc_server_stats *out);

/**
 * Serves until grpc_server_stop() is called, by calling grpc_server_poll()
 * in a loop. Returns GRPC_OK after a stop (the server can then be run again),
 * or the negative error of a failed poll.
 */
GRPC_API grpc_err grpc_server_run(grpc_server *server);

/**
 * Makes grpc_server_run() return after its current poll. Safe to call from
 * any thread and from a signal handler.
 */
GRPC_API void grpc_server_stop(grpc_server *server);

/**
 * Gracefully drains the server: stops accepting new connections, sends HTTP/2
 * GOAWAY to all active clients, and polls until all in-flight calls finish or
 * ``timeout_ms`` elapses (-1: wait indefinitely, 0: non-blocking check).
 * Returns GRPC_OK once drained, or GRPC_ERR_TIMEOUT if active calls remain.
 * Must be called from the server thread, outside grpc_server_run(). Listeners
 * are not reopened and the stop flag is set, so follow it with
 * grpc_server_free().
 */
GRPC_API grpc_err grpc_server_drain(grpc_server *server, int timeout_ms);

/**
 * Runs one iteration of the event loop: writes the responses queued since
 * the last call, waits up to ``timeout_ms`` (-1: no limit, 0: no wait) for
 * network activity, and dispatches handlers.
 *
 * Returns the number of events of grpc_server_register_path() methods,
 * read with grpc_server_events(), or a negative grpc_err.
 */
GRPC_API int grpc_server_poll(grpc_server *server, int timeout_ms);

/**
 * The events of the last grpc_server_poll(), valid until the next one.
 * Stores their number in ``*count`` (if non-null). The array is owned by the
 * server.
 */
GRPC_API const grpc_event *grpc_server_events(grpc_server *server, size_t *count);

/**
 * Writes the queued responses now instead of at the next poll.
 */
GRPC_API void grpc_server_flush(grpc_server *server);

/**
 * Runs ``fn(arg)`` on the server thread soon. Safe to call from any thread:
 * this is how worker threads hand results back. Tasks run in the order they
 * were posted. The caller keeps ownership of ``arg``.
 */
GRPC_API grpc_err grpc_server_post(grpc_server *server, void (*fn)(void *arg), void *arg);

/**
 * The TLS library's own context, see grpc_tls.h. nullptr without TLS.
 */
GRPC_API void *grpc_server_tls_native(grpc_server *server);

/**
 * The license text of grpc, a static string.
 */
GRPC_API const char *grpc_license(void);
