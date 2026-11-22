#pragma once

/* core.h - the library's internal data model
 *
 * Defines the private structs behind the public handles (grpc_server, grpc_call) and the
 * transport state under them: grpc_session (one connection) and grpc_stream (one HTTP/2 or HTTP/3
 * request stream), plus routes, buffers and the internal functions shared between source files.
 * Every source file in src/ includes it.
 *
 * Transports: the call layer never calls nghttp2/nghttp3 directly. Each session carries a
 * grpc_transport_ops table (HTTP/2 in internal_nghttp2_callbacks.c, HTTP/3 in
 * grpc_http3_server.c) and the transport-independent request handling lives in
 * grpc_stream_core.c.
 *
 * Threading: everything is owned by the server loop thread, except the atomics (call refcount,
 * stop flag, log level, statistics) and the grpc_server_post() queue guarded by post_mutex.
 * Ownership: a stream owns one reference on its call until call_detach(); the application, the
 * ready queue and events hold further references.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <stddef.h>
#include "internal/grpc_threads.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>
#include <nghttp2/nghttp2.h>

#include "grpc_platform.h"

#include "grpc.h"
#include "common.h"
#include "grpc_header.h"
#include "grpc_msg_queue.h"
#include "tls_ctx_pool.h"

/* ---- logging ------------------------------------------------------------- */

#ifndef _MSC_VER
[[gnu::format(printf, 3, 4)]]
#endif
/* Log a timestamped line for `server` if `level` passes its threshold; usually reached through
 * the LOG_* macros. Does nothing for a nullptr server. Safe from any thread. */
void server_log(grpc_server *server, grpc_log_level level, const char *fmt, ...)
    GRPC_PRINTF_FORMAT(3, 4);

/* server_log() at GRPC_LOG_TRACE with "@func: " before the message (LOG_TRACE). */
void server_log_trace(grpc_server *server, const char *func, const char *fmt, ...)
    GRPC_PRINTF_FORMAT(3, 4);

/* The format string is the first variadic argument (a string literal, joined to the prefix), so
 * that a call without further arguments needs no empty __VA_ARGS__. */
#define LOG_ERROR(server, ...)   server_log(server, GRPC_LOG_ERROR, "error: " __VA_ARGS__)
#define LOG_WARNING(server, ...) server_log(server, GRPC_LOG_WARNING, "warning: " __VA_ARGS__)
#define LOG_INFO(server, ...)    server_log(server, GRPC_LOG_INFO, __VA_ARGS__)
#define LOG_DEBUG(server, ...)   server_log(server, GRPC_LOG_DEBUG, __VA_ARGS__)
/* Per-frame logs are compiled in only with T=1. */
#ifdef GRPC_TRACE
#define LOG_TRACE(server, ...) server_log_trace(server, __func__, __VA_ARGS__)
#else
#define LOG_TRACE(server, ...) \
  do {                         \
  } while (0)
#endif

/* ---- buffers --------------------------------------------------------------- */

/* A growable byte buffer from the grpc allocator; see buf_reserve()/buf_append()/buf_free() in
 * internal_helpers.h. A zeroed grpc_buf is empty and valid. */
typedef struct grpc_buf grpc_buf;
struct grpc_buf {
  u8 *data;
  size_t len;
  size_t cap;
};

/* ---- routes ---------------------------------------------------------------- */

typedef enum route_kind {
  ROUTE_UNARY,  /* grpc_server_handle(). */
  ROUTE_STREAM, /* grpc_server_handle_stream() */
  ROUTE_EVENTS, /* grpc_server_register_path() */
} route_kind;

/* A registered method. It stays at a fixed address while the server lives
 * because calls hold pointers to it. */
typedef struct grpc_route grpc_route;
struct grpc_route {
  char *path;
  size_t path_len;
  route_kind kind;
  grpc_handler_fn handler;
  grpc_stream_handler stream;
  void *userdata;
  /* Disabled routes reject new calls with UNIMPLEMENTED. */
  bool disabled;
};

/* ---- calls ----------------------------------------------------------------- */

typedef struct grpc_stream grpc_stream;
typedef struct grpc_session grpc_session;

/* A header field to send, laid out exactly like nghttp2_nv and nghttp3_nv (checked with
 * static_assert in each transport) so arrays can be passed to either library without copying. */
typedef struct grpc_nv grpc_nv;
struct grpc_nv {
  uint8_t *name;
  uint8_t *value;
  size_t namelen;
  size_t valuelen;
  uint8_t flags;
};

/* Arrays of grpc_nv are handed to nghttp2 (server and client) as nghttp2_nv arrays as they are. */
static_assert(sizeof(grpc_nv) == sizeof(nghttp2_nv) &&
                  offsetof(grpc_nv, name) == offsetof(nghttp2_nv, name) &&
                  offsetof(grpc_nv, value) == offsetof(nghttp2_nv, value) &&
                  offsetof(grpc_nv, namelen) == offsetof(nghttp2_nv, namelen) &&
                  offsetof(grpc_nv, valuelen) == offsetof(nghttp2_nv, valuelen) &&
                  offsetof(grpc_nv, flags) == offsetof(nghttp2_nv, flags),
              "grpc_nv must match nghttp2_nv");

/* Returns p without its const qualifier. Only for C APIs that take a non-const pointer but do not
 * write through it (nghttp2 / nghttp3 header entries, uv_buf_t for writes, ngtcp2 paths), and for
 * freeing memory the library allocated but hands out through a const pointer. Used instead of a
 * cast so that -Wcast-qual keeps flagging every other removal of const. */
static inline void *grpc_unconst(const void *p) {
  union {
    const void *c;
    void *m;
  } u = {.c = p};
  return u.m;
}

/* A header entry whose name and value are only read (nghttp2 and nghttp3 never write them). */
static inline grpc_nv grpc_nv_make(const char *name, size_t namelen, const char *value,
                                   size_t valuelen) {
  return (grpc_nv){grpc_unconst(name), grpc_unconst(value), namelen, valuelen, 0};
}

/* grpc_nv_make() for a string-literal name and value. */
#define GRPC_NV(name, value) grpc_nv_make("" name, sizeof(name) - 1, "" value, sizeof(value) - 1)

/* Why a transport should reset a request stream (mapped to the protocol's error code). */
typedef enum grpc_reset_reason {
  GRPC_RESET_PROTOCOL_ERROR,
  GRPC_RESET_INTERNAL_ERROR,
} grpc_reset_reason;

/* What the call layer and grpc_stream_core.c need from a transport. Every function runs on the
 * loop thread; the int-returning ones return 0 on success or a grpc_err on failure
 * (GRPC_ERR_NOMEM when the HTTP library ran out of memory, GRPC_ERR_PROTOCOL otherwise). Frames are only
 * queued; the session flush sends them. */
typedef struct grpc_transport_ops grpc_transport_ops;
struct grpc_transport_ops {
  /* "h2" or "h3", for logs. */
  const char *name;
  /* Queue response headers; end_stream makes them the last frame (trailers-only, 204, 415). */
  int (*submit_headers)(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                        size_t nvlen, bool end_stream);
  /* Start sending the stream's queued messages; the transport pulls them with
   * stream_core_pull() until it reports EOF. Called only while stream->is_data_active is false. */
  int (*start_data)(grpc_session *session, grpc_stream *stream);
  /* Queue the trailers, ending the stream. Never called while data is active. */
  int (*submit_trailers)(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                         size_t nvlen);
  /* Reset the stream (RST_STREAM / RESET_STREAM). */
  void (*reset_stream)(grpc_session *session, grpc_stream *stream, grpc_reset_reason reason);
  /* Take / drop a reference on a received header buffer (nghttp2_rcbuf / nghttp3_rcbuf). The
   * buffers are NUL-terminated and outlive the session while referenced. */
  void (*hdr_incref)(void *ref);
  void (*hdr_decref)(void *ref);

  /* Session-level operations, used by the maintenance timer, drain and the flush/close paths. */
  /* Queue a graceful shutdown notice (HTTP/2 GOAWAY NO_ERROR / HTTP/3 GOAWAY): no new streams,
   * running ones may finish. */
  void (*goaway)(grpc_session *session);
  /* Queue the first step of a graceful shutdown (gRPC's two-step GOAWAY): a notice that new
   * streams will soon be refused, without naming the last stream yet, so requests already in
   * flight are still accepted. HTTP/2 follows it with a PING whose acknowledgment
   * (ping_pending turning false) shows the client has seen it. goaway() completes it. */
  void (*shutdown_notice)(grpc_session *session);
  /* Queue a keepalive ping; 0 if one was queued. nullptr when the transport keeps the
   * connection alive by itself (QUIC). */
  int (*ping)(grpc_session *session);
  /* Write pending output / tear the connection down. nullptr selects the TCP implementation:
   * session_flush() in internal_session_io.c / session_close() in internal_helpers.c. */
  void (*flush)(grpc_session *session);
  void (*close)(grpc_session *session);
};

/* An owned list of response headers. */
typedef struct nv_list nv_list;
struct nv_list {
  grpc_nv *nva;
  size_t len;
  size_t cap;
};

/* One RPC as seen by the application (behind the public grpc_call handle). Reference-counted;
 * outlives its stream, which is cleared when the HTTP/2 stream closes. */
struct grpc_call {
  atomic_size_t refs;
  grpc_server *server;
  grpc_route *route;

  /* The request stream, until it closes. */
  grpc_session *session;
  grpc_stream *stream;
  int64_t stream_id;
  /* The transport that owns the retained header buffers (they outlive the session). */
  const grpc_transport_ops *ops;

  u64 start_ms;
  /* The client's deadline; 0 means there is none. */
  u64 deadline_ms;
  char peer[INET6_ADDRSTRLEN + 8];
  char peer_cert_subject[256];

  /* Header views and transport buffer refs retained for the call lifetime. */
  grpc_header req_headers[CONFIG_STREAM_MAX_HEADERS];
  /* one block holding every name and value of req_headers (NUL-terminated copies) */
  char *req_header_data;
  size_t nb_req_headers;

  /* Received messages not yet delivered to the application. */
  grpc_msg_queue pending;
  /* The request for a ROUTE_UNARY call. */
  grpc_message *request;

  /* Response headers and trailers set by the application. */
  nv_list headers;
  nv_list trailers;

  grpc_status cancel_reason;

  bool headers_sent : 1;
  /* Encoding announced with the first compressed response message. */
  grpc_compression_algorithm compression;
  /* Automatic compression configured for uncompressed response messages. */
  grpc_compression_algorithm auto_compression;
  int auto_compression_level;
  bool is_grpc_web : 1;
  bool is_grpc_web_text : 1;
  bool finished : 1;
  bool closed : 1;
  bool cancelled : 1;
  bool cancel_delivered : 1;
  /* The client half-closed its request stream. */
  bool half_closed : 1;
  bool end_delivered : 1;
  /* Interceptors ran and the application has seen the call. */
  bool started : 1;
  bool rejected : 1;
  bool request_delivered : 1;
  /* Interceptors' on_done callbacks ran. */
  bool done_reported : 1;
  bool queued : 1;
  bool stats_done : 1;

  char *method_override;
  void *userdata;
  void (*userdata_free)(void *);
  void (*cancel_fn)(grpc_call *, void *);
  void *cancel_userdata;
  void (*writable_fn)(grpc_call *, void *);
  void *writable_userdata;

  /* Link in grpc_server.ready_head. */
  grpc_call *ready_next;
};

/* ---- streams and sessions ----------------------------------------------------- */

/* Small request messages are buffered inside the stream. */
enum { GRPC_STREAM_INLINE_RECV = 256 };

/* Transport state of one request stream: request headers, receive buffer and outgoing message
 * queue. Owned by its session and freed when the transport closes the stream. */
struct grpc_stream {
  int64_t stream_id;
  const grpc_transport_ops *ops;

  /* Intrusive links in grpc_session.streams. */
  grpc_stream *prev;
  grpc_stream *next;

  size_t nb_headers;
  /* Names and values point into the transport buffers held by header_refs. */
  grpc_header headers[CONFIG_STREAM_MAX_HEADERS];
  void *header_refs[CONFIG_STREAM_MAX_HEADERS][2];

  bool is_method_post : 1;
  bool is_method_options : 1;
  bool is_scheme_http : 1;
  /* True when the te header contains trailers. */
  bool is_te_trailers : 1;
  /* True when the content type starts with application/grpc. */
  bool is_content_grpc : 1;
  bool is_grpc_web : 1;
  bool is_grpc_web_text : 1;
  bool is_deadline_set : 1;
  /* A data provider was submitted and has not returned EOF. */
  bool is_data_active : 1;
  /* The request was rejected; ignore any remaining client data. */
  bool is_discarding : 1;
  bool was_backpressured : 1;
  uint64_t deadline_ms;

  /* These values point into headers. */
  const char *hcontent_type;
  const char *hpath;
  size_t hpath_len;
  const char *hgrpc_encoding;
  grpc_compression_algorithm grpc_encoding;
  /* grpc-encoding names a codec this build cannot decode: compressed messages get UNIMPLEMENTED */
  bool grpc_encoding_unsupported;
  /* request messages parsed so far (a unary request may have only one) */
  size_t nb_recv_messages;
  const char *hgrpc_accept_encoding;
  const char *hgrpc_timeout;

  /* The method selected by :path. */
  grpc_route *route;
  /* The call for a routed, valid request. */
  grpc_call *call;

  /* recv_data points to recv_inline until the buffer needs to grow. */
  size_t recv_start;
  size_t recv_len;
  size_t recv_cap;
  u8 *recv_data;
  u8 recv_inline[GRPC_STREAM_INLINE_RECV];

  /* Bytes from the front message already handed to nghttp2, including its
   * five-byte prefix. */
  size_t trunc_seek;
  size_t queued_send_bytes;
  grpc_msg_queue lqueue_omessages;
  bool send_eof_on_empty : 1;

  /* Trailers submitted while DATA is queued. They are sent after the data
   * provider reaches EOF so they cannot overtake the DATA. */
  grpc_nv *pending_trailers;
  size_t nb_pending_trailers;

  /* Transport-private per-stream state (HTTP/3 send buffers). */
  void *transport;
};

/* One client connection: socket handle, optional TLS session, nghttp2 session and its streams.
 * Lives in a slot of grpc_server.sessions that is reused after the connection closes. */
struct grpc_session {
  const grpc_transport_ops *ops;
  nghttp2_session *ngsession;
  /* Index in grpc_server.sessions. */
  size_t index;
  socklen_t socklen;
  /* Parent server context. */
  grpc_server *server;

  struct sockaddr_storage sockaddr;
  /* Peer address in "ip:port" or "[ip]:port" form. */
  char peer[INET6_ADDRSTRLEN + 8];
  /* Verified TLS client certificate subject, if any. */
  char peer_cert_subject[256];

  /* Open streams, allocated when a HEADERS frame is about to arrive. */
  grpc_stream *streams;
  size_t nb_streams;

  struct grpc_tls_session *tls;
  /* the server TLS context `tls` was created from (released to server->tls_pool) */
  void *tls_ctx_used;
  bool is_tls : 1;
  bool tls_handshake_done : 1;

  /* The QUIC connection of an HTTP/3 session (grpc_http3_server.c); nullptr for TCP sessions. */
  struct grpc_quic_conn *quic;

  union {
    uv_tcp_t tcp;
    uv_pipe_t pipe;
  } handle;
  bool is_unix : 1;
  bool is_active : 1;
  bool is_closing : 1;

  /* Keepalive, idle, age, and ping-strike tracking (monotonic ms). */
  u64 created_ms;
  u64 last_activity_ms;
  u64 ping_sent_ms;
  u64 last_client_ping_ms;
  /* when the connection last had no stream left (max_connection_idle_ms counts from here) */
  u64 idle_since_ms;
  u64 goaway_sent_ms;
  uint32_t ping_strikes;
  bool ping_pending : 1;
  bool goaway_sent : 1;

  /* Bytes serialized by nghttp2 (and encrypted, if TLS) but not yet accepted
   * by the kernel. */
  grpc_buf obuf;
  /* Buffer owned by the in-flight write_req. */
  grpc_buf wbuf;
  uv_write_t write_req;
  bool is_writing : 1;

  /* Link in grpc_server.flush_head. It survives a session reset because a
   * closed slot may still be queued. */
  bool is_flush_queued : 1;
  grpc_session *flush_next;
  /* Output stopped because a buffer could not grow; server->flush_retry_timer flushes it again. */
  bool is_flush_blocked : 1;
  /* a stream could not be reset for lack of memory: the next flush closes the connection, so its
   * client does not wait for a response that will never come */
  bool close_on_flush : 1;
};

/* ---- the server ------------------------------------------------------------ */

/* A task queued by grpc_server_post(), run on the loop thread. */
typedef struct post_task post_task;
struct post_task {
  void (*fn)(void *);
  void *arg;
  post_task *next;
};

struct health_state;
struct reflection_state;

/* The server behind the public grpc_server handle: configuration, libuv loop and listeners,
 * session slots, routes, interceptors, events and statistics. */
struct grpc_server {
  /* Configuration with defaults applied. */
  grpc_config config;
  char *ipv4_addr;
  char *ipv6_addr;
  char *unix_path;

  atomic_int log_level; /* Current per-server threshold. */
  FILE *log_file;       /* Non-owned log stream. */
  char log_topic[40];
  int log_topic_id;
  bool log_topic_initialized;
  struct grpc_server *log_registry_next;

  grpc_route **routes;
  size_t nb_routes;
  size_t cap_routes;
  grpc_route *fallback_route;

  nghttp2_session_callbacks *ngcallbacks;

  uv_os_sock_t ipv4_sockfd;
  uv_os_sock_t ipv6_sockfd;

  uv_loop_t loop;
  uv_tcp_t ipv4_listener;
  uv_tcp_t ipv6_listener;
  uv_pipe_t unix_listener;
  /* Timeout used by grpc_server_poll(). */
  uv_timer_t timer;
  /* Fires at the earliest stream deadline. */
  uv_timer_t deadline_timer;
  /* Periodic timer for HTTP/2 keepalive, idle, and connection age checks. */
  uv_timer_t maintenance_timer;
  /* Retries the flush of sessions whose output was blocked by an allocation failure. */
  uv_timer_t flush_retry_timer;
  /* Wakes the loop for grpc_server_post() and grpc_server_stop(). */
  uv_async_t async;

  bool loop_initialized : 1;
  bool ipv4_listener_active : 1;
  bool ipv6_listener_active : 1;
  bool unix_listener_active : 1;
  /* unix_path was bound by this server, so grpc_server_free() removes it */
  bool unix_bound : 1;
  bool timer_active : 1;
  bool deadline_timer_active : 1;
  bool maintenance_timer_active : 1;
  bool flush_retry_timer_active : 1;
  /* expire_deadlines() is walking the sessions: grpc_server_flush() must not close one now */
  bool in_deadline_scan : 1;
  bool async_active : 1;
  bool is_tls : 1;

  atomic_bool stop;

  /* Runtime metrics (updated on the event-loop thread, readable from any thread). */
  atomic_uint_fast64_t stat_active_connections;
  atomic_uint_fast64_t stat_total_connections;
  atomic_uint_fast64_t stat_active_streams;
  atomic_uint_fast64_t stat_total_calls;
  atomic_uint_fast64_t stat_calls_ok;
  atomic_uint_fast64_t stat_calls_error;
  atomic_uint_fast64_t stat_bytes_received;
  atomic_uint_fast64_t stat_bytes_sent;
  atomic_uint_fast64_t stat_messages_received;
  atomic_uint_fast64_t stat_messages_sent;

  int ready_events;

  /* One thread drives the loop. Each session reads into this buffer and
   * passes the bytes to nghttp2 before the next read. */
  u8 *rbuf;
  size_t rbuf_len;
  /* TLS plaintext output, batched for encryption. */
  grpc_buf tls_obuf;

  /* Sessions with output to flush on the next poll. */
  grpc_session *flush_head;

  /* Deadline for the timer; UINT64_MAX means it is disarmed. */
  u64 next_deadline_ms;

  size_t nb_sessions;
  grpc_session *sessions;
  /* Stack of indices for free slots in sessions. */
  size_t nb_free_sessions;
  size_t *free_sessions;

  size_t nb_interceptors;
  grpc_interceptor *interceptors;

  /* Calls with data to deliver to the application. */
  grpc_call *ready_head;
  grpc_call *ready_tail;

  /* Events from the last poll. Each holds a reference to its call and owns
   * its message. */
  grpc_event *events;
  size_t nb_events;
  size_t cap_events;

  /* Queue used by grpc_server_post(). */
  pthread_mutex_t post_mutex;
  post_task *post_head;
  post_task *post_tail;

  struct health_state *health;
  struct reflection_state *reflection;

  /* the active TLS context and replaced ones still used by live connections */
  tls_ctx_pool tls_pool;

  /* HTTP/3 listener state (grpc_http3_server.c), when config.http3.enable. */
  struct grpc_h3_server *h3;
  /* Value of the alt-svc header added to HTTP/2 responses ("" = none). */
  char alt_svc[24];
};

/* ---- internal functions ------------------------------------------------------ */

/* grpc_call.c, grpc_call_response.c (call_abort), grpc_call_dispatch.c (dispatch and events) */
/* Create the call for a stream whose request headers were accepted. The returned reference is
 * owned by the stream and dropped by call_detach(). nullptr when out of memory. */
grpc_call *call_new(grpc_session *session, grpc_stream *stream);
/* Detach the stream and cancel the call unless it already finished. */
void call_detach(grpc_call *call, grpc_status reason);
/* End the call with status on behalf of the library and cancel it. */
void call_abort(grpc_call *call, grpc_status status, const char *message);
/* Queue the call for server_dispatch(). */
void call_queue(grpc_call *call);
/* Deliver queued calls to handlers or convert them into events. */
void server_dispatch(grpc_server *server);
/* Release events from the previous poll. */
void server_clear_events(grpc_server *server);
/* Drop queued calls without dispatching them. */
void server_drop_ready(grpc_server *server);

/* grpc_server_routes.c */
/* Return the route whose path equals the `len` bytes at `path` (enabled or disabled), or
 * nullptr. */
grpc_route *server_find_route(grpc_server *server, const char *path, size_t len);
/* Register `path` (copied; must start with '/') as a route of `kind`, or re-enable a disabled
 * route with the same path. Copies *stream. Returns GRPC_OK, GRPC_ERR_EXISTS, GRPC_ERR_INVALID
 * or GRPC_ERR_NOMEM. Loop thread only. */
grpc_err server_add_route(grpc_server *server, const char *path, route_kind kind,
                          grpc_handler_fn handler, const grpc_stream_handler *stream,
                          void *userdata);

/* grpc_interceptor.c */
/* Run on_call interceptors; return false if one ends the call. */
bool interceptors_run_on_call(grpc_call *call);
/* Run on_recv_message interceptors; return false if one finished the call (drop the message). */
bool interceptors_run_on_recv(grpc_call *call, const grpc_message *message);
/* Run on_send_message interceptors; return false if one finished the call (drop the message). */
bool interceptors_run_on_send(grpc_call *call, const grpc_message *message);
/* Run on_done interceptors once per call, only for calls that went through on_call. */
void interceptors_run_on_done(grpc_call *call, grpc_status status);
/* Free the interceptor list, calling each userdata_free. */
void interceptors_free(grpc_server *server);

/* grpc_health.c, grpc_reflection.c */
/* Free the health / reflection state of a server being destroyed (no-op if never created). */
void health_free(grpc_server *server);
void reflection_free(grpc_server *server);

/* grpc_tls.c */
/* Build a TLS context from `cfg` (PEM lengths already filled in) and make it the active one; the
 * previous context is retired, not freed. GRPC_ERR_INVALID if the context cannot be built,
 * GRPC_ERR_UNSUPPORTED without TLS. */
grpc_err tls_enable(grpc_server *server, const grpc_tls_config *cfg);
/* Free the active and retired TLS contexts. */
void tls_free(grpc_server *server);

/* grpc_message.c: a message with a compressed flag */
/* Allocate a message with `len` payload bytes and the given compressed-flag byte, copying `data`
 * if it is non-null. nullptr when out of memory or `len` exceeds UINT32_MAX. Free with
 * grpc_message_free(). */
grpc_message *message_create(u8 cflag, const void *data, size_t len);

/* Strict standard-alphabet Base64 decode (grpc_metadata.c). Accepts padded and unpadded input;
 * rejects any other character, misplaced or excess padding and a final group of one character.
 * With concatenated, padding may end any 4-character group and decoding continues after it
 * (gRPC-Web-Text bodies are separately encoded chunks); otherwise padding ends the value. On
 * success *out is a grpc_malloc buffer (at least 1 byte) holding *out_len bytes. Returns
 * GRPC_OK, GRPC_ERR_INVALID or GRPC_ERR_NOMEM; on error *out is nullptr. */
grpc_err base64_decode(const char *in, size_t len, bool concatenated, uint8_t **out,
                       size_t *out_len);

/* True if an application metadata entry may be sent (grpc_metadata.c): a key of lower-case
 * letters, digits, '-', '_' and '.', not an HTTP connection field, content-type or te (nor, in a
 * response, grpc-status, grpc-message or the encoding headers, which the library writes); a
 * value of printable ASCII (0x20-0x7E), or Base64 text for keys ending in
 * "-bin" (encoded with grpc_metadata_encode_bin()). CR, LF and other control bytes are never
 * allowed, so a value cannot inject further fields. */
bool metadata_entry_valid(const char *key, size_t klen, const char *value, size_t vlen,
                          bool response);
