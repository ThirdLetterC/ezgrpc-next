#pragma once

/* internal_client.h - the client's internal data model, shared by its two transports
 *
 * grpc_client.c implements the client API on top of subchannels, with the HTTP/2 transport
 * (nghttp2 over TCP / Unix sockets) in grpc_client_connect.c (connecting, TLS, tearing down) and
 * grpc_client_io.c (socket I/O, nghttp2 callbacks), and response handling in
 * grpc_client_response.c. grpc_http3_client.c implements HTTP/3 (ngtcp2 + nghttp3 over UDP) for subchannels of a client
 * created with GRPC_TRANSPORT_HTTP3. Both feed responses through client_resp_header() /
 * client_resp_data() so headers, trailers, status, decompression and message delivery are shared.
 * Each subchannel runs its own libuv loop only while the calling thread waits; nothing here is
 * thread-safe.
 */
#include "core.h"
#include "grpc_client.h"

#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
#define GRPC_CLIENT_HAS_OPENSSL 1
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#endif

typedef struct client_subchannel client_subchannel;

/* One RPC on a subchannel. send_buf holds framed request bytes not yet taken by nghttp2,
 * recv_buf holds response bytes not yet forming a complete message, and resp accumulates
 * headers, trailers and status until grpc_client_stream_finish hands it to the caller. */
struct grpc_client_stream {
  client_subchannel *sub;
  int64_t stream_id;
  /* HTTP/3 transport state of the stream (grpc_http3_client.c), or nullptr. */
  void *h3;
  bool send_closed;
  bool stream_done;
  bool headers_received;
  bool timed_out;
  grpc_err err;
  grpc_compression_algorithm resp_encoding;
  /* the response's grpc-encoding names a codec this build cannot decode */
  bool resp_encoding_unsupported;
  /* a grpc-status header was received (it overrides the HTTP status mapping) */
  bool grpc_status_seen;
  /* the client rejected the response (stream_reject()): later headers do not change it */
  bool rejected;
  grpc_buf send_buf;
  size_t send_off;
  grpc_buf recv_buf;
  grpc_msg_queue recv_queue; /* received messages not yet taken by grpc_client_stream_recv() */
  grpc_client_response resp;
  void (*on_message)(const grpc_message *msg, void *userdata);
  void *on_message_userdata;
};

/* One connection to one target, with its own loop; obuf collects bytes to write, wbuf is the
 * buffer owned by an in-flight uv_write, and tls_obuf stages nghttp2 output before encryption. */
struct client_subchannel {
  grpc_client *parent;
  char *host;
  uint16_t port;
  char *unix_path;
  char authority[280];

  uv_loop_t loop;
  bool loop_initialized;
  union {
    uv_tcp_t tcp;
    uv_pipe_t pipe;
  } handle;
  bool is_unix;
  bool handle_initialized;
  bool connected;
  bool closed;
  int connect_status;
  /* why the connection was marked closed by a local failure (see sub_fail()), or GRPC_OK */
  grpc_err fail_err;
  /* set by the connect/handshake deadline timer (config.timeout_ms) */
  bool connect_deadline_hit;
  bool handle_close_done;

  uv_timer_t timer;
  bool timer_initialized;

  nghttp2_session *ngsession;
  u8 rbuf[64 * 1024];
  grpc_buf obuf;
  grpc_buf wbuf;
  grpc_buf tls_obuf;
  uv_write_t write_req;
  bool is_writing;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  SSL *ssl;
  bool tls_handshake_done;
#endif

  grpc_client_stream *active_stream;

  /* The QUIC connection when the client uses HTTP/3 (grpc_http3_client.c), or nullptr. */
  struct h3c_conn *h3c;
};

/* The channel: a copy of the config plus the subchannel pool and its round-robin cursor. */
struct grpc_client {
  grpc_client_config config;
  client_subchannel *subs;
  size_t nb_subs;
  size_t rr_next;
#if defined(GRPC_CLIENT_HAS_OPENSSL)
  SSL_CTX *ssl_ctx;
#endif
  /* QUIC TLS context for HTTP/3 (a grpc_quic_tls_ctx), or nullptr. */
  void *quic_tls_ctx;
};

/* Records a response header field for the stream: grpc-status, grpc-message and grpc-encoding are
 * interpreted, and every field is stored in resp.headers, or in resp.trailers when `trailer` (a
 * trailing header block, or a trailers-only response). */
void client_resp_header(grpc_client_stream *st, const uint8_t *name, size_t namelen,
                        const uint8_t *value, size_t valuelen, bool trailer);

/* Appends response body bytes and delivers every complete message. Fails the stream with
 * GRPC_ERR_LIMIT past max_message_size. Returns 0, or -1 when out of memory (the stream fails). */
int client_resp_data(grpc_client_stream *st, const uint8_t *data, size_t len);

/* Returns the subchannel handle as a uv_stream_t, whichever of TCP or pipe it is. */
static inline uv_stream_t *sub_stream(client_subchannel *s) {
  return s->is_unix ? (uv_stream_t *)&s->handle.pipe : (uv_stream_t *)&s->handle.tcp;
}

/* Returns the subchannel handle as a uv_handle_t, whichever of TCP or pipe it is. */
static inline uv_handle_t *sub_handle(client_subchannel *s) {
  return s->is_unix ? (uv_handle_t *)&s->handle.pipe : (uv_handle_t *)&s->handle.tcp;
}

/* ---- HTTP/2 subchannel I/O (grpc_client_io.c) ---- */

/* Installs the client's nghttp2 callbacks (headers, data, frames, stream close) in cbs. */
void client_h2_set_callbacks(nghttp2_session_callbacks *cbs);

/* Starts reading the subchannel's handle into its nghttp2 session. Returns uv_read_start()'s
 * result. */
int sub_read_start(client_subchannel *s);

/* Pulls all pending frames out of nghttp2 (through SSL when TLS is on) into obuf and writes
 * them: first with uv_try_write, then the remainder with an async uv_write after swapping obuf
 * into wbuf, so obuf can keep accumulating while the write is in flight. Only one uv_write is
 * outstanding at a time; while it is, this returns early and on_sub_write calls back. Errors
 * mark the active stream done with an error and, for I/O errors, the subchannel closed. Before
 * the TLS handshake completes, only handshake bytes are sent. */
void sub_flush(client_subchannel *s);

/* Marks the connection unusable after nghttp2 output was lost (nghttp2 already considers bytes it
 * handed out as sent, so the session and the server are now out of sync), records why in
 * s->fail_err for sub_connect(), and fails the active RPC. The next RPC reconnects. */
void sub_fail(client_subchannel *s, grpc_err err);

/* nghttp2 data-provider callback for the request body: copies queued bytes from send_buf, sets
 * EOF once the send side is half-closed and drained, and returns NGHTTP2_ERR_DEFERRED when there
 * is nothing to send yet (resumed by nghttp2_session_resume_data on the next send/close). The
 * stream is looked up through the subchannel rather than the data source, which may outlive it:
 * once the application freed the stream (e.g. after the server answered before the whole
 * request was sent) the callback fails it, and nghttp2 resets that stream. */
ssize_t client_data_read(nghttp2_session *session, int32_t stream_id, uint8_t *buf, size_t length,
                         uint32_t *data_flags, nghttp2_data_source *source, void *user_data);

/* ---- Subchannel lifecycle (grpc_client_connect.c) ---- */

#if defined(GRPC_CLIENT_HAS_OPENSSL)
/* Builds the shared client SSL_CTX: TLS 1.2+, ALPN "h2", peer verification against ca_file,
 * ca_pem or the system default paths (unless insecure_skip_verify), and an optional client
 * certificate/key for mTLS (both files are required together). Returns nullptr on any error. */
SSL_CTX *client_ssl_ctx_new(const grpc_client_config *cfg);
#endif

/* Tears down the connection: frees the nghttp2 session and SSL object, closes the handle and
 * timer, runs the loop until all close callbacks (and any pending connect request) complete,
 * and closes the loop. Keeps the target strings and buffers so sub_connect can reuse them. Safe
 * to call on a never-connected or already disconnected subchannel. */
void sub_disconnect(client_subchannel *s);

/* True if a connected subchannel can still carry a new RPC. Its loop only runs during RPCs, so
 * this first catches up: refreshes the loop clock and handles what arrived meanwhile (an EOF, a
 * GOAWAY), then asks the transport: nghttp2 refuses new streams after a GOAWAY, and a QUIC
 * connection may have idled out. */
bool sub_usable(client_subchannel *s);

/* Ensures the subchannel is connected, reconnecting from scratch if it was closed or can no
 * longer carry requests (sub_usable()). Creates a fresh loop, connects to the Unix path or to each
 * resolved address in turn (IP literals skip getaddrinfo, which otherwise blocks), performs the
 * TLS handshake when configured, creates the nghttp2 session and queues SETTINGS. The whole
 * sequence is bounded by bound_ms when nonzero (the smaller of config.timeout_ms and the RPC's
 * own deadline). Blocks the caller while it runs the private loop. Returns GRPC_OK, or
 * GRPC_ERR_IO / GRPC_ERR_NOMEM / GRPC_ERR_PROTOCOL after disconnecting again. */
grpc_err sub_connect(client_subchannel *s, uint32_t bound_ms);

/* Parses a target string: "unix:path" or "unix://path" sets *out_unix, otherwise "host:port" or
 * "[ipv6]:port" sets *out_host and *out_port (1-65535; characters after the digits are ignored).
 * Outputs are grpc_malloc strings owned by the caller. Returns GRPC_OK, GRPC_ERR_INVALID for
 * malformed input or GRPC_ERR_NOMEM, with nothing left allocated on failure. */
grpc_err client_parse_target(const char *target, char **out_host, uint16_t *out_port,
                             char **out_unix);

/* Fills in a zeroed subchannel's target (copying host or unix_path) and the :authority value
 * ("localhost" for Unix sockets). Does not connect. Returns GRPC_ERR_NOMEM if a copy fails; the
 * partially initialized subchannel is still released by grpc_client_free. */
grpc_err client_init_subchannel(client_subchannel *s, grpc_client *parent, const char *host,
                                uint16_t port, const char *unix_path);

/* ---- HTTP/3 transport (grpc_http3_client.c; stubs without GRPC_ENABLE_HTTP3) ---- */

/* Builds the QUIC TLS context for config->tls; nullptr on failure. */
void *h3c_tls_ctx_new(const grpc_client_config *config);
void h3c_tls_ctx_free(void *ctx);

/* Connects the subchannel over QUIC and completes the handshake, running s->loop (bounded by
 * s->connect_deadline_hit). s->loop and s->timer are initialized by the caller. Returns GRPC_OK or
 * GRPC_ERR_IO; on failure the caller runs sub_disconnect(). */
grpc_err h3c_connect(client_subchannel *s);

/* Frees the QUIC connection and closes its handles (the caller then drains and closes the loop). */
void h3c_disconnect(client_subchannel *s);

/* True if the subchannel's QUIC connection can carry another request: not idled out (checked
 * against the clock now, since its timers only run during RPCs), closing or draining. */
bool h3c_usable(client_subchannel *s);

/* Number of request stream states the connection keeps for streams whose RPC already ended but
 * that nghttp3 has not closed yet. They go away as those streams close; tests use this to check
 * that none is kept for a stream nghttp3 never knew. */
size_t h3c_orphan_count(const client_subchannel *s);

/* The connection's count of datagrams queued because the socket buffer was full, dropped because
 * the queue was full too, and of batches sent with one GSO call (0 without a connection). For
 * tests. */
void h3c_send_counters(const client_subchannel *s, uint64_t *queued, uint64_t *dropped,
                       uint64_t *gso_sends);

/* Opens a request stream, submits the headers with st->send_buf as the body source and sets
 * st->stream_id. Returns 0, or -1. */
int h3c_submit_request(client_subchannel *s, grpc_client_stream *st, const grpc_nv *nva,
                       size_t nvlen);

/* More request bytes (or the half-close) are ready: wakes the stream's data reader. */
void h3c_resume(client_subchannel *s, grpc_client_stream *st);

/* Cancels the stream (RESET_STREAM / STOP_SENDING with H3_REQUEST_CANCELLED). */
void h3c_reset(client_subchannel *s, grpc_client_stream *st);

/* Writes pending QUIC packets and re-arms the connection timer. */
void h3c_flush(client_subchannel *s);

/* Releases the stream's transport state (called when the stream is freed). */
void h3c_stream_free(grpc_client_stream *st);
