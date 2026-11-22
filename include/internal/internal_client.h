#pragma once

/* internal_client.h - the client's internal data model, shared by its two transports
 *
 * grpc_client.c implements the client and its HTTP/2 transport (nghttp2 over TCP / Unix sockets);
 * grpc_http3_client.c implements HTTP/3 (ngtcp2 + nghttp3 over UDP) for subchannels of a client
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
