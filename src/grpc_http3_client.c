/* grpc_http3_client.c - the HTTP/3 (QUIC) client transport
 *
 * A client created with GRPC_TRANSPORT_HTTP3 gives each subchannel a QUIC connection (ngtcp2) on a
 * connected UDP socket instead of a TCP/TLS/nghttp2 connection; HTTP/3 framing runs in nghttp3.
 * Everything else is shared with grpc_client.c: the subchannel's private libuv loop (run only
 * while the caller waits), the connect and RPC deadlines on s->timer, one active RPC per
 * subchannel, retries, round-robin, and response handling through client_resp_header() /
 * client_resp_data().
 *
 * Request bodies are copied from the stream's send_buf into chunks that stay alive until QUIC
 * acknowledges them, since nghttp3 sends from the caller's memory. A stream freed by the
 * application before its data is acknowledged hands its chunks to the connection (orphans), so
 * a retransmission never reads freed memory.
 */
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <nghttp3/nghttp3.h>
#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>

#include "core.h"
#include "internal_client.h"
#include "internal_helpers.h"
#include "internal_quic_tls.h"

/* ngtcp2 and nghttp3 keep a pointer to their memory table, so these live for the program. */
static const ngtcp2_mem quic_mem = {
    .user_data = nullptr,
    .malloc = grpc_mem_hook_malloc,
    .free = grpc_mem_hook_free,
    .calloc = grpc_mem_hook_calloc,
    .realloc = grpc_mem_hook_realloc,
};
static const nghttp3_mem h3_mem = {
    .user_data = nullptr,
    .malloc = grpc_mem_hook_malloc,
    .free = grpc_mem_hook_free,
    .calloc = grpc_mem_hook_calloc,
    .realloc = grpc_mem_hook_realloc,
};

enum { H3C_CID_LEN = 18 };
enum { H3C_CHUNK = 16384 };
enum { H3C_MAX_UDP_PAYLOAD = 1452 };

/* A request-body chunk handed to nghttp3, freed once acknowledged. */
typedef struct h3c_chunk h3c_chunk;
struct h3c_chunk {
  h3c_chunk *next;
  size_t len;
  uint8_t data[H3C_CHUNK];
};

/* Per-stream state (grpc_client_stream.h3), also the nghttp3 stream user data. It outlives the
 * RPC when the application frees the stream before nghttp3 closes it (st becomes nullptr and the
 * state moves to the connection's orphan list until h3c_on_stream_close() or disconnect). */
typedef struct h3c_stream h3c_stream;
struct h3c_stream {
  grpc_client_stream *st;
  h3c_chunk *head;
  h3c_chunk *tail;
  size_t head_acked;
  /* nghttp3 closed the stream; nothing references the chunks any more. */
  bool closed;
  h3c_stream *orphan_prev;
  h3c_stream *orphan_next;
};

/* The QUIC connection of a subchannel (client_subchannel.h3c). */
struct h3c_conn {
  client_subchannel *s;
  ngtcp2_conn *conn;
  nghttp3_conn *h3;
  grpc_quic_tls_session *tls;
  ngtcp2_crypto_conn_ref conn_ref;
  uv_udp_t udp;
  uv_timer_t timer;
  int open_handles;
  struct sockaddr_storage local;
  socklen_t locallen;
  struct sockaddr_storage remote;
  socklen_t remotelen;
  ngtcp2_ccerr ccerr;
  bool handshake_done;
  /* Stream states whose RPC was freed before nghttp3 closed the stream. */
  h3c_stream *orphans;
  uint8_t rbuf[65536];
  uint8_t sbuf[H3C_MAX_UDP_PAYLOAD];
};
typedef struct h3c_conn h3c_conn;

/* ---- helpers ---- */

static ngtcp2_tstamp h3c_now(void) {
  return uv_hrtime();
}

static void h3c_random(uint8_t *dest, size_t len) {
  if (uv_random(nullptr, nullptr, dest, len, 0, nullptr) != 0) {
    for (size_t i = 0; i < len; i++)
      dest[i] = (uint8_t)(uv_hrtime() >> (i % 8));
  }
}

static void cb_rand(uint8_t *dest, size_t destlen, const ngtcp2_rand_ctx *rand_ctx) {
  (void)rand_ctx;
  h3c_random(dest, destlen);
}

/* The active stream if it is `stream_id`, else nullptr. */
static grpc_client_stream *active(h3c_conn *hc, int64_t stream_id) {
  grpc_client_stream *st = hc->s->active_stream;
  return st != nullptr && st->stream_id == stream_id ? st : nullptr;
}

/* Marks the connection broken and fails the active RPC with `err`. */
static void h3c_fail(h3c_conn *hc, grpc_err err) {
  hc->s->closed = true;
  grpc_client_stream *st = hc->s->active_stream;
  if (st != nullptr && !st->stream_done) {
    st->err = err;
    st->stream_done = true;
  }
}

/* Returns flow-control credit for consumed bytes. */
static void h3c_consume(h3c_conn *hc, int64_t stream_id, size_t n) {
  ngtcp2_conn_extend_max_stream_offset(hc->conn, stream_id, n);
  ngtcp2_conn_extend_max_offset(hc->conn, n);
}

/* Frees a chunk list. */
static void free_chunks(h3c_chunk *c) {
  while (c != nullptr) {
    h3c_chunk *next = c->next;
    grpc_free(c);
    c = next;
  }
}

/* Unlinks an orphaned stream state from the connection and frees it with its chunks. */
static void orphan_free(h3c_conn *hc, h3c_stream *hs) {
  if (hs->orphan_prev != nullptr)
    hs->orphan_prev->orphan_next = hs->orphan_next;
  else
    hc->orphans = hs->orphan_next;
  if (hs->orphan_next != nullptr)
    hs->orphan_next->orphan_prev = hs->orphan_prev;
  free_chunks(hs->head);
  grpc_free(hs);
}

/* ---- nghttp3 callbacks ---- */

/* Request-body data reader: moves send_buf bytes into an acknowledged-later chunk; EOF (with
 * END_STREAM) once the send side is closed and drained, WOULDBLOCK while waiting for more. */
static nghttp3_ssize h3c_read_data(nghttp3_conn *conn, int64_t stream_id, nghttp3_vec *vec,
                                   size_t veccnt, uint32_t *pflags, void *conn_user_data,
                                   void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)conn_user_data;
  h3c_stream *hs = stream_user_data;
  grpc_client_stream *st = hs != nullptr ? hs->st : nullptr;
  if (st == nullptr) { /* freed by the application: its body is complete */
    *pflags = NGHTTP3_DATA_FLAG_EOF;
    return 0;
  }
  size_t rem = st->send_buf.len - st->send_off;
  if (rem == 0) {
    if (st->send_closed) {
      *pflags = NGHTTP3_DATA_FLAG_EOF;
      return 0;
    }
    return NGHTTP3_ERR_WOULDBLOCK;
  }
  if (veccnt == 0)
    return NGHTTP3_ERR_WOULDBLOCK;
  h3c_chunk *c = grpc_malloc(sizeof(*c));
  if (c == nullptr)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  c->len = rem < sizeof(c->data) ? rem : sizeof(c->data);
  c->next = nullptr;
  memcpy(c->data, st->send_buf.data + st->send_off, c->len);
  st->send_off += c->len;
  if (st->send_off == st->send_buf.len) {
    st->send_buf.len = 0;
    st->send_off = 0;
    if (st->send_closed)
      *pflags = NGHTTP3_DATA_FLAG_EOF;
  }
  if (hs->tail != nullptr)
    hs->tail->next = c;
  else
    hs->head = c;
  hs->tail = c;
  vec[0] = (nghttp3_vec){.base = c->data, .len = c->len};
  return 1;
}

static const nghttp3_data_reader h3c_data_reader = {.read_data = h3c_read_data};

/* acked_stream_data: frees fully acknowledged request chunks (orphaned ones at disconnect). */
static int h3c_on_acked_stream_data(nghttp3_conn *conn, int64_t stream_id, uint64_t datalen,
                                    void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)conn_user_data;
  h3c_stream *hs = stream_user_data;
  while (hs != nullptr && datalen > 0 && hs->head != nullptr) {
    size_t left = hs->head->len - hs->head_acked;
    size_t take = datalen < left ? (size_t)datalen : left;
    hs->head_acked += take;
    datalen -= take;
    if (hs->head_acked == hs->head->len) {
      h3c_chunk *c = hs->head;
      hs->head = c->next;
      if (hs->head == nullptr)
        hs->tail = nullptr;
      hs->head_acked = 0;
      grpc_free(c);
    }
  }
  return 0;
}

/* recv_header / recv_trailer: response fields of the active stream. */
static int h3c_on_recv_header(nghttp3_conn *conn, int64_t stream_id, int32_t token,
                              nghttp3_rcbuf *name, nghttp3_rcbuf *value, uint8_t flags,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)token;
  (void)flags;
  (void)stream_user_data;
  grpc_client_stream *st = active(conn_user_data, stream_id);
  if (st == nullptr)
    return 0;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  client_resp_header(st, n.base, n.len, v.base, v.len, st->headers_received);
  return 0;
}

/* end_headers: the first header block is complete. A block that also ends the stream is a
 * trailers-only response, so its fields move to the trailers. */
static int h3c_on_end_headers(nghttp3_conn *conn, int64_t stream_id, int fin, void *conn_user_data,
                              void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_client_stream *st = active(conn_user_data, stream_id);
  if (st == nullptr)
    return 0;
  if (!st->headers_received && fin) {
    grpc_metadata *t = st->resp.trailers;
    size_t nt = st->resp.nb_trailers;
    st->resp.trailers = st->resp.headers;
    st->resp.nb_trailers = st->resp.nb_headers;
    st->resp.headers = t;
    st->resp.nb_headers = nt;
  }
  st->headers_received = true;
  return 0;
}

/* recv_data: response body bytes for the active stream. */
static int h3c_on_recv_data(nghttp3_conn *conn, int64_t stream_id, const uint8_t *data,
                            size_t datalen, void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3c_conn *hc = conn_user_data;
  h3c_consume(hc, stream_id, datalen);
  grpc_client_stream *st = active(hc, stream_id);
  if (st != nullptr && client_resp_data(st, data, datalen) != 0)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  return 0;
}

static int h3c_on_deferred_consume(nghttp3_conn *conn, int64_t stream_id, size_t consumed,
                                   void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3c_consume(conn_user_data, stream_id, consumed);
  return 0;
}

/* end_stream: the server finished the response. */
static int h3c_on_end_stream(nghttp3_conn *conn, int64_t stream_id, void *conn_user_data,
                             void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_client_stream *st = active(conn_user_data, stream_id);
  if (st != nullptr)
    st->stream_done = true;
  return 0;
}

/* stream_close: the stream is gone; an error code other than H3_NO_ERROR fails the RPC unless it
 * already completed. */
static int h3c_on_stream_close(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                               void *conn_user_data, void *stream_user_data) {
  (void)conn;
  h3c_conn *hc = conn_user_data;
  h3c_stream *hs = stream_user_data;
  if (hs != nullptr) {
    hs->closed = true;
    if (hs->st == nullptr) /* orphan: the RPC is gone, nothing else references it */
      orphan_free(hc, hs);
  }
  grpc_client_stream *st = active(conn_user_data, stream_id);
  if (st != nullptr) {
    if (app_error_code != NGHTTP3_H3_NO_ERROR && st->err == GRPC_OK && !st->stream_done)
      st->err = GRPC_ERR_CLOSED;
    st->stream_done = true;
  }
  return 0;
}

static int h3c_on_stop_sending(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                               void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3c_conn *hc = conn_user_data;
  (void)ngtcp2_conn_shutdown_stream_read(hc->conn, 0, stream_id, app_error_code);
  return 0;
}

static int h3c_on_reset_stream(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                               void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3c_conn *hc = conn_user_data;
  (void)ngtcp2_conn_shutdown_stream_write(hc->conn, 0, stream_id, app_error_code);
  return 0;
}

/* Creates the client nghttp3 connection with its control and QPACK streams. */
static int h3c_setup(h3c_conn *hc) {
  nghttp3_callbacks callbacks = {
      .acked_stream_data = h3c_on_acked_stream_data,
      .stream_close = h3c_on_stream_close,
      .recv_data = h3c_on_recv_data,
      .deferred_consume = h3c_on_deferred_consume,
      .recv_header = h3c_on_recv_header,
      .end_headers = h3c_on_end_headers,
      .recv_trailer = h3c_on_recv_header,
      .stop_sending = h3c_on_stop_sending,
      .end_stream = h3c_on_end_stream,
      .reset_stream = h3c_on_reset_stream,
  };
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  settings.qpack_max_dtable_capacity = 4096;
  settings.qpack_blocked_streams = 100;
  if (nghttp3_conn_client_new(&hc->h3, &callbacks, &settings, &h3_mem, hc) != 0)
    return -1;
  int64_t ctrl_id, qenc_id, qdec_id;
  if (ngtcp2_conn_open_uni_stream(hc->conn, &ctrl_id, nullptr) != 0 ||
      nghttp3_conn_bind_control_stream(hc->h3, ctrl_id) != 0 ||
      ngtcp2_conn_open_uni_stream(hc->conn, &qenc_id, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(hc->conn, &qdec_id, nullptr) != 0 ||
      nghttp3_conn_bind_qpack_streams(hc->h3, qenc_id, qdec_id) != 0)
    return -1;
  return 0;
}

/* ---- ngtcp2 callbacks ---- */

static int cb_recv_stream_data(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                               uint64_t offset, const uint8_t *data, size_t datalen,
                               void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  h3c_conn *hc = user_data;
  if (hc->h3 == nullptr)
    return 0;
  nghttp3_ssize n = nghttp3_conn_read_stream(hc->h3, stream_id, data, datalen,
                                             (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0);
  if (n < 0) {
    ngtcp2_ccerr_set_application_error(&hc->ccerr, nghttp3_err_infer_quic_app_error_code((int)n),
                                       nullptr, 0);
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  h3c_consume(hc, stream_id, (size_t)n);
  return 0;
}

static int cb_acked_stream_data_offset(ngtcp2_conn *conn, int64_t stream_id, uint64_t offset,
                                       uint64_t datalen, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  h3c_conn *hc = user_data;
  if (hc->h3 != nullptr && nghttp3_conn_add_ack_offset(hc->h3, stream_id, datalen) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

static int cb_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                           uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3c_conn *hc = user_data;
  if (hc->h3 == nullptr)
    return 0;
  if (!(flags & NGTCP2_STREAM_CLOSE_FLAG_APP_ERROR_CODE_SET))
    app_error_code = NGHTTP3_H3_NO_ERROR;
  int rv = nghttp3_conn_close_stream(hc->h3, stream_id, app_error_code);
  if (rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND) {
    ngtcp2_ccerr_set_application_error(&hc->ccerr, nghttp3_err_infer_quic_app_error_code(rv),
                                       nullptr, 0);
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

static int cb_stream_reset(ngtcp2_conn *conn, int64_t stream_id, uint64_t final_size,
                           uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)final_size;
  (void)app_error_code;
  (void)stream_user_data;
  h3c_conn *hc = user_data;
  if (hc->h3 != nullptr && nghttp3_conn_shutdown_stream_read(hc->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

static int cb_stream_stop_sending(ngtcp2_conn *conn, int64_t stream_id, uint64_t app_error_code,
                                  void *user_data, void *stream_user_data) {
  return cb_stream_reset(conn, stream_id, 0, app_error_code, user_data, stream_user_data);
}

static int cb_extend_max_stream_data(ngtcp2_conn *conn, int64_t stream_id, uint64_t max_data,
                                     void *user_data, void *stream_user_data) {
  (void)conn;
  (void)max_data;
  (void)stream_user_data;
  h3c_conn *hc = user_data;
  if (hc->h3 != nullptr && nghttp3_conn_unblock_stream(hc->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* handshake_completed: the connection is usable; set up HTTP/3. */
static int cb_handshake_completed(ngtcp2_conn *conn, void *user_data) {
  (void)conn;
  h3c_conn *hc = user_data;
  hc->handshake_done = true;
  return h3c_setup(hc) == 0 ? 0 : NGTCP2_ERR_CALLBACK_FAILURE;
}

static int cb_get_new_connection_id(ngtcp2_conn *conn, ngtcp2_cid *cid, uint8_t *token,
                                    size_t cidlen, void *user_data) {
  (void)conn;
  (void)user_data;
  uint8_t buf[NGTCP2_MAX_CIDLEN];
  h3c_random(buf, cidlen);
  ngtcp2_cid_init(cid, buf, cidlen);
  h3c_random(token, NGTCP2_STATELESS_RESET_TOKENLEN);
  return 0;
}

static ngtcp2_conn *get_conn(ngtcp2_crypto_conn_ref *ref) {
  return ((h3c_conn *)ref->user_data)->conn;
}

/* ---- I/O ---- */

static void on_h3c_timer(uv_timer_t *timer);

static void h3c_arm_timer(h3c_conn *hc) {
  ngtcp2_tstamp expiry = ngtcp2_conn_get_expiry(hc->conn);
  if (expiry == UINT64_MAX) {
    uv_timer_stop(&hc->timer);
    return;
  }
  ngtcp2_tstamp now = h3c_now();
  uint64_t ms = expiry > now ? (expiry - now + NGTCP2_MILLISECONDS - 1) / NGTCP2_MILLISECONDS : 0;
  uv_timer_start(&hc->timer, on_h3c_timer, ms, 0);
}

/* Sends one datagram on the connected socket; a full buffer drops it (QUIC retransmits). */
static void h3c_send(h3c_conn *hc, const uint8_t *data, size_t len) {
  uv_buf_t buf = uv_buf_init((char *)data, (unsigned int)len);
  (void)uv_udp_try_send(&hc->udp, &buf, 1, nullptr);
}

void h3c_flush(client_subchannel *s) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr || hc->conn == nullptr || s->closed)
    return;
  ngtcp2_tstamp ts = h3c_now();
  for (;;) {
    int64_t stream_id = -1;
    int fin = 0;
    nghttp3_vec vec[16];
    nghttp3_ssize sveccnt = 0;
    if (hc->h3 != nullptr && ngtcp2_conn_get_max_data_left(hc->conn)) {
      sveccnt = nghttp3_conn_writev_stream(hc->h3, &stream_id, &fin, vec, 16);
      if (sveccnt < 0) {
        h3c_fail(hc, GRPC_ERR_PROTOCOL);
        return;
      }
    }
    ngtcp2_ssize ndatalen = -1;
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    uint32_t flags = NGTCP2_WRITE_STREAM_FLAG_MORE | (fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0);
    ngtcp2_ssize nwrite =
        ngtcp2_conn_writev_stream(hc->conn, &ps.path, &pi, hc->sbuf, sizeof(hc->sbuf), &ndatalen,
                                  flags, stream_id, (const ngtcp2_vec *)vec, (size_t)sveccnt, ts);
    if (nwrite < 0) {
      switch (nwrite) {
      case NGTCP2_ERR_STREAM_DATA_BLOCKED:
        nghttp3_conn_block_stream(hc->h3, stream_id);
        continue;
      case NGTCP2_ERR_STREAM_SHUT_WR:
        nghttp3_conn_shutdown_stream_write(hc->h3, stream_id);
        continue;
      case NGTCP2_ERR_WRITE_MORE:
        if (nghttp3_conn_add_write_offset(hc->h3, stream_id, (size_t)ndatalen) != 0) {
          h3c_fail(hc, GRPC_ERR_PROTOCOL);
          return;
        }
        continue;
      default:
        h3c_fail(hc, GRPC_ERR_IO);
        return;
      }
    }
    if (ndatalen >= 0 && stream_id >= 0 &&
        nghttp3_conn_add_write_offset(hc->h3, stream_id, (size_t)ndatalen) != 0) {
      h3c_fail(hc, GRPC_ERR_PROTOCOL);
      return;
    }
    if (nwrite == 0)
      break;
    h3c_send(hc, hc->sbuf, (size_t)nwrite);
  }
  ngtcp2_conn_update_pkt_tx_time(hc->conn, ts);
  h3c_arm_timer(hc);
}

/* Connection timer: ngtcp2's expiry handling (retransmission, ACKs, idle timeout), then write. */
static void on_h3c_timer(uv_timer_t *timer) {
  h3c_conn *hc = timer->data;
  if (hc->conn == nullptr)
    return;
  if (ngtcp2_conn_handle_expiry(hc->conn, h3c_now()) != 0) {
    h3c_fail(hc, GRPC_ERR_CLOSED);
    return;
  }
  h3c_flush(hc->s);
}

static void on_h3c_alloc(uv_handle_t *handle, size_t suggested, uv_buf_t *buf) {
  (void)suggested;
  h3c_conn *hc = handle->data;
  *buf = uv_buf_init((char *)hc->rbuf, sizeof(hc->rbuf));
}

/* UDP read callback: feeds the datagram to ngtcp2 and writes whatever it answers. */
static void on_h3c_read(uv_udp_t *udp, ssize_t nread, const uv_buf_t *buf,
                        const struct sockaddr *addr, unsigned flags) {
  (void)flags;
  h3c_conn *hc = udp->data;
  if (nread <= 0 || hc->conn == nullptr)
    return;
  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)&hc->local, .addrlen = hc->locallen},
      .remote = {.addr = (ngtcp2_sockaddr *)(addr != nullptr ? (const void *)addr
                                                             : (const void *)&hc->remote),
                 .addrlen = hc->remotelen},
  };
  ngtcp2_pkt_info pi = {0};
  int rv = ngtcp2_conn_read_pkt(hc->conn, &path, &pi, (const uint8_t *)buf->base, (size_t)nread,
                                h3c_now());
  if (rv != 0) {
    h3c_fail(hc, rv == NGTCP2_ERR_CRYPTO ? GRPC_ERR_IO : GRPC_ERR_CLOSED);
    return;
  }
  h3c_flush(hc->s);
}

/* ---- transport hooks used by grpc_client.c ---- */

void *h3c_tls_ctx_new(const grpc_client_config *config) {
  grpc_quic_tls_client_config cfg = {
      .ca_file = config->tls.ca_file,
      .ca_pem = config->tls.ca_pem,
      .ca_pem_len = config->tls.ca_pem_len,
      .cert_file = config->tls.cert_file,
      .key_file = config->tls.key_file,
      .insecure_skip_verify = config->tls.insecure_skip_verify,
  };
  return quic_tls_client_ctx_new(&cfg);
}

void h3c_tls_ctx_free(void *ctx) {
  quic_tls_ctx_free(ctx);
}

/* Resolves s->host:s->port for UDP into *out (IP literals avoid getaddrinfo). */
static bool resolve(client_subchannel *s, struct sockaddr_storage *out, socklen_t *outlen) {
  if (uv_ip4_addr(s->host, s->port, (struct sockaddr_in *)out) == 0) {
    *outlen = sizeof(struct sockaddr_in);
    return true;
  }
  if (uv_ip6_addr(s->host, s->port, (struct sockaddr_in6 *)out) == 0) {
    *outlen = sizeof(struct sockaddr_in6);
    return true;
  }
  struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_DGRAM};
  struct addrinfo *res = nullptr;
  char port_str[16];
  snprintf(port_str, sizeof(port_str), "%u", s->port);
  if (getaddrinfo(s->host, port_str, &hints, &res) != 0 || res == nullptr)
    return false;
  memcpy(out, res->ai_addr, res->ai_addrlen);
  *outlen = (socklen_t)res->ai_addrlen;
  freeaddrinfo(res);
  return true;
}

grpc_err h3c_connect(client_subchannel *s) {
  grpc_client *c = s->parent;
  h3c_conn *hc = grpc_calloc(1, sizeof(*hc));
  if (hc == nullptr)
    return GRPC_ERR_NOMEM;
  hc->s = s;
  s->h3c = hc;
  ngtcp2_ccerr_default(&hc->ccerr);

  if (!resolve(s, &hc->remote, &hc->remotelen))
    return GRPC_ERR_IO;
  int family = hc->remote.ss_family;
  if (uv_udp_init_ex(&s->loop, &hc->udp, (unsigned)family) != 0)
    return GRPC_ERR_IO;
  hc->udp.data = hc;
  hc->open_handles++;
  if (uv_timer_init(&s->loop, &hc->timer) != 0)
    return GRPC_ERR_IO;
  hc->timer.data = hc;
  hc->open_handles++;
  if (uv_udp_connect(&hc->udp, (const struct sockaddr *)&hc->remote) != 0)
    return GRPC_ERR_IO;
  int namelen = sizeof(hc->local);
  if (uv_udp_getsockname(&hc->udp, (struct sockaddr *)&hc->local, &namelen) != 0)
    return GRPC_ERR_IO;
  hc->locallen = (socklen_t)namelen;
  if (uv_udp_recv_start(&hc->udp, on_h3c_alloc, on_h3c_read) != 0)
    return GRPC_ERR_IO;

  uint8_t buf[H3C_CID_LEN];
  ngtcp2_cid dcid, scid;
  h3c_random(buf, sizeof(buf));
  ngtcp2_cid_init(&dcid, buf, sizeof(buf));
  h3c_random(buf, sizeof(buf));
  ngtcp2_cid_init(&scid, buf, sizeof(buf));
  ngtcp2_path path = {
      .local = {.addr = (ngtcp2_sockaddr *)&hc->local, .addrlen = hc->locallen},
      .remote = {.addr = (ngtcp2_sockaddr *)&hc->remote, .addrlen = hc->remotelen},
  };
  ngtcp2_callbacks callbacks = {
      .client_initial = ngtcp2_crypto_client_initial_cb,
      .recv_crypto_data = ngtcp2_crypto_recv_crypto_data_cb,
      .handshake_completed = cb_handshake_completed,
      .encrypt = ngtcp2_crypto_encrypt_cb,
      .decrypt = ngtcp2_crypto_decrypt_cb,
      .hp_mask = ngtcp2_crypto_hp_mask_cb,
      .recv_stream_data = cb_recv_stream_data,
      .acked_stream_data_offset = cb_acked_stream_data_offset,
      .stream_close = cb_stream_close,
      .recv_retry = ngtcp2_crypto_recv_retry_cb,
      .rand = cb_rand,
      .get_new_connection_id = cb_get_new_connection_id,
      .update_key = ngtcp2_crypto_update_key_cb,
      .stream_reset = cb_stream_reset,
      .extend_max_stream_data = cb_extend_max_stream_data,
      .delete_crypto_aead_ctx = ngtcp2_crypto_delete_crypto_aead_ctx_cb,
      .delete_crypto_cipher_ctx = ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
      .get_path_challenge_data = ngtcp2_crypto_get_path_challenge_data_cb,
      .stream_stop_sending = cb_stream_stop_sending,
      .version_negotiation = ngtcp2_crypto_version_negotiation_cb,
  };
  ngtcp2_settings settings;
  ngtcp2_settings_default(&settings);
  settings.initial_ts = h3c_now();
  settings.max_tx_udp_payload_size = H3C_MAX_UDP_PAYLOAD;
  ngtcp2_transport_params params;
  ngtcp2_transport_params_default(&params);
  uint64_t window = (uint64_t)c->config.max_message_size + 5;
  params.initial_max_stream_data_bidi_local = window;
  params.initial_max_stream_data_bidi_remote = window;
  params.initial_max_stream_data_uni = window;
  params.initial_max_data = window * 4 > 16 * 1024 * 1024 ? window * 4 : 16 * 1024 * 1024;
  params.initial_max_streams_bidi = 0;
  params.initial_max_streams_uni = 3;
  params.max_idle_timeout = 30 * NGTCP2_SECONDS;
  if (ngtcp2_conn_client_new(&hc->conn, &dcid, &scid, &path, NGTCP2_PROTO_VER_V1, &callbacks,
                             &settings, &params, &quic_mem, hc) != 0)
    return GRPC_ERR_IO;

  const char *sni = c->config.tls.server_name != nullptr ? c->config.tls.server_name : s->host;
  hc->conn_ref = (ngtcp2_crypto_conn_ref){.get_conn = get_conn, .user_data = hc};
  hc->tls = quic_tls_session_new(c->quic_tls_ctx, &hc->conn_ref, false, sni);
  if (hc->tls == nullptr)
    return GRPC_ERR_IO;
  ngtcp2_conn_set_tls_native_handle(hc->conn, quic_tls_session_native(hc->tls));

  h3c_flush(s);
  while (!hc->handshake_done && !s->closed && !s->connect_deadline_hit)
    uv_run(&s->loop, UV_RUN_ONCE);
  if (!hc->handshake_done || s->closed)
    return GRPC_ERR_IO;
  s->connected = true;
  return GRPC_OK;
}

/* Close callback of the UDP socket and timer; frees the connection after the last one. */
static void on_h3c_handle_closed(uv_handle_t *handle) {
  h3c_conn *hc = handle->data;
  if (--hc->open_handles == 0)
    grpc_free(hc);
}

void h3c_disconnect(client_subchannel *s) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr)
    return;
  s->h3c = nullptr;
  if (hc->conn != nullptr && hc->handshake_done && !ngtcp2_conn_in_closing_period(hc->conn) &&
      !ngtcp2_conn_in_draining_period(hc->conn)) {
    ngtcp2_path_storage ps;
    ngtcp2_path_storage_zero(&ps);
    ngtcp2_pkt_info pi;
    ngtcp2_ssize n = ngtcp2_conn_write_connection_close(hc->conn, &ps.path, &pi, hc->sbuf,
                                                        sizeof(hc->sbuf), &hc->ccerr, h3c_now());
    if (n > 0)
      h3c_send(hc, hc->sbuf, (size_t)n);
  }
  /* a stream still open loses its transport now */
  if (s->active_stream != nullptr && s->active_stream->h3 != nullptr) {
    h3c_stream *hs = s->active_stream->h3;
    free_chunks(hs->head);
    hs->head = hs->tail = nullptr;
    hs->closed = true;
  }
  if (hc->h3 != nullptr)
    nghttp3_conn_del(hc->h3);
  hc->h3 = nullptr;
  if (hc->conn != nullptr)
    ngtcp2_conn_del(hc->conn);
  hc->conn = nullptr;
  quic_tls_session_free(hc->tls);
  hc->tls = nullptr;
  while (hc->orphans != nullptr)
    orphan_free(hc, hc->orphans);
  int handles = hc->open_handles;
  if (handles == 0) {
    grpc_free(hc);
    return;
  }
  /* the subchannel then runs its loop until these close callbacks have fired */
  if (hc->open_handles >= 1 && hc->udp.loop != nullptr)
    uv_close((uv_handle_t *)&hc->udp, on_h3c_handle_closed);
  if (hc->open_handles >= 2)
    uv_close((uv_handle_t *)&hc->timer, on_h3c_handle_closed);
}

int h3c_submit_request(client_subchannel *s, grpc_client_stream *st, const grpc_nv *nva,
                       size_t nvlen) {
  h3c_conn *hc = s->h3c;
  if (hc == nullptr || hc->h3 == nullptr)
    return -1;
  h3c_stream *hs = grpc_calloc(1, sizeof(*hs));
  if (hs == nullptr)
    return -1;
  int64_t stream_id;
  if (ngtcp2_conn_open_bidi_stream(hc->conn, &stream_id, nullptr) != 0) {
    grpc_free(hs);
    return -1;
  }
  hs->st = st;
  st->h3 = hs;
  st->stream_id = stream_id;
  if (nghttp3_conn_submit_request(hc->h3, stream_id, (const nghttp3_nv *)nva, nvlen,
                                  &h3c_data_reader, hs) != 0) {
    (void)ngtcp2_conn_shutdown_stream(hc->conn, 0, stream_id, NGHTTP3_H3_INTERNAL_ERROR);
    return -1;
  }
  return 0;
}

void h3c_resume(client_subchannel *s, grpc_client_stream *st) {
  if (s->h3c != nullptr && s->h3c->h3 != nullptr)
    (void)nghttp3_conn_resume_stream(s->h3c->h3, st->stream_id);
}

void h3c_reset(client_subchannel *s, grpc_client_stream *st) {
  if (s->h3c != nullptr && s->h3c->conn != nullptr)
    (void)ngtcp2_conn_shutdown_stream(s->h3c->conn, 0, st->stream_id, NGHTTP3_H3_REQUEST_CANCELLED);
}

void h3c_stream_free(grpc_client_stream *st) {
  h3c_stream *hs = st->h3;
  if (hs == nullptr)
    return;
  st->h3 = nullptr;
  hs->st = nullptr;
  h3c_conn *hc = st->sub != nullptr ? st->sub->h3c : nullptr;
  if (hc != nullptr && hc->h3 != nullptr && !hs->closed) {
    /* nghttp3 may still retransmit the request bytes: keep the state until it closes the stream */
    hs->orphan_prev = nullptr;
    hs->orphan_next = hc->orphans;
    if (hc->orphans != nullptr)
      hc->orphans->orphan_prev = hs;
    hc->orphans = hs;
    return;
  }
  free_chunks(hs->head);
  grpc_free(hs);
}
