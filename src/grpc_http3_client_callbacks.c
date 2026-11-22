/* grpc_http3_client_callbacks.c - the nghttp3 and ngtcp2 callbacks of client connections
 *
 * Part of the HTTP/3 client transport described in grpc_http3_client.c.
 */

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "core.h"
#include "internal_client.h"
#include "internal_helpers.h"
#include "internal_http3.h"
#include "internal_http3_client.h"
#include "internal_quic_tls.h"

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

const nghttp3_data_reader h3c_data_reader = {.read_data = h3c_read_data};

/* nghttp3 shutdown: the server sent GOAWAY (or the shutdown notice that precedes it). Requests
 * already open finish; the next call opens a new connection (h3c_usable()), since nghttp3 would
 * refuse a new request on this one. */
static int h3c_on_shutdown(nghttp3_conn *conn, int64_t id, void *conn_user_data) {
  (void)conn;
  (void)id;
  ((h3c_conn *)conn_user_data)->goaway_received = true;
  return 0;
}

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
  grpc_client_stream *st = h3c_active(conn_user_data, stream_id);
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
  grpc_client_stream *st = h3c_active(conn_user_data, stream_id);
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
  grpc_client_stream *st = h3c_active(hc, stream_id);
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
  grpc_client_stream *st = h3c_active(conn_user_data, stream_id);
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
      h3c_orphan_free(hc, hs);
  }
  grpc_client_stream *st = h3c_active(conn_user_data, stream_id);
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
      .shutdown = h3c_on_shutdown,
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
  if (nghttp3_conn_client_new(&hc->h3, &callbacks, &settings, &grpc_h3_mem, hc) != 0)
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

/* ngtcp2 rand callback. */
static void cb_rand(uint8_t *dest, size_t destlen, const ngtcp2_rand_ctx *rand_ctx) {
  (void)rand_ctx;
  h3c_random(dest, destlen);
}

void h3c_set_quic_callbacks(ngtcp2_callbacks *cb) {
  cb->handshake_completed = cb_handshake_completed;
  cb->recv_stream_data = cb_recv_stream_data;
  cb->acked_stream_data_offset = cb_acked_stream_data_offset;
  cb->stream_close = cb_stream_close;
  cb->rand = cb_rand;
  cb->get_new_connection_id = cb_get_new_connection_id;
  cb->stream_reset = cb_stream_reset;
  cb->extend_max_stream_data = cb_extend_max_stream_data;
  cb->stream_stop_sending = cb_stream_stop_sending;
}
