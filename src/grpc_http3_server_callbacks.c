/* grpc_http3_server_callbacks.c - per-stream state and the nghttp3 / ngtcp2 callbacks of server connections
 *
 * Part of the HTTP/3 server transport described in grpc_http3_server.c.
 */

#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "core.h"
#include "grpc_stream_core.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "internal_http3.h"
#include "internal_http3_server.h"
#include "internal_quic_tls.h"

/* ---- per-stream state ---- */

void h3_stream_free(grpc_stream *stream) {
  h3_stream *hs = stream->transport;
  if (hs == nullptr)
    return;
  while (hs->head != nullptr) {
    h3_chunk *c = hs->head;
    hs->head = c->next;
    grpc_free(c);
  }
  grpc_free(hs->headers);
  grpc_free(hs);
  stream->transport = nullptr;
}

/* ---- nghttp3 callbacks ---- */

/* nghttp3 data reader for response bodies. Pulls queued messages into a new chunk (kept until
 * acknowledged) and returns it as one vector. When the queue is empty: ends the stream for
 * gRPC-Web, submits parked trailers (EOF without END_STREAM), or blocks until start_data()
 * resumes the stream. */
static nghttp3_ssize h3_read_data(nghttp3_conn *conn, int64_t stream_id, nghttp3_vec *vec,
                                  size_t veccnt, uint32_t *pflags, void *conn_user_data,
                                  void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  h3_stream *hs = stream != nullptr ? stream->transport : nullptr;
  if (hs == nullptr || veccnt == 0)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  if (hs->body_done) {
    *pflags = NGHTTP3_DATA_FLAG_EOF | NGHTTP3_DATA_FLAG_NO_END_STREAM;
    return 0;
  }

  h3_chunk *chunk = grpc_malloc(sizeof(*chunk));
  if (chunk == nullptr)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  bool eof = false, end_stream = false;
  chunk->len = stream_core_pull(stream, chunk->data, sizeof(chunk->data), &eof, &end_stream);
  chunk->next = nullptr;
  nghttp3_ssize cnt = 0;
  if (chunk->len > 0) {
    if (hs->tail != nullptr)
      hs->tail->next = chunk;
    else
      hs->head = chunk;
    hs->tail = chunk;
    vec[0] = (nghttp3_vec){.base = chunk->data, .len = chunk->len};
    cnt = 1;
  } else {
    grpc_free(chunk);
  }

  if (eof) {
    if (end_stream) {
      hs->body_done = true;
      *pflags = NGHTTP3_DATA_FLAG_EOF;
    } else if (stream->pending_trailers != nullptr) {
      hs->body_done = true;
      *pflags = NGHTTP3_DATA_FLAG_EOF | NGHTTP3_DATA_FLAG_NO_END_STREAM;
      if (stream_core_flush_pending_trailers(qc->session, stream) != 0)
        return NGHTTP3_ERR_CALLBACK_FAILURE;
    } else if (cnt == 0) {
      return NGHTTP3_ERR_WOULDBLOCK;
    }
  }
  return cnt;
}

const nghttp3_data_reader h3_data_reader = {.read_data = h3_read_data};

/* nghttp3 acked_stream_data: QUIC acknowledged datalen more body bytes; frees chunks that are
 * fully acknowledged. */
static int h3_on_acked_stream_data(nghttp3_conn *conn, int64_t stream_id, uint64_t datalen,
                                   void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)conn_user_data;
  grpc_stream *stream = stream_user_data;
  h3_stream *hs = stream != nullptr ? stream->transport : nullptr;
  while (hs != nullptr && datalen > 0 && hs->head != nullptr) {
    size_t left = hs->head->len - hs->head_acked;
    size_t take = datalen < left ? (size_t)datalen : left;
    hs->head_acked += take;
    datalen -= take;
    if (hs->head_acked == hs->head->len) {
      h3_chunk *c = hs->head;
      hs->head = c->next;
      if (hs->head == nullptr)
        hs->tail = nullptr;
      hs->head_acked = 0;
      grpc_free(c);
    }
  }
  return 0;
}

/* nghttp3 begin_headers: a request stream starts; allocates its grpc_stream. */
static int h3_on_begin_headers(nghttp3_conn *conn, int64_t stream_id, void *conn_user_data,
                               void *stream_user_data) {
  grpc_quic_conn *qc = conn_user_data;
  if (stream_user_data != nullptr)
    return 0;
  grpc_stream *stream = stream_new(stream_id, &grpc_h3_transport_ops);
  if (stream == nullptr)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  stream->transport = grpc_calloc(1, sizeof(h3_stream));
  if (stream->transport == nullptr) {
    stream_free(stream);
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  }
  stream_link(qc->session, stream);
  nghttp3_conn_set_stream_user_data(conn, stream_id, stream);
  return 0;
}

/* nghttp3 recv_header: hands the field and its rcbufs to the stream core. Too many fields reset
 * just this stream. */
static int h3_on_recv_header(nghttp3_conn *conn, int64_t stream_id, int32_t token,
                             nghttp3_rcbuf *name, nghttp3_rcbuf *value, uint8_t flags,
                             void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  (void)token;
  (void)flags;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream == nullptr || stream->is_discarding)
    return 0;
  nghttp3_vec n = nghttp3_rcbuf_get_buf(name);
  nghttp3_vec v = nghttp3_rcbuf_get_buf(value);
  if (stream_core_on_header(qc->session, stream, n.base, n.len, v.base, v.len, name, value) != 0) {
    stream->is_discarding = true;
    grpc_h3_transport_ops.reset_stream(qc->session, stream, GRPC_RESET_INTERNAL_ERROR);
  }
  return 0;
}

/* nghttp3 end_headers: the request header block is complete. */
static int h3_on_end_headers(nghttp3_conn *conn, int64_t stream_id, int fin, void *conn_user_data,
                             void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream != nullptr)
    stream_core_on_headers_end(qc->session, stream, fin != 0);
  return 0;
}

/* Returns credit for consumed body bytes to the peer (stream and connection flow control). */
static void h3_consume(grpc_quic_conn *qc, int64_t stream_id, size_t n) {
  ngtcp2_conn_extend_max_stream_offset(qc->conn, stream_id, n);
  ngtcp2_conn_extend_max_offset(qc->conn, n);
}

/* nghttp3 recv_data: a chunk of request body; buffered and parsed by the stream core. */
static int h3_on_recv_data(nghttp3_conn *conn, int64_t stream_id, const uint8_t *data,
                           size_t datalen, void *conn_user_data, void *stream_user_data) {
  (void)conn;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  h3_consume(qc, stream_id, datalen);
  if (stream == nullptr)
    return 0;
  if (stream_core_on_data(qc->session, stream, data, datalen) != 0)
    return NGHTTP3_ERR_CALLBACK_FAILURE;
  stream_core_on_data_end(qc->session, stream, false);
  return 0;
}

/* nghttp3 deferred_consume: framing bytes nghttp3 consumed later (e.g. after QPACK unblocking). */
static int h3_on_deferred_consume(nghttp3_conn *conn, int64_t stream_id, size_t consumed,
                                  void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  h3_consume(conn_user_data, stream_id, consumed);
  return 0;
}

/* nghttp3 end_stream: the client finished its request body. */
static int h3_on_end_stream(nghttp3_conn *conn, int64_t stream_id, void *conn_user_data,
                            void *stream_user_data) {
  (void)conn;
  (void)stream_id;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream != nullptr)
    stream_core_on_data_end(qc->session, stream, true);
  return 0;
}

/* nghttp3 stream_close: the request stream is gone; detaches its call and frees it. (The stream
 * credit is returned in cb_stream_close(), which also sees streams nghttp3 never knew.) */
static int h3_on_stream_close(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)app_error_code;
  grpc_quic_conn *qc = conn_user_data;
  grpc_stream *stream = stream_user_data;
  if (stream != nullptr) {
    h3_stream_free(stream);
    stream_core_on_close(qc->session, stream);
  }
  return 0;
}

/* nghttp3 stop_sending: the peer asked us to stop sending on the stream. */
static int h3_on_stop_sending(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_quic_conn *qc = conn_user_data;
  (void)ngtcp2_conn_shutdown_stream_read(qc->conn, 0, stream_id, app_error_code);
  return 0;
}

/* nghttp3 reset_stream: nghttp3 wants the stream's sending side reset. */
static int h3_on_reset_stream(nghttp3_conn *conn, int64_t stream_id, uint64_t app_error_code,
                              void *conn_user_data, void *stream_user_data) {
  (void)conn;
  (void)stream_user_data;
  grpc_quic_conn *qc = conn_user_data;
  (void)ngtcp2_conn_shutdown_stream_write(qc->conn, 0, stream_id, app_error_code);
  return 0;
}

/* Creates the server's nghttp3 connection and its control and QPACK streams. Called when the
 * 1-RTT keys are installed, so the HTTP/3 layer exists before any request stream arrives. */
static int h3_setup(grpc_quic_conn *qc) {
  nghttp3_callbacks callbacks = {
      .acked_stream_data = h3_on_acked_stream_data,
      .stream_close = h3_on_stream_close,
      .recv_data = h3_on_recv_data,
      .deferred_consume = h3_on_deferred_consume,
      .begin_headers = h3_on_begin_headers,
      .recv_header = h3_on_recv_header,
      .end_headers = h3_on_end_headers,
      .stop_sending = h3_on_stop_sending,
      .end_stream = h3_on_end_stream,
      .reset_stream = h3_on_reset_stream,
  };
  nghttp3_settings settings;
  nghttp3_settings_default(&settings);
  settings.qpack_max_dtable_capacity = 4096;
  settings.qpack_blocked_streams = 100;
  if (nghttp3_conn_server_new(&qc->h3, &callbacks, &settings, &grpc_h3_mem, qc) != 0)
    return -1;

  const ngtcp2_transport_params *params = ngtcp2_conn_get_local_transport_params(qc->conn);
  nghttp3_conn_set_max_client_streams_bidi(qc->h3, params->initial_max_streams_bidi);

  int64_t ctrl_id, qenc_id, qdec_id;
  if (ngtcp2_conn_open_uni_stream(qc->conn, &ctrl_id, nullptr) != 0 ||
      nghttp3_conn_bind_control_stream(qc->h3, ctrl_id) != 0 ||
      ngtcp2_conn_open_uni_stream(qc->conn, &qenc_id, nullptr) != 0 ||
      ngtcp2_conn_open_uni_stream(qc->conn, &qdec_id, nullptr) != 0 ||
      nghttp3_conn_bind_qpack_streams(qc->h3, qenc_id, qdec_id) != 0)
    return -1;
  return 0;
}

/* ---- ngtcp2 callbacks ---- */

/* ngtcp2 recv_stream_data: feeds stream bytes to nghttp3 and returns credit for the framing it
 * consumed (body bytes are credited in h3_on_recv_data). */
static int cb_recv_stream_data(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                               uint64_t offset, const uint8_t *data, size_t datalen,
                               void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 == nullptr)
    return 0;
  nghttp3_ssize nconsumed = nghttp3_conn_read_stream(qc->h3, stream_id, data, datalen,
                                                     (flags & NGTCP2_STREAM_DATA_FLAG_FIN) != 0);
  if (nconsumed < 0) {
    ngtcp2_ccerr_set_application_error(
        &qc->ccerr, nghttp3_err_infer_quic_app_error_code((int)nconsumed), nullptr, 0);
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  h3_consume(qc, stream_id, (size_t)nconsumed);
  return 0;
}

/* ngtcp2 acked_stream_data_offset: tells nghttp3 how much of a stream was acknowledged. */
static int cb_acked_stream_data_offset(ngtcp2_conn *conn, int64_t stream_id, uint64_t offset,
                                       uint64_t datalen, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)offset;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr && nghttp3_conn_add_ack_offset(qc->h3, stream_id, datalen) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* ngtcp2 stream_close: a finished client request stream makes room for one more (also one that
 * was reset before nghttp3 saw any of it, or the credit would be lost for good), then the close
 * is forwarded to nghttp3, which calls h3_on_stream_close(). */
static int cb_stream_close(ngtcp2_conn *conn, uint32_t flags, int64_t stream_id,
                           uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (ngtcp2_is_bidi_stream(stream_id) && !ngtcp2_conn_is_local_stream(conn, stream_id))
    ngtcp2_conn_extend_max_streams_bidi(conn, 1);
  if (qc->h3 == nullptr)
    return 0;
  if (!(flags & NGTCP2_STREAM_CLOSE_FLAG_APP_ERROR_CODE_SET))
    app_error_code = NGHTTP3_H3_NO_ERROR;
  int rv = nghttp3_conn_close_stream(qc->h3, stream_id, app_error_code);
  if (rv != 0 && rv != NGHTTP3_ERR_STREAM_NOT_FOUND) {
    ngtcp2_ccerr_set_application_error(&qc->ccerr, nghttp3_err_infer_quic_app_error_code(rv),
                                       nullptr, 0);
    return NGTCP2_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

/* ngtcp2 stream_reset / stream_stop_sending: the peer abandoned a direction of the stream. */
static int cb_stream_reset(ngtcp2_conn *conn, int64_t stream_id, uint64_t final_size,
                           uint64_t app_error_code, void *user_data, void *stream_user_data) {
  (void)conn;
  (void)final_size;
  (void)app_error_code;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr && nghttp3_conn_shutdown_stream_read(qc->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}
static int cb_stream_stop_sending(ngtcp2_conn *conn, int64_t stream_id, uint64_t app_error_code,
                                  void *user_data, void *stream_user_data) {
  return cb_stream_reset(conn, stream_id, 0, app_error_code, user_data, stream_user_data);
}

/* ngtcp2 extend_max_remote_streams_bidi: the client may open more request streams. */
static int cb_extend_max_remote_streams_bidi(ngtcp2_conn *conn, uint64_t max_streams,
                                             void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr)
    nghttp3_conn_set_max_client_streams_bidi(qc->h3, max_streams);
  return 0;
}

/* ngtcp2 extend_max_stream_data: the peer opened its window; a blocked stream may write. */
static int cb_extend_max_stream_data(ngtcp2_conn *conn, int64_t stream_id, uint64_t max_data,
                                     void *user_data, void *stream_user_data) {
  (void)conn;
  (void)max_data;
  (void)stream_user_data;
  grpc_quic_conn *qc = user_data;
  if (qc->h3 != nullptr && nghttp3_conn_unblock_stream(qc->h3, stream_id) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* ngtcp2 recv_tx_key: once the 1-RTT keys exist, set up HTTP/3. */
static int cb_recv_tx_key(ngtcp2_conn *conn, ngtcp2_encryption_level level, void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  if (level != NGTCP2_ENCRYPTION_LEVEL_1RTT || qc->h3 != nullptr)
    return 0;
  return h3_setup(qc) == 0 ? 0 : NGTCP2_ERR_CALLBACK_FAILURE;
}

/* ngtcp2 handshake_completed: records the verified client certificate subject. */
static int cb_handshake_completed(ngtcp2_conn *conn, void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  qc->session->tls_handshake_done = true;
  if (qc->unvalidated) {
    qc->unvalidated = false;
    qc->h3s->nb_unvalidated--;
  }
  if (quic_tls_peer_subject(qc->tls, qc->session->peer_cert_subject,
                            sizeof(qc->session->peer_cert_subject)) != 0)
    qc->session->peer_cert_subject[0] = '\0';
  return 0;
}

/* ngtcp2 get_new_connection_id: issues a random connection ID with its stateless reset token and
 * routes it to this connection. */
static int cb_get_new_connection_id(ngtcp2_conn *conn, ngtcp2_cid *cid, uint8_t *token,
                                    size_t cidlen, void *user_data) {
  (void)conn;
  grpc_quic_conn *qc = user_data;
  uint8_t buf[NGTCP2_MAX_CIDLEN];
  h3_random(buf, cidlen);
  ngtcp2_cid_init(cid, buf, cidlen);
  if (ngtcp2_crypto_generate_stateless_reset_token(token, qc->h3s->secret, sizeof(qc->h3s->secret),
                                                   cid) != 0 ||
      h3_cid_add(qc, cid) != 0)
    return NGTCP2_ERR_CALLBACK_FAILURE;
  return 0;
}

/* ngtcp2 remove_connection_id: the ID is retired; stop routing it. */
static int cb_remove_connection_id(ngtcp2_conn *conn, const ngtcp2_cid *cid, void *user_data) {
  (void)conn;
  h3_cid_remove(user_data, cid);
  return 0;
}

/* ngtcp2 rand callback. */
static void cb_rand(uint8_t *dest, size_t destlen, const ngtcp2_rand_ctx *rand_ctx) {
  (void)rand_ctx;
  h3_random(dest, destlen);
}

void h3_server_set_quic_callbacks(ngtcp2_callbacks *cb) {
  cb->handshake_completed = cb_handshake_completed;
  cb->recv_stream_data = cb_recv_stream_data;
  cb->acked_stream_data_offset = cb_acked_stream_data_offset;
  cb->stream_close = cb_stream_close;
  cb->rand = cb_rand;
  cb->get_new_connection_id = cb_get_new_connection_id;
  cb->remove_connection_id = cb_remove_connection_id;
  cb->stream_reset = cb_stream_reset;
  cb->extend_max_remote_streams_bidi = cb_extend_max_remote_streams_bidi;
  cb->extend_max_stream_data = cb_extend_max_stream_data;
  cb->stream_stop_sending = cb_stream_stop_sending;
  cb->recv_tx_key = cb_recv_tx_key;
}
