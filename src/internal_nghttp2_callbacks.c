/* internal_nghttp2_callbacks.c - nghttp2 server callbacks: from HTTP/2 frames to gRPC calls
 *
 * nghttp2 runs these callbacks synchronously from nghttp2_session_mem_recv() and
 * nghttp2_session_mem_send(), i.e. on the loop thread while internal_session_io.c feeds it received
 * bytes or serializes output. A grpc_stream is created when a request's HEADERS begins, kept as
 * nghttp2 stream user data and in the session's stream list, and freed when nghttp2 closes the
 * stream. Request headers are not copied: the stream keeps references on nghttp2's rcbufs.
 *
 * The callbacks are thin adapters: request handling (validation, routing, body buffering, message
 * parsing, the response body) is shared with HTTP/3 in grpc_stream_core.c, and this file also
 * provides grpc_h2_transport_ops, through which the call layer submits HTTP/2 frames. Callbacks
 * never write to the socket; they only submit frames, which the next flush sends.
 *
 * Return values follow nghttp2: 0 continues, NGHTTP2_ERR_CALLBACK_FAILURE (or any other nonzero
 * value) aborts nghttp2_session_mem_recv()/mem_send(), and the connection is then closed.
 */
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include "core.h"
#include "grpc_status.h"
#include "grpc_timeout.h"
#include "grpc_compression.h"
#include "grpc_tls.h"

#include "internal_helpers.h"
#include "internal_nghttp2_callbacks.h"
#include "grpc_stream_core.h"
#include <stddef.h>

/* The grpc_nv used by the call layer must be usable as an nghttp2_nv array as is. */
static_assert(sizeof(grpc_nv) == sizeof(nghttp2_nv) &&
                  offsetof(grpc_nv, name) == offsetof(nghttp2_nv, name) &&
                  offsetof(grpc_nv, value) == offsetof(nghttp2_nv, value) &&
                  offsetof(grpc_nv, namelen) == offsetof(nghttp2_nv, namelen) &&
                  offsetof(grpc_nv, valuelen) == offsetof(nghttp2_nv, valuelen) &&
                  offsetof(grpc_nv, flags) == offsetof(nghttp2_nv, flags),
              "grpc_nv must match nghttp2_nv");

/* ----------- HTTP/2 TRANSPORT OPS ------------------*/

/* nghttp2 data source read callback, installed by h2_start_data(). nghttp2 calls it from
 * nghttp2_session_mem_send() whenever the stream may send a DATA frame; it copies queued response
 * messages into buf with stream_core_pull(). When the queue runs empty the data source ends: with
 * gRPC-Web the stream ends with the data; otherwise the stream stays open and any pending trailers
 * are submitted as the final HEADERS. Returns the bytes written to buf, or
 * NGHTTP2_ERR_CALLBACK_FAILURE if the trailers cannot be submitted. */
static ssize_t h2_data_read(nghttp2_session *ngsession, i32 stream_id, u8 *buf, size_t buf_len,
                            u32 *data_flags, nghttp2_data_source *source, void *user_data) {
  (void)ngsession;
  (void)stream_id;
  grpc_session *ezsession = user_data;
  grpc_stream *ezstream = source->ptr;
  bool eof = false, end_stream = false;
  size_t n = stream_core_pull(ezstream, buf, buf_len, &eof, &end_stream);
  if (eof) {
    *data_flags = NGHTTP2_DATA_FLAG_EOF;
    if (!end_stream) {
      *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      if (stream_core_flush_pending_trailers(ezsession, ezstream) != 0)
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
  }
  return (ssize_t)n;
}

/* Maps an nghttp2 submit result to the transport-op convention: 0, or a grpc_err. */
static int h2_err(int rv) {
  return rv >= 0 ? 0 : rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
}

/* submit_headers op: one HEADERS frame, with END_STREAM when end_stream. */
static int h2_submit_headers(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                             size_t nvlen, bool end_stream) {
  return h2_err(nghttp2_submit_headers(
      session->ngsession, end_stream ? NGHTTP2_FLAG_END_STREAM : NGHTTP2_FLAG_NONE,
      (i32)stream->stream_id, nullptr, (const nghttp2_nv *)nva, nvlen, nullptr));
}

/* start_data op: submits a data provider over h2_data_read(). It ends with the stream only for
 * gRPC-Web; otherwise h2_data_read() keeps the stream open for the trailers. */
static int h2_start_data(grpc_session *session, grpc_stream *stream) {
  nghttp2_data_provider provider = {.source.ptr = stream, .read_callback = h2_data_read};
  return h2_err(nghttp2_submit_data(session->ngsession, NGHTTP2_FLAG_END_STREAM,
                                    (i32)stream->stream_id, &provider));
}

/* submit_trailers op: a trailing HEADERS frame ending the stream. */
static int h2_submit_trailers(grpc_session *session, grpc_stream *stream, const grpc_nv *nva,
                              size_t nvlen) {
  return h2_err(nghttp2_submit_trailer(session->ngsession, (i32)stream->stream_id,
                                       (const nghttp2_nv *)nva, nvlen));
}

/* reset_stream op: RST_STREAM with PROTOCOL_ERROR or INTERNAL_ERROR. */
static void h2_reset_stream(grpc_session *session, grpc_stream *stream, grpc_reset_reason reason) {
  if (nghttp2_submit_rst_stream(session->ngsession, NGHTTP2_FLAG_NONE, (i32)stream->stream_id,
                                reason == GRPC_RESET_PROTOCOL_ERROR
                                    ? NGHTTP2_PROTOCOL_ERROR
                                    : NGHTTP2_INTERNAL_ERROR) != 0) {
    /* not even the reset could be queued (out of memory): drop the connection at the next flush
     * (not now: the caller may be a handler still using this stream's call) */
    session->close_on_flush = true;
    session_schedule_flush(session);
  }
}

/* hdr_incref / hdr_decref ops over nghttp2_rcbuf. */
static void h2_hdr_incref(void *ref) {
  nghttp2_rcbuf_incref(ref);
}
static void h2_hdr_decref(void *ref) {
  nghttp2_rcbuf_decref(ref);
}

/* goaway op: GOAWAY NO_ERROR naming the last stream the server processed. */
static void h2_goaway(grpc_session *session) {
  int32_t last_id = nghttp2_session_get_last_proc_stream_id(session->ngsession);
  (void)nghttp2_submit_goaway(session->ngsession, NGHTTP2_FLAG_NONE, last_id, NGHTTP2_NO_ERROR,
                              nullptr, 0);
}

/* ping op: a keepalive PING with a fixed payload. */
static int h2_ping(grpc_session *session) {
  static const uint8_t ping_data[8] = {'e', 'z', 'g', 'r', 'p', 'c', 'k', 'a'};
  return nghttp2_submit_ping(session->ngsession, NGHTTP2_FLAG_NONE, ping_data) == 0 ? 0 : -1;
}

/* shutdown_notice op: GOAWAY with the largest stream id (nghttp2_submit_shutdown_notice()) and
 * a PING to time the final GOAWAY by. */
static void h2_shutdown_notice(grpc_session *session) {
  if (nghttp2_submit_shutdown_notice(session->ngsession) == 0 && h2_ping(session) == 0)
    session->ping_pending = true;
}

const grpc_transport_ops grpc_h2_transport_ops = {
    .name = "h2",
    .submit_headers = h2_submit_headers,
    .start_data = h2_start_data,
    .submit_trailers = h2_submit_trailers,
    .reset_stream = h2_reset_stream,
    .hdr_incref = h2_hdr_incref,
    .hdr_decref = h2_hdr_decref,
    .goaway = h2_goaway,
    .shutdown_notice = h2_shutdown_notice,
    .ping = h2_ping,
    /* flush and close: the TCP implementation (internal_session_io.c, internal_helpers.c) */
};

/* ----------- BEGIN NGHTTP2 CALLBACKS ------------------*/

/* nghttp2 begin-headers callback, invoked when a HEADERS frame opening a new stream arrives and
 * before its header fields. Allocates the grpc_stream, links it into the session and attaches it
 * as nghttp2 stream user data. Returns 0, or NGHTTP2_ERR_CALLBACK_FAILURE when out of memory
 * (fatal to the connection). */
static int on_begin_headers_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                     void *user_data) {
  grpc_session *ezsession = user_data;
  /* nghttp2 also calls this for a trailing HEADERS block: that belongs to the existing stream */
  if (frame->hd.type != NGHTTP2_HEADERS || frame->headers.cat != NGHTTP2_HCAT_REQUEST)
    return 0;
  grpc_stream *ezstream = stream_new(frame->hd.stream_id, &grpc_h2_transport_ops);
  if (ezstream == nullptr)
    return NGHTTP2_ERR_CALLBACK_FAILURE;

  stream_link(ezsession, ezstream);
  nghttp2_session_set_stream_user_data(session, frame->hd.stream_id, ezstream);

  LOG_TRACE(ezsession->server, "stream id %d\n", frame->hd.stream_id);
  return 0;
}

/* nghttp2 header callback, invoked once per header field of a HEADERS frame; hands the field and
 * its rcbufs to stream_core_on_header(). Returns 0, or NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE when
 * the stream has more than CONFIG_STREAM_MAX_HEADERS fields: nghttp2 then resets just that stream
 * (RST_STREAM INTERNAL_ERROR). Any other non-zero value would be fatal to the session and drop
 * every stream on the connection. */
static int on_header_callback2([[maybe_unused]] nghttp2_session *session,
                               const nghttp2_frame *frame, nghttp2_rcbuf *name_rcbuf,
                               nghttp2_rcbuf *value_rcbuf, [[maybe_unused]] u8 flags,
                               void *user_data) {
  grpc_session *ezsession = user_data;
  grpc_stream *ezstream =
      nghttp2_session_get_stream_user_data(ezsession->ngsession, frame->hd.stream_id);
  /* request trailers carry nothing gRPC uses: only the request header block is recorded */
  if (frame->hd.type != NGHTTP2_HEADERS || frame->headers.cat != NGHTTP2_HCAT_REQUEST ||
      ezstream == nullptr)
    return 0;
  nghttp2_vec vname = nghttp2_rcbuf_get_buf(name_rcbuf);
  nghttp2_vec vvalue = nghttp2_rcbuf_get_buf(value_rcbuf);
  if (stream_core_on_header(ezsession, ezstream, vname.base, vname.len, vvalue.base, vvalue.len,
                            name_rcbuf, value_rcbuf) != 0)
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  return 0;
}

/* Handles a complete HEADERS block: the request headers go to stream_core_on_headers_end(); a
 * trailing block (request trailers, always with END_STREAM) ends the request body like a final
 * empty DATA frame would. Always returns 0. */
static inline int on_frame_recv_headers(grpc_session *ezsession, const nghttp2_frame *frame) {
  grpc_stream *ezstream =
      nghttp2_session_get_stream_user_data(ezsession->ngsession, frame->hd.stream_id);
  if (ezstream == nullptr || !(frame->hd.flags & NGHTTP2_FLAG_END_HEADERS))
    return 0;
  if (frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
      stream_core_on_data_end(ezsession, ezstream, true);
    return 0;
  }
  stream_core_on_headers_end(ezsession, ezstream, frame->hd.flags & NGHTTP2_FLAG_END_STREAM);
  return 0;
}

/* Handles a received SETTINGS frame. nghttp2 applies and acknowledges SETTINGS itself, so ACKs
 * need nothing here. For other SETTINGS it submits RST_STREAM on stream 0, which nghttp2 rejects
 * because 0 is not a stream, so the branch has no effect. Always returns 0. */
static inline int on_frame_recv_settings(grpc_session *ezsession, const nghttp2_frame *frame) {
  if (frame->hd.stream_id != 0) {
    LOG_DEBUG(ezsession->server,
              "received a SETTINGS frame at stream_id 0. immediately returning\n");
    return 0;
  }
  if (frame->settings.hd.flags & NGHTTP2_FLAG_ACK && frame->settings.hd.length == 0) {
    /* Acknowledgement frames need no further action. */
  } else {
    nghttp2_submit_rst_stream(ezsession->ngsession, NGHTTP2_FLAG_NONE, frame->hd.stream_id, 1);
  }
  return 0;
}

/* Handles the end of a DATA frame on a served stream: stream_core_on_data_end() parses the
 * buffered messages (and half-closes the call at END_STREAM). Always returns 0: errors end the
 * call, not the connection. */
static inline int on_frame_recv_data(grpc_session *ezsession, const nghttp2_frame *frame) {
  grpc_stream *ezstream =
      nghttp2_session_get_stream_user_data(ezsession->ngsession, frame->hd.stream_id);
  if (ezstream == nullptr)
    return 0;
  stream_core_on_data_end(ezsession, ezstream, frame->hd.flags & NGHTTP2_FLAG_END_STREAM);
  return 0;
}

/* nghttp2 frame-sent callback, invoked after a frame has been serialized for sending. Logs the
 * frame (compiled in with GRPC_TRACE). When the response ended (END_STREAM on trailers, or on
 * the last DATA frame for gRPC-Web) while the client is still sending its request, resets the
 * stream with NO_ERROR, as gRPC servers do, so the client stops sending a request that will
 * never be read. Returns 0. */
static int on_frame_send_callback(nghttp2_session *session, const nghttp2_frame *frame,
                                  [[maybe_unused]] void *user_data) {
#ifdef GRPC_TRACE
  grpc_session *ezsession = user_data;
#endif
  if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) &&
      nghttp2_session_get_stream_remote_close(session, frame->hd.stream_id) == 0)
    (void)nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE, frame->hd.stream_id,
                                    NGHTTP2_NO_ERROR);

  switch (frame->hd.type) {
  case NGHTTP2_SETTINGS:
    LOG_TRACE(ezsession->server, "> FRAME[SETTINGS, sid=%d, ack=%d]\n", frame->hd.stream_id,
              !!(frame->hd.flags & NGHTTP2_FLAG_ACK));
    return 0;
  case NGHTTP2_HEADERS:
    LOG_TRACE(ezsession->server, "> FRAME[HEADERS, sid=%d, eos=%d]\n", frame->hd.stream_id,
              !!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM));
    for (size_t i = 0; i < frame->headers.nvlen; i++)
      LOG_TRACE(ezsession->server, "  %s: %s\n", frame->headers.nva[i].name,
                frame->headers.nva[i].value);
    return 0;
  case NGHTTP2_DATA:
    LOG_TRACE(ezsession->server, "> FRAME[DATA, sid=%d, eos=%d, len=%zu]\n", frame->hd.stream_id,
              !!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM), frame->hd.length);
    return 0;
  case NGHTTP2_WINDOW_UPDATE:
    LOG_TRACE(ezsession->server, "> FRAME[WINDOW_UPDATE, sid=%d, eos=%d, len=%zu, wsi=%d]\n",
              frame->hd.stream_id, !!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM), frame->hd.length,
              frame->window_update.window_size_increment);
    break;
  default:
    break;
  }
  return 0;
}

/* nghttp2 frame-received callback, invoked once a whole frame has been received (after its
 * header fields or data chunks). Dispatches SETTINGS, HEADERS and DATA to the handlers above;
 * RST_STREAM is handled by on_stream_close_callback(). A PING ACK clears the keepalive wait; a
 * client PING closer than min_ping_interval_ms (default 5 s) to the previous one is a strike,
 * and more than max_ping_strikes in a row (when enabled) earn a GOAWAY with ENHANCE_YOUR_CALM
 * "too_many_pings". Returns the handler's result, otherwise 0. */
static int on_frame_recv_callback([[maybe_unused]] nghttp2_session *session,
                                  const nghttp2_frame *frame, void *user_data) {
  grpc_session *ezsession = user_data;
  grpc_stream *ezstream =
      nghttp2_session_get_stream_user_data(ezsession->ngsession, frame->hd.stream_id);
  switch (frame->hd.type) {
  case NGHTTP2_SETTINGS:
    LOG_TRACE(ezsession->server, "< FRAME[SETTINGS, sid=%d, ack=%d]\n", frame->hd.stream_id,
              !!(frame->hd.flags & NGHTTP2_FLAG_ACK));
    return on_frame_recv_settings(ezsession, frame);
  case NGHTTP2_HEADERS:
    LOG_TRACE(ezsession->server, "< FRAME[HEADERS, sid=%d, eos=%d]\n", frame->hd.stream_id,
              !!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM));
    if (ezstream != nullptr) {
      for (size_t i = 0; i < ezstream->nb_headers; i++)
        LOG_TRACE(ezsession->server, "  %s: %s\n", ezstream->headers[i].name,
                  ezstream->headers[i].value);
    }
    return on_frame_recv_headers(ezsession, frame);
  case NGHTTP2_DATA:
    LOG_TRACE(ezsession->server, "< FRAME[DATA, sid=%d, eos=%d, len=%zu]\n", frame->hd.stream_id,
              !!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM), frame->hd.length);
    return on_frame_recv_data(ezsession, frame);
  case NGHTTP2_RST_STREAM:
    LOG_TRACE(ezsession->server, "< FRAME[RST_STREAM, sid=%d, error_code=%u]\n",
              frame->hd.stream_id, frame->rst_stream.error_code);
    /* on_stream_close_callback() cancels the call */
    return 0;
  case NGHTTP2_PING:
    if (frame->hd.flags & NGHTTP2_FLAG_ACK) {
      ezsession->ping_pending = false;
    } else if (ezsession->server->config.max_ping_strikes > 0) {
      uint64_t now = grpc_time_now_ms();
      uint32_t min_iv = ezsession->server->config.min_ping_interval_ms
                            ? ezsession->server->config.min_ping_interval_ms
                            : 5000;
      if (ezsession->last_client_ping_ms > 0 && now >= ezsession->last_client_ping_ms &&
          (now - ezsession->last_client_ping_ms) < min_iv) {
        ezsession->ping_strikes++;
        if (ezsession->ping_strikes > ezsession->server->config.max_ping_strikes) {
          int32_t last_id = nghttp2_session_get_last_proc_stream_id(ezsession->ngsession);
          static const uint8_t msg[] = "too_many_pings";
          (void)nghttp2_submit_goaway(ezsession->ngsession, NGHTTP2_FLAG_NONE, last_id,
                                      NGHTTP2_ENHANCE_YOUR_CALM, msg, sizeof(msg) - 1);
          ezsession->goaway_sent = true;
          ezsession->goaway_sent_ms = now;
          session_schedule_flush(ezsession);
        }
      } else {
        ezsession->ping_strikes = 0;
      }
      ezsession->last_client_ping_ms = now;
    }
    return 0;
  case NGHTTP2_WINDOW_UPDATE:
    LOG_TRACE(ezsession->server, "< FRAME[WINDOW_UPDATE, sid=%d, eos=%d, len=%zu, wsi=%d]\n",
              frame->hd.stream_id, !!(frame->hd.flags & NGHTTP2_FLAG_END_STREAM), frame->hd.length,
              frame->window_update.window_size_increment);
    break;
  default:
    break;
  }
  return 0;
}

/* nghttp2 data-chunk callback, invoked for each piece of DATA payload as it is received; buffers
 * it with stream_core_on_data(). Returns 0, or NGHTTP2_ERR_CALLBACK_FAILURE when out of memory
 * (fatal to the connection). */
static int on_data_chunk_recv_callback(nghttp2_session *session, [[maybe_unused]] u8 flags,
                                       i32 stream_id, const u8 *data, size_t len, void *user_data) {
  grpc_session *ezsession = user_data;
  grpc_stream *ezstream = nghttp2_session_get_stream_user_data(session, stream_id);
  if (ezstream == nullptr)
    return 0;
  if (stream_core_on_data(ezsession, ezstream, data, len) != 0)
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  LOG_TRACE(ezsession->server, "data chunk recv %zu bytes at stream %d\n", len, stream_id);
  return 0;
}

/* nghttp2 stream-close callback, invoked when a stream closes for any reason (completed, reset by
 * either side, or torn down with the session); stream_core_on_close() detaches the call and frees
 * the stream. Returns 0. */
static int on_stream_close_callback(nghttp2_session *session, i32 stream_id,
                                    [[maybe_unused]] u32 error_code, void *user_data) {
  grpc_session *ezsession = user_data;
  grpc_stream *ezstream = nghttp2_session_get_stream_user_data(session, stream_id);
  if (ezstream == nullptr)
    return 0;
  stream_core_on_close(ezsession, ezstream);
  return 0;
}

/* ----------- END NGHTTP2 CALLBACKS ------------------*/

/* Installs the callbacks above into the nghttp2_session_callbacks shared by all sessions of a
 * server; called once from grpc_server_new_ex(). */
void server_setup_ngcallbacks(nghttp2_session_callbacks *ngcallbacks) {
  /* no send/recv callbacks: the event loop moves the bytes with
   * nghttp2_session_mem_recv() and nghttp2_session_mem_send() */

  /* prepares and allocates memory for the incoming HEADERS frame */
  nghttp2_session_callbacks_set_on_begin_headers_callback(ngcallbacks, on_begin_headers_callback);
  /* we received that HEADERS frame. now here's the http2 fields */
  nghttp2_session_callbacks_set_on_header_callback2(ngcallbacks, on_header_callback2);
  /* we received a frame. do something about it */
  nghttp2_session_callbacks_set_on_frame_recv_callback(ngcallbacks, on_frame_recv_callback);
  nghttp2_session_callbacks_set_on_frame_send_callback(ngcallbacks, on_frame_send_callback);

  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(ngcallbacks,
                                                            on_data_chunk_recv_callback);
  nghttp2_session_callbacks_set_on_stream_close_callback(ngcallbacks, on_stream_close_callback);
}
