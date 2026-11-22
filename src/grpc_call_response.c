/* grpc_call_response.c - the response side of a call: headers, messages, trailers and finishing
 *
 * Part of the call layer described in grpc_call.c.
 */

#include <stdarg.h>
#include <stdio.h>
#include "internal/grpc_platform.h"

#include "core.h"
#include "internal_call.h"
#include "internal_helpers.h"
#include "grpc_timeout.h"

/* ---- responses ---------------------------------------------------------------- */

/* Percent-encodes a grpc-message, as the gRPC HTTP/2 protocol requires: bytes outside
 * 0x20..0x7e and '%' itself become %XX. Returns a grpc_malloc()'d NUL-terminated string and
 * stores its length in *len, or nullptr on allocation failure. */
static char *encode_grpc_message(const char *message, size_t *len) {
  size_t n = 0;
  for (const u8 *p = (const u8 *)message; *p; p++)
    n += (*p < 0x20 || *p > 0x7e || *p == '%') ? 3 : 1;
  char *out = grpc_malloc(n + 1);
  if (out == nullptr)
    return nullptr;
  static const char hex[] = "0123456789ABCDEF";
  char *o = out;
  for (const u8 *p = (const u8 *)message; *p; p++) {
    if (*p < 0x20 || *p > 0x7e || *p == '%') {
      *o++ = '%';
      *o++ = hex[*p >> 4];
      *o++ = hex[*p & 0xf];
    } else {
      *o++ = (char)*p;
    }
  }
  *o = '\0';
  *len = n;
  return out;
}

/* Builds a grpc_nv from two string literals, so sizeof() gives their lengths at compile time. */
#define GRPC_NV(name, value) \
  ((grpc_nv){(uint8_t *)(name), (uint8_t *)(value), sizeof(name) - 1, sizeof(value) - 1, 0})

/* Submits the response HEADERS: :status, content-type (grpc, grpc-web or grpc-web-text),
 * grpc-accept-encoding with the codecs compiled in, grpc-encoding unless call->compression is
 * identity, the CORS headers for gRPC-Web, the application's metadata and `extra` (the trailers of
 * a Trailers-Only response, sent with `end_stream`). On success marks the headers as sent and
 * frees call->headers. Requires a live stream. */
static grpc_err submit_response_headers(grpc_call *call, bool end_stream, const grpc_nv *extra,
                                        size_t nb_extra) {
#if defined(GRPC_ENABLE_ZLIB)
#define ACCEPT_ZLIB "gzip,deflate,"
#else
#define ACCEPT_ZLIB ""
#endif
#if defined(GRPC_ENABLE_ZSTD) && defined(GRPC_ENABLE_SNAPPY)
  const char *accepted = ACCEPT_ZLIB "zstd,snappy,identity";
#elif defined(GRPC_ENABLE_ZSTD)
  const char *accepted = ACCEPT_ZLIB "zstd,identity";
#elif defined(GRPC_ENABLE_SNAPPY)
  const char *accepted = ACCEPT_ZLIB "snappy,identity";
#else
  const char *accepted = ACCEPT_ZLIB "identity";
#endif
#undef ACCEPT_ZLIB
  const char *ctype = call->is_grpc_web_text ? "application/grpc-web-text"
                      : call->is_grpc_web    ? "application/grpc-web"
                                             : "application/grpc";
  grpc_nv base[] = {
      GRPC_NV(":status", "200"),
      {(uint8_t *)"content-type", (uint8_t *)ctype, 12, strlen(ctype), 0},
      {(uint8_t *)"grpc-accept-encoding", (uint8_t *)accepted, sizeof("grpc-accept-encoding") - 1,
       0, 0},
      GRPC_NV("grpc-encoding", "gzip"),
      GRPC_NV("access-control-allow-origin", "*"),
      GRPC_NV("access-control-expose-headers",
              "grpc-status, grpc-message, grpc-status-details-bin"),
  };
  const char *encoding = "gzip";
  switch (call->compression) {
  case GRPC_COMPRESS_DEFLATE:
    encoding = "deflate";
    break;
  case GRPC_COMPRESS_ZSTD:
    encoding = "zstd";
    break;
  case GRPC_COMPRESS_SNAPPY:
    encoding = "snappy";
    break;
  default:
    break;
  }
  base[2].valuelen = strlen(accepted);
  base[3].value = (uint8_t *)encoding;
  base[3].valuelen = strlen(encoding);
  size_t nb_base = call->compression != GRPC_COMPRESS_IDENTITY ? 4 : 3;
  size_t nb_cors = call->is_grpc_web ? 2 : 0;
  /* HTTP/2 responses advertise the HTTP/3 listener when configured (http3.advertise_alt_svc) */
  const char *alt_svc = call->server->alt_svc;
  size_t nb_alt = alt_svc[0] != '\0' && call->session->ops->submit_headers != nullptr &&
                          strcmp(call->session->ops->name, "h2") == 0
                      ? 1
                      : 0;
  size_t total = nb_base + nb_cors + nb_alt + call->headers.len + nb_extra;
  grpc_nv *nva = grpc_malloc(total * sizeof(*nva));
  if (nva == nullptr)
    return GRPC_ERR_NOMEM;
  size_t off = 0;
  memcpy(nva + off, base, nb_base * sizeof(*nva));
  off += nb_base;
  if (nb_cors > 0) {
    memcpy(nva + off, &base[4], nb_cors * sizeof(*nva));
    off += nb_cors;
  }
  if (nb_alt > 0)
    nva[off++] = (grpc_nv){(uint8_t *)"alt-svc", (uint8_t *)alt_svc, 7, strlen(alt_svc), 0};
  if (call->headers.len > 0) {
    memcpy(nva + off, call->headers.nva, call->headers.len * sizeof(*nva));
    off += call->headers.len;
  }
  if (nb_extra > 0)
    memcpy(nva + off, extra, nb_extra * sizeof(*nva));

  int res = call->session->ops->submit_headers(call->session, call->stream, nva, total, end_stream);
  grpc_free(nva);
  if (res != 0)
    return (grpc_err)res;
  call->headers_sent = true;
  nv_list_free(&call->headers);
  return GRPC_OK;
}

#undef GRPC_NV

grpc_err call_finish_internal(grpc_call *call, grpc_status status, const char *message) {
  if (call->stream == nullptr)
    return GRPC_ERR_CLOSED;
  char grpc_status[16];
  int status_len = snprintf(grpc_status, sizeof(grpc_status), "%d", (int)status);
  if (message == nullptr && status != GRPC_STATUS_OK)
    message = grpc_status_str(status);

  size_t message_len = 0;
  char *encoded = nullptr;
  if (message != nullptr) {
    encoded = encode_grpc_message(message, &message_len);
    if (encoded == nullptr)
      return GRPC_ERR_NOMEM;
  }

  /* grpc-status, grpc-message and the application's trailers */
  size_t total = 1 + (encoded != nullptr) + call->trailers.len;
  grpc_nv *nva = grpc_malloc(total * sizeof(*nva));
  if (nva == nullptr) {
    grpc_free(encoded);
    return GRPC_ERR_NOMEM;
  }
  size_t n = 0;
  nva[n++] = (grpc_nv){(uint8_t *)"grpc-status", (uint8_t *)grpc_status, 11, (size_t)status_len, 0};
  if (encoded != nullptr)
    nva[n++] = (grpc_nv){(uint8_t *)"grpc-message", (uint8_t *)encoded, 12, message_len, 0};
  if (call->trailers.len > 0)
    memcpy(nva + n, call->trailers.nva, call->trailers.len * sizeof(*nva));
  n += call->trailers.len;

  grpc_err err = GRPC_OK;
  grpc_stream *stream = call->stream;
  if (call->is_grpc_web) {
    /* gRPC-Web encodes trailers inside a DATA frame with flag byte 0x80. */
    if (!call->headers_sent)
      err = submit_response_headers(call, false, nullptr, 0);
    if (err == GRPC_OK) {
      grpc_buf tbuf = {0};
      for (size_t i = 0; i < n; i++) {
        if (buf_append(&tbuf, nva[i].name, nva[i].namelen) || buf_append(&tbuf, ": ", 2) ||
            buf_append(&tbuf, nva[i].value, nva[i].valuelen) || buf_append(&tbuf, "\r\n", 2)) {
          err = GRPC_ERR_NOMEM;
          break;
        }
      }
      if (err == GRPC_OK) {
        grpc_message *tmsg = nullptr;
        if (call->is_grpc_web_text) {
          size_t raw_len = 5 + tbuf.len;
          uint8_t *raw = grpc_malloc(raw_len);
          if (raw == nullptr) {
            err = GRPC_ERR_NOMEM;
          } else {
            raw[0] = 0x80;
            uint32_t blen = htonl((uint32_t)tbuf.len);
            memcpy(raw + 1, &blen, 4);
            if (tbuf.len > 0)
              memcpy(raw + 5, tbuf.data, tbuf.len);
            size_t b64_len = 0;
            char *b64 = grpc_metadata_encode_bin(raw, raw_len, &b64_len);
            grpc_free(raw);
            if (b64 == nullptr)
              err = GRPC_ERR_NOMEM;
            else {
              tmsg = message_create(0xFE, b64, b64_len);
              grpc_free(b64);
              if (tmsg == nullptr)
                err = GRPC_ERR_NOMEM;
            }
          }
        } else {
          tmsg = message_create(0x80, tbuf.data, tbuf.len);
          if (tmsg == nullptr)
            err = GRPC_ERR_NOMEM;
        }
        if (tmsg != nullptr) {
          stream->send_eof_on_empty = true;
          if (msg_queue_push(&stream->lqueue_omessages, tmsg) != GRPC_OK) {
            grpc_message_free(tmsg);
            err = GRPC_ERR_NOMEM;
          } else if (!stream->is_data_active) {
            int rv = call->session->ops->start_data(call->session, stream);
            if (rv != 0)
              err = (grpc_err)rv;
            else
              stream->is_data_active = true;
          }
        }
      }
      buf_free(&tbuf);
    }
  } else if (!call->headers_sent) {
    /* nothing was sent: a single Trailers-Only HEADERS */
    err = submit_response_headers(call, true, nva, n);
  } else if (stream->is_data_active) {
    /* the trailers must not overtake the queued DATA. the data provider
     * submits them once it has written the last message */
    stream->pending_trailers = call_nva_dup(nva, n);
    stream->nb_pending_trailers = n;
    if (stream->pending_trailers == nullptr)
      err = GRPC_ERR_NOMEM;
  } else {
    int rv = call->session->ops->submit_trailers(call->session, stream, nva, n);
    if (rv != 0)
      err = (grpc_err)rv;
  }
  grpc_free(nva);
  grpc_free(encoded);
  if (err != GRPC_OK)
    return err;

  call->finished = true;
  stream->is_deadline_set = false;
  nv_list_free(&call->trailers);
  session_schedule_flush(call->session);
  call_record_done_stat(call, status);
  interceptors_run_on_done(call, status);
  return GRPC_OK;
}

/* Ends a call whose status could not be sent (out of memory building the trailers): resets the
 * stream with INTERNAL_ERROR, which needs no new allocation of ours, so the client learns at once
 * instead of waiting for its deadline, and marks the call finished. */
static void call_reset_unfinished(grpc_call *call, grpc_status status) {
  if (call->finished || call->closed || call->stream == nullptr)
    return;
  call->finished = true;
  call->stream->is_deadline_set = false;
  call->session->ops->reset_stream(call->session, call->stream, GRPC_RESET_INTERNAL_ERROR);
  session_schedule_flush(call->session);
  call_record_done_stat(call, status);
  interceptors_run_on_done(call, status);
}

/* Ends the call from inside the library (e.g. deadline exceeded, malformed request) with
 * `status`, then cancels it so the application learns the call is over. When the status cannot
 * be sent, the stream is reset instead. No-op if the call is already finished or closed. */
void call_abort(grpc_call *call, grpc_status status, const char *message) {
  if (call->finished || call->closed)
    return;
  if (call_finish_internal(call, status, message) != GRPC_OK)
    call_reset_unfinished(call, status);
  call_cancel(call, status);
}

/* Returns GRPC_OK if the application may still respond on the call, or the error explaining
 * why not. */
static grpc_err call_check(const grpc_call *call) {
  if (call == nullptr)
    return GRPC_ERR_INVALID;
  if (call->finished)
    return GRPC_ERR_FINISHED;
  if (call->closed)
    return GRPC_ERR_CLOSED;
  return GRPC_OK;
}

/* Only records the codec in auto_compression; grpc_call_send() applies it to each uncompressed
 * message. Silently falls back to identity when the client did not list the codec in
 * grpc-accept-encoding. */
GRPC_API grpc_err grpc_call_set_compression(grpc_call *call, grpc_compression_algorithm algorithm,
                                            int level) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  if (call->headers_sent)
    return GRPC_ERR_STATE;
  if (algorithm != GRPC_COMPRESS_IDENTITY && !call_codec_supported(algorithm))
    return GRPC_ERR_UNSUPPORTED;
  if (algorithm == GRPC_COMPRESS_IDENTITY || !call_client_accepts_encoding(call, algorithm)) {
    call->auto_compression = GRPC_COMPRESS_IDENTITY;
    call->auto_compression_level = GRPC_COMPRESSION_LEVEL_DEFAULT;
    return GRPC_OK;
  }
  call->auto_compression = algorithm;
  call->auto_compression_level = level;
  return GRPC_OK;
}

/* Buffers the metadata in call->headers; it goes out with the response HEADERS, sent by the first
 * message or by the status. Fails with GRPC_ERR_STATE once the headers are sent. */
GRPC_API grpc_err grpc_call_send_metadata(grpc_call *call, const grpc_metadata *metadata,
                                          size_t count) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  if (call->headers_sent)
    return GRPC_ERR_STATE;
  /* sent with the first message, or with the status */
  return nv_list_add_metadata(&call->headers, metadata, count);
}

/* Buffers the trailers in call->trailers until call_finish_internal() sends them. */
GRPC_API grpc_err grpc_call_add_trailers(grpc_call *call, const grpc_metadata *metadata,
                                         size_t count) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  return nv_list_add_metadata(&call->trailers, metadata, count);
}

/* Takes ownership of `message` and frees it on every error path. In order: enforces
 * max_send_queue_bytes (rejecting only when something is already queued, so one oversized
 * message can still go out), runs the on_send interceptors, applies auto compression, sends the
 * response HEADERS with the first message (fixing grpc-encoding for the rest of the call), wraps
 * the frame in base64 for grpc-web-text, then queues the message on the stream and starts the
 * nghttp2 data provider if it is idle. Sets was_backpressured so that the writable callback fires
 * once the queue drains. */
GRPC_API grpc_err grpc_call_send(grpc_call *call, grpc_message *message) {
  grpc_err err = message != nullptr ? call_check(call) : GRPC_ERR_INVALID;
  if (err != GRPC_OK) {
    grpc_message_free(message);
    return err;
  }

  grpc_stream *stream = call->stream;
  size_t max_q = call->server->config.max_send_queue_bytes;
  if (max_q > 0 && stream->queued_send_bytes > 0 &&
      stream->queued_send_bytes + message->len > max_q) {
    stream->was_backpressured = true;
    grpc_message_free(message);
    return GRPC_ERR_LIMIT;
  }

  if (!interceptors_run_on_send(call, message)) {
    grpc_message_free(message);
    return GRPC_ERR_FINISHED;
  }

  if (message->cflag == 0 && call->auto_compression != GRPC_COMPRESS_IDENTITY) {
    grpc_message *compressed =
        grpc_message_compress(message, call->auto_compression, call->auto_compression_level);
    if (compressed != nullptr) {
      grpc_message_free(message);
      message = compressed;
    }
  }

  if (!call->headers_sent) {
    call->compression = message->cflag ? message->compression : GRPC_COMPRESS_IDENTITY;
    err = submit_response_headers(call, false, nullptr, 0);
    if (err != GRPC_OK) {
      grpc_message_free(message);
      return err;
    }
  } else if (message->cflag != 0 && (call->compression == GRPC_COMPRESS_IDENTITY ||
                                     message->compression != call->compression)) {
    /* the encoding was announced with the first message */
    grpc_message_free(message);
    return GRPC_ERR_STATE;
  }

  if (call->is_grpc_web_text) {
    size_t raw_len = 5 + (size_t)message->len;
    uint8_t *raw = grpc_malloc(raw_len);
    if (raw == nullptr) {
      grpc_message_free(message);
      return GRPC_ERR_NOMEM;
    }
    raw[0] = message->cflag;
    uint32_t blen = htonl(message->len);
    memcpy(raw + 1, &blen, 4);
    if (message->len > 0)
      memcpy(raw + 5, message->data, message->len);
    size_t b64_len = 0;
    char *b64 = grpc_metadata_encode_bin(raw, raw_len, &b64_len);
    grpc_free(raw);
    grpc_message_free(message);
    if (b64 == nullptr)
      return GRPC_ERR_NOMEM;
    message = message_create(0xFE, b64, b64_len);
    grpc_free(b64);
    if (message == nullptr)
      return GRPC_ERR_NOMEM;
  }

  if (msg_queue_push(&stream->lqueue_omessages, message) != GRPC_OK) {
    grpc_message_free(message);
    return GRPC_ERR_NOMEM;
  }
  stream->queued_send_bytes += message->len;
  if (max_q > 0 && stream->queued_send_bytes >= max_q)
    stream->was_backpressured = true;
  atomic_fetch_add(&call->server->stat_messages_sent, 1);

  /* an active data provider picks up the new message by itself */
  if (!stream->is_data_active) {
    int rv = call->session->ops->start_data(call->session, stream);
    if (rv != 0)
      return (grpc_err)rv;
    stream->is_data_active = true;
  }
  session_schedule_flush(call->session);
  return GRPC_OK;
}

/* Copies `data` into a new message and sends it with grpc_call_send(). */
GRPC_API grpc_err grpc_call_send_data(grpc_call *call, const void *data, size_t len) {
  if (call == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  grpc_message *message = grpc_message_new(data, len);
  if (message == nullptr)
    return GRPC_ERR_NOMEM;
  return grpc_call_send(call, message);
}

/* grpc_call_send() followed by an OK grpc_call_finish(). */
GRPC_API grpc_err grpc_call_reply(grpc_call *call, grpc_message *message) {
  grpc_err err = grpc_call_send(call, message);
  if (err != GRPC_OK)
    return err;
  return grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

/* grpc_call_send_data() followed by an OK grpc_call_finish(). */
GRPC_API grpc_err grpc_call_reply_data(grpc_call *call, const void *data, size_t len) {
  grpc_err err = grpc_call_send_data(call, data, len);
  if (err != GRPC_OK)
    return err;
  return grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

/* Checks that the call can still respond, then ends it with call_finish_internal(). When the
 * status cannot be sent for lack of memory, the stream is reset instead (the client sees the call
 * fail at once rather than at its deadline) and GRPC_ERR_NOMEM returned; the call is over. */
GRPC_API grpc_err grpc_call_finish(grpc_call *call, grpc_status status, const char *message) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  err = call_finish_internal(call, status, message);
  if (err == GRPC_ERR_NOMEM)
    call_reset_unfinished(call, status);
  return err;
}
