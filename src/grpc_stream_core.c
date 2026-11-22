/* grpc_stream_core.c - transport-independent handling of gRPC request streams
 *
 * The HTTP/2 and HTTP/3 transports feed the same events into these functions: header fields, end
 * of headers, body chunks, end of body and stream close. Here the request is validated (content
 * type, :method, :scheme, te, route) and either answered directly (CORS preflight, 415, stream
 * reset, UNIMPLEMENTED) and then discarded, or turned into a grpc_call. Body bytes are buffered per
 * stream, split into length-prefixed gRPC messages (decompressed per grpc-encoding, within
 * max_message_size) and queued on the call; server_dispatch() delivers them after the loop
 * iteration. The response body is produced by stream_core_pull(), which each transport calls from
 * its own data callback. Nothing here writes to a socket; frames are queued through session->ops
 * and the next flush sends them.
 */
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "core.h"
#include "grpc_compression.h"
#include "grpc_status.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "grpc_stream_core.h"

/* Why parse_grpc_message() stopped: a gRPC status and its message (nullptr on success). */
typedef struct {
  grpc_status status;
  const char *message;
} parse_result;

/* Splits gRPC length-prefixed frames (1-byte compressed flag, 4-byte big-endian length,
 * payload) and appends the messages to *out. Stops at the first incomplete message; *consumed
 * is the number of bytes taken by complete ones, and the caller keeps the rest for the next
 * chunk. Every message, including the incomplete one, must declare at most max_size bytes, and a
 * compressed one (flag 1) must decompress with the stream's grpc-encoding to at most max_size
 * bytes. A unary request may hold only one message (max_count 1; 0 = unlimited, counted with
 * *count). Returns a non-OK status on the first violation, as the gRPC spec maps them:
 * RESOURCE_EXHAUSTED for size limits (and running out of memory), UNIMPLEMENTED for a codec this
 * build lacks, INTERNAL for a bad flag, a compressed message without grpc-encoding, corrupt
 * compressed data or a second unary message. The messages appended so far stay in *out. */
static parse_result parse_grpc_message(const grpc_stream *stream, const uint8_t *wire, size_t len,
                                       grpc_msg_queue *out, size_t max_size, size_t max_count,
                                       size_t *count, size_t *consumed) {
  *consumed = 0;
  size_t seek = 0;
  while (len - seek >= 5) {
    uint8_t cflag = wire[seek];
    uint32_t msg_len = ntohl(uread_u32(wire + seek + 1));
    /* checked as soon as the prefix is in, so an oversized message is never buffered */
    if (msg_len > max_size)
      return (parse_result){GRPC_STATUS_RESOURCE_EXHAUSTED,
                            "request message larger than the server accepts"};
    if (cflag > 1)
      return (parse_result){GRPC_STATUS_INTERNAL, "invalid compressed flag"};
    if (msg_len > len - seek - 5)
      break;
    if (max_count > 0 && *count >= max_count)
      return (parse_result){GRPC_STATUS_INTERNAL, "more than one request message"};
    const uint8_t *msg_data = wire + seek + 5;

    grpc_message *msg = nullptr;
    if (cflag == 1) {
      if (stream->grpc_encoding_unsupported)
        return (parse_result){GRPC_STATUS_UNIMPLEMENTED, "unsupported grpc-encoding"};
      if (stream->grpc_encoding == GRPC_COMPRESS_IDENTITY)
        return (parse_result){GRPC_STATUS_INTERNAL, "compressed message without grpc-encoding"};
      uint8_t *decomp = nullptr;
      size_t decomp_len = 0;
      grpc_err result = grpc_decompress_limited(stream->grpc_encoding, msg_data, msg_len, &decomp,
                                                &decomp_len, max_size);
      if (result == GRPC_ERR_LIMIT)
        return (parse_result){GRPC_STATUS_RESOURCE_EXHAUSTED,
                              "decompressed request exceeds server limit"};
      if (result == GRPC_ERR_NOMEM)
        return (parse_result){GRPC_STATUS_RESOURCE_EXHAUSTED, "server out of memory"};
      if (result != GRPC_OK)
        return (parse_result){GRPC_STATUS_INTERNAL, "failed to decompress request message"};
      msg = message_create(0, decomp, decomp_len);
      grpc_free(decomp);
    } else {
      msg = message_create(0, msg_data, msg_len);
    }
    if (msg == nullptr)
      return (parse_result){GRPC_STATUS_RESOURCE_EXHAUSTED, "server out of memory"};
    if (msg_queue_push(out, msg) != GRPC_OK) {
      grpc_message_free(msg);
      return (parse_result){GRPC_STATUS_RESOURCE_EXHAUSTED, "server out of memory"};
    }
    (*count)++;
    seek += 5 + (size_t)msg_len;
    *consumed = seek;
  }
  return (parse_result){GRPC_STATUS_OK, nullptr};
}

/* Returns true if the value_len bytes at value equal the NUL-terminated expected, exactly. */
static bool header_matches(const u8 *value, size_t value_len, const char *expected) {
  size_t expected_len = strlen(expected);
  return value_len == expected_len && memcmp(value, expected, expected_len) == 0;
}

/* Returns true if the value_len bytes at value start with the NUL-terminated prefix. */
/* True if the content-type `value` is the media type `type`, alone or with a "+codec" suffix or
 * parameters ("application/grpc+proto", "application/grpc; charset=..."), but not a longer name
 * such as "application/grpcXYZ" or "application/grpc-web" for "application/grpc". */
static bool header_starts_with(const u8 *value, size_t value_len, const char *type) {
  size_t type_len = strlen(type);
  if (value_len < type_len || memcmp(value, type, type_len) != 0)
    return false;
  return value_len == type_len || value[type_len] == '+' || value[type_len] == ';';
}

/* Brace initializer for a grpc_nv from two string literals, so sizeof() gives their lengths at
 * compile time (usable in static arrays). */

int stream_core_on_header(grpc_session *session, grpc_stream *stream, const uint8_t *name,
                          size_t namelen, const uint8_t *value, size_t valuelen, void *name_ref,
                          void *value_ref) {
  if (stream->nb_headers + 1 > CONFIG_STREAM_MAX_HEADERS) {
    LOG_ERROR(session->server, "stream max headers exceeded\n");
    return -1;
  }

  /* keep the transport's buffers alive instead of copying them */
  stream->ops->hdr_incref(name_ref);
  stream->ops->hdr_incref(value_ref);
  stream->header_refs[stream->nb_headers][0] = name_ref;
  stream->header_refs[stream->nb_headers][1] = value_ref;
  stream->headers[stream->nb_headers].name = (const char *)name;
  stream->headers[stream->nb_headers].value = (const char *)value;
  stream->headers[stream->nb_headers].namelen = namelen;
  stream->headers[stream->nb_headers].valuelen = valuelen;
  stream->nb_headers++;

  if (header_matches(name, namelen, ":method")) {
    stream->is_method_post = header_matches(value, valuelen, "POST");
    stream->is_method_options = header_matches(value, valuelen, "OPTIONS");
  } else if (header_matches(name, namelen, ":scheme")) {
    stream->is_scheme_http =
        header_matches(value, valuelen, "http") || header_matches(value, valuelen, "https");
  } else if (header_matches(name, namelen, ":path")) {
    stream->hpath = (const char *)value;
    stream->hpath_len = valuelen;
    stream->route = server_find_route(session->server, (const char *)value, valuelen);
  } else if (header_matches(name, namelen, "content-type")) {
    stream->hcontent_type = (const char *)value;
    if (header_starts_with(value, valuelen, "application/grpc-web-text")) {
      stream->is_content_grpc = true;
      stream->is_grpc_web = true;
      stream->is_grpc_web_text = true;
    } else if (header_starts_with(value, valuelen, "application/grpc-web")) {
      stream->is_content_grpc = true;
      stream->is_grpc_web = true;
    } else {
      stream->is_content_grpc = header_starts_with(value, valuelen, "application/grpc");
    }
  } else if (header_matches(name, namelen, "grpc-encoding")) {
    stream->hgrpc_encoding = (const char *)value;
    bool supported = true;
    if (header_matches(value, valuelen, "gzip") || header_matches(value, valuelen, "deflate")) {
      stream->grpc_encoding =
          header_matches(value, valuelen, "gzip") ? GRPC_COMPRESS_GZIP : GRPC_COMPRESS_DEFLATE;
#if !defined(GRPC_ENABLE_ZLIB)
      supported = false;
#endif
    } else if (header_matches(value, valuelen, "zstd")) {
      stream->grpc_encoding = GRPC_COMPRESS_ZSTD;
#if !defined(GRPC_ENABLE_ZSTD)
      supported = false;
#endif
    } else if (header_matches(value, valuelen, "snappy")) {
      stream->grpc_encoding = GRPC_COMPRESS_SNAPPY;
#if !defined(GRPC_ENABLE_SNAPPY)
      supported = false;
#endif
    } else {
      stream->grpc_encoding = GRPC_COMPRESS_IDENTITY;
      supported = header_matches(value, valuelen, "identity");
    }
    stream->grpc_encoding_unsupported = !supported;
  } else if (header_matches(name, namelen, "grpc-accept-encoding")) {
    stream->hgrpc_accept_encoding = (const char *)value;
  } else if (header_matches(name, namelen, "grpc-timeout")) {
    stream->hgrpc_timeout = (const char *)value;
  } else if (header_matches(name, namelen, "te")) {
    stream->is_te_trailers = header_matches(value, valuelen, "trailers");
  }
  return 0;
}

/* Submits a "trailers-only" gRPC response (one header block with :status 200, grpc-status and
 * grpc-message, ending the stream), used for requests that are not served. Submission errors are
 * ignored. */
static void submit_trailers_only(grpc_session *session, grpc_stream *stream, grpc_status status,
                                 const char *message) {
  char status_str[16];
  int len = snprintf(status_str, sizeof(status_str), "%d", (int)status);
  grpc_nv nva[] = {
      GRPC_NV(":status", "200"),
      GRPC_NV("content-type", "application/grpc"),
      grpc_nv_make("grpc-status", 11, status_str, (size_t)len),
      grpc_nv_make("grpc-message", 12, message, strlen(message)),
  };
  (void)session->ops->submit_headers(session, stream, nva, 4, true);
}

void stream_core_on_headers_end(grpc_session *session, grpc_stream *stream, bool end_stream) {
  grpc_server *server = session->server;
  /* a second header block would be request trailers, which gRPC clients do not send */
  if (stream->call != nullptr || stream->is_discarding)
    return;

  /* gRPC-Web CORS preflight */
  if (stream->is_method_options) {
    const grpc_nv cors_nva[] = {
        GRPC_NV(":status", "204"),
        GRPC_NV("access-control-allow-origin", "*"),
        GRPC_NV("access-control-allow-methods", "POST, OPTIONS"),
        GRPC_NV("access-control-allow-headers",
                "content-type, x-grpc-web, x-user-agent, grpc-timeout, authorization"),
        GRPC_NV("access-control-expose-headers",
                "grpc-status, grpc-message, grpc-status-details-bin"),
        GRPC_NV("access-control-max-age", "86400"),
    };
    (void)session->ops->submit_headers(session, stream, cors_nva,
                                       sizeof(cors_nva) / sizeof(cors_nva[0]), true);
    stream->is_discarding = true;
    return;
  }

  if (!stream->is_content_grpc) {
    const grpc_nv nva[] = {GRPC_NV(":status", "415")};
    (void)session->ops->submit_headers(session, stream, nva, 1, true);
    stream->is_discarding = true;
    return;
  }

  if (!(stream->is_method_post && stream->is_scheme_http &&
        (stream->is_te_trailers || stream->is_grpc_web))) {
    LOG_TRACE(server, "is_method_post %d && is_scheme_http %d && is_te_trailers %d. resetting\n",
              stream->is_method_post, stream->is_scheme_http, stream->is_te_trailers);
    session->ops->reset_stream(session, stream, GRPC_RESET_PROTOCOL_ERROR);
    stream->is_discarding = true;
    return;
  }

  if (stream->route == nullptr || stream->route->disabled) {
    if (server->fallback_route != nullptr && !server->fallback_route->disabled) {
      stream->route = server->fallback_route;
    } else {
      submit_trailers_only(session, stream, GRPC_STATUS_UNIMPLEMENTED, "unknown method");
      stream->is_discarding = true;
      return;
    }
  }

  grpc_call *call = call_new(session, stream);
  if (call == nullptr) {
    session->ops->reset_stream(session, stream, GRPC_RESET_INTERNAL_ERROR);
    stream->is_discarding = true;
    return;
  }
  stream->call = call;

  if (stream->hgrpc_timeout != nullptr) {
    int64_t ms = grpc_timeout_parse_ms(stream->hgrpc_timeout);
    if (ms > 0) {
      stream->is_deadline_set = true;
      stream->deadline_ms = grpc_time_now_ms() + (uint64_t)ms;
      call->deadline_ms = stream->deadline_ms;
      server_arm_deadline(server, stream->deadline_ms);
    }
  }

  /* a request stream without messages, e.g. an empty client stream */
  if (end_stream) {
    call->half_closed = true;
    call_queue(call);
  } else if (stream->route->kind == ROUTE_STREAM && stream->route->stream.on_start != nullptr) {
    call_queue(call);
  }
}

int stream_core_on_data(grpc_session *session, grpc_stream *stream, const uint8_t *data,
                        size_t len) {
  /* nothing to buffer for streams that are not served */
  if (stream->call == nullptr || stream->is_discarding)
    return 0;

  /* gRPC frames are parsed at the end of every DATA frame, and each message's declared length is
   * checked against max_message_size as soon as its prefix arrives, so the buffer never holds
   * more than one frame plus the unparsed rest of one allowed message. A gRPC-Web-Text body is
   * buffered whole as base64 and decoded at the end: bound it by one message of max_message_size
   * (4/3 larger, plus the padding of separately encoded chunks). */
  uint64_t max_msg = session->server->config.max_message_size;
  uint64_t web_text_bound = (5 + max_msg + 2) / 3 * 4 + 64;
  if (stream->is_grpc_web_text && (uint64_t)stream->recv_len + len > web_text_bound) {
    LOG_DEBUG(session->server, "%s: a request message exceeds max_message_size\n", session->peer);
    stream->is_discarding = true;
    stream->recv_len = 0;
    call_abort(stream->call, GRPC_STATUS_RESOURCE_EXHAUSTED,
               "request message larger than the server accepts");
    return 0;
  }

  if (stream->recv_start + stream->recv_len + len > stream->recv_cap && stream->recv_start > 0) {
    memmove(stream->recv_data, stream->recv_data + stream->recv_start, stream->recv_len);
    stream->recv_start = 0;
  }
  if (stream->recv_start + stream->recv_len + len > stream->recv_cap) {
    size_t needed = stream->recv_len + len;
    size_t cap = stream->recv_cap;
    while (cap < needed)
      cap *= 2;
    u8 *recv_data = grpc_malloc(cap);
    if (recv_data == nullptr)
      return -1;
    memcpy(recv_data, stream->recv_data + stream->recv_start, stream->recv_len);
    if (stream->recv_data != stream->recv_inline)
      grpc_free(stream->recv_data);
    stream->recv_data = recv_data;
    stream->recv_cap = cap;
    stream->recv_start = 0;
  }
  memcpy(stream->recv_data + stream->recv_start + stream->recv_len, data, len);
  stream->recv_len += len;
  return 0;
}

/* Parses complete messages from data straight onto the call's pending queue, counts them and
 * queues the call. Returns GRPC_OK, or an error after aborting the call with the status
 * parse_grpc_message() chose; the stream then discards the rest of the request (messages already
 * queued are dropped with the finished call). */
static grpc_err receive_messages(grpc_session *session, grpc_stream *stream, grpc_call *call,
                                 const uint8_t *data, size_t len, size_t *consumed) {
  size_t max_count = call->route != nullptr && call->route->kind == ROUTE_UNARY ? 1 : 0;
  size_t before = msg_queue_count(&call->pending);
  parse_result res = parse_grpc_message(stream, data, len, &call->pending,
                                        session->server->config.max_message_size, max_count,
                                        &stream->nb_recv_messages, consumed);
  size_t added = msg_queue_count(&call->pending) - before;
  if (added > 0)
    atomic_fetch_add(&session->server->stat_messages_received, added);
  if (res.status != GRPC_STATUS_OK) {
    LOG_DEBUG(session->server, "%s: request rejected: %s\n", session->peer, res.message);
    stream->is_discarding = true;
    stream->recv_len = 0;
    stream->recv_start = 0;
    call_abort(call, res.status, res.message);
    return GRPC_ERR_INVALID;
  }
  if (added > 0)
    call_queue(call);
  return GRPC_OK;
}

void stream_core_on_data_end(grpc_session *session, grpc_stream *stream, bool end_stream) {
  if (stream->call == nullptr || stream->is_discarding)
    return;
  grpc_call *call = stream->call;

  if (stream->is_grpc_web_text) {
    /* base64 can only be decoded as a whole: wait for the end of the body */
    if (!end_stream)
      return;
    size_t dec_len = 0;
    uint8_t *dec = nullptr;
    if (stream->recv_len > 0) {
      /* the body may be several separately padded chunks */
      grpc_err derr = base64_decode((const char *)(stream->recv_data + stream->recv_start),
                                    stream->recv_len, true, &dec, &dec_len);
      if (derr != GRPC_OK) {
        stream->is_discarding = true;
        stream->recv_len = 0;
        if (derr == GRPC_ERR_INVALID)
          call_abort(call, GRPC_STATUS_INVALID_ARGUMENT, "invalid grpc-web-text base64");
        else
          call_abort(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "server out of memory");
        return;
      }
    }
    size_t consumed = 0;
    grpc_err err =
        dec != nullptr ? receive_messages(session, stream, call, dec, dec_len, &consumed) : GRPC_OK;
    grpc_free(dec);
    stream->recv_len = 0;
    stream->recv_start = 0;
    if (err != GRPC_OK)
      return;
    if (consumed < dec_len) {
      call_abort(call, GRPC_STATUS_DATA_LOSS, "truncated message");
    } else {
      call->half_closed = true;
      call_queue(call);
    }
    return;
  }

  size_t consumed = 0;
  if (receive_messages(session, stream, call, stream->recv_data + stream->recv_start,
                       stream->recv_len, &consumed) != GRPC_OK)
    return;
  if (consumed > 0) {
    assert(consumed <= stream->recv_len);
    stream->recv_start += consumed;
    stream->recv_len -= consumed;
    if (stream->recv_len == 0)
      stream->recv_start = 0;
  }

  if (end_stream) {
    if (stream->recv_len > 0) {
      /* the stream ended in the middle of a message */
      call_abort(call, GRPC_STATUS_DATA_LOSS, "truncated message");
    } else {
      call->half_closed = true;
      call_queue(call);
    }
  }
}

void stream_core_on_close(grpc_session *session, grpc_stream *stream) {
  /* unless it was finished, the client reset or abandoned it */
  if (stream->call != nullptr)
    call_detach(stream->call, GRPC_STATUS_CANCELLED);
  stream_unlink(session, stream);
  stream_free(stream);
}

size_t stream_core_pull(grpc_stream *stream, uint8_t *buf, size_t cap, bool *eof,
                        bool *end_stream) {
  grpc_msg_queue *lmessages = &stream->lqueue_omessages;
  grpc_message *msg;
  *eof = false;
  *end_stream = false;

  /* each message goes out as a 5 byte prefix followed by its data. trunc_seek tracks how much of
   * the front message was already written, so a message (and even its prefix) may span several
   * frames. */
  size_t buf_seek = 0;
  while (buf_seek < cap && (msg = msg_queue_peek(lmessages)) != nullptr) {
    size_t seek = stream->trunc_seek;
    bool raw_frame = (msg->cflag == 0xFE);
    size_t prefix_len = raw_frame ? 0 : 5;
    size_t total = prefix_len + (size_t)msg->len;

    if (!raw_frame && seek < 5) {
      u8 prefix[5];
      prefix[0] = msg->cflag;
      uint32_t len = htonl(msg->len);
      memcpy(prefix + 1, &len, 4);
      size_t n = 5 - seek < cap - buf_seek ? 5 - seek : cap - buf_seek;
      memcpy(buf + buf_seek, prefix + seek, n);
      buf_seek += n;
      seek += n;
    }
    if (seek >= prefix_len) {
      size_t n = total - seek < cap - buf_seek ? total - seek : cap - buf_seek;
      memcpy(buf + buf_seek, msg->data + (seek - prefix_len), n);
      buf_seek += n;
      seek += n;
    }

    if (seek == total) {
      (void)msg_queue_pop(lmessages);
      if (stream->queued_send_bytes >= msg->len)
        stream->queued_send_bytes -= msg->len;
      else
        stream->queued_send_bytes = 0;
      grpc_message_free(msg);
      seek = 0;
      if (stream->was_backpressured && stream->call != nullptr &&
          grpc_call_is_writable(stream->call)) {
        stream->was_backpressured = false;
        if (stream->call->writable_fn != nullptr)
          stream->call->writable_fn(stream->call, stream->call->writable_userdata);
      }
    }
    stream->trunc_seek = seek;
  }

  if (msg_queue_is_empty(lmessages)) {
    *eof = true;
    /* with send_eof_on_empty the trailers were already sent in the body (gRPC-Web); otherwise the
     * stream stays open for them */
    *end_stream = stream->send_eof_on_empty;
    stream->is_data_active = false;
  }
  return buf_seek;
}

int stream_core_flush_pending_trailers(grpc_session *session, grpc_stream *stream) {
  if (stream->pending_trailers == nullptr)
    return 0;
  int rv = session->ops->submit_trailers(session, stream, stream->pending_trailers,
                                         stream->nb_pending_trailers);
  grpc_free(stream->pending_trailers);
  stream->pending_trailers = nullptr;
  stream->nb_pending_trailers = 0;
  return rv;
}
