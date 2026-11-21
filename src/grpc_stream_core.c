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

/* Splits len bytes of the gRPC wire format (1-byte compressed flag, 4-byte big-endian length,
 * payload) into messages. Stops at the first incomplete message and returns the number of bytes
 * consumed by complete ones; the caller keeps the rest for the next chunk. *messages is allocated
 * on the first complete message and stays nullptr otherwise; the caller owns it. Compressed
 * messages (flag 1) are decompressed with encoding, limited to max_output bytes: exceeding it sets
 * *too_large and returns 0 (the caller frees any messages already parsed), while any other
 * decompression failure keeps the message as received, still flagged compressed. */
static size_t parse_grpc_message(void *data, size_t len, grpc_compression_algorithm encoding,
                                 grpc_messages **messages, size_t max_output, bool *too_large) {
  i8 *wire = data;
  *messages = nullptr;
  *too_large = false;
  /* quick check if there's any message to parse */
  if (len < 5)
    return 0;

  size_t seek = 0, lseek = 0;
  while (seek < len) {
    if (seek + 5 > len) {
      return lseek;
    }
    i8 cflag = wire[seek];
    seek += 1;

    uint32_t msg_len = ntohl(uread_u32(wire + seek));
    seek += 4;
    void *msg_data = wire + seek;

    if (seek + msg_len > len) {
      return lseek;
    }

    if (*messages == nullptr)
      *messages = grpc_messages_new(nullptr);
    grpc_message *msg = nullptr;
    if (cflag == 1) {
      uint8_t *decomp = nullptr;
      size_t decomp_len = 0;
      grpc_err result = grpc_decompress_limited(encoding, (const uint8_t *)msg_data, msg_len,
                                                &decomp, &decomp_len, max_output);
      if (result == GRPC_ERR_LIMIT) {
        *too_large = true;
        return 0;
      }
      if (result == GRPC_OK) {
        msg = message_create(0, decomp, decomp_len);
        grpc_free(decomp);
      } else {
        msg = message_create((u8)cflag, msg_data, msg_len);
      }
    } else {
      msg = message_create((u8)cflag, msg_data, msg_len);
    }
    assert(msg != nullptr);
    grpc_messages_push(*messages, msg);
    seek += msg_len;
    lseek = seek;
  }

  return lseek;
}

/* Returns true if the value_len bytes at value equal the NUL-terminated expected, exactly. */
static bool header_matches(const u8 *value, size_t value_len, const char *expected) {
  size_t expected_len = strlen(expected);
  return value_len == expected_len && memcmp(value, expected, expected_len) == 0;
}

/* Returns true if the value_len bytes at value start with the NUL-terminated prefix. */
static bool header_starts_with(const u8 *value, size_t value_len, const char *prefix) {
  size_t prefix_len = strlen(prefix);
  return value_len >= prefix_len && memcmp(value, prefix, prefix_len) == 0;
}

/* Brace initializer for a grpc_nv from two string literals, so sizeof() gives their lengths at
 * compile time (usable in static arrays). */
#define GRPC_NV(name, value) \
  {(uint8_t *)(name), (uint8_t *)(value), sizeof(name) - 1, sizeof(value) - 1, 0}

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
  stream->headers[stream->nb_headers].name = (char *)name;
  stream->headers[stream->nb_headers].value = (char *)value;
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
    if (header_matches(value, valuelen, "gzip"))
      stream->grpc_encoding = GRPC_COMPRESS_GZIP;
    else if (header_matches(value, valuelen, "deflate"))
      stream->grpc_encoding = GRPC_COMPRESS_DEFLATE;
    else if (header_matches(value, valuelen, "zstd"))
      stream->grpc_encoding = GRPC_COMPRESS_ZSTD;
    else if (header_matches(value, valuelen, "snappy"))
      stream->grpc_encoding = GRPC_COMPRESS_SNAPPY;
    else
      stream->grpc_encoding = GRPC_COMPRESS_IDENTITY;
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
  char grpc_status[16];
  int len = snprintf(grpc_status, sizeof(grpc_status), "%d", (int)status);
  grpc_nv nva[] = {
      GRPC_NV(":status", "200"),
      GRPC_NV("content-type", "application/grpc"),
      {(uint8_t *)"grpc-status", (uint8_t *)grpc_status, 11, (size_t)len, 0},
      {(uint8_t *)"grpc-message", (uint8_t *)message, 12, strlen(message), 0},
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
    static const grpc_nv cors_nva[] = {
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

  /* a message may not grow past max_message_size: end the call, keep the connection */
  if ((stream->recv_len + len) > session->server->config.max_message_size) {
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

/* Moves parsed messages onto the call's pending queue, counts them and queues the call. */
static void deliver_messages(grpc_session *session, grpc_call *call, grpc_messages *messages) {
  atomic_fetch_add(&session->server->stat_messages_received,
                   grpc_list_count((grpc_list *)messages));
  grpc_list_concat_and_empty_src(&call->pending, (grpc_list *)messages);
  grpc_list_free((grpc_list *)messages);
  call_queue(call);
}

void stream_core_on_data_end(grpc_session *session, grpc_stream *stream, bool end_stream) {
  if (stream->call == nullptr || stream->is_discarding)
    return;
  grpc_call *call = stream->call;

  grpc_messages *messages = nullptr;
  bool too_large = false;
  size_t max_out = session->server->config.max_message_size > 5
                       ? session->server->config.max_message_size - 5
                       : 0;
  if (stream->is_grpc_web_text) {
    /* base64 can only be decoded as a whole: wait for the end of the body */
    if (!end_stream)
      return;
    size_t dec_len = 0;
    uint8_t *dec = grpc_metadata_decode_bin((const char *)(stream->recv_data + stream->recv_start),
                                            stream->recv_len, &dec_len);
    if (dec == nullptr && stream->recv_len > 0) {
      stream->recv_len = 0;
      call_abort(call, GRPC_STATUS_INVALID_ARGUMENT, "invalid grpc-web-text base64");
      return;
    }
    size_t lseek = dec != nullptr ? parse_grpc_message(dec, dec_len, stream->grpc_encoding,
                                                       &messages, max_out, &too_large)
                                  : 0;
    grpc_free(dec);
    stream->recv_len = 0;
    stream->recv_start = 0;
    if (too_large) {
      if (messages != nullptr)
        grpc_messages_free(messages);
      stream->is_discarding = true;
      call_abort(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "decompressed request exceeds server limit");
      return;
    }
    if (messages != nullptr)
      deliver_messages(session, call, messages);
    if (lseek < dec_len) {
      call_abort(call, GRPC_STATUS_DATA_LOSS, "truncated message");
    } else {
      call->half_closed = true;
      call_queue(call);
    }
    return;
  }

  size_t lseek = parse_grpc_message(stream->recv_data + stream->recv_start, stream->recv_len,
                                    stream->grpc_encoding, &messages, max_out, &too_large);
  if (too_large) {
    if (messages != nullptr)
      grpc_messages_free(messages);
    stream->is_discarding = true;
    stream->recv_len = 0;
    call_abort(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "decompressed request exceeds server limit");
    return;
  }
  if (lseek) {
    assert(lseek <= stream->recv_len);
    stream->recv_start += lseek;
    stream->recv_len -= lseek;
    if (stream->recv_len == 0)
      stream->recv_start = 0;
  }
  if (messages != nullptr)
    deliver_messages(session, call, messages);

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
  grpc_list *lmessages = &stream->lqueue_omessages;
  grpc_message *msg;
  *eof = false;
  *end_stream = false;

  /* each message goes out as a 5 byte prefix followed by its data. trunc_seek tracks how much of
   * the front message was already written, so a message (and even its prefix) may span several
   * frames. */
  size_t buf_seek = 0;
  while (buf_seek < cap && (msg = grpc_list_peek_front(lmessages)) != nullptr) {
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
      (void)grpc_list_pop_front(lmessages);
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

  if (grpc_list_is_empty(lmessages)) {
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

#undef GRPC_NV
