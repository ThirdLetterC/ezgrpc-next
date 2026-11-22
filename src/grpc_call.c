/* grpc_call.c - calls: their lifetime, dispatch and the call API
 *
 * A grpc_call is the server-side state of one RPC: the request headers retained from the transport,
 * the received messages not yet delivered, the response headers and trailers being built, and
 * the application's userdata. Calls are refcounted. The request stream owns the first reference
 * and drops it in call_detach() when the stream goes away; the ready queue, each grpc_event and
 * each posted async op hold their own. A call can therefore outlive its stream: once detached
 * (call->stream == nullptr) the response functions fail with GRPC_ERR_CLOSED.
 *
 * Everything here runs on the server (event loop) thread, except grpc_call_ref(),
 * grpc_call_unref() and the *_async functions. Those copy their arguments and post a task with
 * grpc_server_post() that replays the synchronous function on the server thread.
 *
 * Dispatch: grpc_stream_core.c (fed by the HTTP/2 or HTTP/3 transport) appends received messages
 * to call->pending and puts the call on
 * the server's ready queue with call_queue(); cancellation does the same. server_dispatch() then
 * delivers each ready call to its route (unary handler, stream handler or grpc_event entries),
 * running the interceptors on the way. Responses are queued on the stream and handed to the
 * transport through session->ops; the transport pulls the body with stream_core_pull() when the
 * session is flushed.
 *
 * Copyright (c) 2023-2025 M. N. Yoshie & Al-buharie Amjari
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 *   2. Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdarg.h>
#include <stdio.h>
#include "internal/grpc_platform.h"

#include "core.h"
#include "internal_helpers.h"
#include "grpc_timeout.h"

/* ---- header lists ------------------------------------------------------------ */

/* Appends one header to `list`, growing it as needed. The name and value are copied into one
 * grpc_malloc() block, each NUL-terminated. Returns GRPC_ERR_NOMEM on allocation failure. */
static grpc_err nv_list_add(nv_list *list, const char *name, size_t namelen, const char *value,
                            size_t valuelen) {
  if (list->len == list->cap) {
    size_t cap = list->cap ? list->cap * 2 : 4;
    grpc_nv *nva = grpc_realloc(list->nva, cap * sizeof(*nva));
    if (nva == nullptr)
      return GRPC_ERR_NOMEM;
    list->nva = nva;
    list->cap = cap;
  }
  /* name and value share one allocation, freed through the name */
  u8 *block = grpc_malloc(namelen + valuelen + 2);
  if (block == nullptr)
    return GRPC_ERR_NOMEM;
  memcpy(block, name, namelen);
  block[namelen] = '\0';
  memcpy(block + namelen + 1, value, valuelen);
  block[namelen + 1 + valuelen] = '\0';
  list->nva[list->len++] = (grpc_nv){
      .name = block,
      .namelen = namelen,
      .value = block + namelen + 1,
      .valuelen = valuelen,
      .flags = 0,
  };
  return GRPC_OK;
}

/* Frees every header of `list` and the array, and resets it to empty so it can be reused. */
static void nv_list_free(nv_list *list) {
  for (size_t i = 0; i < list->len; i++)
    grpc_free(list->nva[i].name);
  grpc_free(list->nva);
  *list = (nv_list){};
}

/* Appends application metadata to `list`. A zero key_len or value_len means the string is
 * NUL-terminated. Rejects with GRPC_ERR_INVALID anything metadata_entry_valid() refuses: null,
 * empty or malformed keys, pseudo-headers and reserved names, and values with control bytes. On
 * error the entries already appended stay in the list. */
static grpc_err nv_list_add_metadata(nv_list *list, const grpc_metadata *metadata, size_t count) {
  if (metadata == nullptr && count > 0)
    return GRPC_ERR_INVALID;
  for (size_t i = 0; i < count; i++) {
    const grpc_metadata *md = &metadata[i];
    if (md->key == nullptr || md->value == nullptr)
      return GRPC_ERR_INVALID;
    size_t klen = md->key_len ? md->key_len : strlen(md->key);
    size_t vlen = md->value_len ? md->value_len : strlen(md->value);
    if (!metadata_entry_valid(md->key, klen, md->value, vlen, true))
      return GRPC_ERR_INVALID;
    grpc_err err = nv_list_add(list, md->key, klen, md->value, vlen);
    if (err != GRPC_OK)
      return err;
  }
  return GRPC_OK;
}

/* Copies `nva` (the array and every name and value) into a single grpc_malloc() block, so one
 * grpc_free() releases it. Names and values are not NUL-terminated in the copy. Returns nullptr
 * on allocation failure. */
static grpc_nv *nva_dup(const grpc_nv *nva, size_t nvlen) {
  size_t size = nvlen * sizeof(*nva);
  for (size_t i = 0; i < nvlen; i++)
    size += nva[i].namelen + nva[i].valuelen;
  grpc_nv *copy = grpc_malloc(size);
  if (copy == nullptr)
    return nullptr;
  u8 *p = (u8 *)(copy + nvlen);
  for (size_t i = 0; i < nvlen; i++) {
    copy[i] = nva[i];
    copy[i].name = p;
    memcpy(p, nva[i].name, nva[i].namelen);
    p += nva[i].namelen;
    copy[i].value = p;
    memcpy(p, nva[i].value, nva[i].valuelen);
    p += nva[i].valuelen;
  }
  return copy;
}

/* ---- lifetime ------------------------------------------------------------------ */

/* Frees the received messages that were not delivered to the application (the queue's buffer
 * is kept for later messages; call_free() releases it). */
static void drop_pending(grpc_call *call) {
  grpc_message *msg;
  while ((msg = msg_queue_pop(&call->pending)) != nullptr)
    grpc_message_free(msg);
}

/* Releases everything the call owns, runs the userdata destructor and frees the call. Called
 * only by grpc_call_unref() when the last reference goes away. */
static void call_free(grpc_call *call) {
  msg_queue_clear(&call->pending);
  grpc_message_free(call->request);
  nv_list_free(&call->headers);
  nv_list_free(&call->trailers);
  grpc_free(call->req_header_data);
  if (call->userdata_free != nullptr)
    call->userdata_free(call->userdata);
  grpc_free(call->method_override);
  grpc_free(call);
}

/* Copies the stream's request headers into one call-owned block (names and values
 * NUL-terminated), so they outlive the stream and its session. The call must not keep references
 * on the transport's header buffers: those are shared with the connection's HPACK/QPACK decoder
 * through non-atomic reference counts, and the call's last reference may be dropped by a worker
 * thread while the loop thread decodes the next request. Returns false when out of memory. */
static bool retain_request_headers(grpc_call *call, const grpc_stream *stream) {
  call->ops = stream->ops;
  size_t total = 0;
  for (size_t i = 0; i < stream->nb_headers; i++)
    total += stream->headers[i].namelen + 1 + stream->headers[i].valuelen + 1;
  char *block = total > 0 ? grpc_malloc(total) : nullptr;
  if (total > 0 && block == nullptr)
    return false;
  char *p = block;
  for (size_t i = 0; i < stream->nb_headers; i++) {
    const grpc_header *h = &stream->headers[i];
    grpc_header *c = &call->req_headers[i];
    memcpy(p, h->name, h->namelen);
    p[h->namelen] = '\0';
    *c = (grpc_header){.name = p, .namelen = h->namelen};
    p += h->namelen + 1;
    memcpy(p, h->value, h->valuelen);
    p[h->valuelen] = '\0';
    c->value = p;
    c->valuelen = h->valuelen;
    p += h->valuelen + 1;
  }
  call->req_header_data = block;
  call->nb_req_headers = stream->nb_headers;
  return true;
}

/* Returns true if `alg` is identity or a codec compiled into this build. */
static bool codec_is_supported(grpc_compression_algorithm alg) {
  switch (alg) {
  case GRPC_COMPRESS_IDENTITY:
    return true;
  case GRPC_COMPRESS_GZIP:
  case GRPC_COMPRESS_DEFLATE:
#if defined(GRPC_ENABLE_ZLIB)
    return true;
#else
    return false;
#endif
  case GRPC_COMPRESS_ZSTD:
#if defined(GRPC_ENABLE_ZSTD)
    return true;
#else
    return false;
#endif
  case GRPC_COMPRESS_SNAPPY:
#if defined(GRPC_ENABLE_SNAPPY)
    return true;
#else
    return false;
#endif
  default:
    return false;
  }
}

/* Returns the grpc-encoding token of `alg`; unknown values map to "identity". */
static const char *codec_token(grpc_compression_algorithm alg) {
  switch (alg) {
  case GRPC_COMPRESS_GZIP:
    return "gzip";
  case GRPC_COMPRESS_DEFLATE:
    return "deflate";
  case GRPC_COMPRESS_ZSTD:
    return "zstd";
  case GRPC_COMPRESS_SNAPPY:
    return "snappy";
  default:
    return "identity";
  }
}

/* Returns true if the client's grpc-accept-encoding header lists `alg`. */
static bool client_accepts_encoding(const grpc_call *call, grpc_compression_algorithm alg) {
  if (alg == GRPC_COMPRESS_IDENTITY)
    return true;
  const char *tok = codec_token(alg);
  size_t tok_len = strlen(tok);
  size_t idx = 0, val_len = 0;
  const char *val;
  while ((val = grpc_call_metadata_next(call, "grpc-accept-encoding", &idx, &val_len)) != nullptr) {
    const char *p = val;
    const char *end = val + val_len;
    while (p < end) {
      while (p < end && (*p == ' ' || *p == '\t' || *p == ','))
        p++;
      const char *start = p;
      while (p < end && *p != ',')
        p++;
      const char *trim = p;
      while (trim > start && (trim[-1] == ' ' || trim[-1] == '\t'))
        trim--;
      if ((size_t)(trim - start) == tok_len && strncasecmp(start, tok, tok_len) == 0)
        return true;
    }
  }
  return false;
}

/* Creates the call for a stream whose request HEADERS were accepted. Runs on the server thread
 * from the nghttp2 headers callback. The returned call holds one reference, owned by the stream
 * and dropped by call_detach(). Applies the server's default compression if the client accepts
 * it. Returns nullptr on allocation failure. */
grpc_call *call_new(grpc_session *session, grpc_stream *stream) {
  grpc_call *call = grpc_calloc(1, sizeof(*call));
  if (call == nullptr)
    return nullptr;
  if (!retain_request_headers(call, stream)) {
    grpc_free(call);
    return nullptr;
  }
  /* the stream's reference, dropped by call_detach() */
  atomic_init(&call->refs, 1);
  call->server = session->server;
  call->route = stream->route;
  call->session = session;
  call->stream = stream;
  call->stream_id = stream->stream_id;
  call->start_ms = grpc_time_now_ms();
  call->is_grpc_web = stream->is_grpc_web;
  call->is_grpc_web_text = stream->is_grpc_web_text;
  if (stream->route == session->server->fallback_route && stream->hpath != nullptr) {
    call->method_override = grpc_strndup(stream->hpath, stream->hpath_len);
    if (call->method_override == nullptr) { /* the fallback handler needs the real method */
      grpc_free(call->req_header_data);
      grpc_free(call);
      return nullptr;
    }
  }
  memcpy(call->peer, session->peer, sizeof(call->peer));
  memcpy(call->peer_cert_subject, session->peer_cert_subject, sizeof(call->peer_cert_subject));
  atomic_fetch_add(&session->server->stat_total_calls, 1);
  grpc_compression_algorithm def_alg = session->server->config.default_compression;
  if (def_alg != GRPC_COMPRESS_IDENTITY && codec_is_supported(def_alg) &&
      client_accepts_encoding(call, def_alg)) {
    call->auto_compression = def_alg;
    call->auto_compression_level = session->server->config.default_compression_level == 0
                                       ? GRPC_COMPRESSION_LEVEL_DEFAULT
                                       : session->server->config.default_compression_level;
  }
  return call;
}

/* Relaxed ordering suffices: the caller already holds a reference, so the count cannot reach
 * zero concurrently. */
GRPC_API grpc_call *grpc_call_ref(grpc_call *call) {
  if (call != nullptr)
    atomic_fetch_add_explicit(&call->refs, 1, memory_order_relaxed);
  return call;
}

/* acq_rel makes all writes made under other references visible to the thread that frees the
 * call. */
GRPC_API void grpc_call_unref(grpc_call *call) {
  if (call != nullptr && atomic_fetch_sub_explicit(&call->refs, 1, memory_order_acq_rel) == 1)
    call_free(call);
}

/* Appends the call to the server's ready queue, unless it is already queued, so that the next
 * server_dispatch() delivers what is new. The queue holds a reference, dropped after dispatch.
 * Server thread only. */
void call_queue(grpc_call *call) {
  if (call->queued)
    return;
  call->queued = true;
  grpc_call_ref(call);
  grpc_server *server = call->server;
  call->ready_next = nullptr;
  if (server->ready_tail != nullptr)
    server->ready_tail->ready_next = call;
  else
    server->ready_head = call;
  server->ready_tail = call;
}

/* Counts the call once in the server's ok/error statistics. */
static void call_record_done_stat(grpc_call *call, grpc_status status) {
  if (call->stats_done)
    return;
  call->stats_done = true;
  if (status == GRPC_STATUS_OK)
    atomic_fetch_add(&call->server->stat_calls_ok, 1);
  else
    atomic_fetch_add(&call->server->stat_calls_error, 1);
}

/* Marks the call cancelled (once), records the statistics, runs the on_done interceptors and
 * queues the call so that dispatch delivers the cancellation to the application. */
static void call_cancel(grpc_call *call, grpc_status reason) {
  if (call->cancelled)
    return;
  call->cancelled = true;
  call->cancel_reason = reason;
  call_record_done_stat(call, reason);
  interceptors_run_on_done(call, reason);
  call_queue(call);
}

/* Detaches the call from its stream, which is going away (stream closed or connection lost).
 * An unfinished call is cancelled with `reason`. Drops the stream's reference, so the call may be
 * freed here unless the application or the ready queue still holds one. Idempotent. */
void call_detach(grpc_call *call, grpc_status reason) {
  if (call->closed)
    return;
  call->closed = true;
  call->stream = nullptr;
  call->session = nullptr;
  if (!call->finished)
    call_cancel(call, reason);
  grpc_call_unref(call);
}

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

/* Ends the call: sends grpc-status, grpc-message (defaulting to the status name when `message`
 * is null and the status is not OK) and the application's trailers. Depending on what was sent:
 *  - gRPC-Web: the trailers go in a final DATA frame flagged 0x80 (base64 for grpc-web-text);
 *  - no headers yet: one Trailers-Only HEADERS frame;
 *  - DATA still queued: the trailers are stored on the stream for the data provider to submit
 *    after the last message, so they cannot overtake it;
 *  - otherwise: a trailing HEADERS frame right away.
 * On success marks the call finished, disarms the deadline, schedules a flush and runs the
 * on_done interceptors. Does not check `finished`; callers do. Returns GRPC_ERR_CLOSED if the
 * stream is gone. */
static grpc_err call_finish_internal(grpc_call *call, grpc_status status, const char *message) {
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
    stream->pending_trailers = nva_dup(nva, n);
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
  if (algorithm != GRPC_COMPRESS_IDENTITY && !codec_is_supported(algorithm))
    return GRPC_ERR_UNSUPPORTED;
  if (algorithm == GRPC_COMPRESS_IDENTITY || !client_accepts_encoding(call, algorithm)) {
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

/* ---- accessors ---------------------------------------------------------------- */

/* Returns the unary request; nullptr for streaming calls or before it was delivered. */
GRPC_API const grpc_message *grpc_call_request(const grpc_call *call) {
  return call != nullptr ? call->request : nullptr;
}

/* Returns the request :path for calls routed to the fallback handler, else the route's path. */
GRPC_API const char *grpc_call_method(const grpc_call *call) {
  if (call == nullptr)
    return nullptr;
  return call->method_override != nullptr ? call->method_override : call->route->path;
}

/* Returns the peer address copied from the session when the call was created. */
GRPC_API const char *grpc_call_peer(const grpc_call *call) {
  return call != nullptr ? call->peer : nullptr;
}

/* Returns the TLS client certificate subject, or nullptr when there is none. */
GRPC_API const char *grpc_call_peer_cert_subject(const grpc_call *call) {
  return call != nullptr && call->peer_cert_subject[0] != '\0' ? call->peer_cert_subject : nullptr;
}

/* Linear, case-insensitive search of the request headers from *index. On a match sets *index
 * past it; otherwise sets *index to the end so further calls return nullptr. The returned value
 * is backed by an nghttp2 rcbuf and lives as long as the call. */
GRPC_API const char *grpc_call_metadata_next(const grpc_call *call, const char *key, size_t *index,
                                             size_t *len) {
  if (call == nullptr || key == nullptr || index == nullptr)
    return nullptr;
  size_t klen = strlen(key);
  for (size_t i = *index; i < call->nb_req_headers; i++) {
    const grpc_header *h = &call->req_headers[i];
    if (h->namelen == klen && strncasecmp(h->name, key, klen) == 0) {
      *index = i + 1;
      if (len != nullptr)
        *len = h->valuelen;
      return h->value;
    }
  }
  *index = call->nb_req_headers;
  return nullptr;
}

/* Returns the first request header named `key`. */
GRPC_API const char *grpc_call_metadata(const grpc_call *call, const char *key, size_t *len) {
  size_t idx = 0;
  return grpc_call_metadata_next(call, key, &idx, len);
}

/* Returns the number of retained request headers, pseudo-headers included. */
GRPC_API size_t grpc_call_metadata_count(const grpc_call *call) {
  return call != nullptr ? call->nb_req_headers : 0;
}

/* Fills *out with the request header at `index`, borrowing the call's storage. */
GRPC_API bool grpc_call_metadata_at(const grpc_call *call, size_t index, grpc_metadata *out) {
  if (call == nullptr || out == nullptr || index >= call->nb_req_headers)
    return false;
  const grpc_header *h = &call->req_headers[index];
  *out = (grpc_metadata){
      .key = h->name,
      .key_len = h->namelen,
      .value = h->value,
      .value_len = h->valuelen,
  };
  return true;
}

/* Returns the milliseconds left before the grpc-timeout deadline, 0 once it passed, or -1 when
 * the client set none. */
GRPC_API int64_t grpc_call_deadline(const grpc_call *call) {
  if (call == nullptr || call->deadline_ms == 0)
    return -1;
  u64 now = grpc_time_now_ms();
  return now >= call->deadline_ms ? 0 : (int64_t)(call->deadline_ms - now);
}

/* Returns the cancelled flag set by call_cancel(). */
GRPC_API bool grpc_call_is_cancelled(const grpc_call *call) {
  return call != nullptr && call->cancelled;
}

/* Returns the cancellation status, or GRPC_STATUS_OK if the call is not cancelled. */
GRPC_API grpc_status grpc_call_cancel_reason(const grpc_call *call) {
  return call != nullptr && call->cancelled ? call->cancel_reason : GRPC_STATUS_OK;
}

/* Returns the server the call belongs to. */
GRPC_API grpc_server *grpc_call_server(const grpc_call *call) {
  return call != nullptr ? call->server : nullptr;
}

/* Replaces the userdata, running the previous destructor unless the same pointer is set
 * again. The last destructor runs in call_free(). */
GRPC_API void grpc_call_set_userdata(grpc_call *call, void *data, void (*free_fn)(void *data)) {
  if (call == nullptr)
    return;
  if (call->userdata_free != nullptr && call->userdata != data)
    call->userdata_free(call->userdata);
  call->userdata = data;
  call->userdata_free = free_fn;
}

/* Returns the userdata set with grpc_call_set_userdata(). */
GRPC_API void *grpc_call_userdata(const grpc_call *call) {
  return call != nullptr ? call->userdata : nullptr;
}

/* Registers the cancel callback, replacing any previous one (including a stream route's
 * on_cancel, installed when dispatch starts the call). */
GRPC_API void grpc_call_on_cancel(grpc_call *call, void (*fn)(grpc_call *call, void *userdata),
                                  void *userdata) {
  if (call == nullptr)
    return;
  call->cancel_fn = fn;
  call->cancel_userdata = userdata;
}

/* Returns the bytes queued on the stream and not yet handed to nghttp2; 0 once detached. */
GRPC_API size_t grpc_call_send_queue_bytes(const grpc_call *call) {
  return call != nullptr && call->stream != nullptr ? call->stream->queued_send_bytes : 0;
}

/* Returns true while the call can respond and its send queue is below max_send_queue_bytes
 * (always, when the limit is 0). */
GRPC_API bool grpc_call_is_writable(const grpc_call *call) {
  if (call == nullptr || call->stream == nullptr || call->finished || call->closed)
    return false;
  size_t max_q = call->server->config.max_send_queue_bytes;
  return max_q == 0 || call->stream->queued_send_bytes < max_q;
}

/* Registers the callback the data provider runs when a backpressured send queue drains below
 * the limit. */
GRPC_API void grpc_call_on_writable(grpc_call *call, void (*fn)(grpc_call *call, void *userdata),
                                    void *userdata) {
  if (call == nullptr)
    return;
  call->writable_fn = fn;
  call->writable_userdata = userdata;
}

/* Returns true for the characters W3C traceparent allows in hex fields: 0-9 and a-f. */
static bool is_hex_lower_or_digit(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

/* Returns the value of a lowercase hex digit, or -1. */
static int hex_val(char c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'a' && c <= 'f')
    return 10 + (c - 'a');
  return -1;
}

/* Parses a version-00 W3C traceparent header, rejecting uppercase hex and all-zero trace or span
 * ids. *out is zeroed up front and again on the final validity check, but a non-hex character
 * returns early and leaves the digits copied so far in *out (valid stays false). */
GRPC_API bool grpc_call_trace_context(const grpc_call *call, grpc_trace_context *out) {
  if (out != nullptr)
    memset(out, 0, sizeof(*out));
  if (call == nullptr || out == nullptr)
    return false;
  size_t len = 0;
  const char *tp = grpc_call_metadata(call, "traceparent", &len);
  /* W3C traceparent: "00-<32 hex>-<16 hex>-<2 hex>" = 55 chars */
  if (tp == nullptr || len != 55 || tp[0] != '0' || tp[1] != '0' || tp[2] != '-' || tp[35] != '-' ||
      tp[52] != '-')
    return false;
  bool trace_nonzero = false, span_nonzero = false;
  for (size_t i = 0; i < 32; i++) {
    char c = tp[3 + i];
    if (!is_hex_lower_or_digit(c))
      return false;
    if (c != '0')
      trace_nonzero = true;
    out->trace_id[i] = c;
  }
  out->trace_id[32] = '\0';
  for (size_t i = 0; i < 16; i++) {
    char c = tp[36 + i];
    if (!is_hex_lower_or_digit(c))
      return false;
    if (c != '0')
      span_nonzero = true;
    out->span_id[i] = c;
  }
  out->span_id[16] = '\0';
  int hi = hex_val(tp[53]), lo = hex_val(tp[54]);
  if (!trace_nonzero || !span_nonzero || hi < 0 || lo < 0) {
    memset(out, 0, sizeof(*out));
    return false;
  }
  out->trace_flags = (uint8_t)((hi << 4) | lo);
  out->valid = true;
  return true;
}

/* Formats a version-00 traceparent; `buf` needs room for 55 characters and the NUL. */
GRPC_API grpc_err grpc_traceparent_format(const grpc_trace_context *ctx, char *buf,
                                          size_t buf_len) {
  if (ctx == nullptr || buf == nullptr || buf_len < 56 || !ctx->valid)
    return GRPC_ERR_INVALID;
  if (strlen(ctx->trace_id) != 32 || strlen(ctx->span_id) != 16)
    return GRPC_ERR_INVALID;
  snprintf(buf, buf_len, "00-%s-%s-%02x", ctx->trace_id, ctx->span_id, ctx->trace_flags);
  return GRPC_OK;
}

/* ---- from other threads -------------------------------------------------------- */

typedef enum async_op_kind {
  ASYNC_OP_SEND,
  ASYNC_OP_REPLY,
  ASYNC_OP_FINISH,
  ASYNC_OP_METADATA,
  ASYNC_OP_TRAILERS,
} async_op_kind;

typedef struct async_op async_op;
struct async_op {
  async_op_kind kind;
  grpc_call *call;
  grpc_message *message;
  grpc_status status;
  char *text;
  grpc_metadata *metadata;
  size_t metadata_count;
};

/* Frees an op and the copies it owns (metadata strings, finish message). The message is not
 * freed here: run_async_op() hands it to the call, post_async_op() frees it on failure. */
static void async_op_free(async_op *op) {
  if (op == nullptr)
    return;
  for (size_t i = 0; i < op->metadata_count; i++) {
    grpc_free((void *)op->metadata[i].key);
    grpc_free((void *)op->metadata[i].value);
  }
  grpc_free(op->metadata);
  grpc_free(op->text);
  grpc_free(op);
}

/* Server-thread side of the *_async functions: replays the synchronous call function, which
 * takes ownership of op->message, then drops the reference taken by post_async_op(). Errors
 * are ignored; there is no one to report them to. */
static void run_async_op(void *arg) {
  async_op *op = arg;
  switch (op->kind) {
  case ASYNC_OP_SEND:
    (void)grpc_call_send(op->call, op->message);
    break;
  case ASYNC_OP_REPLY:
    (void)grpc_call_reply(op->call, op->message);
    break;
  case ASYNC_OP_FINISH:
    (void)grpc_call_finish(op->call, op->status, op->text);
    break;
  case ASYNC_OP_METADATA:
    (void)grpc_call_send_metadata(op->call, op->metadata, op->metadata_count);
    break;
  case ASYNC_OP_TRAILERS:
    (void)grpc_call_add_trailers(op->call, op->metadata, op->metadata_count);
    break;
  }
  grpc_call_unref(op->call);
  async_op_free(op);
}

/* Takes a reference on the call for the op and posts it to the server thread. On failure
 * releases the reference, the message and the op, so the caller owns nothing afterwards. */
static grpc_err post_async_op(async_op *op) {
  /* the caller holds a reference, so the call is alive to take another */
  grpc_call_ref(op->call);
  grpc_err err = grpc_server_post(op->call->server, run_async_op, op);
  if (err != GRPC_OK) {
    grpc_call_unref(op->call);
    grpc_message_free(op->message);
    async_op_free(op);
  }
  return err;
}

/* Validates and deep-copies `metadata` (the caller's strings may go away once this returns),
 * then posts a send-metadata or add-trailers op. Validation matches nv_list_add_metadata() so
 * that bad input is reported to the caller instead of being dropped on the server thread. */
static grpc_err post_async_metadata(grpc_call *call, async_op_kind kind,
                                    const grpc_metadata *metadata, size_t count) {
  if (call == nullptr || (metadata == nullptr && count > 0))
    return GRPC_ERR_INVALID;
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr)
    return GRPC_ERR_NOMEM;
  op->kind = kind;
  op->call = call;
  if (count > 0) {
    op->metadata = grpc_calloc(count, sizeof(*op->metadata));
    if (op->metadata == nullptr) {
      grpc_free(op);
      return GRPC_ERR_NOMEM;
    }
    for (size_t i = 0; i < count; i++) {
      if (metadata[i].key == nullptr || metadata[i].value == nullptr) {
        op->metadata_count = i;
        async_op_free(op);
        return GRPC_ERR_INVALID;
      }
      size_t klen = metadata[i].key_len ? metadata[i].key_len : strlen(metadata[i].key);
      size_t vlen = metadata[i].value_len ? metadata[i].value_len : strlen(metadata[i].value);
      if (!metadata_entry_valid(metadata[i].key, klen, metadata[i].value, vlen, true)) {
        op->metadata_count = i;
        async_op_free(op);
        return GRPC_ERR_INVALID;
      }
      char *k = grpc_strndup(metadata[i].key, klen);
      char *v = grpc_strndup(metadata[i].value, vlen);
      if (k == nullptr || v == nullptr) {
        grpc_free(k);
        grpc_free(v);
        op->metadata_count = i;
        async_op_free(op);
        return GRPC_ERR_NOMEM;
      }
      op->metadata[i] = (grpc_metadata){.key = k, .key_len = klen, .value = v, .value_len = vlen};
    }
    op->metadata_count = count;
  }
  return post_async_op(op);
}

/* Posts grpc_call_send_metadata() with a copy of `metadata`. */
GRPC_API grpc_err grpc_call_send_metadata_async(grpc_call *call, const grpc_metadata *metadata,
                                                size_t count) {
  return post_async_metadata(call, ASYNC_OP_METADATA, metadata, count);
}

/* Posts grpc_call_add_trailers() with a copy of `metadata`. */
GRPC_API grpc_err grpc_call_add_trailers_async(grpc_call *call, const grpc_metadata *metadata,
                                               size_t count) {
  return post_async_metadata(call, ASYNC_OP_TRAILERS, metadata, count);
}

/* Posts grpc_call_send(). Takes ownership of `message`, freeing it on every error. */
GRPC_API grpc_err grpc_call_send_async(grpc_call *call, grpc_message *message) {
  if (call == nullptr || message == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_INVALID;
  }
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_NOMEM;
  }
  op->kind = ASYNC_OP_SEND;
  op->call = call;
  op->message = message;
  return post_async_op(op);
}

/* Copies `data` into a message and posts it with grpc_call_send_async(). */
GRPC_API grpc_err grpc_call_send_data_async(grpc_call *call, const void *data, size_t len) {
  if (call == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  grpc_message *message = grpc_message_new(data, len);
  if (message == nullptr)
    return GRPC_ERR_NOMEM;
  return grpc_call_send_async(call, message);
}

/* Posts grpc_call_reply(). Takes ownership of `message`, freeing it on every error. */
GRPC_API grpc_err grpc_call_reply_async(grpc_call *call, grpc_message *message) {
  if (call == nullptr || message == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_INVALID;
  }
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr) {
    grpc_message_free(message);
    return GRPC_ERR_NOMEM;
  }
  op->kind = ASYNC_OP_REPLY;
  op->call = call;
  op->message = message;
  return post_async_op(op);
}

/* Posts grpc_call_finish() with a copy of `message`. */
GRPC_API grpc_err grpc_call_finish_async(grpc_call *call, grpc_status status, const char *message) {
  if (call == nullptr)
    return GRPC_ERR_INVALID;
  async_op *op = grpc_calloc(1, sizeof(*op));
  if (op == nullptr)
    return GRPC_ERR_NOMEM;
  op->kind = ASYNC_OP_FINISH;
  op->call = call;
  op->status = status;
  if (message != nullptr && (op->text = grpc_strdup(message)) == nullptr) {
    grpc_free(op);
    return GRPC_ERR_NOMEM;
  }
  return post_async_op(op);
}

/* ---- dispatch ------------------------------------------------------------------ */

/* Appends an event for grpc_server_events(), taking a reference on the call and ownership of
 * `message`. If the array cannot grow, logs and drops the event (freeing the message). */
static void push_event(grpc_server *server, grpc_event_type type, grpc_call *call,
                       grpc_message *message) {
  if (server->nb_events == server->cap_events) {
    size_t cap = server->cap_events ? server->cap_events * 2 : 64;
    grpc_event *events = grpc_realloc(server->events, cap * sizeof(*events));
    if (events == nullptr) {
      LOG_ERROR(server, "out of memory, dropping an event\n");
      grpc_message_free(message);
      return;
    }
    server->events = events;
    server->cap_events = cap;
  }
  server->events[server->nb_events++] = (grpc_event){
      .type = type,
      .call = grpc_call_ref(call),
      .message = message,
      .userdata = call->route->userdata,
  };
}

/* Releases the events returned by the previous poll: frees their messages and drops their call
 * references. Called at the start of each grpc_server_poll() and on server shutdown. */
void server_clear_events(grpc_server *server) {
  for (size_t i = 0; i < server->nb_events; i++) {
    grpc_message_free((grpc_message *)server->events[i].message);
    grpc_call_unref(server->events[i].call);
  }
  server->nb_events = 0;
}

/* Delivers a unary call once the client half-closed: exactly one request message is required,
 * otherwise the call ends with INTERNAL. The request stays owned by the call (call->request), so
 * the handler may keep using it while it holds a reference. */
static void dispatch_unary(grpc_call *call) {
  if (call->finished || call->closed) {
    drop_pending(call);
    return;
  }
  /* wait for the whole request */
  if (call->request_delivered || !call->half_closed)
    return;

  size_t count = msg_queue_count(&call->pending);
  if (count != 1) {
    drop_pending(call);
    (void)call_finish_internal(call, GRPC_STATUS_INTERNAL,
                               count == 0 ? "missing request message"
                                          : "more than one request message");
    return;
  }
  call->request = msg_queue_pop(&call->pending);
  call->request_delivered = true;
  if (!interceptors_run_on_recv(call, call->request))
    return;
  call->route->handler(call, call->request, call->route->userdata);
}

/* Delivers the pending messages of a streaming call to on_message (each freed after the
 * callback returns), then on_end once the client half-closed. Stops as soon as the call is
 * finished, discarding the rest. */
static void dispatch_stream(grpc_call *call) {
  const grpc_stream_handler *handler = &call->route->stream;
  grpc_message *msg;
  while (!call->finished && (msg = msg_queue_pop(&call->pending)) != nullptr) {
    if (!interceptors_run_on_recv(call, msg)) {
      grpc_message_free(msg);
      break;
    }
    if (handler->on_message != nullptr)
      handler->on_message(call, msg, call->route->userdata);
    grpc_message_free(msg);
  }
  /* finished calls take no more messages */
  drop_pending(call);
  if (call->half_closed && !call->end_delivered && !call->finished) {
    call->end_delivered = true;
    if (handler->on_end != nullptr)
      handler->on_end(call, call->route->userdata);
  }
}

/* Event-route counterpart of dispatch_stream(): turns pending messages, half-close and
 * cancellation into grpc_event entries (the events own the messages) and runs the cancel
 * callback once. */
static void dispatch_events(grpc_call *call) {
  grpc_server *server = call->server;
  grpc_message *msg;
  while (!call->finished && (msg = msg_queue_pop(&call->pending)) != nullptr) {
    if (!interceptors_run_on_recv(call, msg)) {
      grpc_message_free(msg);
      break;
    }
    push_event(server, GRPC_EVENT_MESSAGE, call, msg);
  }
  drop_pending(call);
  if (call->half_closed && !call->end_delivered && !call->finished) {
    call->end_delivered = true;
    push_event(server, GRPC_EVENT_END, call, nullptr);
  }
  if (call->cancelled && !call->cancel_delivered) {
    call->cancel_delivered = true;
    push_event(server, GRPC_EVENT_CANCEL, call, nullptr);
    if (call->cancel_fn != nullptr)
      call->cancel_fn(call, call->cancel_userdata);
  }
}

/* Delivers what is new on one call. On first dispatch starts it: installs the stream route's
 * on_cancel, runs the on_call interceptors (a rejection finishes the call and discards all
 * further input) and runs on_start. Calls cancelled before starting are dropped without the
 * application ever seeing them. Finally runs the cancel callback once the application has seen
 * the call (event routes do this in dispatch_events()). */
static void dispatch_call(grpc_call *call) {
  if (!call->started) {
    /* cancelled before the application saw it */
    if (call->cancelled) {
      drop_pending(call);
      return;
    }
    call->started = true;
    if (call->route->kind == ROUTE_STREAM && call->route->stream.on_cancel != nullptr) {
      call->cancel_fn = call->route->stream.on_cancel;
      call->cancel_userdata = call->route->userdata;
    }
    if (!interceptors_run_on_call(call)) {
      call->rejected = true;
    } else if (call->route->kind == ROUTE_STREAM && call->route->stream.on_start != nullptr &&
               !call->finished) {
      call->route->stream.on_start(call, call->route->userdata);
    }
  }
  if (call->rejected) {
    drop_pending(call);
    return;
  }

  switch (call->route->kind) {
  case ROUTE_UNARY:
    dispatch_unary(call);
    break;
  case ROUTE_STREAM:
    dispatch_stream(call);
    break;
  case ROUTE_EVENTS:
    dispatch_events(call);
    return;
  }

  /* a unary call is cancelled for the application once its handler ran */
  bool seen = call->route->kind != ROUTE_UNARY || call->request_delivered;
  if (call->cancelled && !call->cancel_delivered && seen) {
    call->cancel_delivered = true;
    if (call->cancel_fn != nullptr)
      call->cancel_fn(call, call->cancel_userdata);
  }
}

/* Dispatches every call on the ready queue and drops the queue's reference on each. Server thread
 * only; handlers may queue calls again while this runs, and those are dispatched in the same
 * pass. */
void server_dispatch(grpc_server *server) {
  grpc_call *call;
  while ((call = server->ready_head) != nullptr) {
    server->ready_head = call->ready_next;
    if (server->ready_head == nullptr)
      server->ready_tail = nullptr;
    call->ready_next = nullptr;
    call->queued = false;
    dispatch_call(call);
    grpc_call_unref(call);
  }
}

/* Empties the ready queue without dispatching, dropping its references. Used on shutdown. */
void server_drop_ready(grpc_server *server) {
  grpc_call *call;
  while ((call = server->ready_head) != nullptr) {
    server->ready_head = call->ready_next;
    call->ready_next = nullptr;
    call->queued = false;
    grpc_call_unref(call);
  }
  server->ready_tail = nullptr;
}
