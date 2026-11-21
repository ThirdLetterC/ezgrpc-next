/* grpc_call.c - calls: their lifetime, dispatch and the call API
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
#include "internal_nghttp2_callbacks.h"
#include "grpc_timeout.h"

/* ---- header lists ------------------------------------------------------------ */

static grpc_err nv_list_add(nv_list *list, const char *name, size_t namelen, const char *value,
                               size_t valuelen) {
  if (list->len == list->cap) {
    size_t cap = list->cap ? list->cap * 2 : 4;
    nghttp2_nv *nva = grpc_realloc(list->nva, cap * sizeof(*nva));
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
  list->nva[list->len++] = (nghttp2_nv){
      .name = block,
      .namelen = namelen,
      .value = block + namelen + 1,
      .valuelen = valuelen,
      .flags = NGHTTP2_NV_FLAG_NONE,
  };
  return GRPC_OK;
}

static void nv_list_free(nv_list *list) {
  for (size_t i = 0; i < list->len; i++)
    grpc_free(list->nva[i].name);
  grpc_free(list->nva);
  *list = (nv_list){};
}

static grpc_err nv_list_add_metadata(nv_list *list, const grpc_metadata *metadata,
                                        size_t count) {
  if (metadata == nullptr && count > 0)
    return GRPC_ERR_INVALID;
  for (size_t i = 0; i < count; i++) {
    const grpc_metadata *md = &metadata[i];
    if (md->key == nullptr || md->value == nullptr)
      return GRPC_ERR_INVALID;
    size_t klen = md->key_len ? md->key_len : strlen(md->key);
    size_t vlen = md->value_len ? md->value_len : strlen(md->value);
    /* pseudo-headers belong to HTTP/2 itself */
    if (klen == 0 || md->key[0] == ':')
      return GRPC_ERR_INVALID;
    grpc_err err = nv_list_add(list, md->key, klen, md->value, vlen);
    if (err != GRPC_OK)
      return err;
  }
  return GRPC_OK;
}

/* copies nva into a single allocation */
static nghttp2_nv *nva_dup(const nghttp2_nv *nva, size_t nvlen) {
  size_t size = nvlen * sizeof(*nva);
  for (size_t i = 0; i < nvlen; i++)
    size += nva[i].namelen + nva[i].valuelen;
  nghttp2_nv *copy = grpc_malloc(size);
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

static void drop_pending(grpc_call *call) {
  grpc_message *msg;
  while ((msg = grpc_list_pop_front(&call->pending)) != nullptr)
    grpc_message_free(msg);
}

static void call_free(grpc_call *call) {
  drop_pending(call);
  grpc_message_free(call->request);
  nv_list_free(&call->headers);
  nv_list_free(&call->trailers);
  for (size_t i = 0; i < call->nb_req_headers; i++) {
    nghttp2_rcbuf_decref(call->req_header_rcbufs[i][0]);
    nghttp2_rcbuf_decref(call->req_header_rcbufs[i][1]);
  }
  if (call->userdata_free != nullptr)
    call->userdata_free(call->userdata);
  grpc_free(call);
}

/* Retain nghttp2's header storage instead of copying every name and value. */
static void retain_request_headers(grpc_call *call, const grpc_stream *stream) {
  for (size_t i = 0; i < stream->nb_headers; i++) {
    call->req_headers[i] = stream->headers[i];
    call->req_header_rcbufs[i][0] = stream->header_rcbufs[i][0];
    call->req_header_rcbufs[i][1] = stream->header_rcbufs[i][1];
    nghttp2_rcbuf_incref(call->req_header_rcbufs[i][0]);
    nghttp2_rcbuf_incref(call->req_header_rcbufs[i][1]);
  }
  call->nb_req_headers = stream->nb_headers;
}

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

grpc_call *call_new(grpc_session *session, grpc_stream *stream) {
  grpc_call *call = grpc_calloc(1, sizeof(*call));
  if (call == nullptr)
    return nullptr;
  retain_request_headers(call, stream);
  /* the stream's reference, dropped by call_detach() */
  atomic_init(&call->refs, 1);
  call->server = session->server;
  call->route = stream->route;
  call->session = session;
  call->stream = stream;
  call->stream_id = stream->stream_id;
  call->start_ms = grpc_time_now_ms();
  memcpy(call->peer, session->peer, sizeof(call->peer));
  memcpy(call->peer_cert_subject, session->peer_cert_subject, sizeof(call->peer_cert_subject));
  grpc_list_init(&call->pending);
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

GRPC_API grpc_call *grpc_call_ref(grpc_call *call) {
  if (call != nullptr)
    atomic_fetch_add_explicit(&call->refs, 1, memory_order_relaxed);
  return call;
}

GRPC_API void grpc_call_unref(grpc_call *call) {
  if (call != nullptr && atomic_fetch_sub_explicit(&call->refs, 1, memory_order_acq_rel) == 1)
    call_free(call);
}

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

static void call_cancel(grpc_call *call, grpc_status reason) {
  if (call->cancelled)
    return;
  call->cancelled = true;
  call->cancel_reason = reason;
  interceptors_run_on_done(call, reason);
  call_queue(call);
}

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

/* percent-encodes a grpc-message, as the gRPC HTTP/2 protocol requires */
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

/* submits the response HEADERS: the fixed headers, the application's
 * metadata and `extra` (the trailers of a Trailers-Only response) */
/* String literals keep the nghttp2 name and value lengths at compile time. */
#define NGHTTP2_NV(name, value)                                                                    \
  ((nghttp2_nv){(uint8_t *)(name), (uint8_t *)(value), sizeof(name) - 1, sizeof(value) - 1,       \
                NGHTTP2_NV_FLAG_NONE})

static grpc_err submit_response_headers(grpc_call *call, uint8_t flags,
                                           const nghttp2_nv *extra, size_t nb_extra) {
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
  nghttp2_nv base[] = {
      NGHTTP2_NV(":status", "200"),
      NGHTTP2_NV("content-type", "application/grpc"),
      { (uint8_t *)"grpc-accept-encoding", (uint8_t *)accepted, sizeof("grpc-accept-encoding") - 1, 0, NGHTTP2_NV_FLAG_NONE },
      NGHTTP2_NV("grpc-encoding", "gzip"),
  };
  const char *encoding = "gzip";
  switch (call->compression) {
  case GRPC_COMPRESS_DEFLATE: encoding = "deflate"; break;
  case GRPC_COMPRESS_ZSTD: encoding = "zstd"; break;
  case GRPC_COMPRESS_SNAPPY: encoding = "snappy"; break;
  default: break;
  }
  base[2].valuelen = strlen(accepted);
  base[3].value = (uint8_t *)encoding;
  base[3].valuelen = strlen(encoding);
  size_t nb_base = call->compression != GRPC_COMPRESS_IDENTITY ? 4 : 3;
  size_t total = nb_base + call->headers.len + nb_extra;
  nghttp2_nv *nva = grpc_malloc(total * sizeof(*nva));
  if (nva == nullptr)
    return GRPC_ERR_NOMEM;
  memcpy(nva, base, nb_base * sizeof(*nva));
  if (call->headers.len > 0)
    memcpy(nva + nb_base, call->headers.nva, call->headers.len * sizeof(*nva));
  if (nb_extra > 0)
    memcpy(nva + nb_base + call->headers.len, extra, nb_extra * sizeof(*nva));

  int res = nghttp2_submit_headers(call->session->ngsession, flags, call->stream_id, nullptr, nva,
                                   total, nullptr);
  grpc_free(nva);
  if (res != 0)
    return GRPC_ERR_PROTOCOL;
  call->headers_sent = true;
  nv_list_free(&call->headers);
  return GRPC_OK;
}

#undef NGHTTP2_NV

static grpc_err call_finish_internal(grpc_call *call, grpc_status status,
                                        const char *message) {
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
  nghttp2_nv *nva = grpc_malloc(total * sizeof(*nva));
  if (nva == nullptr) {
    grpc_free(encoded);
    return GRPC_ERR_NOMEM;
  }
  size_t n = 0;
  nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-status", (uint8_t *)grpc_status, 11,
                          (size_t)status_len, NGHTTP2_NV_FLAG_NONE};
  if (encoded != nullptr)
    nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-message", (uint8_t *)encoded, 12, message_len,
                            NGHTTP2_NV_FLAG_NONE};
  if (call->trailers.len > 0)
    memcpy(nva + n, call->trailers.nva, call->trailers.len * sizeof(*nva));
  n += call->trailers.len;

  grpc_err err = GRPC_OK;
  grpc_stream *stream = call->stream;
  if (!call->headers_sent) {
    /* nothing was sent: a single Trailers-Only HEADERS */
    err = submit_response_headers(call, NGHTTP2_FLAG_END_STREAM, nva, n);
  } else if (stream->is_data_active) {
    /* the trailers must not overtake the queued DATA. the data provider
     * submits them once it has written the last message */
    stream->pending_trailers = nva_dup(nva, n);
    stream->nb_pending_trailers = n;
    if (stream->pending_trailers == nullptr)
      err = GRPC_ERR_NOMEM;
  } else if (nghttp2_submit_trailer(call->session->ngsession, call->stream_id, nva, n) != 0) {
    err = GRPC_ERR_PROTOCOL;
  }
  grpc_free(nva);
  grpc_free(encoded);
  if (err != GRPC_OK)
    return err;

  call->finished = true;
  stream->is_deadline_set = false;
  nv_list_free(&call->trailers);
  session_schedule_flush(call->session);
  interceptors_run_on_done(call, status);
  return GRPC_OK;
}

void call_abort(grpc_call *call, grpc_status status, const char *message) {
  if (call->finished || call->closed)
    return;
  (void)call_finish_internal(call, status, message);
  call_cancel(call, status);
}

static grpc_err call_check(const grpc_call *call) {
  if (call == nullptr)
    return GRPC_ERR_INVALID;
  if (call->finished)
    return GRPC_ERR_FINISHED;
  if (call->closed)
    return GRPC_ERR_CLOSED;
  return GRPC_OK;
}

GRPC_API grpc_err grpc_call_set_compression(grpc_call *call,
                                            grpc_compression_algorithm algorithm, int level) {
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

GRPC_API grpc_err grpc_call_send_metadata(grpc_call *call,
                                                   const grpc_metadata *metadata,
                                                   size_t count) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  if (call->headers_sent)
    return GRPC_ERR_STATE;
  /* sent with the first message, or with the status */
  return nv_list_add_metadata(&call->headers, metadata, count);
}

GRPC_API grpc_err grpc_call_add_trailers(grpc_call *call,
                                                  const grpc_metadata *metadata,
                                                  size_t count) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  return nv_list_add_metadata(&call->trailers, metadata, count);
}

GRPC_API grpc_err grpc_call_send(grpc_call *call, grpc_message *message) {
  grpc_err err = message != nullptr ? call_check(call) : GRPC_ERR_INVALID;
  if (err != GRPC_OK) {
    grpc_message_free(message);
    return err;
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
    err = submit_response_headers(call, NGHTTP2_FLAG_NONE, nullptr, 0);
    if (err != GRPC_OK) {
      grpc_message_free(message);
      return err;
    }
  } else if (message->cflag != 0 &&
             (call->compression == GRPC_COMPRESS_IDENTITY || message->compression != call->compression)) {
    /* the encoding was announced with the first message */
    grpc_message_free(message);
    return GRPC_ERR_STATE;
  }

  grpc_stream *stream = call->stream;
  if (grpc_list_push_back(&stream->lqueue_omessages, message) != 0) {
    grpc_message_free(message);
    return GRPC_ERR_NOMEM;
  }

  /* an active data provider picks up the new message by itself */
  if (!stream->is_data_active) {
    nghttp2_data_provider data_provider = {
        .source.ptr = stream,
        .read_callback = data_source_read_callback2,
    };
    if (nghttp2_submit_data(call->session->ngsession, NGHTTP2_FLAG_NONE, call->stream_id,
                            &data_provider) != 0)
      return GRPC_ERR_PROTOCOL;
    stream->is_data_active = true;
  }
  session_schedule_flush(call->session);
  return GRPC_OK;
}

GRPC_API grpc_err grpc_call_send_data(grpc_call *call, const void *data,
                                               size_t len) {
  if (call == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  grpc_message *message = grpc_message_new(data, len);
  if (message == nullptr)
    return GRPC_ERR_NOMEM;
  return grpc_call_send(call, message);
}

GRPC_API grpc_err grpc_call_reply(grpc_call *call, grpc_message *message) {
  grpc_err err = grpc_call_send(call, message);
  if (err != GRPC_OK)
    return err;
  return grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

GRPC_API grpc_err grpc_call_reply_data(grpc_call *call, const void *data, size_t len) {
  grpc_err err = grpc_call_send_data(call, data, len);
  if (err != GRPC_OK)
    return err;
  return grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

GRPC_API grpc_err grpc_call_finish(grpc_call *call, grpc_status status,
                                            const char *message) {
  grpc_err err = call_check(call);
  if (err != GRPC_OK)
    return err;
  return call_finish_internal(call, status, message);
}

/* ---- accessors ---------------------------------------------------------------- */

GRPC_API const grpc_message *grpc_call_request(const grpc_call *call) {
  return call != nullptr ? call->request : nullptr;
}

GRPC_API const char *grpc_call_method(const grpc_call *call) {
  return call != nullptr ? call->route->path : nullptr;
}

GRPC_API const char *grpc_call_peer(const grpc_call *call) {
  return call != nullptr ? call->peer : nullptr;
}

GRPC_API const char *grpc_call_peer_cert_subject(const grpc_call *call) {
  return call != nullptr && call->peer_cert_subject[0] != '\0' ? call->peer_cert_subject : nullptr;
}

GRPC_API const char *grpc_call_metadata_next(const grpc_call *call, const char *key,
                                             size_t *index, size_t *len) {
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

GRPC_API const char *grpc_call_metadata(const grpc_call *call, const char *key,
                                              size_t *len) {
  size_t idx = 0;
  return grpc_call_metadata_next(call, key, &idx, len);
}

GRPC_API size_t grpc_call_metadata_count(const grpc_call *call) {
  return call != nullptr ? call->nb_req_headers : 0;
}

GRPC_API bool grpc_call_metadata_at(const grpc_call *call, size_t index,
                                          grpc_metadata *out) {
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

GRPC_API int64_t grpc_call_deadline(const grpc_call *call) {
  if (call == nullptr || call->deadline_ms == 0)
    return -1;
  u64 now = grpc_time_now_ms();
  return now >= call->deadline_ms ? 0 : (int64_t)(call->deadline_ms - now);
}

GRPC_API bool grpc_call_is_cancelled(const grpc_call *call) {
  return call != nullptr && call->cancelled;
}

GRPC_API grpc_status grpc_call_cancel_reason(const grpc_call *call) {
  return call != nullptr && call->cancelled ? call->cancel_reason : GRPC_STATUS_OK;
}

GRPC_API grpc_server *grpc_call_server(const grpc_call *call) {
  return call != nullptr ? call->server : nullptr;
}

GRPC_API void grpc_call_set_userdata(grpc_call *call, void *data,
                                           void (*free_fn)(void *data)) {
  if (call == nullptr)
    return;
  if (call->userdata_free != nullptr && call->userdata != data)
    call->userdata_free(call->userdata);
  call->userdata = data;
  call->userdata_free = free_fn;
}

GRPC_API void *grpc_call_userdata(const grpc_call *call) {
  return call != nullptr ? call->userdata : nullptr;
}

GRPC_API void grpc_call_on_cancel(grpc_call *call,
                                        void (*fn)(grpc_call *call, void *userdata),
                                        void *userdata) {
  if (call == nullptr)
    return;
  call->cancel_fn = fn;
  call->cancel_userdata = userdata;
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
      if (klen == 0 || metadata[i].key[0] == ':') {
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

GRPC_API grpc_err grpc_call_send_metadata_async(grpc_call *call, const grpc_metadata *metadata,
                                                size_t count) {
  return post_async_metadata(call, ASYNC_OP_METADATA, metadata, count);
}

GRPC_API grpc_err grpc_call_add_trailers_async(grpc_call *call, const grpc_metadata *metadata,
                                               size_t count) {
  return post_async_metadata(call, ASYNC_OP_TRAILERS, metadata, count);
}

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

GRPC_API grpc_err grpc_call_send_data_async(grpc_call *call, const void *data, size_t len) {
  if (call == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  grpc_message *message = grpc_message_new(data, len);
  if (message == nullptr)
    return GRPC_ERR_NOMEM;
  return grpc_call_send_async(call, message);
}

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

GRPC_API grpc_err grpc_call_finish_async(grpc_call *call, grpc_status status,
                                                  const char *message) {
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

void server_clear_events(grpc_server *server) {
  for (size_t i = 0; i < server->nb_events; i++) {
    grpc_message_free((grpc_message *)server->events[i].message);
    grpc_call_unref(server->events[i].call);
  }
  server->nb_events = 0;
}

static void dispatch_unary(grpc_call *call) {
  if (call->finished || call->closed) {
    drop_pending(call);
    return;
  }
  /* wait for the whole request */
  if (call->request_delivered || !call->half_closed)
    return;

  size_t count = grpc_list_count(&call->pending);
  if (count != 1) {
    drop_pending(call);
    (void)call_finish_internal(call, GRPC_STATUS_INTERNAL,
                               count == 0 ? "missing request message"
                                          : "more than one request message");
    return;
  }
  call->request = grpc_list_pop_front(&call->pending);
  call->request_delivered = true;
  call->route->handler(call, call->request, call->route->userdata);
}

static void dispatch_stream(grpc_call *call) {
  const grpc_stream_handler *handler = &call->route->stream;
  grpc_message *msg;
  while (!call->finished && (msg = grpc_list_pop_front(&call->pending)) != nullptr) {
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

static void dispatch_events(grpc_call *call) {
  grpc_server *server = call->server;
  grpc_message *msg;
  while (!call->finished && (msg = grpc_list_pop_front(&call->pending)) != nullptr)
    push_event(server, GRPC_EVENT_MESSAGE, call, msg);
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
