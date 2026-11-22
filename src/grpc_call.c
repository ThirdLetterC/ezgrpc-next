/* grpc_call.c - calls: their lifetime, dispatch and the call API
 *
 * The call layer is split over grpc_call.c (lifetime, header lists), grpc_call_response.c
 * (responses), grpc_call_access.c (accessors), grpc_call_async.c (the *_async functions) and
 * grpc_call_dispatch.c (dispatch). This comment describes the whole.
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
#include "internal_call.h"
#include "internal_helpers.h"
#include "grpc_timeout.h"

/* ---- header lists ------------------------------------------------------------ */

/* Appends one header to `list`, growing it as needed. The name and value are copied into one
 * grpc_malloc() block, each NUL-terminated. Returns GRPC_ERR_NOMEM on allocation failure. */
static grpc_err nv_list_add(nv_list *list, const char *name, size_t namelen, const char *value,
                            size_t valuelen) {
  if (list->len == list->cap) {
    size_t cap = list->cap ? list->cap * 2 : 4;
    grpc_nv *nva = grpc_realloc_array(list->nva, cap, sizeof(*nva));
    if (nva == nullptr)
      return GRPC_ERR_NOMEM;
    list->nva = nva;
    list->cap = cap;
  }
  /* name and value share one allocation, freed through the name; the lengths come from the
   * application, so their sum is checked */
  size_t block_len;
  if (grpc_size_add(namelen, valuelen, &block_len) || grpc_size_add(block_len, 2, &block_len))
    return GRPC_ERR_NOMEM;
  u8 *block = grpc_malloc(block_len);
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

void nv_list_free(nv_list *list) {
  for (size_t i = 0; i < list->len; i++)
    grpc_free(list->nva[i].name);
  grpc_free(list->nva);
  *list = (nv_list){};
}

grpc_err nv_list_add_metadata(nv_list *list, const grpc_metadata *metadata, size_t count) {
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

grpc_nv *call_nva_dup(const grpc_nv *nva, size_t nvlen) {
  size_t size;
  if (grpc_size_mul(nvlen, sizeof(*nva), &size))
    return nullptr;
  for (size_t i = 0; i < nvlen; i++)
    if (grpc_size_add(size, nva[i].namelen, &size) || grpc_size_add(size, nva[i].valuelen, &size))
      return nullptr;
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

void call_drop_pending(grpc_call *call) {
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

bool call_codec_supported(grpc_compression_algorithm alg) {
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

bool call_client_accepts_encoding(const grpc_call *call, grpc_compression_algorithm alg) {
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
  if (def_alg != GRPC_COMPRESS_IDENTITY && call_codec_supported(def_alg) &&
      call_client_accepts_encoding(call, def_alg)) {
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

void call_record_done_stat(grpc_call *call, grpc_status status) {
  if (call->stats_done)
    return;
  call->stats_done = true;
  if (status == GRPC_STATUS_OK)
    atomic_fetch_add(&call->server->stat_calls_ok, 1);
  else
    atomic_fetch_add(&call->server->stat_calls_error, 1);
}

void call_cancel(grpc_call *call, grpc_status reason) {
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
