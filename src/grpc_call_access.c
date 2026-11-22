/* grpc_call_access.c - reading a call's request, metadata and state, and trace context
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
