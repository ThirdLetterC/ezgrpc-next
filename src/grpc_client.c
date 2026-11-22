/* grpc_client.c - synchronous gRPC client over HTTP/2 (nghttp2 + libuv)
 *
 * The client is split over grpc_client.c (the API, streams, retries), grpc_client_connect.c
 * (subchannel connect / TLS / teardown, targets), grpc_client_io.c (socket I/O and nghttp2
 * callbacks) and grpc_client_response.c (response handling, shared with HTTP/3). This comment
 * describes the whole.
 *
 * A grpc_client owns one subchannel per target (a single host/port or Unix socket, or each entry
 * of config.targets). Each subchannel has its own TCP/pipe handle, nghttp2 client session and a
 * private libuv loop. Nothing runs in the background: the loop is driven with uv_run only while
 * the caller blocks inside a client function (connect, recv, finish), so a client and its streams
 * must be used from one thread at a time. Requests pick a subchannel round-robin; a subchannel
 * that failed is reconnected lazily on its next use.
 *
 * Each subchannel tracks a single active_stream that the nghttp2 callbacks route data to, so one
 * subchannel carries one RPC at a time. A per-subchannel timer bounds connect/TLS handshake
 * (config.timeout_ms) and then the active RPC's deadline, which is also sent as grpc-timeout.
 * Unary and server-streaming calls retry transport failures and UNAVAILABLE with exponential
 * backoff. TLS is supported only with OpenSSL-compatible backends, using memory BIOs so libuv
 * still owns the socket.
 *
 * Responses, messages and metadata handed to the caller are grpc_malloc memory; release them with
 * grpc_client_response_free / grpc_message_free (grpc_free), never libc free.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core.h"
#include "grpc_client.h"
#include "grpc_compression.h"
#include "internal_helpers.h"
#include "internal_client.h"
#include "internal_http3.h"

/* Timer callback: the active RPC's deadline elapsed. Marks it timed out with GRPC_ERR_TIMEOUT;
 * grpc_client_stream_finish then cancels the HTTP/2 stream. */
static void on_sub_timeout(uv_timer_t *timer) {
  client_subchannel *s = timer->data;
  if (s->active_stream != nullptr) {
    s->active_stream->timed_out = true;
    s->active_stream->err = GRPC_ERR_TIMEOUT;
    s->active_stream->stream_done = true;
  }
}

/* ---- Public client lifecycle ------------------------------------------------ */

/* Copies the config by value: host, unix_path and targets are duplicated or parsed here, but
 * tls.server_name is read again on every reconnect and must outlive the client. Applies the
 * defaults (4 MiB max message, 50 ms backoff), builds the SSL_CTX, then connects eagerly: a
 * single target must connect, while a target list succeeds if any target connects (the others
 * reconnect lazily). On failure, frees everything through grpc_client_free. */
GRPC_API grpc_client *grpc_client_new(const grpc_client_config *config, grpc_err *out_err) {
  if (out_err != nullptr)
    *out_err = GRPC_OK;
  ignore_default_sigpipe();
  if (config == nullptr ||
      (config->nb_targets == 0 && config->unix_path == nullptr && config->port == 0) ||
      (config->nb_targets > 0 && config->targets == nullptr)) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_INVALID;
    return nullptr;
  }

  grpc_client *c = grpc_calloc(1, sizeof(*c));
  if (c == nullptr) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  c->config = *config;
  if (c->config.max_message_size == 0)
    c->config.max_message_size = 4u << 20;
  if (c->config.retry_backoff_ms == 0)
    c->config.retry_backoff_ms = 50;

  grpc_err err = GRPC_OK;
  if (config->transport == GRPC_TRANSPORT_HTTP3) {
    /* QUIC always uses TLS, and never a Unix socket */
    bool any_unix = config->unix_path != nullptr;
    for (size_t i = 0; i < config->nb_targets && config->targets != nullptr; i++)
      any_unix = any_unix || strncmp(config->targets[i], "unix:", 5) == 0;
    if (!grpc_http3_available()) {
      err = GRPC_ERR_UNSUPPORTED;
      goto fail;
    }
    if (!config->tls.enable || any_unix) {
      err = GRPC_ERR_INVALID;
      goto fail;
    }
    c->quic_tls_ctx = h3c_tls_ctx_new(config);
    if (c->quic_tls_ctx == nullptr) {
      err = GRPC_ERR_INVALID;
      goto fail;
    }
  } else if (config->transport != GRPC_TRANSPORT_HTTP2) {
    err = GRPC_ERR_INVALID;
    goto fail;
  } else if (config->tls.enable) {
#if defined(GRPC_CLIENT_HAS_OPENSSL)
    c->ssl_ctx = client_ssl_ctx_new(config);
    if (c->ssl_ctx == nullptr) {
      err = GRPC_ERR_INVALID;
      goto fail;
    }
#else
    err = GRPC_ERR_UNSUPPORTED;
    goto fail;
#endif
  }

  c->nb_subs = config->nb_targets > 0 ? config->nb_targets : 1;
  c->subs = grpc_calloc(c->nb_subs, sizeof(*c->subs));
  if (c->subs == nullptr) {
    err = GRPC_ERR_NOMEM;
    goto fail;
  }

  if (config->nb_targets > 0) {
    for (size_t i = 0; i < config->nb_targets; i++) {
      char *host = nullptr, *upath = nullptr;
      uint16_t port = 0;
      err = client_parse_target(config->targets[i], &host, &port, &upath);
      if (err != GRPC_OK)
        goto fail;
      err = client_init_subchannel(&c->subs[i], c, host, port, upath);
      grpc_free(host);
      grpc_free(upath);
      if (err != GRPC_OK)
        goto fail;
    }
    /* Eagerly connect at least one target to fail fast if all are unreachable. */
    bool any_ok = false;
    grpc_err last_err = GRPC_ERR_IO;
    for (size_t i = 0; i < c->nb_subs; i++) {
      grpc_err cerr = sub_connect(&c->subs[i], c->config.timeout_ms);
      if (cerr == GRPC_OK)
        any_ok = true;
      else
        last_err = cerr;
    }
    if (!any_ok) {
      /* report why the (last) target failed, e.g. GRPC_ERR_NOMEM, not always GRPC_ERR_IO */
      err = last_err;
      goto fail;
    }
  } else {
    err = client_init_subchannel(&c->subs[0], c, config->host, config->port, config->unix_path);
    if (err != GRPC_OK)
      goto fail;
    err = sub_connect(&c->subs[0], c->config.timeout_ms);
    if (err != GRPC_OK)
      goto fail;
  }

  return c;

fail:
  if (out_err != nullptr)
    *out_err = err;
  grpc_client_free(c);
  return nullptr;
}

/* Disconnects every subchannel (running each loop until its handles close) and frees all
 * buffers. Any grpc_client_stream still open on this client must be finished or cancelled
 * first, since streams point into the subchannels. */
GRPC_API void grpc_client_free(grpc_client *c) {
  if (c == nullptr)
    return;
  if (c->subs != nullptr) {
    for (size_t i = 0; i < c->nb_subs; i++) {
      sub_disconnect(&c->subs[i]);
      buf_free(&c->subs[i].obuf);
      buf_free(&c->subs[i].wbuf);
      buf_free(&c->subs[i].tls_obuf);
      grpc_free(c->subs[i].host);
      grpc_free(c->subs[i].unix_path);
    }
    grpc_free(c->subs);
  }
#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (c->ssl_ctx != nullptr)
    SSL_CTX_free(c->ssl_ctx);
#endif
  if (c->quic_tls_ctx != nullptr) {
    h3c_tls_ctx_free(c->quic_tls_ctx);
    http3_thread_cleanup();
  }
  grpc_free(c);
}

/* Frees the status message, message and every metadata key/value, then zeroes *response. */
GRPC_API void grpc_client_response_free(grpc_client_response *response) {
  if (response == nullptr)
    return;
  grpc_free(response->status_message);
  grpc_message_free(response->message);
  for (size_t i = 0; i < response->nb_headers; i++) {
    grpc_free(grpc_unconst(response->headers[i].key));
    grpc_free(grpc_unconst(response->headers[i].value));
  }
  grpc_free(response->headers);
  for (size_t i = 0; i < response->nb_trailers; i++) {
    grpc_free(grpc_unconst(response->trailers[i].key));
    grpc_free(grpc_unconst(response->trailers[i].value));
  }
  grpc_free(response->trailers);
  *response = (grpc_client_response){0};
}

/* ---- Streaming & framing helpers -------------------------------------------- */

/* Returns the next subchannel in round-robin order, regardless of its connection state,
 * skipping subchannels busy with an open stream when an idle one exists. If all are busy it
 * returns the next one anyway and stream_open_on_sub() reports GRPC_ERR_STATE. */
static client_subchannel *pick_subchannel(grpc_client *c) {
  for (size_t i = 0; i < c->nb_subs; i++) {
    client_subchannel *s = &c->subs[c->rr_next++ % c->nb_subs];
    if (s->active_stream == nullptr)
      return s;
  }
  return &c->subs[c->rr_next++ % c->nb_subs];
}

/* Appends one gRPC length-prefixed message to dst, compressing it with config.compression first
 * (and setting the compressed flag) unless that is identity. Returns GRPC_OK, the compressor's
 * error, or GRPC_ERR_NOMEM. */
static grpc_err frame_message_into(grpc_client *c, grpc_buf *dst, const void *data, size_t len) {
  uint8_t cflag = 0;
  uint8_t *comp_buf = nullptr;
  const uint8_t *body = data;
  size_t body_len = len;
  if (c->config.compression != GRPC_COMPRESS_IDENTITY) {
    int lvl = c->config.compression_level == 0 ? GRPC_COMPRESSION_LEVEL_DEFAULT
                                               : c->config.compression_level;
    grpc_err cerr = grpc_compress(c->config.compression, lvl, data, len, &comp_buf, &body_len);
    if (cerr != GRPC_OK)
      return cerr;
    cflag = 1;
    body = comp_buf;
  }
  if (body_len > UINT32_MAX) { /* the gRPC length prefix is 32 bits */
    grpc_free(comp_buf);
    return GRPC_ERR_LIMIT;
  }
  uint8_t prefix[5];
  prefix[0] = cflag;
  uint32_t net_len = htonl((uint32_t)body_len);
  memcpy(prefix + 1, &net_len, 4);
  int fail = buf_append(dst, prefix, 5) || (body_len > 0 && buf_append(dst, body, body_len));
  grpc_free(comp_buf);
  return fail ? GRPC_ERR_NOMEM : GRPC_OK;
}

/* Frees a stream and everything it owns, stopping the deadline timer and detaching it from its
 * subchannel. An HTTP/2 stream nghttp2 still has open (the server answered before the request
 * was fully sent, or the client gave up on a local error) is reset with CANCEL so the server
 * stops, and the data provider never reaches the freed stream. */
static void stream_free_internal(grpc_client_stream *st) {
  if (st == nullptr)
    return;
  client_subchannel *sub = st->sub;
  if (sub != nullptr && sub->h3c == nullptr && sub->ngsession != nullptr && !sub->closed &&
      st->stream_id > 0) {
    nghttp2_stream *ns = nghttp2_session_find_stream(sub->ngsession, (int32_t)st->stream_id);
    if (ns != nullptr && nghttp2_stream_get_state(ns) != NGHTTP2_STREAM_STATE_CLOSED) {
      (void)nghttp2_submit_rst_stream(sub->ngsession, NGHTTP2_FLAG_NONE, (int32_t)st->stream_id,
                                      NGHTTP2_CANCEL);
      if (sub->active_stream == st)
        sub->active_stream = nullptr;
      sub_flush(sub);
    }
  }
  if (st->sub != nullptr) {
    if (st->sub->timer_initialized)
      uv_timer_stop(&st->sub->timer);
    if (st->sub->active_stream == st)
      st->sub->active_stream = nullptr;
  }
  msg_queue_clear(&st->recv_queue);
  h3c_stream_free(st);
  buf_free(&st->send_buf);
  buf_free(&st->recv_buf);
  grpc_client_response_free(&st->resp);
  grpc_free(st);
}

/* Opens an RPC on a specific subchannel: (re)connects, builds the request headers (:path,
 * content-type, te, grpc-accept-encoding, grpc-encoding, grpc-timeout from timeout_ms or the
 * config default, then user metadata), submits the request with client_data_read as the body
 * source, makes it the subchannel's active stream and arms the deadline timer. Header values are
 * copied by nghttp2 during submission. Returns the stream, or nullptr with *out_err set. */
static grpc_client_stream *stream_open_on_sub(client_subchannel *s, const char *method,
                                              const grpc_metadata *metadata, size_t nb_metadata,
                                              uint32_t timeout_ms, grpc_err *out_err) {
  /* A subchannel carries one RPC at a time: every nghttp2 callback routes to active_stream, so
   * replacing it would silently orphan the stream already open there. */
  if (s->active_stream != nullptr) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_STATE;
    return nullptr;
  }
  /* the RPC's deadline covers a reconnect too: bound it by that as well */
  grpc_client *c = s->parent;
  uint32_t eff_timeout = timeout_ms ? timeout_ms : c->config.timeout_ms;
  uint64_t start_ms = uv_hrtime() / 1000000u;
  grpc_err err = sub_connect(s, eff_timeout);
  if (err != GRPC_OK) {
    if (out_err != nullptr)
      *out_err = err;
    return nullptr;
  }
  if (eff_timeout > 0) {
    uint64_t spent = uv_hrtime() / 1000000u - start_ms;
    eff_timeout = spent < eff_timeout ? eff_timeout - (uint32_t)spent : 1;
  }
  grpc_client_stream *st = grpc_calloc(1, sizeof(*st));
  if (st == nullptr) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  st->sub = s;
  st->resp.status = GRPC_STATUS_UNKNOWN;

  const char *enc_name = nullptr;
  switch (c->config.compression) {
  case GRPC_COMPRESS_GZIP:
    enc_name = "gzip";
    break;
  case GRPC_COMPRESS_DEFLATE:
    enc_name = "deflate";
    break;
  case GRPC_COMPRESS_ZSTD:
    enc_name = "zstd";
    break;
  case GRPC_COMPRESS_SNAPPY:
    enc_name = "snappy";
    break;
  default:
    break;
  }
  /* grpc-timeout allows at most 8 digits: switch to a coarser unit, rounding up */
  char timeout_str[32] = {0};
  if (eff_timeout > 0) {
    if (eff_timeout <= 99999999u)
      snprintf(timeout_str, sizeof(timeout_str), "%um", eff_timeout);
    else
      snprintf(timeout_str, sizeof(timeout_str), "%uS",
               eff_timeout / 1000u + (eff_timeout % 1000u != 0));
  }

  const char *scheme =
      c->config.tls.enable || c->config.transport == GRPC_TRANSPORT_HTTP3 ? "https" : "http";
  /* up to 9 fixed headers, then the application's metadata */
  size_t nb_nva;
  grpc_nv *nva =
      grpc_size_add(nb_metadata, 9, &nb_nva) ? nullptr : grpc_malloc_array(nb_nva, sizeof(*nva));
  if (nva == nullptr) {
    stream_free_internal(st);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  size_t n = 0;
  nva[n++] = grpc_nv_make(":method", 7, "POST", 4);
  nva[n++] = grpc_nv_make(":scheme", 7, scheme, strlen(scheme));
  nva[n++] = grpc_nv_make(":path", 5, method, strlen(method));
  nva[n++] = grpc_nv_make(":authority", 10, s->authority, strlen(s->authority));
  nva[n++] = grpc_nv_make("content-type", 12, "application/grpc", 16);
  nva[n++] = grpc_nv_make("te", 2, "trailers", 8);
  /* only the codecs this build can decode */
  static const char accept_encoding[] =
#if defined(GRPC_ENABLE_ZLIB)
      "gzip,deflate,"
#endif
#if defined(GRPC_ENABLE_ZSTD)
      "zstd,"
#endif
#if defined(GRPC_ENABLE_SNAPPY)
      "snappy,"
#endif
      "identity";
  nva[n++] = grpc_nv_make("grpc-accept-encoding", 20, accept_encoding, sizeof(accept_encoding) - 1);
  if (enc_name != nullptr)
    nva[n++] = grpc_nv_make("grpc-encoding", 13, enc_name, strlen(enc_name));
  if (eff_timeout > 0)
    nva[n++] = grpc_nv_make("grpc-timeout", 12, timeout_str, strlen(timeout_str));
  for (size_t i = 0; i < nb_metadata; i++) {
    if (metadata[i].key == nullptr || metadata[i].value == nullptr) {
      grpc_free(nva);
      stream_free_internal(st);
      if (out_err != nullptr)
        *out_err = GRPC_ERR_INVALID;
      return nullptr;
    }
    size_t klen = metadata[i].key_len ? metadata[i].key_len : strlen(metadata[i].key);
    size_t vlen = metadata[i].value_len ? metadata[i].value_len : strlen(metadata[i].value);
    if (!metadata_entry_valid(metadata[i].key, klen, metadata[i].value, vlen, false)) {
      grpc_free(nva);
      stream_free_internal(st);
      if (out_err != nullptr)
        *out_err = GRPC_ERR_INVALID;
      return nullptr;
    }
    nva[n++] = grpc_nv_make(metadata[i].key, klen, metadata[i].value, vlen);
  }

  s->active_stream = st;
  int64_t sid;
  if (s->h3c != nullptr) {
    sid = h3c_submit_request(s, st, nva, n) == 0 ? st->stream_id : -1;
  } else {
    nghttp2_data_provider dp = {.source.ptr = st, .read_callback = client_data_read};
    sid = nghttp2_submit_request(s->ngsession, nullptr, (const nghttp2_nv *)nva, n, &dp, st);
  }
  grpc_free(nva);
  if (sid < 0) {
    stream_free_internal(st);
    if (out_err != nullptr)
      *out_err = sid == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
    return nullptr;
  }
  st->stream_id = sid;
  if (eff_timeout > 0) {
    /* the loop clock stops between RPCs: refresh it, or the deadline counts from the end of the
     * previous call */
    uv_update_time(&s->loop);
    uv_timer_start(&s->timer, on_sub_timeout, eff_timeout, 0);
  }
  sub_flush(s);
  if (out_err != nullptr)
    *out_err = GRPC_OK;
  return st;
}

/* Wakes the stream's request-body source after send_buf grew or the send side closed. */
static void sub_resume(grpc_client_stream *st) {
  if (st->sub->h3c != nullptr)
    h3c_resume(st->sub, st);
  else if (st->sub->ngsession != nullptr)
    (void)nghttp2_session_resume_data(st->sub->ngsession, (int32_t)st->stream_id);
}

/* Cancels the stream on the wire: RST_STREAM(CANCEL) or RESET_STREAM(H3_REQUEST_CANCELLED). */
static void sub_reset(grpc_client_stream *st) {
  if (st->sub->h3c != nullptr)
    h3c_reset(st->sub, st);
  else if (st->sub->ngsession != nullptr)
    (void)nghttp2_submit_rst_stream(st->sub->ngsession, NGHTTP2_FLAG_NONE, (int32_t)st->stream_id,
                                    NGHTTP2_CANCEL);
}

/* Validates arguments and opens the stream on the next round-robin subchannel. */
GRPC_API grpc_client_stream *grpc_client_stream_open(grpc_client *client, const char *method,
                                                     const grpc_metadata *metadata,
                                                     size_t nb_metadata, uint32_t timeout_ms,
                                                     grpc_err *out_err) {
  if (out_err != nullptr)
    *out_err = GRPC_OK;
  if (client == nullptr || method == nullptr || method[0] != '/' ||
      (metadata == nullptr && nb_metadata > 0)) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_INVALID;
    return nullptr;
  }
  return stream_open_on_sub(pick_subchannel(client), method, metadata, nb_metadata, timeout_ms,
                            out_err);
}

/* Frames (and maybe compresses) the message into send_buf, resumes the deferred data provider
 * and flushes without waiting for the peer. Returns st->err, so an earlier asynchronous failure
 * on the stream surfaces here. */
GRPC_API grpc_err grpc_client_stream_send(grpc_client_stream *st, const void *data, size_t len) {
  if (st == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  if (st->send_closed)
    return GRPC_ERR_STATE;
  if (st->sub->closed || st->stream_done) /* report why the stream ended when it failed */
    return st->err != GRPC_OK ? st->err : GRPC_ERR_CLOSED;

  grpc_err err = frame_message_into(st->sub->parent, &st->send_buf, data, len);
  if (err != GRPC_OK)
    return err;
  sub_resume(st);
  sub_flush(st->sub);
  return st->err;
}

/* Marks the send side closed so client_data_read emits END_STREAM once send_buf drains.
 * Idempotent; returns st->err. */
GRPC_API grpc_err grpc_client_stream_close_send(grpc_client_stream *st) {
  if (st == nullptr)
    return GRPC_ERR_INVALID;
  if (st->send_closed)
    return GRPC_OK;
  st->send_closed = true;
  if (!st->sub->closed && (st->sub->ngsession != nullptr || st->sub->h3c != nullptr)) {
    sub_resume(st);
    sub_flush(st->sub);
  }
  return st->err;
}

/* Runs the subchannel loop until a message is queued or the stream ends. Returns GRPC_OK with a
 * message, or st->err with *out_msg nullptr at end of stream (GRPC_OK for a normal end). */
GRPC_API grpc_err grpc_client_stream_recv(grpc_client_stream *st, grpc_message **out_msg) {
  if (st == nullptr || out_msg == nullptr)
    return GRPC_ERR_INVALID;
  *out_msg = nullptr;
  while (msg_queue_is_empty(&st->recv_queue) && !st->stream_done && !st->sub->closed)
    uv_run(&st->sub->loop, UV_RUN_ONCE);
  *out_msg = msg_queue_pop(&st->recv_queue);
  if (*out_msg != nullptr)
    return GRPC_OK;
  return st->err;
}

/* Half-closes, runs the loop until the stream ends, and cancels it with RST_STREAM if its
 * deadline fired. Any still-queued messages are drained so that out->message holds the last
 * one (earlier ones are freed). Moves the response into *out, frees the stream and returns the
 * transport error (GRPC_OK if the RPC completed; check out->status for the gRPC status). */
GRPC_API grpc_err grpc_client_stream_finish(grpc_client_stream *st, grpc_client_response *out) {
  if (st == nullptr || out == nullptr)
    return GRPC_ERR_INVALID;
  if (!st->send_closed)
    (void)grpc_client_stream_close_send(st);
  while (!st->stream_done && !st->sub->closed)
    uv_run(&st->sub->loop, UV_RUN_ONCE);
  if (st->timed_out && !st->sub->closed) {
    sub_reset(st);
    sub_flush(st->sub);
  }
  /* Move last queued message (if any) into out->message for unary convenience. */
  grpc_message *m;
  while ((m = msg_queue_pop(&st->recv_queue)) != nullptr) {
    grpc_message_free(st->resp.message);
    st->resp.message = m;
  }
  grpc_err err = st->err;
  *out = st->resp;
  st->resp = (grpc_client_response){0};
  stream_free_internal(st);
  return err;
}

/* Sends RST_STREAM(CANCEL) if the stream is still live and flushes it without waiting, then frees
 * the stream. */
GRPC_API void grpc_client_stream_cancel(grpc_client_stream *st) {
  if (st == nullptr)
    return;
  if (!st->sub->closed && !st->stream_done) {
    sub_reset(st);
    sub_flush(st->sub);
  }
  stream_free_internal(st);
}

/* ---- Unary & server-streaming with retries and round-robin ------------------ */

/* Blocks the calling thread for ms milliseconds (no-op for 0). */
static void sleep_ms(uint32_t ms) {
  if (ms > 0)
    uv_sleep(ms);
}

/* Shared body of unary and server-streaming calls: open a stream on the next subchannel, send
 * the single request, half-close and finish. Retries up to config.max_retries times, each on the
 * next subchannel after a backoff that starts at retry_backoff_ms and doubles while below 5 s,
 * when the attempt fails with GRPC_ERR_IO / GRPC_ERR_CLOSED or completes with UNAVAILABLE. Other
 * errors return immediately. *out is reset before each attempt and holds the last attempt's
 * response. Once on_message has seen a message the call is committed (as in gRPC's retry
 * design) and is not retried, so the application never receives a message twice. */
typedef struct delivery_counter {
  void (*on_message)(const grpc_message *msg, void *userdata);
  void *userdata;
  size_t delivered;
} delivery_counter;

/* on_message trampoline for execute_with_retries(): forwards to the caller's callback and counts
 * deliveries so a committed call is not retried. */
static void count_delivery(const grpc_message *msg, void *userdata) {
  delivery_counter *dc = userdata;
  dc->delivered++;
  dc->on_message(msg, dc->userdata);
}

static grpc_err execute_with_retries(grpc_client *c, const char *method, const void *req_data,
                                     size_t req_len, const grpc_metadata *metadata,
                                     size_t nb_metadata, uint32_t timeout_ms,
                                     void (*on_message)(const grpc_message *msg, void *userdata),
                                     void *userdata, grpc_client_response *out) {
  if (c == nullptr || method == nullptr || method[0] != '/' || out == nullptr ||
      (req_data == nullptr && req_len > 0) || (metadata == nullptr && nb_metadata > 0))
    return GRPC_ERR_INVALID;

  /* 64-bit: max_retries = UINT32_MAX must not wrap to zero attempts */
  uint64_t max_attempts = 1 + (uint64_t)c->config.max_retries;
  uint32_t backoff = c->config.retry_backoff_ms;
  grpc_err last_err = GRPC_OK;
  delivery_counter dc = {.on_message = on_message, .userdata = userdata};

  for (uint64_t attempt = 0; attempt < max_attempts; attempt++) {
    if (attempt > 0) {
      grpc_client_response_free(out);
      sleep_ms(backoff);
      if (backoff < 5000)
        backoff *= 2;
    }
    *out = (grpc_client_response){.status = GRPC_STATUS_UNKNOWN};
    client_subchannel *sub = pick_subchannel(c);
    grpc_err open_err = GRPC_OK;
    grpc_client_stream *st =
        stream_open_on_sub(sub, method, metadata, nb_metadata, timeout_ms, &open_err);
    if (st == nullptr) {
      last_err = open_err;
      if (open_err == GRPC_ERR_IO || open_err == GRPC_ERR_CLOSED)
        continue;
      return open_err;
    }
    if (on_message != nullptr) {
      st->on_message = count_delivery;
      st->on_message_userdata = &dc;
    }
    grpc_err send_err = grpc_client_stream_send(st, req_data, req_len);
    if (send_err != GRPC_OK) {
      stream_free_internal(st);
      last_err = send_err;
      if (send_err == GRPC_ERR_IO || send_err == GRPC_ERR_CLOSED)
        continue;
      return send_err;
    }
    last_err = grpc_client_stream_finish(st, out);
    bool transient = (last_err == GRPC_ERR_IO || last_err == GRPC_ERR_CLOSED) ||
                     (last_err == GRPC_OK && out->status == GRPC_STATUS_UNAVAILABLE);
    /* once the server answered (response headers or a message), the call is committed: it may
     * have run, so it is never sent again, and what arrived is kept */
    bool committed = out->nb_headers > 0 || out->message != nullptr || dc.delivered > 0;
    if (!transient || committed)
      return last_err;
  }
  return last_err;
}

/* Unary call: execute_with_retries without a callback, so the reply ends up in out->message. */
GRPC_API grpc_err grpc_client_unary(grpc_client *client, const char *method, const void *req_data,
                                    size_t req_len, const grpc_metadata *metadata,
                                    size_t nb_metadata, uint32_t timeout_ms,
                                    grpc_client_response *out) {
  return execute_with_retries(client, method, req_data, req_len, metadata, nb_metadata, timeout_ms,
                              nullptr, nullptr, out);
}

/* Server-streaming call: execute_with_retries delivering each response to on_message. */
GRPC_API grpc_err grpc_client_server_stream(
    grpc_client *client, const char *method, const void *req_data, size_t req_len,
    const grpc_metadata *metadata, size_t nb_metadata, uint32_t timeout_ms,
    void (*on_message)(const grpc_message *msg, void *userdata), void *userdata,
    grpc_client_response *out) {
  return execute_with_retries(client, method, req_data, req_len, metadata, nb_metadata, timeout_ms,
                              on_message, userdata, out);
}
