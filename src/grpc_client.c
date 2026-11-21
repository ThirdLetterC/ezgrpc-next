#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core.h"
#include "grpc_client.h"
#include "grpc_compression.h"
#include "internal_helpers.h"

struct grpc_client {
  grpc_client_config config;
  char *host;
  char *unix_path;
  char authority[280];

  uv_loop_t loop;
  bool loop_initialized;
  union {
    uv_tcp_t tcp;
    uv_pipe_t pipe;
  } handle;
  bool is_unix;
  bool connected;
  bool closed;
  int connect_status;

  uv_timer_t timer;
  bool timer_initialized;
  bool timed_out;

  nghttp2_session *ngsession;
  u8 rbuf[64 * 1024];
  grpc_buf obuf;
  grpc_buf wbuf;
  uv_write_t write_req;
  bool is_writing;

  /* Active call state (synchronous per-client call). */
  int32_t active_stream_id;
  bool stream_done;
  bool headers_received;
  grpc_compression_algorithm resp_encoding;
  grpc_buf recv_buf;
  grpc_buf send_buf;
  size_t send_off;
  grpc_client_response *resp;
  void (*on_message)(const grpc_message *msg, void *userdata);
  void *on_message_userdata;
  grpc_err call_err;
};

static inline uv_stream_t *client_stream(grpc_client *c) {
  return c->is_unix ? (uv_stream_t *)&c->handle.pipe : (uv_stream_t *)&c->handle.tcp;
}

static inline uv_handle_t *client_handle(grpc_client *c) {
  return c->is_unix ? (uv_handle_t *)&c->handle.pipe : (uv_handle_t *)&c->handle.tcp;
}

static void client_flush(grpc_client *c);

static void on_client_write(uv_write_t *req, int status) {
  grpc_client *c = req->data;
  c->is_writing = false;
  c->wbuf.len = 0;
  if (c->closed)
    return;
  if (status < 0) {
    c->closed = true;
    c->call_err = GRPC_ERR_IO;
    c->stream_done = true;
    return;
  }
  client_flush(c);
}

static void client_flush(grpc_client *c) {
  if (c->closed || c->is_writing || c->ngsession == nullptr)
    return;
  for (;;) {
    const u8 *data;
    ssize_t n = nghttp2_session_mem_send(c->ngsession, &data);
    if (n < 0) {
      c->call_err = GRPC_ERR_PROTOCOL;
      c->stream_done = true;
      return;
    }
    if (n == 0)
      break;
    if (buf_append(&c->obuf, data, (size_t)n)) {
      c->call_err = GRPC_ERR_NOMEM;
      c->stream_done = true;
      return;
    }
  }
  if (c->obuf.len == 0)
    return;
  uv_buf_t buf = uv_buf_init((char *)c->obuf.data, (unsigned int)c->obuf.len);
  int n = uv_try_write(client_stream(c), &buf, 1);
  if (n == UV_EAGAIN)
    n = 0;
  if (n < 0) {
    c->closed = true;
    c->call_err = GRPC_ERR_IO;
    c->stream_done = true;
    return;
  }
  if ((size_t)n == c->obuf.len) {
    c->obuf.len = 0;
    return;
  }
  grpc_buf tmp = c->wbuf;
  c->wbuf = c->obuf;
  c->obuf = tmp;
  c->obuf.len = 0;
  buf = uv_buf_init((char *)c->wbuf.data + n, (unsigned int)(c->wbuf.len - (size_t)n));
  c->write_req.data = c;
  if (uv_write(&c->write_req, client_stream(c), &buf, 1, on_client_write) != 0) {
    c->closed = true;
    c->call_err = GRPC_ERR_IO;
    c->stream_done = true;
    return;
  }
  c->is_writing = true;
}

static void md_list_append(grpc_metadata **arr, size_t *count, const uint8_t *name, size_t nlen,
                           const uint8_t *val, size_t vlen) {
  grpc_metadata *next = grpc_realloc(*arr, (*count + 1) * sizeof(*next));
  if (next == nullptr)
    return;
  *arr = next;
  char *k = grpc_strndup((const char *)name, nlen);
  char *v = grpc_strndup((const char *)val, vlen);
  if (k == nullptr || v == nullptr) {
    grpc_free(k);
    grpc_free(v);
    return;
  }
  (*arr)[(*count)++] = (grpc_metadata){.key = k, .key_len = nlen, .value = v, .value_len = vlen};
}

static int client_on_header([[maybe_unused]] nghttp2_session *session, const nghttp2_frame *frame,
                            const uint8_t *name, size_t namelen, const uint8_t *value,
                            size_t valuelen, [[maybe_unused]] uint8_t flags, void *user_data) {
  grpc_client *c = user_data;
  if (frame->hd.type != NGHTTP2_HEADERS || frame->hd.stream_id != c->active_stream_id ||
      c->resp == nullptr)
    return 0;

  if (namelen == 11 && memcmp(name, "grpc-status", 11) == 0) {
    char tmp[16];
    size_t n = valuelen < sizeof(tmp) - 1 ? valuelen : sizeof(tmp) - 1;
    memcpy(tmp, value, n);
    tmp[n] = '\0';
    c->resp->status = (grpc_status)atoi(tmp);
  } else if (namelen == 12 && memcmp(name, "grpc-message", 12) == 0) {
    grpc_free(c->resp->status_message);
    c->resp->status_message = grpc_strndup((const char *)value, valuelen);
  } else if (namelen == 13 && memcmp(name, "grpc-encoding", 13) == 0) {
    if (valuelen == 4 && memcmp(value, "gzip", 4) == 0)
      c->resp_encoding = GRPC_COMPRESS_GZIP;
    else if (valuelen == 7 && memcmp(value, "deflate", 7) == 0)
      c->resp_encoding = GRPC_COMPRESS_DEFLATE;
    else if (valuelen == 4 && memcmp(value, "zstd", 4) == 0)
      c->resp_encoding = GRPC_COMPRESS_ZSTD;
    else if (valuelen == 6 && memcmp(value, "snappy", 6) == 0)
      c->resp_encoding = GRPC_COMPRESS_SNAPPY;
    else
      c->resp_encoding = GRPC_COMPRESS_IDENTITY;
  }

  if (!c->headers_received && !(frame->hd.flags & NGHTTP2_FLAG_END_STREAM))
    md_list_append(&c->resp->headers, &c->resp->nb_headers, name, namelen, value, valuelen);
  else
    md_list_append(&c->resp->trailers, &c->resp->nb_trailers, name, namelen, value, valuelen);
  return 0;
}

static void client_drain_messages(grpc_client *c) {
  size_t off = 0;
  while (c->recv_buf.len - off >= 5) {
    uint8_t cflag = c->recv_buf.data[off];
    uint32_t mlen = ntohl(uread_u32(c->recv_buf.data + off + 1));
    if (c->recv_buf.len - off < 5 + (size_t)mlen)
      break;
    const uint8_t *payload = c->recv_buf.data + off + 5;
    grpc_message *msg = nullptr;
    if (cflag == 1 && c->resp_encoding != GRPC_COMPRESS_IDENTITY) {
      uint8_t *decomp = nullptr;
      size_t dlen = 0;
      grpc_err derr = grpc_decompress_limited(c->resp_encoding, payload, mlen, &decomp, &dlen,
                                              c->config.max_message_size);
      if (derr == GRPC_OK) {
        msg = message_create(0, decomp, dlen);
        grpc_free(decomp);
      } else {
        c->call_err = derr;
        c->stream_done = true;
        return;
      }
    } else {
      msg = message_create(cflag, payload, mlen);
    }
    if (msg == nullptr) {
      c->call_err = GRPC_ERR_NOMEM;
      c->stream_done = true;
      return;
    }
    if (c->on_message != nullptr) {
      c->on_message(msg, c->on_message_userdata);
      grpc_message_free(msg);
    } else {
      grpc_message_free(c->resp->message);
      c->resp->message = msg;
    }
    off += 5 + (size_t)mlen;
  }
  if (off > 0) {
    memmove(c->recv_buf.data, c->recv_buf.data + off, c->recv_buf.len - off);
    c->recv_buf.len -= off;
  }
}

static int client_on_data_chunk([[maybe_unused]] nghttp2_session *session,
                                [[maybe_unused]] uint8_t flags, int32_t stream_id,
                                const uint8_t *data, size_t len, void *user_data) {
  grpc_client *c = user_data;
  if (stream_id != c->active_stream_id)
    return 0;
  if (c->recv_buf.len + len > c->config.max_message_size + 5) {
    c->call_err = GRPC_ERR_LIMIT;
    c->stream_done = true;
    return 0;
  }
  if (buf_append(&c->recv_buf, data, len)) {
    c->call_err = GRPC_ERR_NOMEM;
    c->stream_done = true;
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  client_drain_messages(c);
  return 0;
}

static int client_on_frame_recv([[maybe_unused]] nghttp2_session *session,
                                const nghttp2_frame *frame, void *user_data) {
  grpc_client *c = user_data;
  if (frame->hd.stream_id == c->active_stream_id) {
    if (frame->hd.type == NGHTTP2_HEADERS && (frame->hd.flags & NGHTTP2_FLAG_END_HEADERS))
      c->headers_received = true;
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
      c->stream_done = true;
  }
  return 0;
}

static int client_on_stream_close([[maybe_unused]] nghttp2_session *session, int32_t stream_id,
                                  uint32_t error_code, void *user_data) {
  grpc_client *c = user_data;
  if (stream_id == c->active_stream_id) {
    if (error_code != NGHTTP2_NO_ERROR && c->call_err == GRPC_OK)
      c->call_err = GRPC_ERR_CLOSED;
    c->stream_done = true;
  }
  return 0;
}

static ssize_t client_data_read([[maybe_unused]] nghttp2_session *session,
                                [[maybe_unused]] int32_t stream_id, uint8_t *buf, size_t length,
                                uint32_t *data_flags, nghttp2_data_source *source,
                                [[maybe_unused]] void *user_data) {
  grpc_client *c = source->ptr;
  size_t rem = c->send_buf.len - c->send_off;
  size_t n = rem < length ? rem : length;
  if (n > 0) {
    memcpy(buf, c->send_buf.data + c->send_off, n);
    c->send_off += n;
  }
  if (c->send_off == c->send_buf.len)
    *data_flags = NGHTTP2_DATA_FLAG_EOF;
  return (ssize_t)n;
}

static void on_client_alloc(uv_handle_t *handle, [[maybe_unused]] size_t suggested_size,
                            uv_buf_t *buf) {
  grpc_client *c = handle->data;
  *buf = uv_buf_init((char *)c->rbuf, (unsigned int)sizeof(c->rbuf));
}

static void on_client_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  grpc_client *c = stream->data;
  if (nread == 0)
    return;
  if (nread < 0) {
    c->closed = true;
    if (!c->stream_done) {
      c->call_err = GRPC_ERR_CLOSED;
      c->stream_done = true;
    }
    return;
  }
  ssize_t rv = nghttp2_session_mem_recv(c->ngsession, (const uint8_t *)buf->base, (size_t)nread);
  if (rv < 0) {
    c->call_err = GRPC_ERR_PROTOCOL;
    c->stream_done = true;
    return;
  }
  client_flush(c);
}

static void on_client_connect(uv_connect_t *req, int status) {
  grpc_client *c = req->data;
  c->connect_status = status;
  c->connected = (status == 0);
}

static void on_client_timeout(uv_timer_t *timer) {
  grpc_client *c = timer->data;
  c->timed_out = true;
  c->call_err = GRPC_ERR_TIMEOUT;
  c->stream_done = true;
}

GRPC_API grpc_client *grpc_client_new(const grpc_client_config *config, grpc_err *out_err) {
  if (out_err != nullptr)
    *out_err = GRPC_OK;
  if (config == nullptr || (config->unix_path == nullptr && config->port == 0)) {
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

  grpc_err err = GRPC_ERR_NOMEM;
  const char *host = config->host != nullptr ? config->host : "127.0.0.1";
  c->host = grpc_strdup(host);
  if (c->host == nullptr)
    goto fail;
  if (config->unix_path != nullptr) {
    c->unix_path = grpc_strdup(config->unix_path);
    if (c->unix_path == nullptr)
      goto fail;
    c->is_unix = true;
    snprintf(c->authority, sizeof(c->authority), "localhost");
  } else {
    snprintf(c->authority, sizeof(c->authority), "%s:%u", c->host, config->port);
  }

  err = GRPC_ERR_IO;
  if (uv_loop_init(&c->loop) != 0)
    goto fail;
  c->loop_initialized = true;
  if (uv_timer_init(&c->loop, &c->timer) != 0)
    goto fail;
  c->timer.data = c;
  c->timer_initialized = true;

  uv_connect_t conn_req = {.data = c};
  c->connect_status = -1;
  if (c->is_unix) {
    if (uv_pipe_init(&c->loop, &c->handle.pipe, 0) != 0)
      goto fail;
    c->handle.pipe.data = c;
    uv_pipe_connect(&conn_req, &c->handle.pipe, c->unix_path, on_client_connect);
  } else {
    if (uv_tcp_init(&c->loop, &c->handle.tcp) != 0)
      goto fail;
    c->handle.tcp.data = c;
    struct sockaddr_storage ss = {0};
    if (uv_ip4_addr(c->host, c->config.port, (struct sockaddr_in *)&ss) != 0 &&
        uv_ip6_addr(c->host, c->config.port, (struct sockaddr_in6 *)&ss) != 0) {
      struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
      struct addrinfo *res = nullptr;
      char port_str[16];
      snprintf(port_str, sizeof(port_str), "%u", c->config.port);
      if (getaddrinfo(c->host, port_str, &hints, &res) != 0 || res == nullptr) {
        if (res != nullptr)
          freeaddrinfo(res);
        goto fail;
      }
      memcpy(&ss, res->ai_addr, res->ai_addrlen);
      freeaddrinfo(res);
    }
    if (uv_tcp_connect(&conn_req, &c->handle.tcp, (const struct sockaddr *)&ss,
                       on_client_connect) != 0)
      goto fail;
  }

  while (!c->connected && c->connect_status == -1)
    uv_run(&c->loop, UV_RUN_ONCE);
  if (!c->connected)
    goto fail;

  if (!c->is_unix)
    (void)uv_tcp_nodelay(&c->handle.tcp, 1);

  nghttp2_session_callbacks *cbs = nullptr;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    err = GRPC_ERR_NOMEM;
    goto fail;
  }
  nghttp2_session_callbacks_set_on_header_callback(cbs, client_on_header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, client_on_data_chunk);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, client_on_frame_recv);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, client_on_stream_close);
  int rv = nghttp2_session_client_new(&c->ngsession, cbs, c);
  nghttp2_session_callbacks_del(cbs);
  if (rv != 0) {
    err = GRPC_ERR_PROTOCOL;
    goto fail;
  }

  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100},
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, c->config.max_message_size},
  };
  if (nghttp2_submit_settings(c->ngsession, NGHTTP2_FLAG_NONE, iv, 2) != 0) {
    err = GRPC_ERR_PROTOCOL;
    goto fail;
  }
  if (uv_read_start(client_stream(c), on_client_alloc, on_client_read) != 0)
    goto fail;
  client_flush(c);
  return c;

fail:
  if (out_err != nullptr)
    *out_err = err;
  grpc_client_free(c);
  return nullptr;
}

GRPC_API void grpc_client_free(grpc_client *c) {
  if (c == nullptr)
    return;
  c->closed = true;
  if (c->ngsession != nullptr) {
    nghttp2_session_del(c->ngsession);
    c->ngsession = nullptr;
  }
  if (c->loop_initialized) {
    if (c->connected || client_handle(c)->type != 0)
      uv_close(client_handle(c), nullptr);
    if (c->timer_initialized)
      uv_close((uv_handle_t *)&c->timer, nullptr);
    int limit = 100;
    while (uv_loop_alive(&c->loop) && limit-- > 0)
      uv_run(&c->loop, UV_RUN_NOWAIT);
    uv_loop_close(&c->loop);
  }
  buf_free(&c->obuf);
  buf_free(&c->wbuf);
  buf_free(&c->recv_buf);
  buf_free(&c->send_buf);
  grpc_free(c->host);
  grpc_free(c->unix_path);
  grpc_free(c);
}

GRPC_API void grpc_client_response_free(grpc_client_response *response) {
  if (response == nullptr)
    return;
  grpc_free(response->status_message);
  grpc_message_free(response->message);
  for (size_t i = 0; i < response->nb_headers; i++) {
    grpc_free((void *)response->headers[i].key);
    grpc_free((void *)response->headers[i].value);
  }
  grpc_free(response->headers);
  for (size_t i = 0; i < response->nb_trailers; i++) {
    grpc_free((void *)response->trailers[i].key);
    grpc_free((void *)response->trailers[i].value);
  }
  grpc_free(response->trailers);
  *response = (grpc_client_response){0};
}

static grpc_err client_execute_call(grpc_client *c, const char *method, const void *req_data,
                                    size_t req_len, const grpc_metadata *metadata,
                                    size_t nb_metadata, uint32_t timeout_ms,
                                    void (*on_message)(const grpc_message *msg, void *userdata),
                                    void *userdata, grpc_client_response *out) {
  if (c == nullptr || method == nullptr || method[0] != '/' || out == nullptr ||
      (req_data == nullptr && req_len > 0) || (metadata == nullptr && nb_metadata > 0))
    return GRPC_ERR_INVALID;
  if (c->closed || c->ngsession == nullptr)
    return GRPC_ERR_CLOSED;

  *out = (grpc_client_response){.status = GRPC_STATUS_UNKNOWN};
  c->resp = out;
  c->on_message = on_message;
  c->on_message_userdata = userdata;
  c->stream_done = false;
  c->headers_received = false;
  c->resp_encoding = GRPC_COMPRESS_IDENTITY;
  c->recv_buf.len = 0;
  c->send_buf.len = 0;
  c->send_off = 0;
  c->timed_out = false;
  c->call_err = GRPC_OK;

  /* Prepare 5-byte gRPC frame (optionally compressed). */
  uint8_t cflag = 0;
  uint8_t *comp_buf = nullptr;
  const uint8_t *body = req_data;
  size_t body_len = req_len;
  const char *enc_name = nullptr;
  if (c->config.compression != GRPC_COMPRESS_IDENTITY) {
    int lvl = c->config.compression_level == 0 ? GRPC_COMPRESSION_LEVEL_DEFAULT
                                               : c->config.compression_level;
    grpc_err cerr = grpc_compress(c->config.compression, lvl, req_data, req_len, &comp_buf,
                                  &body_len);
    if (cerr != GRPC_OK)
      return cerr;
    cflag = 1;
    body = comp_buf;
    switch (c->config.compression) {
    case GRPC_COMPRESS_GZIP: enc_name = "gzip"; break;
    case GRPC_COMPRESS_DEFLATE: enc_name = "deflate"; break;
    case GRPC_COMPRESS_ZSTD: enc_name = "zstd"; break;
    case GRPC_COMPRESS_SNAPPY: enc_name = "snappy"; break;
    default: break;
    }
  }

  uint8_t prefix[5];
  prefix[0] = cflag;
  uint32_t net_len = htonl((uint32_t)body_len);
  memcpy(prefix + 1, &net_len, 4);
  if (buf_append(&c->send_buf, prefix, 5) || (body_len > 0 && buf_append(&c->send_buf, body, body_len))) {
    grpc_free(comp_buf);
    return GRPC_ERR_NOMEM;
  }
  grpc_free(comp_buf);

  uint32_t eff_timeout = timeout_ms ? timeout_ms : c->config.timeout_ms;
  char timeout_str[32] = {0};
  if (eff_timeout > 0)
    snprintf(timeout_str, sizeof(timeout_str), "%um", eff_timeout);

  size_t max_hdrs = 8 + nb_metadata;
  nghttp2_nv *nva = grpc_malloc(max_hdrs * sizeof(*nva));
  if (nva == nullptr)
    return GRPC_ERR_NOMEM;

  size_t n = 0;
  nva[n++] = (nghttp2_nv){(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":path", (uint8_t *)method, 5, strlen(method), NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":authority", (uint8_t *)c->authority, 10, strlen(c->authority), NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-accept-encoding", (uint8_t *)"gzip,deflate,zstd,snappy,identity", 20, 33, NGHTTP2_NV_FLAG_NONE};
  if (enc_name != nullptr)
    nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-encoding", (uint8_t *)enc_name, 13, strlen(enc_name), NGHTTP2_NV_FLAG_NONE};
  if (eff_timeout > 0)
    nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-timeout", (uint8_t *)timeout_str, 12, strlen(timeout_str), NGHTTP2_NV_FLAG_NONE};
  for (size_t i = 0; i < nb_metadata; i++) {
    if (metadata[i].key == nullptr || metadata[i].value == nullptr) {
      grpc_free(nva);
      return GRPC_ERR_INVALID;
    }
    size_t klen = metadata[i].key_len ? metadata[i].key_len : strlen(metadata[i].key);
    size_t vlen = metadata[i].value_len ? metadata[i].value_len : strlen(metadata[i].value);
    nva[n++] = (nghttp2_nv){(uint8_t *)metadata[i].key, (uint8_t *)metadata[i].value, klen, vlen, NGHTTP2_NV_FLAG_NONE};
  }

  nghttp2_data_provider dp = {.source.ptr = c, .read_callback = client_data_read};
  int32_t sid = nghttp2_submit_request(c->ngsession, nullptr, nva, n, &dp, c);
  grpc_free(nva);
  if (sid < 0)
    return GRPC_ERR_PROTOCOL;
  c->active_stream_id = sid;
  client_flush(c);

  if (eff_timeout > 0)
    uv_timer_start(&c->timer, on_client_timeout, eff_timeout, 0);
  while (!c->stream_done && !c->closed)
    uv_run(&c->loop, UV_RUN_ONCE);
  if (eff_timeout > 0)
    uv_timer_stop(&c->timer);

  if (c->timed_out) {
    (void)nghttp2_submit_rst_stream(c->ngsession, NGHTTP2_FLAG_NONE, sid, NGHTTP2_CANCEL);
    client_flush(c);
  }
  c->active_stream_id = 0;
  c->resp = nullptr;
  return c->call_err;
}

GRPC_API grpc_err grpc_client_unary(grpc_client *client, const char *method, const void *req_data,
                                    size_t req_len, const grpc_metadata *metadata,
                                    size_t nb_metadata, uint32_t timeout_ms,
                                    grpc_client_response *out) {
  return client_execute_call(client, method, req_data, req_len, metadata, nb_metadata, timeout_ms,
                             nullptr, nullptr, out);
}

GRPC_API grpc_err grpc_client_server_stream(
    grpc_client *client, const char *method, const void *req_data, size_t req_len,
    const grpc_metadata *metadata, size_t nb_metadata, uint32_t timeout_ms,
    void (*on_message)(const grpc_message *msg, void *userdata), void *userdata,
    grpc_client_response *out) {
  return client_execute_call(client, method, req_data, req_len, metadata, nb_metadata, timeout_ms,
                             on_message, userdata, out);
}
