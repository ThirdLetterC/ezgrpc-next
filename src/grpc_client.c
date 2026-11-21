#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "core.h"
#include "grpc_client.h"
#include "grpc_compression.h"
#include "internal_helpers.h"

#if defined(GRPC_TLS_OPENSSL) || defined(GRPC_TLS_LIBRESSL) || defined(GRPC_TLS_BORINGSSL) || \
    defined(GRPC_TLS_AWSLC)
#define GRPC_CLIENT_HAS_OPENSSL 1
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#endif

typedef struct client_subchannel client_subchannel;

struct grpc_client_stream {
  client_subchannel *sub;
  int32_t stream_id;
  bool send_closed;
  bool stream_done;
  bool headers_received;
  bool timed_out;
  grpc_err err;
  grpc_compression_algorithm resp_encoding;
  grpc_buf send_buf;
  size_t send_off;
  grpc_buf recv_buf;
  grpc_list recv_queue; /* queued grpc_message* from server */
  grpc_client_response resp;
  void (*on_message)(const grpc_message *msg, void *userdata);
  void *on_message_userdata;
};

struct client_subchannel {
  grpc_client *parent;
  char *host;
  uint16_t port;
  char *unix_path;
  char authority[280];

  uv_loop_t loop;
  bool loop_initialized;
  union {
    uv_tcp_t tcp;
    uv_pipe_t pipe;
  } handle;
  bool is_unix;
  bool handle_initialized;
  bool connected;
  bool closed;
  int connect_status;

  uv_timer_t timer;
  bool timer_initialized;

  nghttp2_session *ngsession;
  u8 rbuf[64 * 1024];
  grpc_buf obuf;
  grpc_buf wbuf;
  grpc_buf tls_obuf;
  uv_write_t write_req;
  bool is_writing;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  SSL *ssl;
  bool tls_handshake_done;
#endif

  grpc_client_stream *active_stream;
};

struct grpc_client {
  grpc_client_config config;
  client_subchannel *subs;
  size_t nb_subs;
  size_t rr_next;
#if defined(GRPC_CLIENT_HAS_OPENSSL)
  SSL_CTX *ssl_ctx;
#endif
};

static inline uv_stream_t *sub_stream(client_subchannel *s) {
  return s->is_unix ? (uv_stream_t *)&s->handle.pipe : (uv_stream_t *)&s->handle.tcp;
}

static inline uv_handle_t *sub_handle(client_subchannel *s) {
  return s->is_unix ? (uv_handle_t *)&s->handle.pipe : (uv_handle_t *)&s->handle.tcp;
}

/* ---- TLS helpers (OpenSSL-compatible backends) ------------------------------ */

#if defined(GRPC_CLIENT_HAS_OPENSSL)
static bool client_load_ca_pem(SSL_CTX *ctx, const char *pem, size_t len) {
  if (pem == nullptr || len == 0 || len > INT_MAX)
    return false;
  BIO *bio = BIO_new_mem_buf(pem, (int)len);
  if (bio == nullptr)
    return false;
  X509_STORE *store = SSL_CTX_get_cert_store(ctx);
  int loaded = 0;
  X509 *cert;
  while ((cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) != nullptr) {
    if (X509_STORE_add_cert(store, cert) > 0)
      loaded++;
    X509_free(cert);
  }
  ERR_clear_error();
  BIO_free(bio);
  return loaded > 0;
}

static SSL_CTX *client_ssl_ctx_new(const grpc_client_config *cfg) {
  SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
  if (ctx == nullptr)
    return nullptr;
  if (!SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION)) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  static const unsigned char alpn[] = {2, 'h', '2'};
  if (SSL_CTX_set_alpn_protos(ctx, alpn, sizeof(alpn)) != 0) {
    SSL_CTX_free(ctx);
    return nullptr;
  }
  if (cfg->tls.insecure_skip_verify) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    if (cfg->tls.ca_file != nullptr) {
      if (SSL_CTX_load_verify_locations(ctx, cfg->tls.ca_file, nullptr) <= 0) {
        SSL_CTX_free(ctx);
        return nullptr;
      }
    } else if (cfg->tls.ca_pem != nullptr) {
      size_t len = cfg->tls.ca_pem_len ? cfg->tls.ca_pem_len : strlen(cfg->tls.ca_pem);
      if (!client_load_ca_pem(ctx, cfg->tls.ca_pem, len)) {
        SSL_CTX_free(ctx);
        return nullptr;
      }
    } else {
      (void)SSL_CTX_set_default_verify_paths(ctx);
    }
  }
  if (cfg->tls.cert_file != nullptr || cfg->tls.key_file != nullptr) {
    if (cfg->tls.cert_file == nullptr || cfg->tls.key_file == nullptr ||
        SSL_CTX_use_certificate_chain_file(ctx, cfg->tls.cert_file) <= 0 ||
        SSL_CTX_use_PrivateKey_file(ctx, cfg->tls.key_file, SSL_FILETYPE_PEM) <= 0 ||
        !SSL_CTX_check_private_key(ctx)) {
      SSL_CTX_free(ctx);
      return nullptr;
    }
  }
  return ctx;
}

static int client_tls_drain(client_subchannel *s) {
  BIO *wbio = SSL_get_wbio(s->ssl);
  size_t pending;
  while ((pending = BIO_ctrl_pending(wbio)) > 0) {
    if (buf_reserve(&s->obuf, pending))
      return 1;
    int n = BIO_read(wbio, s->obuf.data + s->obuf.len, pending > INT_MAX ? INT_MAX : (int)pending);
    if (n <= 0)
      return 1;
    s->obuf.len += (size_t)n;
  }
  return 0;
}
#endif

/* ---- Subchannel I/O & nghttp2 callbacks ------------------------------------- */

static void sub_flush(client_subchannel *s);

static void on_sub_write(uv_write_t *req, int status) {
  client_subchannel *s = req->data;
  s->is_writing = false;
  s->wbuf.len = 0;
  if (s->closed)
    return;
  if (status < 0) {
    s->closed = true;
    if (s->active_stream != nullptr) {
      s->active_stream->err = GRPC_ERR_IO;
      s->active_stream->stream_done = true;
    }
    return;
  }
  sub_flush(s);
}

static void sub_flush(client_subchannel *s) {
  if (s->closed || s->is_writing)
    return;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    if (s->tls_handshake_done && s->ngsession != nullptr) {
      for (;;) {
        const u8 *data;
        ssize_t n = nghttp2_session_mem_send(s->ngsession, &data);
        if (n < 0) {
          if (s->active_stream != nullptr) {
            s->active_stream->err = GRPC_ERR_PROTOCOL;
            s->active_stream->stream_done = true;
          }
          return;
        }
        if (n == 0)
          break;
        if (buf_append(&s->tls_obuf, data, (size_t)n)) {
          if (s->active_stream != nullptr) {
            s->active_stream->err = GRPC_ERR_NOMEM;
            s->active_stream->stream_done = true;
          }
          return;
        }
      }
      if (s->tls_obuf.len > 0) {
        size_t off = 0;
        while (off < s->tls_obuf.len) {
          size_t chunk = s->tls_obuf.len - off;
          ERR_clear_error();
          int wr = SSL_write(s->ssl, s->tls_obuf.data + off,
                             chunk > INT_MAX ? INT_MAX : (int)chunk);
          if (wr <= 0) {
            s->closed = true;
            if (s->active_stream != nullptr) {
              s->active_stream->err = GRPC_ERR_IO;
              s->active_stream->stream_done = true;
            }
            return;
          }
          off += (size_t)wr;
        }
        s->tls_obuf.len = 0;
      }
    }
    if (client_tls_drain(s)) {
      s->closed = true;
      return;
    }
  } else
#endif
  if (s->ngsession != nullptr) {
    for (;;) {
      const u8 *data;
      ssize_t n = nghttp2_session_mem_send(s->ngsession, &data);
      if (n < 0) {
        if (s->active_stream != nullptr) {
          s->active_stream->err = GRPC_ERR_PROTOCOL;
          s->active_stream->stream_done = true;
        }
        return;
      }
      if (n == 0)
        break;
      if (buf_append(&s->obuf, data, (size_t)n)) {
        if (s->active_stream != nullptr) {
          s->active_stream->err = GRPC_ERR_NOMEM;
          s->active_stream->stream_done = true;
        }
        return;
      }
    }
  }

  if (s->obuf.len == 0)
    return;
  uv_buf_t buf = uv_buf_init((char *)s->obuf.data, (unsigned int)s->obuf.len);
  int n = uv_try_write(sub_stream(s), &buf, 1);
  if (n == UV_EAGAIN)
    n = 0;
  if (n < 0) {
    s->closed = true;
    if (s->active_stream != nullptr) {
      s->active_stream->err = GRPC_ERR_IO;
      s->active_stream->stream_done = true;
    }
    return;
  }
  if ((size_t)n == s->obuf.len) {
    s->obuf.len = 0;
    return;
  }
  grpc_buf tmp = s->wbuf;
  s->wbuf = s->obuf;
  s->obuf = tmp;
  s->obuf.len = 0;
  buf = uv_buf_init((char *)s->wbuf.data + n, (unsigned int)(s->wbuf.len - (size_t)n));
  s->write_req.data = s;
  if (uv_write(&s->write_req, sub_stream(s), &buf, 1, on_sub_write) != 0) {
    s->closed = true;
    if (s->active_stream != nullptr) {
      s->active_stream->err = GRPC_ERR_IO;
      s->active_stream->stream_done = true;
    }
    return;
  }
  s->is_writing = true;
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
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st == nullptr || frame->hd.type != NGHTTP2_HEADERS || frame->hd.stream_id != st->stream_id)
    return 0;

  if (namelen == 11 && memcmp(name, "grpc-status", 11) == 0) {
    char tmp[16];
    size_t n = valuelen < sizeof(tmp) - 1 ? valuelen : sizeof(tmp) - 1;
    memcpy(tmp, value, n);
    tmp[n] = '\0';
    st->resp.status = (grpc_status)atoi(tmp);
  } else if (namelen == 12 && memcmp(name, "grpc-message", 12) == 0) {
    grpc_free(st->resp.status_message);
    st->resp.status_message = grpc_strndup((const char *)value, valuelen);
  } else if (namelen == 13 && memcmp(name, "grpc-encoding", 13) == 0) {
    if (valuelen == 4 && memcmp(value, "gzip", 4) == 0)
      st->resp_encoding = GRPC_COMPRESS_GZIP;
    else if (valuelen == 7 && memcmp(value, "deflate", 7) == 0)
      st->resp_encoding = GRPC_COMPRESS_DEFLATE;
    else if (valuelen == 4 && memcmp(value, "zstd", 4) == 0)
      st->resp_encoding = GRPC_COMPRESS_ZSTD;
    else if (valuelen == 6 && memcmp(value, "snappy", 6) == 0)
      st->resp_encoding = GRPC_COMPRESS_SNAPPY;
    else
      st->resp_encoding = GRPC_COMPRESS_IDENTITY;
  }

  if (!st->headers_received && !(frame->hd.flags & NGHTTP2_FLAG_END_STREAM))
    md_list_append(&st->resp.headers, &st->resp.nb_headers, name, namelen, value, valuelen);
  else
    md_list_append(&st->resp.trailers, &st->resp.nb_trailers, name, namelen, value, valuelen);
  return 0;
}

static void stream_drain_messages(grpc_client_stream *st) {
  client_subchannel *s = st->sub;
  size_t off = 0;
  while (st->recv_buf.len - off >= 5) {
    uint8_t cflag = st->recv_buf.data[off];
    uint32_t mlen = ntohl(uread_u32(st->recv_buf.data + off + 1));
    if (st->recv_buf.len - off < 5 + (size_t)mlen)
      break;
    const uint8_t *payload = st->recv_buf.data + off + 5;
    grpc_message *msg = nullptr;
    if (cflag == 1 && st->resp_encoding != GRPC_COMPRESS_IDENTITY) {
      uint8_t *decomp = nullptr;
      size_t dlen = 0;
      grpc_err derr = grpc_decompress_limited(st->resp_encoding, payload, mlen, &decomp, &dlen,
                                              s->parent->config.max_message_size);
      if (derr == GRPC_OK) {
        msg = message_create(0, decomp, dlen);
        grpc_free(decomp);
      } else {
        st->err = derr;
        st->stream_done = true;
        return;
      }
    } else {
      msg = message_create(cflag, payload, mlen);
    }
    if (msg == nullptr) {
      st->err = GRPC_ERR_NOMEM;
      st->stream_done = true;
      return;
    }
    if (st->on_message != nullptr) {
      st->on_message(msg, st->on_message_userdata);
      grpc_message_free(msg);
    } else if (grpc_list_push_back(&st->recv_queue, msg) != 0) {
      grpc_message_free(msg);
      st->err = GRPC_ERR_NOMEM;
      st->stream_done = true;
      return;
    }
    off += 5 + (size_t)mlen;
  }
  if (off > 0) {
    memmove(st->recv_buf.data, st->recv_buf.data + off, st->recv_buf.len - off);
    st->recv_buf.len -= off;
  }
}

static int client_on_data_chunk([[maybe_unused]] nghttp2_session *session,
                                [[maybe_unused]] uint8_t flags, int32_t stream_id,
                                const uint8_t *data, size_t len, void *user_data) {
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st == nullptr || stream_id != st->stream_id)
    return 0;
  if (st->recv_buf.len + len > s->parent->config.max_message_size + 5) {
    st->err = GRPC_ERR_LIMIT;
    st->stream_done = true;
    return 0;
  }
  if (buf_append(&st->recv_buf, data, len)) {
    st->err = GRPC_ERR_NOMEM;
    st->stream_done = true;
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  stream_drain_messages(st);
  return 0;
}

static int client_on_frame_recv([[maybe_unused]] nghttp2_session *session,
                                const nghttp2_frame *frame, void *user_data) {
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st != nullptr && frame->hd.stream_id == st->stream_id) {
    if (frame->hd.type == NGHTTP2_HEADERS && (frame->hd.flags & NGHTTP2_FLAG_END_HEADERS))
      st->headers_received = true;
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
      st->stream_done = true;
  }
  return 0;
}

static int client_on_stream_close([[maybe_unused]] nghttp2_session *session, int32_t stream_id,
                                  uint32_t error_code, void *user_data) {
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st != nullptr && stream_id == st->stream_id) {
    if (error_code != NGHTTP2_NO_ERROR && st->err == GRPC_OK)
      st->err = GRPC_ERR_CLOSED;
    st->stream_done = true;
  }
  return 0;
}

static ssize_t client_data_read([[maybe_unused]] nghttp2_session *session,
                                [[maybe_unused]] int32_t stream_id, uint8_t *buf, size_t length,
                                uint32_t *data_flags, nghttp2_data_source *source,
                                [[maybe_unused]] void *user_data) {
  grpc_client_stream *st = source->ptr;
  size_t rem = st->send_buf.len - st->send_off;
  if (rem == 0) {
    if (st->send_closed) {
      *data_flags = NGHTTP2_DATA_FLAG_EOF;
      return 0;
    }
    return NGHTTP2_ERR_DEFERRED;
  }
  size_t n = rem < length ? rem : length;
  memcpy(buf, st->send_buf.data + st->send_off, n);
  st->send_off += n;
  if (st->send_off == st->send_buf.len) {
    st->send_buf.len = 0;
    st->send_off = 0;
    if (st->send_closed)
      *data_flags = NGHTTP2_DATA_FLAG_EOF;
  }
  return (ssize_t)n;
}

static void on_sub_alloc(uv_handle_t *handle, [[maybe_unused]] size_t suggested_size,
                         uv_buf_t *buf) {
  client_subchannel *s = handle->data;
  *buf = uv_buf_init((char *)s->rbuf, (unsigned int)sizeof(s->rbuf));
}

static void on_sub_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  client_subchannel *s = stream->data;
  if (nread == 0)
    return;
  if (nread < 0) {
    s->closed = true;
    if (s->active_stream != nullptr && !s->active_stream->stream_done) {
      s->active_stream->err = GRPC_ERR_CLOSED;
      s->active_stream->stream_done = true;
    }
    return;
  }

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    BIO *rbio = SSL_get_rbio(s->ssl);
    size_t off = 0;
    while (off < (size_t)nread) {
      size_t chunk = (size_t)nread - off;
      int w = BIO_write(rbio, buf->base + off, chunk > INT_MAX ? INT_MAX : (int)chunk);
      if (w <= 0) {
        s->closed = true;
        return;
      }
      off += (size_t)w;
    }
    if (!s->tls_handshake_done) {
      ERR_clear_error();
      int rc = SSL_do_handshake(s->ssl);
      if (rc == 1) {
        s->tls_handshake_done = true;
      } else {
        int serr = SSL_get_error(s->ssl, rc);
        if (serr != SSL_ERROR_WANT_READ && serr != SSL_ERROR_WANT_WRITE) {
          s->closed = true;
          if (s->active_stream != nullptr) {
            s->active_stream->err = GRPC_ERR_IO;
            s->active_stream->stream_done = true;
          }
          return;
        }
      }
      sub_flush(s);
      if (!s->tls_handshake_done)
        return;
    }
    for (;;) {
      ERR_clear_error();
      int n = SSL_read(s->ssl, s->rbuf, (int)sizeof(s->rbuf));
      if (n > 0) {
        ssize_t rv = nghttp2_session_mem_recv(s->ngsession, s->rbuf, (size_t)n);
        if (rv < 0) {
          if (s->active_stream != nullptr) {
            s->active_stream->err = GRPC_ERR_PROTOCOL;
            s->active_stream->stream_done = true;
          }
          return;
        }
      } else {
        int serr = SSL_get_error(s->ssl, n);
        if (serr == SSL_ERROR_WANT_READ || serr == SSL_ERROR_WANT_WRITE)
          break;
        s->closed = true;
        if (s->active_stream != nullptr && !s->active_stream->stream_done) {
          s->active_stream->err = GRPC_ERR_CLOSED;
          s->active_stream->stream_done = true;
        }
        return;
      }
    }
    sub_flush(s);
    return;
  }
#endif

  ssize_t rv = nghttp2_session_mem_recv(s->ngsession, (const uint8_t *)buf->base, (size_t)nread);
  if (rv < 0) {
    if (s->active_stream != nullptr) {
      s->active_stream->err = GRPC_ERR_PROTOCOL;
      s->active_stream->stream_done = true;
    }
    return;
  }
  sub_flush(s);
}

static void on_sub_connect(uv_connect_t *req, int status) {
  client_subchannel *s = req->data;
  s->connect_status = status;
  s->connected = (status == 0);
}

static void on_sub_timeout(uv_timer_t *timer) {
  client_subchannel *s = timer->data;
  if (s->active_stream != nullptr) {
    s->active_stream->timed_out = true;
    s->active_stream->err = GRPC_ERR_TIMEOUT;
    s->active_stream->stream_done = true;
  }
}

/* ---- Subchannel lifecycle --------------------------------------------------- */

static void sub_disconnect(client_subchannel *s) {
  s->closed = true;
  if (s->ngsession != nullptr) {
    nghttp2_session_del(s->ngsession);
    s->ngsession = nullptr;
  }
#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    SSL_free(s->ssl);
    s->ssl = nullptr;
  }
  s->tls_handshake_done = false;
#endif
  if (s->loop_initialized) {
    if (s->handle_initialized) {
      uv_close(sub_handle(s), nullptr);
      s->handle_initialized = false;
    }
    if (s->timer_initialized) {
      uv_close((uv_handle_t *)&s->timer, nullptr);
      s->timer_initialized = false;
    }
    int limit = 100;
    while (uv_loop_alive(&s->loop) && limit-- > 0)
      uv_run(&s->loop, UV_RUN_NOWAIT);
    uv_loop_close(&s->loop);
    s->loop_initialized = false;
  }
  s->connected = false;
  s->is_writing = false;
  s->obuf.len = 0;
  s->wbuf.len = 0;
  s->tls_obuf.len = 0;
}

static grpc_err sub_connect(client_subchannel *s) {
  if (s->connected && !s->closed && s->ngsession != nullptr)
    return GRPC_OK;
  sub_disconnect(s);
  s->closed = false;

  if (uv_loop_init(&s->loop) != 0)
    return GRPC_ERR_IO;
  s->loop_initialized = true;
  if (uv_timer_init(&s->loop, &s->timer) != 0)
    goto fail_io;
  s->timer.data = s;
  s->timer_initialized = true;

  uv_connect_t conn_req = {.data = s};
  s->connect_status = -1;
  if (s->is_unix) {
    if (uv_pipe_init(&s->loop, &s->handle.pipe, 0) != 0)
      goto fail_io;
    s->handle.pipe.data = s;
    s->handle_initialized = true;
    uv_pipe_connect(&conn_req, &s->handle.pipe, s->unix_path, on_sub_connect);
  } else {
    if (uv_tcp_init(&s->loop, &s->handle.tcp) != 0)
      goto fail_io;
    s->handle.tcp.data = s;
    s->handle_initialized = true;
    struct sockaddr_storage ss = {0};
    if (uv_ip4_addr(s->host, s->port, (struct sockaddr_in *)&ss) != 0 &&
        uv_ip6_addr(s->host, s->port, (struct sockaddr_in6 *)&ss) != 0) {
      struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
      struct addrinfo *res = nullptr;
      char port_str[16];
      snprintf(port_str, sizeof(port_str), "%u", s->port);
      if (getaddrinfo(s->host, port_str, &hints, &res) != 0 || res == nullptr) {
        if (res != nullptr)
          freeaddrinfo(res);
        goto fail_io;
      }
      memcpy(&ss, res->ai_addr, res->ai_addrlen);
      freeaddrinfo(res);
    }
    if (uv_tcp_connect(&conn_req, &s->handle.tcp, (const struct sockaddr *)&ss, on_sub_connect) !=
        0)
      goto fail_io;
  }

  while (!s->connected && s->connect_status == -1)
    uv_run(&s->loop, UV_RUN_ONCE);
  if (!s->connected)
    goto fail_io;

  if (!s->is_unix)
    (void)uv_tcp_nodelay(&s->handle.tcp, 1);

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->parent->ssl_ctx != nullptr) {
    s->ssl = SSL_new(s->parent->ssl_ctx);
    if (s->ssl == nullptr)
      goto fail_io;
    BIO *rbio = BIO_new(BIO_s_mem());
    BIO *wbio = BIO_new(BIO_s_mem());
    if (rbio == nullptr || wbio == nullptr) {
      BIO_free(rbio);
      BIO_free(wbio);
      goto fail_io;
    }
    BIO_set_mem_eof_return(rbio, -1);
    SSL_set_bio(s->ssl, rbio, wbio);
    const char *sni = s->parent->config.tls.server_name != nullptr
                          ? s->parent->config.tls.server_name
                          : s->host;
    if (sni != nullptr && sni[0] != '\0')
      (void)SSL_set_tlsext_host_name(s->ssl, sni);
    SSL_set_connect_state(s->ssl);
  }
#endif

  nghttp2_session_callbacks *cbs = nullptr;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    sub_disconnect(s);
    return GRPC_ERR_NOMEM;
  }
  nghttp2_session_callbacks_set_on_header_callback(cbs, client_on_header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, client_on_data_chunk);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, client_on_frame_recv);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, client_on_stream_close);
  int rv = nghttp2_session_client_new(&s->ngsession, cbs, s);
  nghttp2_session_callbacks_del(cbs);
  if (rv != 0) {
    sub_disconnect(s);
    return GRPC_ERR_PROTOCOL;
  }

  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100},
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, s->parent->config.max_message_size},
  };
  if (nghttp2_submit_settings(s->ngsession, NGHTTP2_FLAG_NONE, iv, 2) != 0) {
    sub_disconnect(s);
    return GRPC_ERR_PROTOCOL;
  }
  if (uv_read_start(sub_stream(s), on_sub_alloc, on_sub_read) != 0)
    goto fail_io;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    ERR_clear_error();
    (void)SSL_do_handshake(s->ssl);
    sub_flush(s);
    while (!s->tls_handshake_done && !s->closed)
      uv_run(&s->loop, UV_RUN_ONCE);
    if (!s->tls_handshake_done || s->closed)
      goto fail_io;
  }
#endif
  sub_flush(s);
  return GRPC_OK;

fail_io:
  sub_disconnect(s);
  return GRPC_ERR_IO;
}

static bool parse_target(const char *target, char **out_host, uint16_t *out_port,
                         char **out_unix) {
  *out_host = nullptr;
  *out_port = 0;
  *out_unix = nullptr;
  if (target == nullptr || target[0] == '\0')
    return false;
  if (strncmp(target, "unix:", 5) == 0) {
    const char *p = target + 5;
    if (strncmp(p, "//", 2) == 0)
      p += 2;
    if (p[0] == '\0')
      return false;
    *out_unix = grpc_strdup(p);
    return *out_unix != nullptr;
  }
  const char *colon = nullptr;
  if (target[0] == '[') {
    const char *rb = strchr(target, ']');
    if (rb == nullptr || rb[1] != ':')
      return false;
    *out_host = grpc_strndup(target + 1, (size_t)(rb - target - 1));
    colon = rb + 1;
  } else {
    colon = strrchr(target, ':');
    if (colon == nullptr || colon == target)
      return false;
    *out_host = grpc_strndup(target, (size_t)(colon - target));
  }
  if (*out_host == nullptr)
    return false;
  long p = strtol(colon + 1, nullptr, 10);
  if (p <= 0 || p > 65535) {
    grpc_free(*out_host);
    *out_host = nullptr;
    return false;
  }
  *out_port = (uint16_t)p;
  return true;
}

static grpc_err init_subchannel(client_subchannel *s, grpc_client *parent, const char *host,
                                uint16_t port, const char *unix_path) {
  s->parent = parent;
  if (unix_path != nullptr) {
    s->unix_path = grpc_strdup(unix_path);
    if (s->unix_path == nullptr)
      return GRPC_ERR_NOMEM;
    s->is_unix = true;
    snprintf(s->authority, sizeof(s->authority), "localhost");
  } else {
    s->host = grpc_strdup(host != nullptr ? host : "127.0.0.1");
    if (s->host == nullptr)
      return GRPC_ERR_NOMEM;
    s->port = port;
    snprintf(s->authority, sizeof(s->authority), "%s:%u", s->host, s->port);
  }
  return GRPC_OK;
}

/* ---- Public client lifecycle ------------------------------------------------ */

GRPC_API grpc_client *grpc_client_new(const grpc_client_config *config, grpc_err *out_err) {
  if (out_err != nullptr)
    *out_err = GRPC_OK;
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
  if (config->tls.enable) {
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
      if (!parse_target(config->targets[i], &host, &port, &upath)) {
        err = GRPC_ERR_INVALID;
        goto fail;
      }
      err = init_subchannel(&c->subs[i], c, host, port, upath);
      grpc_free(host);
      grpc_free(upath);
      if (err != GRPC_OK)
        goto fail;
    }
    /* Eagerly connect at least one target to fail fast if all are unreachable. */
    bool any_ok = false;
    for (size_t i = 0; i < c->nb_subs; i++) {
      if (sub_connect(&c->subs[i]) == GRPC_OK)
        any_ok = true;
    }
    if (!any_ok) {
      err = GRPC_ERR_IO;
      goto fail;
    }
  } else {
    err = init_subchannel(&c->subs[0], c, config->host, config->port, config->unix_path);
    if (err != GRPC_OK)
      goto fail;
    err = sub_connect(&c->subs[0]);
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

/* ---- Streaming & framing helpers -------------------------------------------- */

static client_subchannel *pick_subchannel(grpc_client *c) {
  size_t idx = c->rr_next++ % c->nb_subs;
  return &c->subs[idx];
}

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
  uint8_t prefix[5];
  prefix[0] = cflag;
  uint32_t net_len = htonl((uint32_t)body_len);
  memcpy(prefix + 1, &net_len, 4);
  int fail = buf_append(dst, prefix, 5) || (body_len > 0 && buf_append(dst, body, body_len));
  grpc_free(comp_buf);
  return fail ? GRPC_ERR_NOMEM : GRPC_OK;
}

static void stream_free_internal(grpc_client_stream *st) {
  if (st == nullptr)
    return;
  if (st->sub != nullptr) {
    if (st->sub->timer_initialized)
      uv_timer_stop(&st->sub->timer);
    if (st->sub->active_stream == st)
      st->sub->active_stream = nullptr;
  }
  grpc_message *m;
  while ((m = grpc_list_pop_front(&st->recv_queue)) != nullptr)
    grpc_message_free(m);
  buf_free(&st->send_buf);
  buf_free(&st->recv_buf);
  grpc_client_response_free(&st->resp);
  grpc_free(st);
}

static grpc_client_stream *stream_open_on_sub(client_subchannel *s, const char *method,
                                              const grpc_metadata *metadata, size_t nb_metadata,
                                              uint32_t timeout_ms, grpc_err *out_err) {
  grpc_err err = sub_connect(s);
  if (err != GRPC_OK) {
    if (out_err != nullptr)
      *out_err = err;
    return nullptr;
  }
  grpc_client_stream *st = grpc_calloc(1, sizeof(*st));
  if (st == nullptr) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  st->sub = s;
  st->resp.status = GRPC_STATUS_UNKNOWN;
  grpc_list_init(&st->recv_queue);

  grpc_client *c = s->parent;
  const char *enc_name = nullptr;
  switch (c->config.compression) {
  case GRPC_COMPRESS_GZIP: enc_name = "gzip"; break;
  case GRPC_COMPRESS_DEFLATE: enc_name = "deflate"; break;
  case GRPC_COMPRESS_ZSTD: enc_name = "zstd"; break;
  case GRPC_COMPRESS_SNAPPY: enc_name = "snappy"; break;
  default: break;
  }
  uint32_t eff_timeout = timeout_ms ? timeout_ms : c->config.timeout_ms;
  char timeout_str[32] = {0};
  if (eff_timeout > 0)
    snprintf(timeout_str, sizeof(timeout_str), "%um", eff_timeout);

  const char *scheme = c->config.tls.enable ? "https" : "http";
  nghttp2_nv *nva = grpc_malloc((9 + nb_metadata) * sizeof(*nva));
  if (nva == nullptr) {
    stream_free_internal(st);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  size_t n = 0;
  nva[n++] = (nghttp2_nv){(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":scheme", (uint8_t *)scheme, 7, strlen(scheme), NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":path", (uint8_t *)method, 5, strlen(method), NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":authority", (uint8_t *)s->authority, 10, strlen(s->authority), NGHTTP2_NV_FLAG_NONE};
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
      stream_free_internal(st);
      if (out_err != nullptr)
        *out_err = GRPC_ERR_INVALID;
      return nullptr;
    }
    size_t klen = metadata[i].key_len ? metadata[i].key_len : strlen(metadata[i].key);
    size_t vlen = metadata[i].value_len ? metadata[i].value_len : strlen(metadata[i].value);
    nva[n++] = (nghttp2_nv){(uint8_t *)metadata[i].key, (uint8_t *)metadata[i].value, klen, vlen, NGHTTP2_NV_FLAG_NONE};
  }

  s->active_stream = st;
  nghttp2_data_provider dp = {.source.ptr = st, .read_callback = client_data_read};
  int32_t sid = nghttp2_submit_request(s->ngsession, nullptr, nva, n, &dp, st);
  grpc_free(nva);
  if (sid < 0) {
    stream_free_internal(st);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_PROTOCOL;
    return nullptr;
  }
  st->stream_id = sid;
  if (eff_timeout > 0)
    uv_timer_start(&s->timer, on_sub_timeout, eff_timeout, 0);
  sub_flush(s);
  if (out_err != nullptr)
    *out_err = GRPC_OK;
  return st;
}

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

GRPC_API grpc_err grpc_client_stream_send(grpc_client_stream *st, const void *data, size_t len) {
  if (st == nullptr || (data == nullptr && len > 0))
    return GRPC_ERR_INVALID;
  if (st->send_closed)
    return GRPC_ERR_STATE;
  if (st->sub->closed || st->stream_done)
    return GRPC_ERR_CLOSED;

  grpc_err err = frame_message_into(st->sub->parent, &st->send_buf, data, len);
  if (err != GRPC_OK)
    return err;
  (void)nghttp2_session_resume_data(st->sub->ngsession, st->stream_id);
  sub_flush(st->sub);
  return st->err;
}

GRPC_API grpc_err grpc_client_stream_close_send(grpc_client_stream *st) {
  if (st == nullptr)
    return GRPC_ERR_INVALID;
  if (st->send_closed)
    return GRPC_OK;
  st->send_closed = true;
  if (!st->sub->closed && st->sub->ngsession != nullptr) {
    (void)nghttp2_session_resume_data(st->sub->ngsession, st->stream_id);
    sub_flush(st->sub);
  }
  return st->err;
}

GRPC_API grpc_err grpc_client_stream_recv(grpc_client_stream *st, grpc_message **out_msg) {
  if (st == nullptr || out_msg == nullptr)
    return GRPC_ERR_INVALID;
  *out_msg = nullptr;
  while (grpc_list_is_empty(&st->recv_queue) && !st->stream_done && !st->sub->closed)
    uv_run(&st->sub->loop, UV_RUN_ONCE);
  *out_msg = grpc_list_pop_front(&st->recv_queue);
  if (*out_msg != nullptr)
    return GRPC_OK;
  return st->err;
}

GRPC_API grpc_err grpc_client_stream_finish(grpc_client_stream *st, grpc_client_response *out) {
  if (st == nullptr || out == nullptr)
    return GRPC_ERR_INVALID;
  if (!st->send_closed)
    (void)grpc_client_stream_close_send(st);
  while (!st->stream_done && !st->sub->closed)
    uv_run(&st->sub->loop, UV_RUN_ONCE);
  if (st->timed_out && !st->sub->closed && st->sub->ngsession != nullptr) {
    (void)nghttp2_submit_rst_stream(st->sub->ngsession, NGHTTP2_FLAG_NONE, st->stream_id,
                                    NGHTTP2_CANCEL);
    sub_flush(st->sub);
  }
  /* Move last queued message (if any) into out->message for unary convenience. */
  grpc_message *m;
  while ((m = grpc_list_pop_front(&st->recv_queue)) != nullptr) {
    grpc_message_free(st->resp.message);
    st->resp.message = m;
  }
  grpc_err err = st->err;
  *out = st->resp;
  st->resp = (grpc_client_response){0};
  stream_free_internal(st);
  return err;
}

GRPC_API void grpc_client_stream_cancel(grpc_client_stream *st) {
  if (st == nullptr)
    return;
  if (!st->sub->closed && st->sub->ngsession != nullptr && !st->stream_done) {
    (void)nghttp2_submit_rst_stream(st->sub->ngsession, NGHTTP2_FLAG_NONE, st->stream_id,
                                    NGHTTP2_CANCEL);
    sub_flush(st->sub);
  }
  stream_free_internal(st);
}

/* ---- Unary & server-streaming with retries and round-robin ------------------ */

static void sleep_ms(uint32_t ms) {
  if (ms > 0)
    uv_sleep(ms);
}

static grpc_err execute_with_retries(grpc_client *c, const char *method, const void *req_data,
                                     size_t req_len, const grpc_metadata *metadata,
                                     size_t nb_metadata, uint32_t timeout_ms,
                                     void (*on_message)(const grpc_message *msg, void *userdata),
                                     void *userdata, grpc_client_response *out) {
  if (c == nullptr || method == nullptr || method[0] != '/' || out == nullptr ||
      (req_data == nullptr && req_len > 0) || (metadata == nullptr && nb_metadata > 0))
    return GRPC_ERR_INVALID;

  uint32_t max_attempts = 1 + c->config.max_retries;
  uint32_t backoff = c->config.retry_backoff_ms;
  grpc_err last_err = GRPC_OK;

  for (uint32_t attempt = 0; attempt < max_attempts; attempt++) {
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
    st->on_message = on_message;
    st->on_message_userdata = userdata;
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
    if (!transient)
      return last_err;
  }
  return last_err;
}

GRPC_API grpc_err grpc_client_unary(grpc_client *client, const char *method, const void *req_data,
                                    size_t req_len, const grpc_metadata *metadata,
                                    size_t nb_metadata, uint32_t timeout_ms,
                                    grpc_client_response *out) {
  return execute_with_retries(client, method, req_data, req_len, metadata, nb_metadata, timeout_ms,
                              nullptr, nullptr, out);
}

GRPC_API grpc_err grpc_client_server_stream(
    grpc_client *client, const char *method, const void *req_data, size_t req_len,
    const grpc_metadata *metadata, size_t nb_metadata, uint32_t timeout_ms,
    void (*on_message)(const grpc_message *msg, void *userdata), void *userdata,
    grpc_client_response *out) {
  return execute_with_retries(client, method, req_data, req_len, metadata, nb_metadata, timeout_ms,
                              on_message, userdata, out);
}
