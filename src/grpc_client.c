/* grpc_client.c - synchronous gRPC client over HTTP/2 (nghttp2 + libuv)
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

/* Returns the subchannel handle as a uv_stream_t, whichever of TCP or pipe it is. */
static inline uv_stream_t *sub_stream(client_subchannel *s) {
  return s->is_unix ? (uv_stream_t *)&s->handle.pipe : (uv_stream_t *)&s->handle.tcp;
}

/* Returns the subchannel handle as a uv_handle_t, whichever of TCP or pipe it is. */
static inline uv_handle_t *sub_handle(client_subchannel *s) {
  return s->is_unix ? (uv_handle_t *)&s->handle.pipe : (uv_handle_t *)&s->handle.tcp;
}

/* ---- TLS helpers (OpenSSL-compatible backends) ------------------------------ */

#if defined(GRPC_CLIENT_HAS_OPENSSL)
/* Adds every certificate in a PEM buffer to the context's trust store. Returns true if at least
 * one certificate was added. */
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

/* Builds the shared client SSL_CTX: TLS 1.2+, ALPN "h2", peer verification against ca_file,
 * ca_pem or the system default paths (unless insecure_skip_verify), and an optional client
 * certificate/key for mTLS (both files are required together). Returns nullptr on any error. */
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

/* Moves pending ciphertext from the SSL write BIO into obuf for sending. Returns 0 on success,
 * 1 on allocation or BIO failure. */
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

/* uv_write completion for the wbuf write started by sub_flush: marks the write finished, fails
 * the active stream on error and otherwise flushes anything queued meanwhile. */
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

/* Marks the connection unusable after nghttp2 output was lost (nghttp2 already considers bytes it
 * handed out as sent, so the session and the server are now out of sync), records why in
 * s->fail_err for sub_connect(), and fails the active RPC. The next RPC reconnects. */
static void sub_fail(client_subchannel *s, grpc_err err) {
  s->closed = true;
  s->fail_err = err;
  grpc_client_stream *st = s->active_stream;
  /* nghttp2 may have closed the stream just before (recorded as GRPC_ERR_CLOSED): the
   * connection's failure is the cause to report */
  if (st != nullptr && (st->err == GRPC_OK || st->err == GRPC_ERR_CLOSED)) {
    st->err = err;
    st->stream_done = true;
  }
}

/* Pulls all pending frames out of nghttp2 (through SSL when TLS is on) into obuf and writes
 * them: first with uv_try_write, then the remainder with an async uv_write after swapping obuf
 * into wbuf, so obuf can keep accumulating while the write is in flight. Only one uv_write is
 * outstanding at a time; while it is, this returns early and on_sub_write calls back. Errors
 * mark the active stream done with an error and, for I/O errors, the subchannel closed. Before
 * the TLS handshake completes, only handshake bytes are sent. */
static void sub_flush(client_subchannel *s) {
  if (s->h3c != nullptr) {
    h3c_flush(s);
    return;
  }
  if (s->closed || s->is_writing)
    return;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    if (s->tls_handshake_done && s->ngsession != nullptr) {
      for (;;) {
        const u8 *data;
        ssize_t n = nghttp2_session_mem_send(s->ngsession, &data);
        if (n < 0) {
          sub_fail(s, n == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL);
          return;
        }
        if (n == 0)
          break;
        if (buf_append(&s->tls_obuf, data, (size_t)n)) {
          sub_fail(s, GRPC_ERR_NOMEM);
          return;
        }
      }
      if (s->tls_obuf.len > 0) {
        size_t off = 0;
        while (off < s->tls_obuf.len) {
          size_t chunk = s->tls_obuf.len - off;
          ERR_clear_error();
          int wr =
              SSL_write(s->ssl, s->tls_obuf.data + off, chunk > INT_MAX ? INT_MAX : (int)chunk);
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
        sub_fail(s, n == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL);
        return;
      }
      if (n == 0)
        break;
      if (buf_append(&s->obuf, data, (size_t)n)) {
        sub_fail(s, GRPC_ERR_NOMEM);
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

/* Appends a copy of a header name/value to a grpc_metadata array grown with grpc_realloc.
 * Allocation failures drop the entry silently. */
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

/* nghttp2 header callback: records grpc-status, grpc-message and grpc-encoding for the active
 * stream and stores every header (including those) as response metadata. Fields arriving before
 * the initial HEADERS completes go to headers, unless the frame ends the stream (a
 * trailers-only response); later ones go to trailers. Headers for other streams are ignored. */
static int client_on_header([[maybe_unused]] nghttp2_session *session, const nghttp2_frame *frame,
                            const uint8_t *name, size_t namelen, const uint8_t *value,
                            size_t valuelen, [[maybe_unused]] uint8_t flags, void *user_data) {
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st == nullptr || frame->hd.type != NGHTTP2_HEADERS || frame->hd.stream_id != st->stream_id)
    return 0;
  client_resp_header(st, name, namelen, value, valuelen,
                     st->headers_received || (frame->hd.flags & NGHTTP2_FLAG_END_STREAM));
  return 0;
}

/* Parses a grpc-status value: 1-2 decimal digits naming a status 0..16; anything else (empty,
 * signs, spaces, letters, out of range) is UNKNOWN, as the gRPC spec requires. */
static grpc_status parse_grpc_status(const uint8_t *value, size_t len) {
  if (len == 0 || len > 2)
    return GRPC_STATUS_UNKNOWN;
  unsigned v = 0;
  for (size_t i = 0; i < len; i++) {
    if (value[i] < '0' || value[i] > '9')
      return GRPC_STATUS_UNKNOWN;
    v = v * 10 + (unsigned)(value[i] - '0');
  }
  return v <= 16 ? (grpc_status)v : GRPC_STATUS_UNKNOWN;
}

/* The gRPC status for an HTTP status when the response carries no grpc-status (the gRPC spec's
 * "HTTP to gRPC Status Code Mapping"). */
static grpc_status status_from_http(unsigned http) {
  switch (http) {
  case 400:
    return GRPC_STATUS_INTERNAL;
  case 401:
    return GRPC_STATUS_UNAUTHENTICATED;
  case 403:
    return GRPC_STATUS_PERMISSION_DENIED;
  case 404:
    return GRPC_STATUS_UNIMPLEMENTED;
  case 429:
  case 502:
  case 503:
  case 504:
    return GRPC_STATUS_UNAVAILABLE;
  default:
    return GRPC_STATUS_UNKNOWN;
  }
}

/* Decodes the percent-encoding of grpc-message (%XX; malformed escapes are kept as they are)
 * into a NUL-terminated grpc_malloc string, or nullptr when out of memory. */
static int hex_digit(uint8_t c) {
  return c >= '0' && c <= '9'   ? c - '0'
         : c >= 'A' && c <= 'F' ? c - 'A' + 10
         : c >= 'a' && c <= 'f' ? c - 'a' + 10
                                : -1;
}

static char *percent_decode(const uint8_t *value, size_t len) {
  char *out = grpc_malloc(len + 1);
  if (out == nullptr)
    return nullptr;
  size_t o = 0;
  for (size_t i = 0; i < len; i++) {
    int hi = value[i] == '%' && i + 2 < len ? hex_digit(value[i + 1]) : -1;
    int lo = hi >= 0 ? hex_digit(value[i + 2]) : -1;
    if (lo >= 0) {
      out[o++] = (char)(hi * 16 + lo);
      i += 2;
    } else {
      out[o++] = (char)value[i];
    }
  }
  out[o] = '\0';
  return out;
}

void client_resp_header(grpc_client_stream *st, const uint8_t *name, size_t namelen,
                        const uint8_t *value, size_t valuelen, bool trailer) {
  if (st->rejected) /* the outcome is decided; trailers arriving after it do not change it */
    return;
  if (namelen == 7 && memcmp(name, ":status", 7) == 0) {
    unsigned http = 0;
    for (size_t i = 0; i < valuelen && value[i] >= '0' && value[i] <= '9'; i++)
      http = http * 10 + (unsigned)(value[i] - '0');
    /* without grpc-status, a non-200 response maps to a gRPC status (grpc-status overrides) */
    if (http != 200 && !st->grpc_status_seen)
      st->resp.status = status_from_http(http);
  } else if (namelen == 11 && memcmp(name, "grpc-status", 11) == 0) {
    st->resp.status = parse_grpc_status(value, valuelen);
    st->grpc_status_seen = true;
  } else if (namelen == 12 && memcmp(name, "grpc-message", 12) == 0) {
    grpc_free(st->resp.status_message);
    st->resp.status_message = percent_decode(value, valuelen);
  } else if (namelen == 13 && memcmp(name, "grpc-encoding", 13) == 0) {
    bool supported = true;
    if (valuelen == 4 && memcmp(value, "gzip", 4) == 0)
      st->resp_encoding = GRPC_COMPRESS_GZIP;
    else if (valuelen == 7 && memcmp(value, "deflate", 7) == 0)
      st->resp_encoding = GRPC_COMPRESS_DEFLATE;
    else if (valuelen == 4 && memcmp(value, "zstd", 4) == 0)
      st->resp_encoding = GRPC_COMPRESS_ZSTD;
    else if (valuelen == 6 && memcmp(value, "snappy", 6) == 0)
      st->resp_encoding = GRPC_COMPRESS_SNAPPY;
    else {
      st->resp_encoding = GRPC_COMPRESS_IDENTITY;
      supported = valuelen == 8 && memcmp(value, "identity", 8) == 0;
    }
#if !defined(GRPC_ENABLE_ZLIB)
    if (st->resp_encoding == GRPC_COMPRESS_GZIP || st->resp_encoding == GRPC_COMPRESS_DEFLATE)
      supported = false;
#endif
#if !defined(GRPC_ENABLE_ZSTD)
    if (st->resp_encoding == GRPC_COMPRESS_ZSTD)
      supported = false;
#endif
#if !defined(GRPC_ENABLE_SNAPPY)
    if (st->resp_encoding == GRPC_COMPRESS_SNAPPY)
      supported = false;
#endif
    st->resp_encoding_unsupported = !supported;
  }

  if (!trailer)
    md_list_append(&st->resp.headers, &st->resp.nb_headers, name, namelen, value, valuelen);
  else
    md_list_append(&st->resp.trailers, &st->resp.nb_trailers, name, namelen, value, valuelen);
}

/* Fails the stream on a response it cannot accept: err for the caller, and an INTERNAL status
 * (unless a limit was exceeded) with `why` as its message. */
static void stream_reject(grpc_client_stream *st, grpc_err err, const char *why) {
  st->err = err;
  st->stream_done = true;
  st->rejected = true;
  if (err != GRPC_ERR_LIMIT && err != GRPC_ERR_NOMEM) {
    st->resp.status = GRPC_STATUS_INTERNAL;
    grpc_free(st->resp.status_message);
    st->resp.status_message = grpc_strdup(why);
  }
}

/* Splits complete length-prefixed gRPC messages off the front of recv_buf. Every message,
 * including an incomplete one, must declare at most max_message_size bytes (GRPC_ERR_LIMIT
 * otherwise, as soon as its prefix arrived). Compressed messages (flag 1) are decompressed with
 * the response's grpc-encoding within max_message_size; a compressed message without a usable
 * grpc-encoding, corrupt compressed data or a flag other than 0/1 fail the stream (status
 * INTERNAL, GRPC_ERR_CODEC / GRPC_ERR_PROTOCOL), as the gRPC spec requires. Each message goes to
 * on_message (then freed) when set, otherwise to recv_queue. Leaves a partial trailing message in
 * recv_buf. */
static void stream_drain_messages(grpc_client_stream *st) {
  client_subchannel *s = st->sub;
  uint64_t max_size = s->parent->config.max_message_size;
  size_t off = 0;
  while (st->recv_buf.len - off >= 5) {
    uint8_t cflag = st->recv_buf.data[off];
    uint32_t mlen = ntohl(uread_u32(st->recv_buf.data + off + 1));
    if (mlen > max_size) {
      stream_reject(st, GRPC_ERR_LIMIT, "response message larger than max_message_size");
      return;
    }
    if (cflag > 1) {
      stream_reject(st, GRPC_ERR_PROTOCOL, "invalid compressed flag");
      return;
    }
    if (st->recv_buf.len - off < 5 + (size_t)mlen)
      break;
    const uint8_t *payload = st->recv_buf.data + off + 5;
    grpc_message *msg = nullptr;
    if (cflag == 1) {
      if (st->resp_encoding == GRPC_COMPRESS_IDENTITY || st->resp_encoding_unsupported) {
        stream_reject(st, GRPC_ERR_CODEC, "compressed response without a supported grpc-encoding");
        return;
      }
      uint8_t *decomp = nullptr;
      size_t dlen = 0;
      grpc_err derr = grpc_decompress_limited(st->resp_encoding, payload, mlen, &decomp, &dlen,
                                              (size_t)max_size);
      if (derr != GRPC_OK) {
        stream_reject(st, derr, "failed to decompress the response");
        return;
      }
      msg = message_create(0, decomp, dlen);
      grpc_free(decomp);
    } else {
      msg = message_create(0, payload, mlen);
    }
    if (msg == nullptr) {
      stream_reject(st, GRPC_ERR_NOMEM, "out of memory");
      return;
    }
    if (st->on_message != nullptr) {
      st->on_message(msg, st->on_message_userdata);
      grpc_message_free(msg);
    } else if (msg_queue_push(&st->recv_queue, msg) != GRPC_OK) {
      grpc_message_free(msg);
      stream_reject(st, GRPC_ERR_NOMEM, "out of memory");
      return;
    }
    off += 5 + (size_t)mlen;
  }
  if (off > 0) {
    memmove(st->recv_buf.data, st->recv_buf.data + off, st->recv_buf.len - off);
    st->recv_buf.len -= off;
  }
}

/* nghttp2 DATA callback: appends the active stream's bytes to recv_buf and extracts complete
 * messages (stream_drain_messages() enforces max_message_size per message, so the buffer holds
 * at most one allowed message plus one received chunk). */
static int client_on_data_chunk([[maybe_unused]] nghttp2_session *session,
                                [[maybe_unused]] uint8_t flags, int32_t stream_id,
                                const uint8_t *data, size_t len, void *user_data) {
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st == nullptr || stream_id != st->stream_id)
    return 0;
  return client_resp_data(st, data, len) == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
}

int client_resp_data(grpc_client_stream *st, const uint8_t *data, size_t len) {
  if (st->stream_done)
    return 0;
  if (buf_append(&st->recv_buf, data, len)) {
    st->err = GRPC_ERR_NOMEM;
    st->stream_done = true;
    return -1;
  }
  stream_drain_messages(st);
  return 0;
}

/* nghttp2 frame callback: notes the end of the initial HEADERS (so later fields are trailers)
 * and marks the active stream done on END_STREAM. */
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

/* nghttp2 stream-close callback: marks the active stream done, turning an abnormal close
 * (RST_STREAM / GOAWAY error code) into GRPC_ERR_CLOSED unless an error is already set. */
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

/* nghttp2 data-provider callback for the request body: copies queued bytes from send_buf, sets
 * EOF once the send side is half-closed and drained, and returns NGHTTP2_ERR_DEFERRED when there
 * is nothing to send yet (resumed by nghttp2_session_resume_data on the next send/close). The
 * stream is looked up through the subchannel rather than the data source, which may outlive it:
 * once the application freed the stream (e.g. after the server answered before the whole
 * request was sent) the callback fails it, and nghttp2 resets that stream. */
static ssize_t client_data_read([[maybe_unused]] nghttp2_session *session, int32_t stream_id,
                                uint8_t *buf, size_t length, uint32_t *data_flags,
                                [[maybe_unused]] nghttp2_data_source *source, void *user_data) {
  client_subchannel *s = user_data;
  grpc_client_stream *st = s->active_stream;
  if (st == nullptr || st->stream_id != stream_id)
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
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

/* libuv alloc callback: reads always land in the subchannel's fixed 64 KiB rbuf. */
static void on_sub_alloc(uv_handle_t *handle, [[maybe_unused]] size_t suggested_size,
                         uv_buf_t *buf) {
  client_subchannel *s = handle->data;
  *buf = uv_buf_init((char *)s->rbuf, (unsigned int)sizeof(s->rbuf));
}

/* libuv read callback. EOF or a read error closes the subchannel and fails the active stream
 * with GRPC_ERR_CLOSED. With TLS, feeds ciphertext into the SSL read BIO, advances the handshake
 * if needed, then decrypts into rbuf (reused, as the read data is already in the BIO) and passes
 * the plaintext to nghttp2. Without TLS, passes the bytes to nghttp2 directly. Flushes afterwards
 * so nghttp2 replies (SETTINGS ACK, WINDOW_UPDATE, ...) go out. */
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
          /* the session is unusable after a fatal nghttp2 error: close it, the next RPC
           * reconnects */
          sub_fail(s, rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL);
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
    /* the session is unusable after a fatal nghttp2 error: close it, the next RPC reconnects */
    sub_fail(s, rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL);
    return;
  }
  sub_flush(s);
}

/* Connect completion: records the status that sub_connect's wait loop polls for. */
static void on_sub_connect(uv_connect_t *req, int status) {
  client_subchannel *s = req->data;
  s->connect_status = status;
  s->connected = (status == 0);
}

/* Close callback used while abandoning a failed connect attempt in sub_connect. */
static void on_sub_handle_closed(uv_handle_t *handle) {
  client_subchannel *s = handle->data;
  s->handle_close_done = true;
}

/* Timer callback: the connect + TLS handshake deadline (config.timeout_ms) elapsed. */
static void on_sub_connect_deadline(uv_timer_t *timer) {
  client_subchannel *s = timer->data;
  s->connect_deadline_hit = true;
}

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

/* ---- Subchannel lifecycle --------------------------------------------------- */

/* Tears down the connection: frees the nghttp2 session and SSL object, closes the handle and
 * timer, runs the loop until all close callbacks (and any pending connect request) complete,
 * and closes the loop. Keeps the target strings and buffers so sub_connect can reuse them. Safe
 * to call on a never-connected or already disconnected subchannel. */
static void sub_disconnect(client_subchannel *s) {
  s->closed = true;
  if (s->h3c != nullptr)
    h3c_disconnect(s);
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
    uv_run(&s->loop, UV_RUN_DEFAULT);
    uv_loop_close(&s->loop);
    s->loop_initialized = false;
  }
  s->connected = false;
  s->is_writing = false;
  s->obuf.len = 0;
  s->wbuf.len = 0;
  s->tls_obuf.len = 0;
}

/* True if a connected subchannel can still carry a new RPC. Its loop only runs during RPCs, so
 * this first catches up: refreshes the loop clock and handles what arrived meanwhile (an EOF, a
 * GOAWAY), then asks the transport: nghttp2 refuses new streams after a GOAWAY, and a QUIC
 * connection may have idled out. */
static bool sub_usable(client_subchannel *s) {
  if (!s->connected || s->closed || (s->ngsession == nullptr && s->h3c == nullptr))
    return false;
  /* QUIC first: whether the connection went silent for too long must be judged before reading
   * the packets that queued up since the last call, which would look like fresh activity */
  if (s->h3c != nullptr && !h3c_usable(s))
    return false;
  uv_update_time(&s->loop);
  (void)uv_run(&s->loop, UV_RUN_NOWAIT);
  if (s->closed)
    return false;
  if (s->h3c != nullptr) /* again: a GOAWAY may have arrived just now */
    return h3c_usable(s);
  return s->ngsession != nullptr && nghttp2_session_check_request_allowed(s->ngsession) != 0;
}

/* Ensures the subchannel is connected, reconnecting from scratch if it was closed or can no
 * longer carry requests (sub_usable()). Creates a fresh loop, connects to the Unix path or to each
 * resolved address in turn (IP literals skip getaddrinfo, which otherwise blocks), performs the
 * TLS handshake when configured, creates the nghttp2 session and queues SETTINGS. The whole
 * sequence is bounded by bound_ms when nonzero (the smaller of config.timeout_ms and the RPC's
 * own deadline). Blocks the caller while it runs the private loop. Returns GRPC_OK, or
 * GRPC_ERR_IO / GRPC_ERR_NOMEM / GRPC_ERR_PROTOCOL after disconnecting again. */
static grpc_err sub_connect(client_subchannel *s, uint32_t bound_ms) {
  if (sub_usable(s))
    return GRPC_OK;
  sub_disconnect(s);
  s->closed = false;
  s->fail_err = GRPC_OK;

  if (uv_loop_init(&s->loop) != 0)
    return GRPC_ERR_IO;
  s->loop_initialized = true;
  if (uv_timer_init(&s->loop, &s->timer) != 0)
    goto fail_io;
  s->timer.data = s;
  s->timer_initialized = true;

  /* Bound connect + TLS handshake by the default deadline so an unresponsive
   * peer cannot block the caller forever. */
  s->connect_deadline_hit = false;
  if (bound_ms > 0)
    uv_timer_start(&s->timer, on_sub_connect_deadline, bound_ms, 0);

  if (s->parent->config.transport == GRPC_TRANSPORT_HTTP3) {
    if (h3c_connect(s) != GRPC_OK)
      goto fail_io;
    uv_timer_stop(&s->timer);
    h3c_flush(s);
    return GRPC_OK;
  }

  uv_connect_t conn_req = {.data = s};
  s->connect_status = -1;
  if (s->is_unix) {
    if (uv_pipe_init(&s->loop, &s->handle.pipe, 0) != 0)
      goto fail_io;
    s->handle.pipe.data = s;
    s->handle_initialized = true;
    uv_pipe_connect(&conn_req, &s->handle.pipe, s->unix_path, on_sub_connect);
    while (!s->connected && s->connect_status == -1 && !s->connect_deadline_hit)
      uv_run(&s->loop, UV_RUN_ONCE);
    if (!s->connected)
      goto fail_io;
  } else {
    struct sockaddr_storage ss = {0};
    struct addrinfo *res = nullptr;
    struct addrinfo single_ai = {0};
    if (uv_ip4_addr(s->host, s->port, (struct sockaddr_in *)&ss) == 0) {
      single_ai.ai_addr = (struct sockaddr *)&ss;
      single_ai.ai_addrlen = sizeof(struct sockaddr_in);
    } else if (uv_ip6_addr(s->host, s->port, (struct sockaddr_in6 *)&ss) == 0) {
      single_ai.ai_addr = (struct sockaddr *)&ss;
      single_ai.ai_addrlen = sizeof(struct sockaddr_in6);
    } else {
      struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
      char port_str[16];
      snprintf(port_str, sizeof(port_str), "%u", s->port);
      if (getaddrinfo(s->host, port_str, &hints, &res) != 0 || res == nullptr) {
        if (res != nullptr)
          freeaddrinfo(res);
        goto fail_io;
      }
    }
    struct addrinfo *ai = res != nullptr ? res : &single_ai;
    for (; ai != nullptr; ai = ai->ai_next) {
      s->connected = false;
      s->connect_status = -1;
      if (uv_tcp_init(&s->loop, &s->handle.tcp) != 0)
        continue;
      s->handle.tcp.data = s;
      s->handle_initialized = true;
      if (uv_tcp_connect(&conn_req, &s->handle.tcp, ai->ai_addr, on_sub_connect) == 0) {
        while (!s->connected && s->connect_status == -1 && !s->connect_deadline_hit)
          uv_run(&s->loop, UV_RUN_ONCE);
        if (s->connected)
          break;
      }
      /* Closing cancels a pending connect; libuv completes conn_req (on our stack)
       * before the close callback, so wait for that rather than for the loop to go
       * idle (the deadline timer keeps it alive). */
      s->handle_close_done = false;
      uv_close((uv_handle_t *)&s->handle.tcp, on_sub_handle_closed);
      while (!s->handle_close_done)
        uv_run(&s->loop, UV_RUN_ONCE);
      s->handle_initialized = false;
      if (s->connect_deadline_hit)
        break;
    }
    if (res != nullptr)
      freeaddrinfo(res);
    if (!s->connected)
      goto fail_io;
  }

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
    const char *sni =
        s->parent->config.tls.server_name != nullptr ? s->parent->config.tls.server_name : s->host;
    if (sni != nullptr && sni[0] != '\0') {
      /* the certificate must also be issued for this name (or IP address), not just chain to a
       * trusted CA; SNI carries DNS names only */
      unsigned char addr[16];
      bool is_ip = uv_inet_pton(AF_INET, sni, addr) == 0 || uv_inet_pton(AF_INET6, sni, addr) == 0;
      bool verify = !s->parent->config.tls.insecure_skip_verify;
      if (!is_ip)
        (void)SSL_set_tlsext_host_name(s->ssl, sni);
      if (verify && (is_ip ? X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(s->ssl), sni) != 1
                           : SSL_set1_host(s->ssl, sni) != 1))
        goto fail_io;
    }
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
  nghttp2_mem mem = grpc_nghttp2_mem; /* copied by nghttp2 */
  int rv = nghttp2_session_client_new3(&s->ngsession, cbs, s, nullptr, &mem);
  nghttp2_session_callbacks_del(cbs);
  if (rv != 0) {
    sub_disconnect(s);
    return rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
  }

  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100},
      /* a whole message fits the stream window, within HTTP/2's 2^31 - 1 limit */
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
       s->parent->config.max_message_size > NGHTTP2_MAX_WINDOW_SIZE
           ? (uint32_t)NGHTTP2_MAX_WINDOW_SIZE
           : s->parent->config.max_message_size},
  };
  rv = nghttp2_submit_settings(s->ngsession, NGHTTP2_FLAG_NONE, iv, 2);
  if (rv != 0) {
    sub_disconnect(s);
    return rv == NGHTTP2_ERR_NOMEM ? GRPC_ERR_NOMEM : GRPC_ERR_PROTOCOL;
  }
  if (uv_read_start(sub_stream(s), on_sub_alloc, on_sub_read) != 0)
    goto fail_io;

#if defined(GRPC_CLIENT_HAS_OPENSSL)
  if (s->ssl != nullptr) {
    ERR_clear_error();
    (void)SSL_do_handshake(s->ssl);
    sub_flush(s);
    while (!s->tls_handshake_done && !s->closed && !s->connect_deadline_hit)
      uv_run(&s->loop, UV_RUN_ONCE);
    if (!s->tls_handshake_done || s->closed)
      goto fail_io;
  }
#endif
  uv_timer_stop(&s->timer);
  sub_flush(s);
  if (s->closed) {
    /* the connection preface or SETTINGS could not be queued */
    grpc_err err = s->fail_err != GRPC_OK ? s->fail_err : GRPC_ERR_IO;
    sub_disconnect(s);
    return err;
  }
  return GRPC_OK;

fail_io:
  sub_disconnect(s);
  return GRPC_ERR_IO;
}

/* Parses a target string: "unix:path" or "unix://path" sets *out_unix, otherwise "host:port" or
 * "[ipv6]:port" sets *out_host and *out_port (1-65535; characters after the digits are ignored).
 * Outputs are grpc_malloc strings owned by the caller. Returns GRPC_OK, GRPC_ERR_INVALID for
 * malformed input or GRPC_ERR_NOMEM, with nothing left allocated on failure. */
static grpc_err parse_target(const char *target, char **out_host, uint16_t *out_port,
                             char **out_unix) {
  *out_host = nullptr;
  *out_port = 0;
  *out_unix = nullptr;
  if (target == nullptr || target[0] == '\0')
    return GRPC_ERR_INVALID;
  if (strncmp(target, "unix:", 5) == 0) {
    const char *p = target + 5;
    if (strncmp(p, "//", 2) == 0)
      p += 2;
    if (p[0] == '\0')
      return GRPC_ERR_INVALID;
    *out_unix = grpc_strdup(p);
    return *out_unix != nullptr ? GRPC_OK : GRPC_ERR_NOMEM;
  }
  const char *colon = nullptr;
  const char *host_start = target;
  size_t host_len = 0;
  if (target[0] == '[') {
    const char *rb = strchr(target, ']');
    if (rb == nullptr || rb[1] != ':')
      return GRPC_ERR_INVALID;
    host_start = target + 1;
    host_len = (size_t)(rb - target - 1);
    colon = rb + 1;
  } else {
    colon = strrchr(target, ':');
    if (colon == nullptr || colon == target)
      return GRPC_ERR_INVALID;
    host_len = (size_t)(colon - target);
  }
  long p = strtol(colon + 1, nullptr, 10);
  if (p <= 0 || p > 65535)
    return GRPC_ERR_INVALID;
  *out_host = grpc_strndup(host_start, host_len);
  if (*out_host == nullptr)
    return GRPC_ERR_NOMEM;
  *out_port = (uint16_t)p;
  return GRPC_OK;
}

/* Fills in a zeroed subchannel's target (copying host or unix_path) and the :authority value
 * ("localhost" for Unix sockets). Does not connect. Returns GRPC_ERR_NOMEM if a copy fails; the
 * partially initialized subchannel is still released by grpc_client_free. */
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
    /* an IPv6 address is bracketed in the authority, as in a URL */
    snprintf(s->authority, sizeof(s->authority),
             strchr(s->host, ':') != nullptr ? "[%s]:%u" : "%s:%u", s->host, s->port);
  }
  return GRPC_OK;
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
      err = parse_target(config->targets[i], &host, &port, &upath);
      if (err != GRPC_OK)
        goto fail;
      err = init_subchannel(&c->subs[i], c, host, port, upath);
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
    err = init_subchannel(&c->subs[0], c, config->host, config->port, config->unix_path);
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
  nghttp2_nv *nva = grpc_malloc((9 + nb_metadata) * sizeof(*nva));
  if (nva == nullptr) {
    stream_free_internal(st);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  size_t n = 0;
  nva[n++] = (nghttp2_nv){(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":scheme", (uint8_t *)scheme, 7, strlen(scheme),
                          NGHTTP2_NV_FLAG_NONE};
  nva[n++] =
      (nghttp2_nv){(uint8_t *)":path", (uint8_t *)method, 5, strlen(method), NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)":authority", (uint8_t *)s->authority, 10,
                          strlen(s->authority), NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16,
                          NGHTTP2_NV_FLAG_NONE};
  nva[n++] = (nghttp2_nv){(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE};
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
  nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-accept-encoding", (uint8_t *)accept_encoding, 20,
                          sizeof(accept_encoding) - 1, NGHTTP2_NV_FLAG_NONE};
  if (enc_name != nullptr)
    nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-encoding", (uint8_t *)enc_name, 13, strlen(enc_name),
                            NGHTTP2_NV_FLAG_NONE};
  if (eff_timeout > 0)
    nva[n++] = (nghttp2_nv){(uint8_t *)"grpc-timeout", (uint8_t *)timeout_str, 12,
                            strlen(timeout_str), NGHTTP2_NV_FLAG_NONE};
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
    nva[n++] = (nghttp2_nv){(uint8_t *)metadata[i].key, (uint8_t *)metadata[i].value, klen, vlen,
                            NGHTTP2_NV_FLAG_NONE};
  }

  s->active_stream = st;
  int64_t sid;
  if (s->h3c != nullptr) {
    static_assert(sizeof(grpc_nv) == sizeof(nghttp2_nv), "grpc_nv must match nghttp2_nv");
    sid = h3c_submit_request(s, st, (const grpc_nv *)nva, n) == 0 ? st->stream_id : -1;
  } else {
    nghttp2_data_provider dp = {.source.ptr = st, .read_callback = client_data_read};
    sid = nghttp2_submit_request(s->ngsession, nullptr, nva, n, &dp, st);
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
