/* grpc_client_io.c - subchannel socket I/O and the nghttp2 client callbacks
 *
 * Moves bytes between a subchannel's handle and its nghttp2 session (through the TLS memory BIOs
 * when TLS is on) and routes the session's callbacks to the subchannel's active stream. Part of
 * the client described in grpc_client.c.
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

#if defined(GRPC_CLIENT_HAS_OPENSSL)

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

void sub_fail(client_subchannel *s, grpc_err err) {
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

void sub_flush(client_subchannel *s) {
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

ssize_t client_data_read([[maybe_unused]] nghttp2_session *session, int32_t stream_id, uint8_t *buf,
                         size_t length, uint32_t *data_flags,
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

void client_h2_set_callbacks(nghttp2_session_callbacks *cbs) {
  nghttp2_session_callbacks_set_on_header_callback(cbs, client_on_header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, client_on_data_chunk);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, client_on_frame_recv);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, client_on_stream_close);
}

int sub_read_start(client_subchannel *s) {
  return uv_read_start(sub_stream(s), on_sub_alloc, on_sub_read);
}
