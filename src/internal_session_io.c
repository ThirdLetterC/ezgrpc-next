/* internal_session_io.c - connection I/O: accepting, reading and writing sessions
 *
 * Everything here runs on the server's loop thread. on_listener_connection() takes a free slot
 * of the fixed sessions array, accepts into its embedded uv_tcp_t or uv_pipe_t, and sets up TLS
 * (if enabled) and an nghttp2 server session. Reads land in the server-wide rbuf, which is safe
 * because each read is fully handed to nghttp2 (after decryption, for TLS) before the next one.
 * Output is never written from inside nghttp2 callbacks: code that produces frames calls
 * session_schedule_flush(), and server_flush_sessions() later serializes the frames into obuf and
 * writes them, first with uv_try_write() and, when the socket is full, with one uv_write() from
 * wbuf while obuf keeps collecting.
 *
 * Session slots, closing and the grpc_stream constructors are in internal_helpers.c.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include "internal/grpc_platform.h"
#include "core.h"
#include "grpc_header.h"
#include "grpc_timeout.h"
#include "internal_nghttp2_callbacks.h"
#include "internal_helpers.h"
#include "internal_tls.h"

/* stop pulling frames out of nghttp2 once this many bytes wait for the kernel */
static const size_t GRPC_OBUF_HIGH_WATER = 256 * 1024;

/* Room reserved in the output buffer before taking a chunk from nghttp2_session_mem_send(),
 * which returns at most one frame buffer (a 9-byte header plus up to 16 KiB of payload, since
 * no data_source_read_length callback raises the DATA size). nghttp2 counts the bytes as sent
 * once it hands them out, so they must never fail to fit. */
static const size_t GRPC_SERIALIZE_HEADROOM = 32 * 1024;

/* Delay before retrying a flush that an allocation failure blocked. */
static const uint64_t GRPC_FLUSH_RETRY_MS = 10;

/* session_serialize() results */
enum { SERIALIZE_OK = 0, SERIALIZE_FATAL = 1, SERIALIZE_BLOCKED = 2 };

/* Sets up TLS (when the server has it) and an nghttp2 server session for an accepted
 * connection, using the grpc allocator, and submits the server's SETTINGS (initial window =
 * max_message_size, max frame size, max concurrent streams); the frame goes out on the next
 * flush. Returns 0 on success, 1 on failure; the caller then closes the session, which releases
 * whatever was set up. */
static int session_create(grpc_session *ezsession) {
  grpc_server *server = ezsession->server;

  /* TLS goes first so that a failure never leaves an announced session behind */
  if (server->is_tls && grpc_tls_session_init(ezsession) != 0) {
    return 1;
  }

  /* nghttp2 copies the memory table */
  nghttp2_mem mem = grpc_nghttp2_mem;
  int res = nghttp2_session_server_new3(&ezsession->ngsession, server->ngcallbacks, ezsession,
                                        nullptr, &mem);
  if (res < 0) {
    ezsession->ngsession = nullptr;
    return 1;
  }
  ezsession->ops = &grpc_h2_transport_ops;

  /* settings */
  nghttp2_settings_entry siv[] = {
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, server->config.max_message_size},
      {NGHTTP2_SETTINGS_MAX_FRAME_SIZE, server->config.max_frame_size},
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, server->config.max_concurrent_streams},
  };
  res = nghttp2_submit_settings(ezsession->ngsession, NGHTTP2_FLAG_NONE, siv, 3);
  if (res < 0) {
    return 1;
  }

  return 0;
}

/* Fills session->peer with "unix", "a.b.c.d:port" or "[v6]:port"; for TCP it also stores the
 * peer address in sockaddr/socklen. Leaves peer untouched if getpeername() fails. */
static void session_set_peer(grpc_session *ezsession) {
  if (ezsession->is_unix) {
    snprintf(ezsession->peer, sizeof(ezsession->peer), "unix");
    return;
  }
  int namelen = sizeof(ezsession->sockaddr);
  if (uv_tcp_getpeername(&ezsession->handle.tcp, (struct sockaddr *)&ezsession->sockaddr,
                         &namelen) != 0) {
    return;
  }
  ezsession->socklen = namelen;
  format_peer((const struct sockaddr *)&ezsession->sockaddr, ezsession->peer,
              sizeof(ezsession->peer));
}

void format_peer(const struct sockaddr *sa, char *out, size_t out_len) {
  char addr[INET6_ADDRSTRLEN] = "";
  switch (sa->sa_family) {
  case AF_INET: {
    const struct sockaddr_in *sin = (const struct sockaddr_in *)sa;
    inet_ntop(AF_INET, &sin->sin_addr.s_addr, addr, sizeof(addr));
    snprintf(out, out_len, "%s:%u", addr, ntohs(sin->sin_port));
  } break;
  case AF_INET6: {
    const struct sockaddr_in6 *sin6 = (const struct sockaddr_in6 *)sa;
    inet_ntop(AF_INET6, sin6->sin6_addr.s6_addr, addr, sizeof(addr));
    snprintf(out, out_len, "[%s]:%u", addr, ntohs(sin6->sin6_port));
  } break;
  default:
    break;
  }
}

/* libuv alloc callback, called before each read: hands out the server-wide rbuf. Sharing it is
 * safe because on_session_read() consumes the data before the loop reads again. */
static void on_session_alloc(uv_handle_t *handle, [[maybe_unused]] size_t suggested_size,
                             uv_buf_t *buf) {
  grpc_server *server = ((grpc_session *)handle->data)->server;
  *buf = uv_buf_init((char *)server->rbuf, (unsigned int)server->rbuf_len);
}

/* Feeds received HTTP/2 bytes to nghttp2, which runs the frame callbacks synchronously. Returns
 * 0 on success, 1 on an nghttp2 error (the caller closes the session). */
static int session_recv(grpc_session *ezsession, const u8 *data, size_t len) {
  ssize_t res = nghttp2_session_mem_recv(ezsession->ngsession, data, len);
  if (res < 0) {
    LOG_ERROR(ezsession->server, "@ %s: nghttp2: %s. killing...\n", __func__,
              nghttp2_strerror((int)res));
    return 1;
  }
  return 0;
}

/* Feeds ciphertext to TLS, advances the handshake (recording the client certificate subject when
 * it completes) and passes all available plaintext to session_recv(), reusing rbuf. Returns 0
 * on success or while the handshake or a record is still incomplete, 1 when the session must be
 * closed (TLS error, handshake failure, close_notify from the peer, or an nghttp2 error). */
static int session_recv_tls(grpc_session *ezsession, const u8 *data, size_t len) {
  grpc_server *server = ezsession->server;
  if (grpc_tls_feed(ezsession, data, len)) {
    return 1;
  }

  if (!ezsession->tls_handshake_done) {
    int hres = grpc_tls_handshake(ezsession);
    if (hres < 0) {
      LOG_ERROR(server, "TLS handshake failed on session %p\n", (void *)ezsession);
      return 1;
    }
    if (hres == 0) {
      return 0; // Handshake in progress
    }
    (void)grpc_tls_peer_subject(ezsession, ezsession->peer_cert_subject,
                                sizeof(ezsession->peer_cert_subject));
  }

  /* the ciphertext now lives in the read BIO, so the read buffer can hold the plaintext */
  for (;;) {
    ssize_t n = grpc_tls_read(ezsession, server->rbuf, server->rbuf_len);
    if (n == -2) {
      return 0;
    }
    if (n <= 0) {
      return 1;
    }
    if (session_recv(ezsession, server->rbuf, (size_t)n)) {
      return 1;
    }
  }
}

/* libuv read callback for a session: called with data, with nread < 0 on EOF or error (the
 * session is closed), or with nread == 0 (ignored). Updates the byte counter and activity time,
 * passes the data through TLS and nghttp2, and schedules a flush so that whatever nghttp2 wants
 * to answer (SETTINGS ACK, WINDOW_UPDATE, responses queued by callbacks) goes out. */
static void on_session_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  grpc_session *ezsession = stream->data;
  if (nread == 0 || !ezsession->is_active || ezsession->is_closing) {
    return;
  }
  ezsession->server->ready_events++;

  if (nread < 0) {
    if (nread != UV_EOF) {
      LOG_DEBUG(ezsession->server, "read: %s\n", uv_strerror((int)nread));
    }
    session_close(ezsession);
    return;
  }
  atomic_fetch_add(&ezsession->server->stat_bytes_received, (uint64_t)nread);
  ezsession->last_activity_ms = grpc_time_now_ms();

  int res = ezsession->is_tls ? session_recv_tls(ezsession, (const u8 *)buf->base, (size_t)nread)
                              : session_recv(ezsession, (const u8 *)buf->base, (size_t)nread);
  if (res) {
    LOG_TRACE(ezsession->server, "closing session %p", (void *)ezsession);
    session_close(ezsession);
    return;
  }
  /* nghttp2 may want to answer (SETTINGS ACK, WINDOW_UPDATE, ...) */
  session_schedule_flush(ezsession);
}

/* libuv close callback for a rejected connection: frees its heap-allocated handle. */
static void on_reject_close(uv_handle_t *handle) {
  grpc_free(handle);
}

/* Accepts the pending connection into a temporary heap handle and closes it right away. libuv
 * stops accepting until the pending connection is taken, so a connection that cannot be served
 * must still be accepted. If even the temporary handle cannot be set up, it is left pending. */
static void reject_connection(grpc_server *server, uv_stream_t *listener, bool is_unix) {
  if (is_unix) {
    uv_pipe_t *pipe = grpc_malloc(sizeof(*pipe));
    if (pipe == nullptr)
      return;
    if (uv_pipe_init(&server->loop, pipe, 0) != 0) {
      grpc_free(pipe);
      return;
    }
    (void)uv_accept(listener, (uv_stream_t *)pipe);
    uv_close((uv_handle_t *)pipe, on_reject_close);
    return;
  }
  uv_tcp_t *tcp = grpc_malloc(sizeof(*tcp));
  if (tcp == nullptr) {
    return;
  }
  if (uv_tcp_init(&server->loop, tcp) != 0) {
    grpc_free(tcp);
    return;
  }
  (void)uv_accept(listener, (uv_stream_t *)tcp);
  uv_close((uv_handle_t *)tcp, on_reject_close);
}

/* libuv connection callback for the TCP and Unix listeners. Rejects the connection when no
 * session slot is free; otherwise takes the slot on top of the free stack, initializes the
 * handle, accepts, enables SO_KEEPALIVE and TCP_NODELAY on TCP, records the peer, creates the
 * TLS/nghttp2 session and starts reading. Once the slot is marked active every failure goes
 * through session_close(), which returns the slot from on_session_close(). */
void on_listener_connection(uv_stream_t *listener, int status) {
  grpc_server *server = listener->data;
  if (status < 0) {
    LOG_ERROR(server, "accept: %s\n", uv_strerror(status));
    return;
  }
  bool is_unix = server->unix_listener_active && listener == (uv_stream_t *)&server->unix_listener;

  if (server->nb_free_sessions == 0) {
    LOG_WARNING(server, "max_connections (%zu) reached, rejecting a connection\n",
                server->nb_sessions);
    reject_connection(server, listener, is_unix);
    return;
  }

  size_t ndx = server->free_sessions[server->nb_free_sessions - 1];
  grpc_session *session = &server->sessions[ndx];
  int init_rc = is_unix ? uv_pipe_init(&server->loop, &session->handle.pipe, 0)
                        : uv_tcp_init(&server->loop, &session->handle.tcp);
  if (init_rc != 0) {
    reject_connection(server, listener, is_unix);
    return;
  }
  session = session_slot_acquire(server);
  session->is_unix = is_unix;
  session_handle(session)->data = session;
  /* from here on, session_close() returns the slot */

  if (uv_accept(listener, session_stream(session)) != 0) {
    session_close(session);
    return;
  }
  server->ready_events++;

  if (!is_unix) {
    uv_os_fd_t fd;
    if (uv_fileno((uv_handle_t *)&session->handle.tcp, &fd) == 0)
      (void)setsockopt((uv_os_sock_t)(uintptr_t)fd, SOL_SOCKET, SO_KEEPALIVE,
                       (const char *)&(int){1}, sizeof(int));
    (void)uv_tcp_nodelay(&session->handle.tcp, 1);
  }
  session_set_peer(session);
  LOG_DEBUG(server, "connection from %s\n", session->peer);

  if (session_create(session)) {
    LOG_ERROR(server, "cannot set up the connection from %s\n", session->peer);
    session_close(session);
    return;
  }

  if (uv_read_start(session_stream(session), on_session_alloc, on_session_read) != 0) {
    session_close(session);
    return;
  }

  /* our SETTINGS frame */
  session_schedule_flush(session);
}

/* Pushes the session onto the server's flush list unless it is already queued (is_flush_queued).
 * The list is LIFO; server_flush_sessions() drains it. */
void session_schedule_flush(grpc_session *session) {
  if (session->is_flush_queued)
    return;
  session->is_flush_queued = true;
  session->flush_next = session->server->flush_head;
  session->server->flush_head = session;
}

/* Pops every queued session and flushes it with session_flush(). Called by grpc_server_poll()
 * before and after running the loop, by the maintenance timer, grpc_server_drain(),
 * grpc_server_free() and grpc_server_flush(). A flush may close a session; closing ones are
 * skipped by session_flush(). */
void server_flush_sessions(grpc_server *server) {
  grpc_session *session;
  while ((session = server->flush_head) != nullptr) {
    server->flush_head = session->flush_next;
    session->flush_next = nullptr;
    session->is_flush_queued = false;
    session_flush(session);
  }
}

/* Moves frames queued in nghttp2 into obuf until about GRPC_OBUF_HIGH_WATER bytes are pending.
 * With TLS, the frames are batched in the shared server->tls_obuf, encrypted with a single
 * grpc_tls_write() and the ciphertext drained into obuf; nothing is pulled from nghttp2 before
 * the handshake is done, though handshake output still gets drained. Returns SERIALIZE_OK,
 * SERIALIZE_BLOCKED when a buffer could not grow before anything was lost (what was serialized
 * so far is in obuf; retry later), or SERIALIZE_FATAL on an nghttp2 or TLS error, including
 * their own allocation failures, after which the connection must close. */
static int session_serialize(grpc_session *ezsession) {
  /* TLS application data cannot be written before the handshake completes */
  bool can_send = !ezsession->is_tls || ezsession->tls_handshake_done;
  grpc_buf *out = ezsession->is_tls ? &ezsession->server->tls_obuf : &ezsession->obuf;
  int result = SERIALIZE_OK;

  while (can_send && ezsession->obuf.len + out->len < GRPC_OBUF_HIGH_WATER) {
    /* reserve first: a chunk taken from nghttp2 cannot be given back */
    if (buf_reserve(out, GRPC_SERIALIZE_HEADROOM)) {
      result = SERIALIZE_BLOCKED;
      break;
    }
    const u8 *data;
    ssize_t n = nghttp2_session_mem_send(ezsession->ngsession, &data);
    if (n < 0) {
      LOG_ERROR(ezsession->server, "nghttp2: %s. killing...\n", nghttp2_strerror((int)n));
      return SERIALIZE_FATAL;
    }
    if (n == 0)
      break;
    if (buf_append(out, data, (size_t)n)) /* larger than the headroom: cannot happen */
      return SERIALIZE_FATAL;
  }

  if (ezsession->is_tls) {
    /* one SSL_write for the whole batch keeps the records large */
    if (out->len > 0) {
      ssize_t n = grpc_tls_write(ezsession, out->data, out->len);
      out->len = 0;
      if (n <= 0)
        return SERIALIZE_FATAL;
    }
    int drained = grpc_tls_drain(ezsession, &ezsession->obuf);
    if (drained == TLS_DRAIN_NOMEM)
      return SERIALIZE_BLOCKED;
    if (drained != TLS_DRAIN_OK)
      return SERIALIZE_FATAL;
  }
  return result;
}

/* flush_retry_timer callback: re-queues the sessions whose output was blocked by an allocation
 * failure and flushes them; a session still blocked re-arms the timer. */
static void on_flush_retry(uv_timer_t *handle) {
  grpc_server *server = handle->data;
  for (size_t i = 0; i < server->nb_sessions; i++) {
    grpc_session *s = &server->sessions[i];
    if (s->is_flush_blocked) {
      s->is_flush_blocked = false;
      if (s->is_active && !s->is_closing)
        session_schedule_flush(s);
    }
  }
  server_flush_sessions(server);
}

/* Marks the session's output as blocked by an allocation failure and arms the retry timer. */
static void session_flush_blocked(grpc_session *ezsession) {
  grpc_server *server = ezsession->server;
  ezsession->is_flush_blocked = true;
  if (server->flush_retry_timer_active && !uv_is_active((uv_handle_t *)&server->flush_retry_timer))
    (void)uv_timer_start(&server->flush_retry_timer, on_flush_retry, GRPC_FLUSH_RETRY_MS, 0);
}

/* libuv write callback for the uv_write() of wbuf started by session_flush(); also runs with
 * UV_ECANCELED when the handle is closed first. Marks the write finished, then closes the
 * session on error or resumes the flush, which may start the next write. */
static void on_session_write(uv_write_t *req, int status) {
  grpc_session *ezsession = req->data;
  ezsession->is_writing = false;
  ezsession->wbuf.len = 0;
  if (!ezsession->is_active || ezsession->is_closing)
    return;
  if (status < 0) {
    LOG_DEBUG(ezsession->server, "write: %s\n", uv_strerror(status));
    session_close(ezsession);
    return;
  }
  session_flush(ezsession);
}

/* Serializes and writes the session's pending output. Writes synchronously with uv_try_write()
 * while the socket accepts everything; on a partial write it swaps obuf and wbuf and lets
 * uv_write() send the remainder from wbuf (counted as sent right away) while obuf collects new
 * output, then returns. Does nothing while a write is in flight: on_session_write() resumes it.
 * When an output buffer cannot grow, the session keeps its pending frames and the flush is
 * retried after GRPC_FLUSH_RETRY_MS. Closes the session on other errors (including nghttp2 or
 * the TLS library running out of memory, which they treat as fatal), and once nghttp2 wants
 * neither to read nor to write (e.g. after a GOAWAY exchange). */
void session_flush(grpc_session *ezsession) {
  if (ezsession->ops != nullptr && ezsession->ops->flush != nullptr) {
    ezsession->ops->flush(ezsession);
    return;
  }
  /* an in-flight write resumes the flush from on_session_write */
  if (!ezsession->is_active || ezsession->is_closing || ezsession->is_writing)
    return;
  if (ezsession->close_on_flush) {
    session_close(ezsession);
    return;
  }

  for (;;) {
    int serialized = session_serialize(ezsession);
    if (serialized == SERIALIZE_FATAL) {
      session_close(ezsession);
      return;
    }
    if (serialized == SERIALIZE_BLOCKED && ezsession->obuf.len == 0) {
      /* out of memory with nothing to write: try again shortly instead of dropping the
       * connection (an RPC that needs memory to proceed fails on its own) */
      session_flush_blocked(ezsession);
      return;
    }
    if (ezsession->obuf.len == 0)
      break;

    uv_buf_t buf = uv_buf_init((char *)ezsession->obuf.data, (unsigned int)ezsession->obuf.len);
    int n = uv_try_write(session_stream(ezsession), &buf, 1);
    if (n == UV_EAGAIN)
      n = 0;
    if (n < 0) {
      LOG_DEBUG(ezsession->server, "write: %s\n", uv_strerror(n));
      session_close(ezsession);
      return;
    }
    if (n > 0)
      atomic_fetch_add(&ezsession->server->stat_bytes_sent, (uint64_t)n);
    if ((size_t)n == ezsession->obuf.len) {
      ezsession->obuf.len = 0;
      continue;
    }

    /* the socket buffer is full. let libuv write the rest while obuf keeps collecting */
    grpc_buf tmp = ezsession->wbuf;
    ezsession->wbuf = ezsession->obuf;
    ezsession->obuf = tmp;
    ezsession->obuf.len = 0;
    size_t rem = ezsession->wbuf.len - (size_t)n;
    atomic_fetch_add(&ezsession->server->stat_bytes_sent, (uint64_t)rem);
    buf = uv_buf_init((char *)ezsession->wbuf.data + n, (unsigned int)rem);
    ezsession->write_req.data = ezsession;
    if (uv_write(&ezsession->write_req, session_stream(ezsession), &buf, 1, on_session_write) !=
        0) {
      session_close(ezsession);
      return;
    }
    ezsession->is_writing = true;
    return;
  }

  if (!nghttp2_session_want_read(ezsession->ngsession) &&
      !nghttp2_session_want_write(ezsession->ngsession))
    session_close(ezsession);
}
