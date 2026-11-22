/* internal_helpers.c - session slots and closing, memory hooks, streams, debugging helpers
 *
 * Everything here runs on the server's loop thread. Accepting, reading and writing sessions is in
 * internal_session_io.c.
 *
 * Closing is two-phase. session_close() tears down the streams (cancelling their calls), the
 * nghttp2 session and TLS, then calls uv_close(); only on_session_close() frees the buffers and
 * returns the slot to the free list, so a slot is never reused while libuv still references its
 * handle. Session buffers are grpc_buf, allocated with the grpc allocator (on MSVC
 * _aligned_malloc()), so they must be released with buf_free()/grpc_free(), never libc free().
 *
 * The file also holds small debugging helpers (ezdump, ezgetdt, ezflog) and the grpc_stream
 * constructors.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <stddef.h>
#include "internal/grpc_platform.h"
#include <stdio.h>
#include "core.h"
#include "grpc_header.h"
#include "grpc_timeout.h"
#include "internal_nghttp2_callbacks.h"
#include "internal_helpers.h"
#include "internal_tls.h"

/* Prints len bytes to stdout as hex pairs, with an extra space after the 8th byte. */
static void dump_decode_binary(const i8 *data, size_t len) {
  const i8 look_up[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                          '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  for (size_t i = 0; i < len; i++) {
    putchar(look_up[(data[i] >> 4) & 0x0f]);
    putchar(look_up[data[i] & 0x0f]);
    putchar(' ');
    if (i == 7)
      putchar(' ');
  }
}

/* Prints len bytes to stdout as ASCII, with '.' for control and non-ASCII bytes. */
static void dump_decode_ascii(const i8 *data, size_t len) {
  for (size_t i = 0; i < len; i++) {
    if (data[i] < 0x20) {
      putchar('.');
    } else if (data[i] < 0x7f)
      putchar(data[i]);
    else
      putchar('.');
  }
}

/* Debugging aid: hex dumps len bytes to stdout, 16 per line with an ASCII column; the last line
 * is padded so the columns line up. Prints nothing when len is 0. */
void ezdump(void *vdata, size_t len) {
  const i8 *data = vdata;
  if (len == 0)
    return;

  size_t cur = 0;

  for (; cur < 16 * (len / 16); cur += 16) {
    dump_decode_binary(data + cur, 16);
    printf("| ");
    dump_decode_ascii(data + cur, 16);
    printf(" |");
    puts("");
  }
  /* write the remaining */
  if (len % 16) {
    dump_decode_binary(data + cur, len % 16);
    /* write the empty */
    for (size_t i = 0; i < 16 - (len % 16); i++) {
      printf("   ");
      if (i == 7 && (7 < 16 - (len % 16)))
        putchar(' ');
    }
    printf("| ");
    dump_decode_ascii(data + cur, len % 16);
    for (size_t i = 0; i < 16 - (len % 16); i++) {
      putchar(' ');
    }
    printf(" |");
    puts("");
    cur += len % 16;
  }
  assert(len == cur);
}

/* Writes the local time into buf (at most len bytes) and returns buf. On Unix the format is
 * "YYYY-MM-DD HH:MM:SS.nnnnnnnnn"; elsewhere it is "YYYY/MM/DD HH:MM:SS" with whole seconds. */
char *ezgetdt(char *buf, size_t len) {
#ifdef __unix__
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  char tmp[32] = {};
  struct tm stm;
  localtime_r(&ts.tv_sec, &stm);
  strftime(tmp, sizeof(tmp), "%F %T", &stm);
  snprintf(buf, len, "%s.%09ld", tmp, ts.tv_nsec);
#else
  time_t t = time(nullptr);
  struct tm stm;
#ifdef _WIN32
  localtime_s(&stm, &t);
#else
  localtime_r(&t, &stm);
#endif
  snprintf(buf, len, "%d/%02d/%02d %02d:%02d:%02d", stm.tm_year + 1900, stm.tm_mon + 1, stm.tm_mday,
           stm.tm_hour, stm.tm_min, stm.tm_sec);
#endif
  return buf;
}

/* Prints a "[timestamp] " prefix followed by the formatted message to fp. Unlike server_log(),
 * it ignores log levels and is not tied to a server. */
void ezflog(FILE *fp, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char dt[128] = {};
  fprintf(fp, "[%s] ", ezgetdt(dt, sizeof(dt)));
  vfprintf(fp, fmt, args);
  va_end(args);
}

/* Pops the slot on top of the free stack (the caller checked nb_free_sessions > 0), marks it
 * active and resets the per-connection state and timestamps; counts the connection. Shared by TCP
 * accept and the HTTP/3 server. */
grpc_session *session_slot_acquire(grpc_server *server) {
  size_t ndx = server->free_sessions[--server->nb_free_sessions];
  grpc_session *session = &server->sessions[ndx];
  session->server = server;
  session->index = ndx;
  session->is_active = true;
  session->is_closing = false;
  uint64_t now = grpc_time_now_ms();
  session->created_ms = now;
  session->last_activity_ms = now;
  session->idle_since_ms = now;
  session->last_client_ping_ms = 0;
  session->ping_pending = false;
  session->goaway_sent = false;
  session->ping_strikes = 0;
  atomic_fetch_add(&server->stat_active_connections, 1);
  atomic_fetch_add(&server->stat_total_connections, 1);
  return session;
}

/* Returns a closed session's slot: frees obuf and wbuf (grpc allocator), resets the per
 * connection state and pushes the slot back onto the free stack. The only place a slot is
 * returned; TCP sessions reach it from the uv_close callback, HTTP/3 sessions from theirs. */
void session_slot_release(grpc_session *session) {
  grpc_server *server = session->server;
  size_t ndx = session->index;

  buf_free(&session->obuf);
  buf_free(&session->wbuf);
  session->nb_streams = 0;
  session->tls_handshake_done = false;
  session->is_writing = false;
  session->is_unix = false;
  session->ping_pending = false;
  session->goaway_sent = false;
  session->ping_strikes = 0;
  session->last_client_ping_ms = 0;
  memset(&session->sockaddr, 0, sizeof(session->sockaddr));
  session->peer[0] = '\0';
  session->peer_cert_subject[0] = '\0';
  session->is_closing = false;
  session->is_active = false;
  session->is_flush_blocked = false;
  session->close_on_flush = false;
  session->ops = nullptr;

  atomic_fetch_sub(&server->stat_active_connections, 1);
  server->free_sessions[server->nb_free_sessions++] = ndx;
}

/* uv_close callback of a TCP/pipe session handle: returns the slot. */
static void on_session_close(uv_handle_t *handle) {
  session_slot_release(handle->data);
}

/* Starts closing an open session; does nothing if it is already closing or inactive, so it is
 * safe to call repeatedly from any error path on the loop thread. Stops reading, unlinks and
 * frees every stream (detaching its call with GRPC_STATUS_UNAVAILABLE), deletes the nghttp2
 * session, sends a best-effort TLS close_notify and frees the TLS state, then hands the handle
 * to uv_close(). The slot stays taken (is_active) until on_session_close() runs. */
void session_close(grpc_session *session) {
  if (session == nullptr || !session->is_active || session->is_closing) {
    return;
  }
  if (session->ops != nullptr && session->ops->close != nullptr) {
    session->ops->close(session);
    return;
  }
  session->is_closing = true;
  LOG_DEBUG(session->server, "closing connection from %s\n", session->peer);

  uv_read_stop(session_stream(session));

  /* before nghttp2_session_del(): the streams hold nghttp2 buffers that are
   * released through the session */
  grpc_stream *ezstream;
  while ((ezstream = session->streams) != nullptr) {
    stream_unlink(session, ezstream);
    /* the client is gone: its calls are cancelled */
    if (ezstream->call != nullptr)
      call_detach(ezstream->call, GRPC_STATUS_UNAVAILABLE);
    stream_free(ezstream);
  }

  if (session->ngsession != nullptr) {
    nghttp2_session_del(session->ngsession);
    session->ngsession = nullptr;
  }

  if (session->is_tls) {
    /* best effort close_notify. skipped if it would overtake queued output */
    if (session->tls_handshake_done && !session->is_writing) {
      session->obuf.len = 0;
      grpc_tls_close_notify(session);
      if (!grpc_tls_drain(session, &session->obuf) && session->obuf.len > 0) {
        uv_buf_t buf = uv_buf_init((char *)session->obuf.data, (unsigned int)session->obuf.len);
        (void)uv_try_write(session_stream(session), &buf, 1);
      }
    }
    grpc_tls_session_free(session);
    session->is_tls = false;
  }

  /* pending writes are cancelled before on_session_close runs */
  uv_close(session_handle(session), on_session_close);
}

void ignore_default_sigpipe(void) {
#ifndef _WIN32
  struct sigaction old;
  if (sigaction(SIGPIPE, nullptr, &old) == 0 && !(old.sa_flags & SA_SIGINFO) &&
      old.sa_handler == SIG_DFL) {
    struct sigaction ign = {0};
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    (void)sigaction(SIGPIPE, &ign, nullptr);
  }
#endif
}

void *grpc_mem_hook_malloc(size_t size, [[maybe_unused]] void *user_data) {
  return grpc_malloc(size);
}

void grpc_mem_hook_free(void *ptr, [[maybe_unused]] void *user_data) {
  grpc_free(ptr);
}

void *grpc_mem_hook_calloc(size_t nmemb, size_t size, [[maybe_unused]] void *user_data) {
  return grpc_calloc(nmemb, size);
}

void *grpc_mem_hook_realloc(void *ptr, size_t size, [[maybe_unused]] void *user_data) {
  return grpc_realloc(ptr, size);
}

const nghttp2_mem grpc_nghttp2_mem = {
    .mem_user_data = nullptr,
    .malloc = grpc_mem_hook_malloc,
    .free = grpc_mem_hook_free,
    .calloc = grpc_mem_hook_calloc,
    .realloc = grpc_mem_hook_realloc,
};

/* Allocates a zeroed stream for stream_id whose receive buffer starts as the inline storage.
 * Returns nullptr when out of memory. The caller links it with stream_link(). */
grpc_stream *stream_new(int64_t stream_id, const grpc_transport_ops *ops) {
  grpc_stream *ezstream = grpc_calloc(1, sizeof(*ezstream));
  if (ezstream == nullptr)
    return nullptr;
  ezstream->stream_id = stream_id;
  ezstream->ops = ops;
  ezstream->recv_data = ezstream->recv_inline;
  ezstream->recv_cap = sizeof(ezstream->recv_inline);
  return ezstream;
}

/* Frees a stream that is already unlinked: its heap receive buffer, pending trailers, the
 * references held on nghttp2's header buffers and the queued outgoing messages. Must run before
 * the nghttp2 session is deleted, because the header rcbufs are released through it. Does not
 * touch the stream's call; callers detach it first. Accepts nullptr. */
void stream_free(grpc_stream *ezstream) {
  if (ezstream == nullptr)
    return;
  if (ezstream->recv_data != ezstream->recv_inline)
    grpc_free(ezstream->recv_data);
  grpc_free(ezstream->pending_trailers);

  for (size_t i = 0; i < ezstream->nb_headers; i++) {
    ezstream->ops->hdr_decref(ezstream->header_refs[i][0]);
    ezstream->ops->hdr_decref(ezstream->header_refs[i][1]);
  }

  msg_queue_clear(&ezstream->lqueue_omessages);

  grpc_free(ezstream);
}

/* Adds the stream at the head of the session's stream list and bumps the stream counters. */
void stream_link(grpc_session *session, grpc_stream *stream) {
  stream->prev = nullptr;
  stream->next = session->streams;
  if (session->streams != nullptr)
    session->streams->prev = stream;
  session->streams = stream;
  session->nb_streams++;
  atomic_fetch_add(&session->server->stat_active_streams, 1);
}

/* Removes the stream from the session's list and decrements the stream counters; the stream
 * itself is not freed. */
void stream_unlink(grpc_session *session, grpc_stream *stream) {
  if (stream->prev != nullptr)
    stream->prev->next = stream->next;
  else
    session->streams = stream->next;
  if (stream->next != nullptr)
    stream->next->prev = stream->prev;
  stream->prev = stream->next = nullptr;
  session->nb_streams--;
  if (session->nb_streams == 0)
    session->idle_since_ms = grpc_time_now_ms();
  atomic_fetch_sub(&session->server->stat_active_streams, 1);
}
