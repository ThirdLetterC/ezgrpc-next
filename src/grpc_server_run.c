/* grpc_server_run.c - running a server: poll, run, stop, drain, post and flush
 *
 * Part of the server described in grpc_server.c.
 */

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef _WIN32
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#endif

#include "core.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "internal_nghttp2_callbacks.h"
#include "internal_http3.h"
#include "internal_server.h"
#include "ulog/ulog.h"

/* ---- running ---------------------------------------------------------------------- */

/* Does nothing: the poll timer only exists to make uv_run(UV_RUN_ONCE) return after a
 * grpc_server_poll() timeout. */
static void on_poll_timer([[maybe_unused]] uv_timer_t *handle) {}

/* One iteration of the event loop: releases the previous poll's events, flushes pending output,
 * runs libuv once (blocking for at most timeout_ms; 0 does not block, negative blocks until
 * something happens), dispatches the calls that became ready and flushes the responses. Returns
 * the number of events now available from grpc_server_events(). */
static int server_poll(grpc_server *server, int timeout_ms, bool keep_events);

GRPC_API int grpc_server_poll(grpc_server *server, int timeout_ms) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  return server_poll(server, timeout_ms, false);
}

/* grpc_server_poll(); with keep_events (grpc_server_drain()) the events of earlier iterations are
 * kept, so none is lost before the application sees it. */
static int server_poll(grpc_server *server, int timeout_ms, bool keep_events) {
  if (!keep_events) {
    server_clear_events(server);
    server->ready_events = 0;
  }

  /* write everything queued since the last poll, one write per connection */
  server_flush_sessions(server);

  if (timeout_ms > 0) {
    uv_timer_start(&server->timer, on_poll_timer, (uint64_t)timeout_ms, 0);
    uv_run(&server->loop, UV_RUN_ONCE);
    uv_timer_stop(&server->timer);
  } else if (timeout_ms == 0) {
    uv_run(&server->loop, UV_RUN_NOWAIT);
  } else {
    uv_run(&server->loop, UV_RUN_ONCE);
  }

  server_dispatch(server);
  /* what the handlers answered */
  server_flush_sessions(server);
  return (int)server->nb_events;
}

/* Returns the events produced by the last grpc_server_poll(); they stay valid until the next
 * poll or grpc_server_free(). */
GRPC_API const grpc_event *grpc_server_events(grpc_server *server, size_t *count) {
  if (server == nullptr) {
    if (count != nullptr)
      *count = 0;
    return nullptr;
  }
  if (count != nullptr)
    *count = server->nb_events;
  return server->events;
}

/* Polls without a timeout until the stop flag is set, then clears it so the server can be run
 * again. */
GRPC_API grpc_err grpc_server_run(grpc_server *server) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  while (!atomic_load(&server->stop)) {
    int res = grpc_server_poll(server, -1);
    if (res < 0)
      return (grpc_err)res;
  }
  /* the server can be run again */
  atomic_store(&server->stop, false);
  /* the loop thread may exit now: release its TLS-library caches (wolfSSL FP_ECC) */
  if (server->h3 != nullptr)
    http3_thread_cleanup();
  return GRPC_OK;
}

/* Sets the stop flag and wakes the loop with uv_async_send(), so it is safe from any thread and
 * from a signal handler. grpc_server_run() returns after the current poll. */
GRPC_API void grpc_server_stop(grpc_server *server) {
  if (server == nullptr)
    return;
  atomic_store(&server->stop, true);
  /* async-signal-safe */
  uv_async_send(&server->async);
}

GRPC_API bool grpc_server_stop_requested(const grpc_server *server) {
  return server != nullptr && atomic_load(&server->stop);
}

/* The signals grpc_server_stop_on_signals() watches. libuv reports Ctrl+C (and Ctrl+Break) on a
 * Windows console as SIGINT; SIGTERM is never raised there, but watching it is harmless. */
static const int stop_signal_numbers[2] = {SIGINT, SIGTERM};

/* libuv signal callback, on the loop thread. Stops watching both signals, which gives them their
 * default action back, so a second Ctrl+C ends a shutdown that hangs; then stops the server. */
static void on_stop_signal(uv_signal_t *handle, int signum) {
  grpc_server *server = handle->data;
  for (size_t i = 0; i < 2; i++)
    uv_signal_stop(&server->stop_signals[i]);
  LOG_INFO(server, "received %s, stopping (repeat it to exit at once)\n",
             signum == SIGINT ? "SIGINT" : "SIGTERM");
  grpc_server_stop(server);
}

/* Watches SIGINT and SIGTERM on the server's loop. The handles are initialized once, unreferenced
 * so that they do not keep the loop alive, and closed by grpc_server_free(). */
GRPC_API grpc_err grpc_server_stop_on_signals(grpc_server *server) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  if (!server->stop_signals_active) {
    for (size_t i = 0; i < 2; i++) {
      /* uv_signal_init() cannot fail once the loop is initialized */
      uv_signal_init(&server->loop, &server->stop_signals[i]);
      server->stop_signals[i].data = server;
      uv_unref((uv_handle_t *)&server->stop_signals[i]);
    }
    server->stop_signals_active = true;
  }
  for (size_t i = 0; i < 2; i++) {
    int rc = uv_signal_start(&server->stop_signals[i], on_stop_signal, stop_signal_numbers[i]);
    if (rc != 0) {
      LOG_ERROR(server, "cannot watch signal %d: %s\n", stop_signal_numbers[i], uv_strerror(rc));
      for (size_t j = 0; j < i; j++)
        uv_signal_stop(&server->stop_signals[j]);
      return GRPC_ERR_IO;
    }
  }
  return GRPC_OK;
}

/* Counts the streams of sessions that are open and not closing. */
static size_t server_active_streams(const grpc_server *server) {
  size_t total = 0;
  for (size_t i = 0; i < server->nb_sessions; i++) {
    if (server->sessions[i].is_active && !server->sessions[i].is_closing)
      total += server->sessions[i].nb_streams;
  }
  return total;
}

/* Graceful shutdown on the loop thread: closes the listeners, submits GOAWAY on every open
 * session and sets the stop flag, then polls until no stream is left or timeout_ms elapses (a
 * positive timeout polls at most 100 ms at a time, 0 polls once without blocking, a negative one
 * waits without a limit). Returns GRPC_OK when all streams finished, GRPC_ERR_TIMEOUT
 * otherwise, or a poll error. The listeners are not reopened, so a drained server accepts no new
 * connections. */
GRPC_API grpc_err grpc_server_drain(grpc_server *server, int timeout_ms) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;

  /* Stop accepting new connections. */
  if (server->ipv4_listener_active) {
    uv_close((uv_handle_t *)&server->ipv4_listener, nullptr);
    server->ipv4_listener_active = false;
  }
  if (server->ipv6_listener_active) {
    uv_close((uv_handle_t *)&server->ipv6_listener, nullptr);
    server->ipv6_listener_active = false;
  }
  if (server->unix_listener_active) {
    uv_close((uv_handle_t *)&server->unix_listener, nullptr);
    server->unix_listener_active = false;
  }

  http3_server_stop_accepting(server);

  uint64_t deadline = timeout_ms > 0 ? grpc_time_now_ms() + (uint64_t)timeout_ms : 0;
  /* like a poll, drain starts by releasing the events of the previous one */
  server_clear_events(server);
  server->ready_events = 0;

  /* gRPC's two-step graceful shutdown on every session that has not been told yet: first a
   * notice (GOAWAY with the largest stream id, plus a PING over HTTP/2), so requests already on
   * their way are still accepted; once the client acknowledged the PING (at most 1 s, and never
   * past the drain deadline) the final GOAWAY names the last stream. A second GOAWAY on a
   * session that had one would be a protocol error (and aborts nghttp3). */
  bool noticed = false;
  for (size_t i = 0; i < server->nb_sessions; i++) {
    grpc_session *s = &server->sessions[i];
    if (s->is_active && !s->is_closing && s->ops != nullptr && !s->goaway_sent &&
        s->ops->shutdown_notice != nullptr) {
      s->ops->shutdown_notice(s);
      session_schedule_flush(s);
      noticed = true;
    }
  }
  server_flush_sessions(server);
  if (noticed && timeout_ms != 0) {
    uint64_t notice_end = grpc_time_now_ms() + 1000;
    if (timeout_ms > 0 && deadline < notice_end)
      notice_end = deadline;
    for (;;) {
      bool waiting = false;
      for (size_t i = 0; i < server->nb_sessions && !waiting; i++) {
        grpc_session *s = &server->sessions[i];
        waiting = s->is_active && !s->is_closing && !s->goaway_sent && s->ping_pending;
      }
      uint64_t now = grpc_time_now_ms();
      if (!waiting || now >= notice_end)
        break;
      uint64_t rem = notice_end - now;
      int res = server_poll(server, rem > 100 ? 100 : (int)rem, true);
      if (res < 0)
        return (grpc_err)res;
    }
  }
  uint64_t goaway_ms = grpc_time_now_ms();
  for (size_t i = 0; i < server->nb_sessions; i++) {
    grpc_session *s = &server->sessions[i];
    if (s->is_active && !s->is_closing && s->ops != nullptr && !s->goaway_sent) {
      s->ops->goaway(s);
      s->goaway_sent = true;
      s->goaway_sent_ms = goaway_ms;
      session_schedule_flush(s);
    }
  }
  server_flush_sessions(server);
  atomic_store(&server->stop, true);
  while (server_active_streams(server) > 0) {
    int wait_ms = -1;
    if (timeout_ms == 0) {
      (void)server_poll(server, 0, true);
      break;
    }
    if (timeout_ms > 0) {
      uint64_t now = grpc_time_now_ms();
      if (now >= deadline)
        break;
      uint64_t rem = deadline - now;
      wait_ms = rem > 100 ? 100 : (int)rem;
    }
    int res = server_poll(server, wait_ms, true);
    if (res < 0)
      return (grpc_err)res;
    /* calls on event routes need the application: hand the events back, it answers them and
     * calls grpc_server_drain() again */
    if (server->nb_events > 0)
      return GRPC_ERR_STATE;
  }

  server_flush_sessions(server);
  return server_active_streams(server) == 0 ? GRPC_OK : GRPC_ERR_TIMEOUT;
}

/* Appends a task to the mutex-protected queue and wakes the loop; on_async() runs it on the loop
 * thread. Safe from any thread. Returns GRPC_ERR_NOMEM if the task cannot be allocated. */
GRPC_API grpc_err grpc_server_post(grpc_server *server, void (*fn)(void *arg), void *arg) {
  if (server == nullptr || fn == nullptr)
    return GRPC_ERR_INVALID;
  post_task *task = grpc_malloc(sizeof(*task));
  if (task == nullptr)
    return GRPC_ERR_NOMEM;
  *task = (post_task){.fn = fn, .arg = arg};

  pthread_mutex_lock(&server->post_mutex);
  if (server->post_tail != nullptr)
    server->post_tail->next = task;
  else
    server->post_head = task;
  server->post_tail = task;
  pthread_mutex_unlock(&server->post_mutex);

  uv_async_send(&server->async);
  return GRPC_OK;
}

/* Writes the output of every session queued for flushing now instead of at the next poll. While
 * the deadline scan runs (an on_done callback of an expired call) the output stays queued: the
 * poll writes it as soon as the scan is over, and the scan's walk over the sessions stays safe. */
GRPC_API void grpc_server_flush(grpc_server *server) {
  if (server != nullptr && !server->in_deadline_scan)
    server_flush_sessions(server);
}
