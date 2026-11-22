/* grpc_server.c - creating and freeing a server, its timers and its async wakeup
 *
 * The server is split over grpc_server.c, grpc_server_listen.c (TCP and Unix listeners),
 * grpc_server_routes.c (routes and statistics), grpc_server_log.c (logging) and
 * grpc_server_run.c (poll, run, stop, drain, post). This comment describes the whole.
 *
 * A grpc_server owns one libuv loop, driven by a single thread through grpc_server_poll() or
 * grpc_server_run(). Each poll flushes queued output, runs the loop once (accepting, reading and
 * feeding nghttp2, which queues calls), dispatches the ready calls to handlers or events
 * (server_dispatch() in grpc_call_dispatch.c) and flushes what the handlers answered. Everything except
 * the statistics, the log level, grpc_server_stop() and grpc_server_post() must be used from
 * that thread; stop and post wake the loop through the server's uv_async handle, and the
 * *_async call functions are built on grpc_server_post().
 *
 * Connections live in a fixed array of max_connections sessions handed out from a stack of free
 * indices; internal_session_io.c accepts, reads and writes them and internal_helpers.c closes them. Routes are allocated one
 * by one and are neither moved nor freed before grpc_server_free(), because calls keep pointers
 * to them: unregistering a path only disables its route.
 *
 * Logging goes through ulog. Each server gets its own topic and is linked into a process-wide
 * registry, so the single ulog output callback can write each line to the server's log_file.
 */
#include <errno.h>
#include <inttypes.h>
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

static const int DEFAULT_BACKLOG = 1024;
static const size_t DEFAULT_MAX_CONNECTIONS = 1024;
static const uint32_t DEFAULT_MAX_CONCURRENT_STREAMS = 1024;
static const uint32_t DEFAULT_MAX_MESSAGE_SIZE = 1u << 20;
static const uint32_t DEFAULT_MAX_FRAME_SIZE = 16u * 1024;

/* ---- setup ---------------------------------------------------------------------- */

/* libuv timer callback, every 250 ms when keepalive or connection age/idle limits are
 * configured. Per open session it closes the connection on a keepalive PING timeout or once a
 * GOAWAY's grace period ends (or no streams remain), sends GOAWAY when max_connection_age or
 * max_connection_idle is reached, and sends a keepalive PING after keepalive_time of inactivity.
 * Flushes the affected sessions itself, since it runs inside uv_run(). The timer is unref'd so
 * it never keeps the loop alive on its own. */
static void on_maintenance_timer(uv_timer_t *handle) {
  grpc_server *server = handle->data;
  const grpc_config *cfg = &server->config;
  uint64_t now = grpc_time_now_ms();
  uint32_t ka_timeout = cfg->keepalive_timeout_ms ? cfg->keepalive_timeout_ms : 20000;
  uint32_t age_grace = cfg->max_connection_age_grace_ms ? cfg->max_connection_age_grace_ms : 5000;

  for (size_t i = 0; i < server->nb_sessions; i++) {
    grpc_session *s = &server->sessions[i];
    if (!s->is_active || s->is_closing || s->ops == nullptr)
      continue;

    /* 1. Keepalive PING timeout */
    if (s->ping_pending && now >= s->ping_sent_ms && (now - s->ping_sent_ms) >= ka_timeout) {
      LOG_DEBUG(server, "keepalive timeout on %s, closing\n", s->peer);
      session_close(s);
      continue;
    }

    /* 2. Max connection age / grace period */
    if (s->goaway_sent) {
      if (s->nb_streams == 0 ||
          (now >= s->goaway_sent_ms && (now - s->goaway_sent_ms) >= age_grace)) {
        session_close(s);
        continue;
      }
    } else if (cfg->max_connection_age_ms > 0 && now >= s->created_ms &&
               (now - s->created_ms) >= cfg->max_connection_age_ms) {
      s->ops->goaway(s);
      s->goaway_sent = true;
      s->goaway_sent_ms = now;
      session_schedule_flush(s);
      continue;
    }

    /* 3. Max connection idle: no stream for max_connection_idle_ms (counted from when the last one
     * ended, so a slow call does not count as idle time) */
    if (cfg->max_connection_idle_ms > 0 && s->nb_streams == 0 && now >= s->idle_since_ms &&
        (now - s->idle_since_ms) >= cfg->max_connection_idle_ms) {
      s->ops->goaway(s);
      s->goaway_sent = true;
      s->goaway_sent_ms = now;
      session_schedule_flush(s);
      continue;
    }

    /* 4. Send a keepalive PING if idle (QUIC transports keep themselves alive: ping is nullptr) */
    if (cfg->keepalive_time_ms > 0 && s->ops->ping != nullptr && !s->ping_pending &&
        now >= s->last_activity_ms && (now - s->last_activity_ms) >= cfg->keepalive_time_ms) {
      if (s->ops->ping(s) == 0) {
        s->ping_pending = true;
        s->ping_sent_ms = now;
        session_schedule_flush(s);
      }
    }
  }
  server_flush_sessions(server);
}

/* Detaches the whole grpc_server_post() queue under post_mutex, then runs each task in order and
 * frees it, outside the lock so that tasks may post again (those run on the next wakeup). Loop
 * thread only. */
static void run_posted(grpc_server *server) {
  pthread_mutex_lock(&server->post_mutex);
  post_task *task = server->post_head;
  server->post_head = server->post_tail = nullptr;
  pthread_mutex_unlock(&server->post_mutex);

  while (task != nullptr) {
    post_task *next = task->next;
    task->fn(task->arg);
    grpc_free(task);
    task = next;
  }
}

/* libuv async callback, invoked on the loop thread after grpc_server_stop() or
 * grpc_server_post() called uv_async_send(); libuv may coalesce several sends into one call.
 * Runs the posted tasks; the stop flag itself is checked by grpc_server_run(). */
static void on_async(uv_async_t *handle) {
  grpc_server *server = handle->data;
  server->ready_events++;
  run_posted(server);
}

/* Copies the config and fills in defaults, validates the log level, frame and message limits
 * and TLS settings, then binds the sockets before setting up the loop, timers, async handle,
 * listeners, nghttp2 callbacks, the shared read buffer and the session slots. Addresses are
 * duplicated into the server; the TLS part of the stored config is cleared because the
 * caller's TLS strings are not kept. With no address configured it listens on 0.0.0.0 and ::.
 * On any failure the partially built server is released by grpc_server_free(), which copes with
 * every intermediate state, and nullptr is returned with *out_err set. */
GRPC_API grpc_server *grpc_server_new_ex(const grpc_config *config, grpc_err *out_err) {
  if (out_err != nullptr)
    *out_err = GRPC_OK;
  ignore_default_sigpipe();
  if (config == nullptr) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_INVALID;
    return nullptr;
  }

  grpc_err err = GRPC_ERR_NOMEM;
  grpc_server *server = grpc_calloc(1, sizeof(*server));
  if (server == nullptr) {
    if (out_err != nullptr)
      *out_err = GRPC_ERR_NOMEM;
    return nullptr;
  }
  server->ipv4_sockfd = GRPC_INVALID_SOCKET;
  server->ipv6_sockfd = GRPC_INVALID_SOCKET;
  server->next_deadline_ms = UINT64_MAX;
  pthread_mutex_init(&server->post_mutex, nullptr);

  /* the configuration, with defaults */
  grpc_config *cfg = &server->config;
  *cfg = *config;
  if (cfg->backlog <= 0)
    cfg->backlog = DEFAULT_BACKLOG;
  if (cfg->max_connections == 0)
    cfg->max_connections = DEFAULT_MAX_CONNECTIONS;
  if (cfg->max_concurrent_streams == 0)
    cfg->max_concurrent_streams = DEFAULT_MAX_CONCURRENT_STREAMS;
  if (cfg->max_message_size == 0)
    cfg->max_message_size = DEFAULT_MAX_MESSAGE_SIZE;
  if (cfg->max_frame_size == 0)
    cfg->max_frame_size = DEFAULT_MAX_FRAME_SIZE;
  grpc_log_level initial_level =
      cfg->log_level == GRPC_LOG_DEFAULT ? GRPC_LOG_INFO : cfg->log_level;
  if (initial_level < GRPC_LOG_OFF || initial_level > GRPC_LOG_TRACE) {
    fprintf(stderr, "grpc: invalid log level\n");
    err = GRPC_ERR_INVALID;
    goto err;
  }
  atomic_init(&server->log_level, initial_level);
  server->log_file = cfg->log_file != nullptr ? cfg->log_file : stderr;
  cfg->log_file = server->log_file;
  if ((err = logger_server_add(server)) != GRPC_OK)
    goto err;

  /* HTTP/2 limits: a frame is 16 KiB to 16 MiB, a window at most 2^31 - 1 */
  if (cfg->max_frame_size < (16u * 1024) || cfg->max_frame_size > (1u << 24) - 1 ||
      cfg->max_message_size > INT32_MAX) {
    LOG_ERROR(server, "max_frame_size or max_message_size is out of range\n");
    err = GRPC_ERR_INVALID;
    goto err;
  }

  const char *ipv4_addr = cfg->ipv4_addr, *ipv6_addr = cfg->ipv6_addr;
  const char *unix_path = cfg->unix_path;
  if (ipv4_addr == nullptr && ipv6_addr == nullptr && unix_path == nullptr) {
    ipv4_addr = "0.0.0.0";
    ipv6_addr = "::";
  }
  if ((ipv4_addr != nullptr && (server->ipv4_addr = grpc_strdup(ipv4_addr)) == nullptr) ||
      (ipv6_addr != nullptr && (server->ipv6_addr = grpc_strdup(ipv6_addr)) == nullptr) ||
      (unix_path != nullptr && (server->unix_path = grpc_strdup(unix_path)) == nullptr)) {
    err = GRPC_ERR_NOMEM;
    goto err;
  }
  /* the copies outlive the caller's strings */
  cfg->ipv4_addr = server->ipv4_addr;
  cfg->ipv6_addr = server->ipv6_addr;
  cfg->unix_path = server->unix_path;
  memset(&cfg->tls, 0, sizeof(cfg->tls));

  bool has_tls_files = config->tls.cert_file != nullptr || config->tls.key_file != nullptr;
  bool has_tls_pems = config->tls.cert_pem != nullptr || config->tls.key_pem != nullptr;
  if (has_tls_files && has_tls_pems) {
    LOG_ERROR(server, "TLS cannot specify both file paths and in-memory PEM buffers\n");
    err = GRPC_ERR_INVALID;
    goto err;
  }
  if (has_tls_files && (config->tls.cert_file == nullptr || config->tls.key_file == nullptr)) {
    LOG_ERROR(server, "TLS needs both tls.cert_file and tls.key_file\n");
    err = GRPC_ERR_INVALID;
    goto err;
  }
  if (has_tls_pems && (config->tls.cert_pem == nullptr || config->tls.key_pem == nullptr)) {
    LOG_ERROR(server, "TLS needs both tls.cert_pem and tls.key_pem\n");
    err = GRPC_ERR_INVALID;
    goto err;
  }
  /* the caller's TLS strings are not kept, so HTTP/3 builds its QUIC TLS context from this copy
   * before grpc_server_new_ex() returns */
  grpc_tls_config tls_cfg = {0};
  if (has_tls_files || has_tls_pems) {
    tls_cfg = (grpc_tls_config){
        .cert_file = config->tls.cert_file,
        .key_file = config->tls.key_file,
        .cert_pem = config->tls.cert_pem,
        .cert_pem_len = config->tls.cert_pem != nullptr && config->tls.cert_pem_len == 0
                            ? strlen(config->tls.cert_pem)
                            : config->tls.cert_pem_len,
        .key_pem = config->tls.key_pem,
        .key_pem_len = config->tls.key_pem != nullptr && config->tls.key_pem_len == 0
                           ? strlen(config->tls.key_pem)
                           : config->tls.key_pem_len,
        .ca_file = config->tls.ca_file,
        .ca_pem = config->tls.ca_pem,
        .ca_pem_len = config->tls.ca_pem != nullptr && config->tls.ca_pem_len == 0
                          ? strlen(config->tls.ca_pem)
                          : config->tls.ca_pem_len,
        .require_client_cert = config->tls.require_client_cert,
    };
    if ((err = tls_enable(server, &tls_cfg)) != GRPC_OK)
      goto err;
  }

  /* With port 0 the IPv4 listener picks the port and IPv6 takes the same one, which may happen
   * to be in use on IPv6: then both start over with a new port (a few times). */
  uint16_t bound_port = cfg->port;
  for (int attempt = 0;; attempt++) {
    bound_port = cfg->port;
    if (server->ipv4_addr != nullptr &&
        (server->ipv4_sockfd = server_open_listener(server, AF_INET, server->ipv4_addr, bound_port,
                                                    cfg->backlog, &bound_port, &err)) ==
            GRPC_INVALID_SOCKET)
      goto err;
    if (server->ipv6_addr == nullptr ||
        (server->ipv6_sockfd = server_open_listener(server, AF_INET6, server->ipv6_addr, bound_port,
                                                    cfg->backlog, &bound_port, &err)) !=
            GRPC_INVALID_SOCKET)
      break;
    bool retry =
        cfg->port == 0 && server->ipv4_addr != nullptr && err == GRPC_ERR_IO && attempt < 7;
    if (!retry)
      goto err;
    grpc_socket_close(server->ipv4_sockfd);
    server->ipv4_sockfd = GRPC_INVALID_SOCKET;
  }
  cfg->port = bound_port;

  err = GRPC_ERR_IO;
  if (uv_loop_init(&server->loop) != 0)
    goto err;
  server->loop_initialized = true;

  if (uv_timer_init(&server->loop, &server->timer) != 0)
    goto err;
  server->timer_active = true;
  if (uv_timer_init(&server->loop, &server->deadline_timer) != 0)
    goto err;
  server->deadline_timer.data = server;
  server->deadline_timer_active = true;
  if (uv_timer_init(&server->loop, &server->flush_retry_timer) != 0)
    goto err;
  server->flush_retry_timer.data = server;
  server->flush_retry_timer_active = true;
  if (uv_async_init(&server->loop, &server->async, on_async) != 0)
    goto err;
  server->async.data = server;
  server->async_active = true;

  if (server->ipv4_sockfd != GRPC_INVALID_SOCKET) {
    if (uv_tcp_init(&server->loop, &server->ipv4_listener) != 0)
      goto err;
    server->ipv4_listener_active = true;
    if (!server_start_listener(server, &server->ipv4_listener, &server->ipv4_sockfd))
      goto err;
  }
  if (server->ipv6_sockfd != GRPC_INVALID_SOCKET) {
    if (uv_tcp_init(&server->loop, &server->ipv6_listener) != 0)
      goto err;
    server->ipv6_listener_active = true;
    if (!server_start_listener(server, &server->ipv6_listener, &server->ipv6_sockfd))
      goto err;
  }
  if (server->unix_path != nullptr) {
#ifndef _WIN32
    server_remove_stale_unix_socket(server->unix_path);
#endif
    if (uv_pipe_init(&server->loop, &server->unix_listener, 0) != 0)
      goto err;
    server->unix_listener.data = server;
    server->unix_listener_active = true;
    if (uv_pipe_bind(&server->unix_listener, server->unix_path) != 0)
      goto err;
    server->unix_bound = true;
    if (uv_listen((uv_stream_t *)&server->unix_listener, cfg->backlog, on_listener_connection) != 0)
      goto err;
  }
  if (cfg->keepalive_time_ms > 0 || cfg->max_connection_idle_ms > 0 ||
      cfg->max_connection_age_ms > 0) {
    if (uv_timer_init(&server->loop, &server->maintenance_timer) != 0)
      goto err;
    server->maintenance_timer.data = server;
    server->maintenance_timer_active = true;
    uv_unref((uv_handle_t *)&server->maintenance_timer);
    (void)uv_timer_start(&server->maintenance_timer, on_maintenance_timer, 250, 250);
  }

  err = GRPC_ERR_NOMEM;
  if (nghttp2_session_callbacks_new(&server->ngcallbacks) != 0)
    goto err;
  server_setup_ngcallbacks(server->ngcallbacks);

  server->rbuf_len = 64 * 1024;
  server->rbuf = grpc_malloc(server->rbuf_len);
  if (server->rbuf == nullptr)
    goto err;

  server->nb_sessions = cfg->max_connections;
  server->sessions = grpc_calloc(server->nb_sessions, sizeof(*server->sessions));
  server->free_sessions = grpc_malloc_array(server->nb_sessions, sizeof(*server->free_sessions));
  if (server->sessions == nullptr || server->free_sessions == nullptr)
    goto err;
  /* lowest index on top */
  for (size_t i = 0; i < server->nb_sessions; i++)
    server->free_sessions[i] = server->nb_sessions - 1 - i;
  server->nb_free_sessions = server->nb_sessions;

  if (server->ipv4_addr != nullptr)
    LOG_INFO(server, "listening on %s:%u%s\n", server->ipv4_addr, cfg->port,
             server->is_tls ? " (TLS)" : "");
  if (server->ipv6_addr != nullptr)
    LOG_INFO(server, "listening on [%s]:%u%s\n", server->ipv6_addr, cfg->port,
             server->is_tls ? " (TLS)" : "");
  if (config->http3.enable && (err = http3_server_start(server, &tls_cfg)) != GRPC_OK)
    goto err;
  return server;

err:
  if (out_err != nullptr)
    *out_err = err;
  grpc_server_free(server);
  return nullptr;
}

/* grpc_server_new_ex() without the error code. */
GRPC_API grpc_server *grpc_server_new(const grpc_config *config) {
  return grpc_server_new_ex(config, nullptr);
}

/* Returns the bound TCP port (resolved when the config asked for port 0), or 0. */
GRPC_API uint16_t grpc_server_port(const grpc_server *server) {
  return server != nullptr ? server->config.port : 0;
}

/* Returns the bound HTTP/3 UDP port, or 0 when HTTP/3 is off. */
GRPC_API uint16_t grpc_server_http3_port(const grpc_server *server) {
  return server != nullptr ? http3_server_port(server) : 0;
}

/* Flushes queued output, closes every session (detaching their calls), runs posted tasks so
 * they release their call references, drops ready calls and events, then closes all handles and
 * runs the loop (bounded to 1000 iterations) until the close callbacks have fired, which also
 * returns the session slots. Only then are routes, TLS, buffers and the server freed. Must run
 * on the loop thread with no poll in progress. Also used to unwind a failed grpc_server_new_ex().
 */
GRPC_API void grpc_server_free(grpc_server *server) {
  if (server == nullptr)
    return;

  if (server->sessions != nullptr) {
    /* deliver what the application already queued */
    server_flush_sessions(server);
    for (size_t i = 0; i < server->nb_sessions; i++) {
      if (server->sessions[i].is_active)
        session_close(&server->sessions[i]);
    }
  }
  /* posted work runs (on calls that are now closed) so that it releases its references */
  run_posted(server);
  server_drop_ready(server);
  server_clear_events(server);

  if (server->ipv4_listener_active)
    uv_close((uv_handle_t *)&server->ipv4_listener, nullptr);
  if (server->ipv6_listener_active)
    uv_close((uv_handle_t *)&server->ipv6_listener, nullptr);
  if (server->unix_listener_active)
    uv_close((uv_handle_t *)&server->unix_listener, nullptr);
  http3_server_close_handles(server);
  if (server->timer_active)
    uv_close((uv_handle_t *)&server->timer, nullptr);
  if (server->deadline_timer_active)
    uv_close((uv_handle_t *)&server->deadline_timer, nullptr);
  if (server->maintenance_timer_active)
    uv_close((uv_handle_t *)&server->maintenance_timer, nullptr);
  if (server->flush_retry_timer_active)
    uv_close((uv_handle_t *)&server->flush_retry_timer, nullptr);
  if (server->async_active)
    uv_close((uv_handle_t *)&server->async, nullptr);
  if (server->ipv4_sockfd != GRPC_INVALID_SOCKET)
    grpc_socket_close(server->ipv4_sockfd);
  if (server->ipv6_sockfd != GRPC_INVALID_SOCKET)
    grpc_socket_close(server->ipv6_sockfd);

  if (server->loop_initialized) {
    int drain_limit = 1000;
    while (uv_loop_alive(&server->loop) && drain_limit-- > 0)
      uv_run(&server->loop, UV_RUN_NOWAIT);
    uv_loop_close(&server->loop);
  }

  for (size_t i = 0; i < server->nb_routes; i++) {
    grpc_free(server->routes[i]->path);
    grpc_free(server->routes[i]);
  }
  grpc_free(server->routes);
  if (server->fallback_route != nullptr) {
    grpc_free(server->fallback_route->path);
    grpc_free(server->fallback_route);
  }
  interceptors_free(server);
  health_free(server);
  reflection_free(server);
  tls_free(server);
  if (server->h3 != nullptr)
    http3_thread_cleanup();
  http3_server_free(server);

  if (server->ngcallbacks != nullptr)
    nghttp2_session_callbacks_del(server->ngcallbacks);
  grpc_free(server->events);
  grpc_free(server->rbuf);
  grpc_free(server->tls_obuf.data);
  grpc_free(server->ipv4_addr);
  grpc_free(server->ipv6_addr);
  if (server->unix_path != nullptr) {
#ifndef _WIN32
    if (server->unix_bound) /* never another server's socket */
      (void)unlink(server->unix_path);
#endif
    grpc_free(server->unix_path);
  }
  grpc_free(server->sessions);
  grpc_free(server->free_sessions);
  logger_server_remove(server);
  pthread_mutex_destroy(&server->post_mutex);
  grpc_free(server);
}

/* Returns the library's license text (a static string): exactly the LICENSE file, which
 * src/tests/api.c checks. "The ezGRPC-next Authors" are listed in AUTHORS, including the authors of
 * EZgRPC2, the project this one was forked from. */
GRPC_API const char *grpc_license(void) {
  static const char *license =
      "ezGRPC-next - A lightweight, modular gRPC server in C.\n"
      "https://github.com/ThirdLetterC/ezgrpc-next\n"
      "\n"
      "Copyright (c) 2023-2026 The ezGRPC-next Authors\n"
      "\n"
      "Redistribution and use in source and binary forms, with or without\n"
      "modification, are permitted provided that the following conditions\n"
      "are met:\n"
      "\n"
      "  1. Redistributions of source code must retain the above copyright notice,\n"
      "     this list of conditions and the following disclaimer.\n"
      "\n"
      "  2. Neither the name of the copyright holder nor the names of its\n"
      "     contributors may be used to endorse or promote products derived from\n"
      "     this software without specific prior written permission.\n"
      "\n"
      "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
      /* U+201C / U+201D as UTF-8 bytes, so the text does not depend on the compiler's charset */
      "\xE2\x80\x9C"
      "AS IS"
      "\xE2\x80\x9D"
      " AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
      "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
      "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
      "HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
      "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED\n"
      "TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR\n"
      "PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF\n"
      "LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING\n"
      "NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS\n"
      "SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n";
  return license;
}
