/* grpc_server.c - server setup, routing, logging and the poll/run event loop
 *
 * A grpc_server owns one libuv loop, driven by a single thread through grpc_server_poll() or
 * grpc_server_run(). Each poll flushes queued output, runs the loop once (accepting, reading and
 * feeding nghttp2, which queues calls), dispatches the ready calls to handlers or events
 * (server_dispatch() in grpc_call.c) and flushes what the handlers answered. Everything except
 * the statistics, the log level, grpc_server_stop() and grpc_server_post() must be used from
 * that thread; stop and post wake the loop through the server's uv_async handle, and the
 * *_async call functions are built on grpc_server_post().
 *
 * Connections live in a fixed array of max_connections sessions handed out from a stack of free
 * indices; internal_helpers.c accepts, reads, writes and closes them. Routes are allocated one
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

#include "core.h"
#include "grpc_timeout.h"
#include "internal_helpers.h"
#include "internal_nghttp2_callbacks.h"
#include "internal_http3.h"
#include "ulog/ulog.h"

static const int DEFAULT_BACKLOG = 1024;
static const size_t DEFAULT_MAX_CONNECTIONS = 1024;
static const uint32_t DEFAULT_MAX_CONCURRENT_STREAMS = 1024;
static const uint32_t DEFAULT_MAX_MESSAGE_SIZE = 1u << 20;
static const uint32_t DEFAULT_MAX_FRAME_SIZE = 16u * 1024;

/* ---- logging ------------------------------------------------------------------ */

static pthread_mutex_t ulog_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_registry_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_level_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t logger_once = PTHREAD_ONCE_INIT;
static grpc_server *log_registry;
static atomic_uint_fast64_t next_log_topic = 1;
static ulog_output_id log_output = ULOG_OUTPUT_INVALID;
static ulog_status logger_status = ULOG_STATUS_ERROR;

/* Locks (lock == true) or unlocks ulog_mutex; installed as ulog's lock function so that events
 * from servers on different threads are serialized. Returns ULOG_STATUS_BUSY if the mutex call
 * fails. */
static ulog_status logger_lock(bool lock, [[maybe_unused]] void *arg) {
  int rc = lock ? pthread_mutex_lock(&ulog_mutex) : pthread_mutex_unlock(&ulog_mutex);
  return rc == 0 ? ULOG_STATUS_OK : ULOG_STATUS_BUSY;
}

/* The one ulog output shared by all servers: looks up the server whose topic id matches the
 * event and writes the formatted message to its log_file under log_registry_mutex. Drops
 * events without a topic or for a server no longer registered. Messages are cut to 1023 bytes.
 */
static void logger_output(ulog_event *event, [[maybe_unused]] void *arg) {
  int topic_id = ulog_event_get_topic(event);
  char message[1024];
  if (topic_id == ULOG_TOPIC_ID_INVALID ||
      ulog_event_get_message(event, message, sizeof(message)) != ULOG_STATUS_OK)
    return;

  pthread_mutex_lock(&log_registry_mutex);
  grpc_server *server = log_registry;
  while (server != nullptr && server->log_topic_id != topic_id)
    server = server->log_registry_next;
  if (server != nullptr)
    (void)fwrite(message, 1, strlen(message), server->log_file);
  pthread_mutex_unlock(&log_registry_mutex);
}

/* Runs once per process through pthread_once(): installs logger_lock() and registers
 * logger_output(). On failure logger_status stays ULOG_STATUS_ERROR, so every later
 * logger_server_add(), and hence grpc_server_new_ex(), fails with GRPC_ERR_IO. */
static void logger_initialize(void) {
  if (ulog_lock_set_fn(logger_lock, nullptr) != ULOG_STATUS_OK)
    return;
  log_output = ulog_output_add(logger_output, nullptr, ULOG_LEVEL_TRACE);
  logger_status = log_output == ULOG_OUTPUT_INVALID ? ULOG_STATUS_ERROR : ULOG_STATUS_OK;
}

/* Maps a grpc_log_level to the ulog level of a server topic. OFF and DEFAULT map to ERROR: OFF
 * is enforced by server_log() before ulog is reached, and DEFAULT is resolved by the callers.
 */
static ulog_level ulog_level_for(grpc_log_level level) {
  switch (level) {
  case GRPC_LOG_TRACE:
    return (ulog_level)ULOG_LEVEL_TRACE;
  case GRPC_LOG_DEBUG:
    return (ulog_level)ULOG_LEVEL_DEBUG;
  case GRPC_LOG_INFO:
    return (ulog_level)ULOG_LEVEL_INFO;
  case GRPC_LOG_WARNING:
    return (ulog_level)ULOG_LEVEL_WARN;
  case GRPC_LOG_ERROR:
  case GRPC_LOG_OFF:
  case GRPC_LOG_DEFAULT:
  default:
    return (ulog_level)ULOG_LEVEL_ERROR;
  }
}

/* Gives the server a unique topic ("grpc-server-N") at its current log level and links it into
 * the registry. Returns GRPC_OK, or GRPC_ERR_IO if ulog could not be set up or the topic could
 * not be added (the server is then not registered). */
static grpc_err logger_server_add(grpc_server *server) {
  if (pthread_once(&logger_once, logger_initialize) != 0 || logger_status != ULOG_STATUS_OK)
    return GRPC_ERR_IO;

  uint_fast64_t topic = atomic_fetch_add(&next_log_topic, 1);
  snprintf(server->log_topic, sizeof(server->log_topic), "grpc-server-%" PRIuFAST64, topic);
  server->log_topic_id =
      ulog_topic_add(server->log_topic, log_output,
                     ulog_level_for((grpc_log_level)atomic_load(&server->log_level)));
  if (server->log_topic_id == ULOG_TOPIC_ID_INVALID)
    return GRPC_ERR_IO;

  pthread_mutex_lock(&log_registry_mutex);
  server->log_registry_next = log_registry;
  log_registry = server;
  server->log_topic_initialized = true;
  pthread_mutex_unlock(&log_registry_mutex);
  return GRPC_OK;
}

/* Removes the server's topic and unlinks it from the registry; a no-op unless
 * logger_server_add() succeeded. Once it returns, logger_output() no longer touches the
 * server's log_file. */
static void logger_server_remove(grpc_server *server) {
  if (!server->log_topic_initialized)
    return;

  (void)ulog_topic_remove(server->log_topic);
  pthread_mutex_lock(&log_registry_mutex);
  grpc_server **link = &log_registry;
  while (*link != nullptr && *link != server)
    link = &(*link)->log_registry_next;
  if (*link == server)
    *link = server->log_registry_next;
  server->log_topic_initialized = false;
  pthread_mutex_unlock(&log_registry_mutex);
}

/* Formats a timestamped line and emits it on the server's topic; usually reached through the
 * LOG_* macros. Does nothing when server is nullptr, logging is off or level is more verbose
 * than the server's level. Lines are cut to 1023 bytes. Safe from any thread: the level is
 * atomic and ulog is locked. */
void server_log(grpc_server *server, grpc_log_level level, const char *fmt, ...) {
  if (server == nullptr)
    return;
  int log_level = atomic_load(&server->log_level);
  if (log_level == GRPC_LOG_OFF || level > log_level)
    return;

  /* one write per line, so that lines of several servers do not interleave */
  char line[1024];
  char dt[128] = {};
  int n = snprintf(line, sizeof(line), "[%s] ", ezgetdt(dt, sizeof(dt)));
  va_list ap;
  va_start(ap, fmt);
  if (n >= 0 && (size_t)n < sizeof(line))
    vsnprintf(line + n, sizeof(line) - (size_t)n, fmt, ap);
  va_end(ap);

  switch (level) {
  case GRPC_LOG_ERROR:
    ulog_t_error(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_WARNING:
    ulog_t_warn(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_INFO:
    ulog_t_info(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_DEBUG:
    ulog_t_debug(server->log_topic, "%s", line);
    break;
  case GRPC_LOG_TRACE:
    ulog_t_trace(server->log_topic, "%s", line);
    break;
  default:
    break;
  }
}

/* ---- routes ------------------------------------------------------------------- */

/* Returns the route whose path equals the len bytes at path (not NUL-terminated), whether
 * disabled or not, or nullptr. A linear scan, run for each request's :path header. */
grpc_route *server_find_route(grpc_server *server, const char *path, size_t len) {
  for (size_t i = 0; i < server->nb_routes; i++) {
    grpc_route *route = server->routes[i];
    if (route->path_len == len && memcmp(route->path, path, len) == 0)
      return route;
  }
  return nullptr;
}

/* Registers path (which must start with '/') as a route of the given kind, or re-enables and
 * overwrites a disabled route with the same path so that calls still pointing to it stay valid.
 * Copies path and *stream. Returns GRPC_OK, GRPC_ERR_EXISTS if an enabled route has the path,
 * GRPC_ERR_INVALID or GRPC_ERR_NOMEM. Loop thread only: routes are not locked. */
grpc_err server_add_route(grpc_server *server, const char *path, route_kind kind,
                          grpc_handler_fn handler, const grpc_stream_handler *stream,
                          void *userdata) {
  if (server == nullptr || path == nullptr || path[0] != '/')
    return GRPC_ERR_INVALID;

  size_t len = strlen(path);
  grpc_route *route = server_find_route(server, path, len);
  if (route != nullptr) {
    if (!route->disabled)
      return GRPC_ERR_EXISTS;
  } else {
    /* routes are never moved: calls point to them */
    if (server->nb_routes == server->cap_routes) {
      size_t cap = server->cap_routes ? server->cap_routes * 2 : 16;
      grpc_route **routes = grpc_realloc(server->routes, cap * sizeof(*routes));
      if (routes == nullptr)
        return GRPC_ERR_NOMEM;
      server->routes = routes;
      server->cap_routes = cap;
    }
    route = grpc_calloc(1, sizeof(*route));
    if (route == nullptr)
      return GRPC_ERR_NOMEM;
    route->path = grpc_strndup(path, len);
    if (route->path == nullptr) {
      grpc_free(route);
      return GRPC_ERR_NOMEM;
    }
    route->path_len = len;
    server->routes[server->nb_routes++] = route;
  }

  route->kind = kind;
  route->handler = handler;
  route->stream = stream != nullptr ? *stream : (grpc_stream_handler){};
  route->userdata = userdata;
  route->disabled = false;
  return GRPC_OK;
}

/* Registers a ROUTE_UNARY route; the handler is required. */
GRPC_API grpc_err grpc_server_handle(grpc_server *server, const char *path, grpc_handler_fn handler,
                                     void *userdata) {
  if (handler == nullptr)
    return GRPC_ERR_INVALID;
  return server_add_route(server, path, ROUTE_UNARY, handler, nullptr, userdata);
}

/* Registers a ROUTE_STREAM route; the handler (copied) needs on_message or on_start. */
GRPC_API grpc_err grpc_server_handle_stream(grpc_server *server, const char *path,
                                            const grpc_stream_handler *handler, void *userdata) {
  if (handler == nullptr || (handler->on_message == nullptr && handler->on_start == nullptr))
    return GRPC_ERR_INVALID;
  return server_add_route(server, path, ROUTE_STREAM, nullptr, handler, userdata);
}

/* Registers a ROUTE_EVENTS route: its calls are reported as events by grpc_server_poll()
 * instead of being handed to a callback. */
GRPC_API grpc_err grpc_server_register_path(grpc_server *server, const char *path, void *userdata) {
  return server_add_route(server, path, ROUTE_EVENTS, nullptr, nullptr, userdata);
}

/* Disables the route instead of freeing it, since calls in progress point to it. New requests
 * for the path then go to the fallback route, or get UNIMPLEMENTED. */
GRPC_API grpc_err grpc_server_unregister(grpc_server *server, const char *path) {
  if (server == nullptr || path == nullptr)
    return GRPC_ERR_INVALID;
  grpc_route *route = server_find_route(server, path, strlen(path));
  if (route == nullptr || route->disabled)
    return GRPC_ERR_NOT_FOUND;
  /* kept, since calls in progress point to it */
  route->disabled = true;
  return GRPC_OK;
}

/* Installs, updates or (handler == nullptr) disables the catch-all route used for unknown or
 * disabled paths. The route (a ROUTE_STREAM with a placeholder path) is allocated once and kept,
 * like other routes, until grpc_server_free(). */
GRPC_API grpc_err grpc_server_set_fallback(grpc_server *server, const grpc_stream_handler *handler,
                                           void *userdata) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;
  if (handler == nullptr) {
    if (server->fallback_route != nullptr)
      server->fallback_route->disabled = true;
    return GRPC_OK;
  }
  if (handler->on_message == nullptr && handler->on_start == nullptr)
    return GRPC_ERR_INVALID;
  if (server->fallback_route == nullptr) {
    grpc_route *route = grpc_calloc(1, sizeof(*route));
    if (route == nullptr)
      return GRPC_ERR_NOMEM;
    route->path = grpc_strdup("/*");
    if (route->path == nullptr) {
      grpc_free(route);
      return GRPC_ERR_NOMEM;
    }
    route->path_len = 2;
    route->kind = ROUTE_STREAM;
    server->fallback_route = route;
  }
  server->fallback_route->stream = *handler;
  server->fallback_route->userdata = userdata;
  server->fallback_route->disabled = false;
  return GRPC_OK;
}

/* Copies the atomic counters one by one; callable from any thread. The snapshot is not
 * atomic as a whole. */
GRPC_API grpc_err grpc_server_get_stats(const grpc_server *server, grpc_server_stats *out) {
  if (server == nullptr || out == nullptr)
    return GRPC_ERR_INVALID;
  *out = (grpc_server_stats){
      .active_connections = atomic_load(&server->stat_active_connections),
      .total_connections = atomic_load(&server->stat_total_connections),
      .active_streams = atomic_load(&server->stat_active_streams),
      .total_calls = atomic_load(&server->stat_total_calls),
      .calls_ok = atomic_load(&server->stat_calls_ok),
      .calls_error = atomic_load(&server->stat_calls_error),
      .bytes_received = atomic_load(&server->stat_bytes_received),
      .bytes_sent = atomic_load(&server->stat_bytes_sent),
      .messages_received = atomic_load(&server->stat_messages_received),
      .messages_sent = atomic_load(&server->stat_messages_sent),
  };
  return GRPC_OK;
}

/* ---- setup ---------------------------------------------------------------------- */

#ifdef _WIN32
/* Initializes Winsock 2.2 before the first socket is created; run once through uv_once(). */
static void grpc_winsock_init(void) {
  WSADATA data;
  WSAStartup(MAKEWORD(2, 2), &data);
}
#endif

/* Creates a TCP socket for family (AF_INET or AF_INET6), binds it to addr:port and listens with
 * backlog. Sets SO_REUSEADDR, SO_REUSEPORT where available, and IPV6_V6ONLY for IPv6 so that the
 * IPv4 and IPv6 listeners can share a port. If bound_port is not nullptr it receives the port
 * actually bound, which resolves port 0. Returns the socket, or GRPC_INVALID_SOCKET after
 * logging and storing GRPC_ERR_INVALID or GRPC_ERR_IO in *out_err (if not nullptr). */
static uv_os_sock_t open_listener(grpc_server *server, int family, const char *addr, uint16_t port,
                                  int backlog, uint16_t *bound_port, grpc_err *out_err) {
  struct sockaddr_storage ss = {};
  int sslen;
  if (family == AF_INET) {
    struct sockaddr_in *sin = (struct sockaddr_in *)&ss;
    sin->sin_family = AF_INET;
    sin->sin_port = htons(port);
    if (inet_pton(AF_INET, addr, &sin->sin_addr) != 1) {
      LOG_ERROR(server, "invalid IPv4 address '%s'\n", addr);
      if (out_err != nullptr)
        *out_err = GRPC_ERR_INVALID;
      return GRPC_INVALID_SOCKET;
    }
    sslen = sizeof(*sin);
  } else {
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&ss;
    sin6->sin6_family = AF_INET6;
    sin6->sin6_port = htons(port);
    if (inet_pton(AF_INET6, addr, &sin6->sin6_addr) != 1) {
      LOG_ERROR(server, "invalid IPv6 address '%s'\n", addr);
      if (out_err != nullptr)
        *out_err = GRPC_ERR_INVALID;
      return GRPC_INVALID_SOCKET;
    }
    sslen = sizeof(*sin6);
  }

#ifdef _WIN32
  static uv_once_t winsock_once = UV_ONCE_INIT;
  uv_once(&winsock_once, grpc_winsock_init);
  uv_os_sock_t fd =
      WSASocketW(family, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
#else
  uv_os_sock_t fd = socket(family, SOCK_STREAM | SOCK_CLOEXEC, 0);
#endif
  if (fd == GRPC_INVALID_SOCKET) {
    LOG_ERROR(server, "socket: %s\n", grpc_socket_error());
    if (out_err != nullptr)
      *out_err = GRPC_ERR_IO;
    return GRPC_INVALID_SOCKET;
  }
  (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&(int){1}, sizeof(int));
#ifdef SO_REUSEPORT
  /* lets one server per thread share the port; the kernel spreads connections */
  (void)setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (const char *)&(int){1}, sizeof(int));
#endif
  if (family == AF_INET6)
    (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, (const char *)&(int){1}, sizeof(int));

  if (bind(fd, (struct sockaddr *)&ss, sslen) == -1) {
    LOG_ERROR(server, "bind %s port %u: %s\n", addr, port, grpc_socket_error());
    grpc_socket_close(fd);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_IO;
    return GRPC_INVALID_SOCKET;
  }
  if (listen(fd, backlog) == -1) {
    LOG_ERROR(server, "listen %s port %u: %s\n", addr, port, grpc_socket_error());
    grpc_socket_close(fd);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_IO;
    return GRPC_INVALID_SOCKET;
  }
  if (bound_port != nullptr) {
    struct sockaddr_storage bound = {};
    socklen_t blen = sizeof(bound);
    if (getsockname(fd, (struct sockaddr *)&bound, &blen) == 0) {
      if (bound.ss_family == AF_INET)
        *bound_port = ntohs(((struct sockaddr_in *)&bound)->sin_port);
      else if (bound.ss_family == AF_INET6)
        *bound_port = ntohs(((struct sockaddr_in6 *)&bound)->sin6_port);
    }
  }
  return fd;
}

/* Hands an open socket to an initialized uv_tcp_t and starts listening with
 * on_listener_connection(). On a successful uv_tcp_open() *fd is reset to GRPC_INVALID_SOCKET,
 * since the handle now owns (and closes) it. Returns false on failure. */
static bool start_listener(grpc_server *server, uv_tcp_t *listener, uv_os_sock_t *fd) {
  listener->data = server;
  if (uv_tcp_open(listener, *fd) != 0)
    return false;
  *fd = GRPC_INVALID_SOCKET;
  return uv_listen((uv_stream_t *)listener, server->config.backlog, on_listener_connection) == 0;
}

/* Does nothing: the poll timer only exists to make uv_run(UV_RUN_ONCE) return after a
 * grpc_server_poll() timeout. */
static void on_poll_timer([[maybe_unused]] uv_timer_t *handle) {}

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

    /* 3. Max connection idle (no active streams) */
    if (cfg->max_connection_idle_ms > 0 && s->nb_streams == 0 && now >= s->last_activity_ms &&
        (now - s->last_activity_ms) >= cfg->max_connection_idle_ms) {
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

  uint16_t bound_port = cfg->port;
  if (server->ipv4_addr != nullptr &&
      (server->ipv4_sockfd = open_listener(server, AF_INET, server->ipv4_addr, bound_port,
                                           cfg->backlog, &bound_port, &err)) == GRPC_INVALID_SOCKET)
    goto err;
  if (server->ipv6_addr != nullptr &&
      (server->ipv6_sockfd = open_listener(server, AF_INET6, server->ipv6_addr, bound_port,
                                           cfg->backlog, &bound_port, &err)) == GRPC_INVALID_SOCKET)
    goto err;
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
    if (!start_listener(server, &server->ipv4_listener, &server->ipv4_sockfd))
      goto err;
  }
  if (server->ipv6_sockfd != GRPC_INVALID_SOCKET) {
    if (uv_tcp_init(&server->loop, &server->ipv6_listener) != 0)
      goto err;
    server->ipv6_listener_active = true;
    if (!start_listener(server, &server->ipv6_listener, &server->ipv6_sockfd))
      goto err;
  }
  if (server->unix_path != nullptr) {
#ifndef _WIN32
    (void)unlink(server->unix_path);
#endif
    if (uv_pipe_init(&server->loop, &server->unix_listener, 0) != 0)
      goto err;
    server->unix_listener.data = server;
    server->unix_listener_active = true;
    if (uv_pipe_bind(&server->unix_listener, server->unix_path) != 0 ||
        uv_listen((uv_stream_t *)&server->unix_listener, cfg->backlog, on_listener_connection) != 0)
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
  server->free_sessions = grpc_malloc(server->nb_sessions * sizeof(*server->free_sessions));
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

/* Changes the ulog topic level and the server's atomic level together under log_level_mutex so
 * concurrent callers cannot leave them out of sync. Safe from any thread. */
GRPC_API grpc_err grpc_server_set_log_level(grpc_server *server, grpc_log_level level) {
  if (server == nullptr || level < GRPC_LOG_DEFAULT || level > GRPC_LOG_TRACE)
    return GRPC_ERR_INVALID;
  if (level == GRPC_LOG_DEFAULT)
    level = GRPC_LOG_INFO;

  pthread_mutex_lock(&log_level_mutex);
  ulog_status status = ulog_topic_level_set(server->log_topic, ulog_level_for(level));
  if (status == ULOG_STATUS_OK)
    atomic_store(&server->log_level, level);
  pthread_mutex_unlock(&log_level_mutex);
  return status == ULOG_STATUS_OK ? GRPC_OK : GRPC_ERR_IO;
}

/* ---- running ---------------------------------------------------------------------- */

/* One iteration of the event loop: releases the previous poll's events, flushes pending output,
 * runs libuv once (blocking for at most timeout_ms; 0 does not block, negative blocks until
 * something happens), dispatches the calls that became ready and flushes the responses. Returns
 * the number of events now available from grpc_server_events(). */
GRPC_API int grpc_server_poll(grpc_server *server, int timeout_ms) {
  if (server == nullptr)
    return GRPC_ERR_INVALID;

  server_clear_events(server);
  server->ready_events = 0;

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

  /* Send GOAWAY on every active session. */
  for (size_t i = 0; i < server->nb_sessions; i++) {
    grpc_session *s = &server->sessions[i];
    if (s->is_active && !s->is_closing && s->ops != nullptr) {
      s->ops->goaway(s);
      session_schedule_flush(s);
    }
  }
  server_flush_sessions(server);
  atomic_store(&server->stop, true);

  uint64_t deadline = timeout_ms > 0 ? grpc_time_now_ms() + (uint64_t)timeout_ms : 0;
  while (server_active_streams(server) > 0) {
    int wait_ms = -1;
    if (timeout_ms == 0) {
      (void)grpc_server_poll(server, 0);
      break;
    }
    if (timeout_ms > 0) {
      uint64_t now = grpc_time_now_ms();
      if (now >= deadline)
        break;
      uint64_t rem = deadline - now;
      wait_ms = rem > 100 ? 100 : (int)rem;
    }
    int res = grpc_server_poll(server, wait_ms);
    if (res < 0)
      return (grpc_err)res;
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

/* Writes the output of every session queued for flushing now instead of at the next poll. */
GRPC_API void grpc_server_flush(grpc_server *server) {
  if (server != nullptr)
    server_flush_sessions(server);
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
