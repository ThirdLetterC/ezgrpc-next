/* grpc_server_routes.c - routes (handlers, stream handlers, event paths, the fallback) and statistics
 *
 * Part of the server described in grpc_server.c.
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
