/* grpc_server_listen.c - the server's TCP and Unix socket listeners
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

#ifdef _WIN32
/* Initializes Winsock 2.2 before the first socket is created; run once through uv_once(). */
static void grpc_winsock_init(void) {
  WSADATA data;
  WSAStartup(MAKEWORD(2, 2), &data);
}
#endif

uv_os_sock_t server_open_listener(grpc_server *server, int family, const char *addr, uint16_t port,
                                  int backlog, uint16_t *bound_port, grpc_err *out_err) {
  struct sockaddr_storage ss = {};
  socklen_t sslen;
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
#ifdef _WIN32
  (void)setsockopt(fd, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, (const char *)&(int){1}, sizeof(int));
#else
  /* only allows rebinding over TIME_WAIT connections, never a port another socket listens on */
  (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&(int){1}, sizeof(int));
#endif
#ifdef SO_REUSEPORT
  /* opt-in: lets one server per thread share the port, the kernel spreading connections */
  if (server->config.reuse_port)
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

bool server_start_listener(grpc_server *server, uv_tcp_t *listener, uv_os_sock_t *fd) {
  listener->data = server;
  if (uv_tcp_open(listener, *fd) != 0)
    return false;
  *fd = GRPC_INVALID_SOCKET;
  return uv_listen((uv_stream_t *)listener, server->config.backlog, on_listener_connection) == 0;
}

#ifndef _WIN32
void server_remove_stale_unix_socket(const char *path) {
  struct stat st;
  if (lstat(path, &st) != 0 || !S_ISSOCK(st.st_mode))
    return;
  struct sockaddr_un sun = {.sun_family = AF_UNIX};
  size_t len = strlen(path);
  if (len >= sizeof(sun.sun_path))
    return;
  memcpy(sun.sun_path, path, len + 1);
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    return;
  bool live = connect(fd, (struct sockaddr *)&sun, sizeof(sun)) == 0 || errno != ECONNREFUSED;
  close(fd);
  if (!live)
    (void)unlink(path);
}
#endif
