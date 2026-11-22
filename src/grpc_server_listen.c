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

/* Fills ss with family addr:port. Returns its length, or 0 when addr does not parse. */
static socklen_t make_sockaddr(struct sockaddr_storage *ss, int family, const char *addr,
                               uint16_t port) {
  memset(ss, 0, sizeof(*ss));
  if (family == AF_INET) {
    struct sockaddr_in *sin = (struct sockaddr_in *)ss;
    sin->sin_family = AF_INET;
    sin->sin_port = htons(port);
    return inet_pton(AF_INET, addr, &sin->sin_addr) == 1 ? (socklen_t)sizeof(*sin) : 0;
  }
  struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)ss;
  sin6->sin6_family = AF_INET6;
  sin6->sin6_port = htons(port);
  return inet_pton(AF_INET6, addr, &sin6->sin6_addr) == 1 ? (socklen_t)sizeof(*sin6) : 0;
}

/* A new socket of `type` for family, not inherited by child processes. */
static uv_os_sock_t open_socket(int family, int type) {
#ifdef _WIN32
  static uv_once_t winsock_once = UV_ONCE_INIT;
  uv_once(&winsock_once, grpc_winsock_init);
  return WSASocketW(family, type, 0, NULL, 0, WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
#else
  return socket(family, type | SOCK_CLOEXEC, 0);
#endif
}

uv_os_sock_t server_open_listener(grpc_server *server, int family, const char *addr, uint16_t port,
                                  int backlog, uint16_t *bound_port, grpc_err *out_err) {
  struct sockaddr_storage ss;
  socklen_t sslen = make_sockaddr(&ss, family, addr, port);
  if (sslen == 0) {
    LOG_ERROR(server, "invalid %s address '%s'\n", family == AF_INET ? "IPv4" : "IPv6", addr);
    if (out_err != nullptr)
      *out_err = GRPC_ERR_INVALID;
    return GRPC_INVALID_SOCKET;
  }

  uv_os_sock_t fd = open_socket(family, SOCK_STREAM);
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

bool server_udp_port_free(int family, const char *addr, uint16_t port) {
  struct sockaddr_storage ss;
  socklen_t sslen = make_sockaddr(&ss, family, addr, port);
  if (sslen == 0)
    return false;
  uv_os_sock_t fd = open_socket(family, SOCK_DGRAM);
  if (fd == GRPC_INVALID_SOCKET)
    return false;
  if (family == AF_INET6)
    (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, (const char *)&(int){1}, sizeof(int));
  bool ok = bind(fd, (struct sockaddr *)&ss, sslen) == 0;
  grpc_socket_close(fd);
  return ok;
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
