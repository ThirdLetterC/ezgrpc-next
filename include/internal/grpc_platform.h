#pragma once

/* grpc_platform.h - socket and string portability shims
 *
 * Includes libuv and the platform socket headers, and maps the few POSIX/Winsock differences the
 * library relies on: strncasecmp/strcasecmp, grpc_socket_close, GRPC_INVALID_SOCKET and
 * grpc_socket_error().
 */

#include <errno.h>
#include <string.h>
#include <uv.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#define strncasecmp         _strnicmp
#define strcasecmp          _stricmp
#define grpc_socket_close   closesocket
#define GRPC_INVALID_SOCKET INVALID_SOCKET
/* Describes the last socket error (WSAGetLastError()); the string is static. */
static inline const char *grpc_socket_error(void) {
  return uv_strerror(uv_translate_sys_error(WSAGetLastError()));
}
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#define grpc_socket_close   close
#define GRPC_INVALID_SOCKET (-1)
/* Describes the last socket error (errno), via strerror(). */
static inline const char *grpc_socket_error(void) {
  return strerror(errno);
}
#endif
