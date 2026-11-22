#pragma once

/* internal_server.h - helpers shared by the server's files (grpc_server*.c)
 *
 * grpc_server.c (creation, timers, freeing), grpc_server_log.c, grpc_server_routes.c,
 * grpc_server_listen.c and grpc_server_run.c. Not used outside them.
 */
#include "core.h"

/* Gives the server a unique topic ("grpc-server-N") at its current log level and links it into
 * the registry. Returns GRPC_OK, or GRPC_ERR_IO if ulog could not be set up or the topic could
 * not be added (the server is then not registered). */
grpc_err logger_server_add(grpc_server *server);

/* Removes the server's topic and unlinks it from the registry; a no-op unless
 * logger_server_add() succeeded. Once it returns, logger_output() no longer touches the
 * server's log_file. */
void logger_server_remove(grpc_server *server);

/* Creates a TCP socket for family (AF_INET or AF_INET6), binds it to addr:port and listens with
 * backlog. Sets SO_REUSEADDR (on Windows SO_EXCLUSIVEADDRUSE instead, since its SO_REUSEADDR lets
 * another socket take the port), SO_REUSEPORT with config.reuse_port, and IPV6_V6ONLY for IPv6
 * so that the IPv4 and IPv6 listeners can share a port. If bound_port is not nullptr it receives the port
 * actually bound, which resolves port 0. Returns the socket, or GRPC_INVALID_SOCKET after
 * logging and storing GRPC_ERR_INVALID or GRPC_ERR_IO in *out_err (if not nullptr). */
uv_os_sock_t server_open_listener(grpc_server *server, int family, const char *addr, uint16_t port,
                                  int backlog, uint16_t *bound_port, grpc_err *out_err);

/* Hands an open socket to an initialized uv_tcp_t and starts listening with
 * on_listener_connection(). On a successful uv_tcp_open() *fd is reset to GRPC_INVALID_SOCKET,
 * since the handle now owns (and closes) it. Returns false on failure. */
bool server_start_listener(grpc_server *server, uv_tcp_t *listener, uv_os_sock_t *fd);

#ifndef _WIN32
/* Removes a Unix socket file left behind by a server that is gone, so it can be bound again: only
 * a socket that refuses connections is removed. A live server's socket, or anything that is not
 * a socket, is left alone; binding then fails. */
void server_remove_stale_unix_socket(const char *path);
#endif
