#pragma once

/* grpc_channelz.h - minimal gRPC Channelz introspection service
 *
 * Registers the ``grpc.channelz.v1.Channelz`` GetServers and GetServerSockets methods on a
 * server. GetServers reports one server with the call counters from grpc_server_get_stats();
 * GetServerSockets lists the active connections with their peer addresses.
 */

#include "grpc_defs.h"
#include "grpc_error.h"

typedef struct grpc_server grpc_server;

/** Method paths registered by grpc_channelz_enable(). */
#define GRPC_CHANNELZ_PATH_GET_SERVERS        "/grpc.channelz.v1.Channelz/GetServers"
#define GRPC_CHANNELZ_PATH_GET_SERVER_SOCKETS "/grpc.channelz.v1.Channelz/GetServerSockets"

/**
 * Enables the gRPC Channelz introspection service (GetServers and
 * GetServerSockets) reporting live call counters and active sockets.
 * Registers both paths with grpc_server_handle(); returns its error (e.g.
 * GRPC_ERR_EXISTS) and registers nothing if either registration fails, or
 * GRPC_ERR_INVALID if ``server`` is nullptr.
 */
GRPC_API grpc_err grpc_channelz_enable(grpc_server *server);
