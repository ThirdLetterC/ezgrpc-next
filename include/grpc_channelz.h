#pragma once

#include "grpc_defs.h"
#include "grpc_error.h"

typedef struct grpc_server grpc_server;

#define GRPC_CHANNELZ_PATH_GET_SERVERS "/grpc.channelz.v1.Channelz/GetServers"
#define GRPC_CHANNELZ_PATH_GET_SERVER_SOCKETS "/grpc.channelz.v1.Channelz/GetServerSockets"

/**
 * Enables the gRPC Channelz introspection service (GetServers and
 * GetServerSockets) reporting live call counters and active sockets.
 */
GRPC_API grpc_err grpc_channelz_enable(grpc_server *server);
