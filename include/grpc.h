#pragma once

/* grpc.h - umbrella header of ezGRPC-next, a gRPC server library in C
 *
 * Include this one header to get the whole public API: the server (grpc_server.h), calls
 * (grpc_call.h), messages and metadata, interceptors, health checking, reflection, channelz,
 * compression, TLS and the blocking client (grpc_client.h). The protobuf-c and flatcc helpers are
 * included only when the library is built with GRPC_ENABLE_PROTOBUF_C / GRPC_ENABLE_FLATCC.
 * See docs/getting-started.md.
 */

#include "grpc_defs.h"
#include "grpc_version.h"
#include "grpc_error.h"
#include "grpc_status.h"
#include "grpc_mem.h"
#include "grpc_message.h"
#include "grpc_metadata.h"
#include "grpc_call.h"
#include "grpc_event.h"
#include "grpc_server.h"
#include "grpc_interceptor.h"
#include "grpc_status_details.h"
#include "grpc_health.h"
#include "grpc_reflection.h"
#include "grpc_channelz.h"
#include "grpc_compression.h"
#include "grpc_tls.h"
#include "grpc_http3.h"
#include "grpc_client.h"

#if defined(GRPC_ENABLE_PROTOBUF_C)
#include "grpc_protobuf_c.h"
#endif

#if defined(GRPC_ENABLE_FLATCC)
#include "grpc_flatcc.h"
#endif
