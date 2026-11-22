#pragma once

/* grpc_reflection.h - gRPC server reflection service
 *
 * Lets tools such as grpcurl list the services of a server and, with descriptors added through
 * grpc_reflection_add_file_descriptor(), describe their schemas.
 */

#include "grpc_defs.h"
#include "grpc_error.h"

typedef struct grpc_server grpc_server;

/** Method paths registered by grpc_reflection_enable(). */
#define GRPC_REFLECTION_V1ALPHA_PATH \
  "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
#define GRPC_REFLECTION_V1_PATH "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"

/**
 * Serves gRPC server reflection (v1 and v1alpha). The services are those of
 * the methods registered on the server. Registers both paths as streaming
 * methods; returns the error of the registration that failed (e.g.
 * GRPC_ERR_EXISTS).
 */
GRPC_API grpc_err grpc_reflection_enable(grpc_server *server);

/**
 * Adds a serialized FileDescriptorProto, returned for ``symbol_or_filename``,
 * so that tools like ``grpcurl describe`` can show the schema. ``descriptor``
 * and ``symbol_or_filename`` are copied (at full length). Call it from
 * the server thread or before the server runs.
 */
GRPC_API grpc_err grpc_reflection_add_file_descriptor(grpc_server *server,
                                                      const char *symbol_or_filename,
                                                      const uint8_t *descriptor, size_t len);
