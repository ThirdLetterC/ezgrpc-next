#pragma once

#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_message.h"
#include "grpc_metadata.h"
#include "grpc_status.h"

typedef struct grpc_client grpc_client;

/** Configuration for a gRPC client channel. */
typedef struct grpc_client_config {
  /** Server hostname or IPv4/IPv6 literal (default "127.0.0.1" when unix_path is nullptr). */
  const char *host;
  /** Server TCP port (required unless unix_path is set). */
  uint16_t port;
  /** Optional Unix Domain Socket path (or Windows named pipe). */
  const char *unix_path;
  /** Default RPC timeout in milliseconds (0 = no default deadline). */
  uint32_t timeout_ms;
  /** Maximum response message size in bytes (default 4 MiB). */
  uint32_t max_message_size;
  /** Compression algorithm for outgoing request messages (default GRPC_COMPRESS_IDENTITY). */
  grpc_compression_algorithm compression;
  /** Compression level (0 or GRPC_COMPRESSION_LEVEL_DEFAULT for codec default). */
  int compression_level;
} grpc_client_config;

/** Result of a completed client RPC. Release with grpc_client_response_free(). */
typedef struct grpc_client_response {
  grpc_status status;
  char *status_message;
  grpc_message *message;
  grpc_metadata *headers;
  size_t nb_headers;
  grpc_metadata *trailers;
  size_t nb_trailers;
} grpc_client_response;

/**
 * Creates a client channel and connects to the target server.
 * Returns nullptr on failure and writes the error code to ``*out_err`` if non-null.
 */
GRPC_API GRPC_NODISCARD grpc_client *grpc_client_new(const grpc_client_config *config,
                                                     grpc_err *out_err);

/** Closes the client connection and frees all resources. Accepts nullptr. */
GRPC_API void grpc_client_free(grpc_client *client);

/** Frees buffers owned by a grpc_client_response and zeroes it. */
GRPC_API void grpc_client_response_free(grpc_client_response *response);

/**
 * Executes a unary gRPC call on ``method`` (e.g. ``"/helloworld.Greeter/SayHello"``).
 * Populates ``out`` with the response message, headers, trailers, and gRPC status.
 */
GRPC_API grpc_err grpc_client_unary(grpc_client *client, const char *method, const void *req_data,
                                    size_t req_len, const grpc_metadata *metadata,
                                    size_t nb_metadata, uint32_t timeout_ms,
                                    grpc_client_response *out);

/**
 * Executes a server-streaming gRPC call. Invokes ``on_message`` for each
 * response message as it arrives, and populates ``out`` with headers, trailers,
 * and final status.
 */
GRPC_API grpc_err grpc_client_server_stream(
    grpc_client *client, const char *method, const void *req_data, size_t req_len,
    const grpc_metadata *metadata, size_t nb_metadata, uint32_t timeout_ms,
    void (*on_message)(const grpc_message *msg, void *userdata), void *userdata,
    grpc_client_response *out);
