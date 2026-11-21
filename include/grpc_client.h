#pragma once

#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_message.h"
#include "grpc_metadata.h"
#include "grpc_status.h"

typedef struct grpc_client grpc_client;
typedef struct grpc_client_stream grpc_client_stream;

/** Configuration for a gRPC client channel (single target or round-robin pool). */
typedef struct grpc_client_config {
  /** Server hostname or IPv4/IPv6 literal (default "127.0.0.1" when unix_path/targets unset). */
  const char *host;
  /** Server TCP port (required unless unix_path or targets is set). */
  uint16_t port;
  /** Optional Unix Domain Socket path (or Windows named pipe). */
  const char *unix_path;
  /** Optional list of endpoints ("host:port" or "unix:/path") for round-robin load balancing. */
  const char *const *targets;
  size_t nb_targets;
  /** Default RPC timeout in milliseconds (0 = no default deadline). */
  uint32_t timeout_ms;
  /** Maximum response message size in bytes (default 4 MiB). */
  uint32_t max_message_size;
  /** Compression algorithm for outgoing request messages (default GRPC_COMPRESS_IDENTITY). */
  grpc_compression_algorithm compression;
  /** Compression level (0 or GRPC_COMPRESSION_LEVEL_DEFAULT for codec default). */
  int compression_level;
  /** Maximum automatic retries on transient UNAVAILABLE / transport failures (0 = disabled). */
  uint32_t max_retries;
  /** Initial retry backoff in milliseconds (doubles each attempt; default 50 ms). */
  uint32_t retry_backoff_ms;
  /** Client-side TLS & mTLS configuration. */
  struct {
    bool enable;
    const char *ca_file;
    const char *ca_pem;
    size_t ca_pem_len;
    const char *cert_file;
    const char *key_file;
    const char *server_name;
    bool insecure_skip_verify;
  } tls;
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
 * Creates a client channel (or round-robin connection pool) and connects.
 * Returns nullptr on failure and writes the error code to ``*out_err`` if non-null.
 */
GRPC_API GRPC_NODISCARD grpc_client *grpc_client_new(const grpc_client_config *config,
                                                     grpc_err *out_err);

/** Closes all client connections and frees resources. Accepts nullptr. */
GRPC_API void grpc_client_free(grpc_client *client);

/** Frees buffers owned by a grpc_client_response and zeroes it. */
GRPC_API void grpc_client_response_free(grpc_client_response *response);

/**
 * Executes a unary gRPC call on ``method`` (e.g. ``"/helloworld.Greeter/SayHello"``).
 * Honors configured retries and round-robin target selection.
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

/**
 * Opens a client-streaming or bidirectional-streaming RPC.
 * Send messages with grpc_client_stream_send(), half-close with
 * grpc_client_stream_close_send(), read responses with grpc_client_stream_recv(),
 * and complete with grpc_client_stream_finish() (or abort with grpc_client_stream_cancel()).
 */
GRPC_API GRPC_NODISCARD grpc_client_stream *
grpc_client_stream_open(grpc_client *client, const char *method, const grpc_metadata *metadata,
                        size_t nb_metadata, uint32_t timeout_ms, grpc_err *out_err);

/** Sends one message on an open client/bidi stream. */
GRPC_API grpc_err grpc_client_stream_send(grpc_client_stream *stream, const void *data, size_t len);

/** Half-closes the client send direction (signals no more request messages). */
GRPC_API grpc_err grpc_client_stream_close_send(grpc_client_stream *stream);

/**
 * Receives the next response message from the server. On success, sets
 * ``*out_msg`` (caller frees with grpc_message_free()) or sets ``*out_msg`` to
 * nullptr when the server has finished sending.
 */
GRPC_API grpc_err grpc_client_stream_recv(grpc_client_stream *stream, grpc_message **out_msg);

/**
 * Half-closes the send side if still open, waits for the server to finish,
 * populates ``out`` with headers, trailers, and final status, and frees ``stream``.
 */
GRPC_API grpc_err grpc_client_stream_finish(grpc_client_stream *stream, grpc_client_response *out);

/** Cancels the stream (RST_STREAM CANCEL) and frees ``stream``. Accepts nullptr. */
GRPC_API void grpc_client_stream_cancel(grpc_client_stream *stream);
