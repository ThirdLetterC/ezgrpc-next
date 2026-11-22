#pragma once

/* grpc_client.h - blocking HTTP/2 gRPC client
 *
 * A grpc_client is a channel to one target or a round-robin pool of targets (TCP host/port, Unix
 * socket or Windows named pipe), optionally over TLS/mTLS. Every call blocks the calling thread:
 * the client runs its own event loop inside each function until the operation completes or times
 * out. A client and its streams are not thread-safe; use one client per thread or serialize
 * access. Each connection carries one RPC at a time.
 *
 * Responses, messages and metadata the client returns are allocated with the library allocator
 * and released with grpc_client_response_free() / grpc_message_free(). See docs/client.md.
 */

#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_message.h"
#include "grpc_metadata.h"
#include "grpc_status.h"
#include "grpc_http3.h"

/** A client channel created with grpc_client_new(). Opaque. */
typedef struct grpc_client grpc_client;
/** An open client- or bidi-streaming RPC from grpc_client_stream_open(). Opaque. */
typedef struct grpc_client_stream grpc_client_stream;

/**
 * Configuration for a gRPC client channel (single target or round-robin pool).
 * grpc_client_new() copies the struct, and the host, unix_path and targets
 * strings; ``tls.server_name`` is read again on every reconnect and must stay
 * valid for the lifetime of the client.
 */
typedef struct grpc_client_config {
  /** Server hostname or IPv4/IPv6 literal (default "127.0.0.1" when unix_path/targets unset). */
  const char *host;
  /** Server TCP port (required unless unix_path or targets is set). */
  uint16_t port;
  /** Optional Unix Domain Socket path (or Windows named pipe). */
  const char *unix_path;
  /** Optional list of endpoints ("host:port" or "unix:/path") for round-robin load balancing. */
  const char *const *targets;
  /** Number of entries in ``targets``. */
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
  /** Wire protocol (default GRPC_TRANSPORT_HTTP2). GRPC_TRANSPORT_HTTP3 requires ``tls.enable``
   * and a build with HTTP/3 (else GRPC_ERR_UNSUPPORTED), and rejects Unix-socket targets
   * (GRPC_ERR_INVALID). See grpc_http3.h. */
  grpc_transport transport;
  /**
   * Client-side TLS & mTLS configuration. Only available when the library is
   * built with an OpenSSL-compatible TLS backend; otherwise enabling it makes
   * grpc_client_new() fail with GRPC_ERR_UNSUPPORTED.
   */
  struct {
    /** Use TLS (ALPN ``h2``, TLS 1.2 or newer). */
    bool enable;
    /** CA bundle file used to verify the server. */
    const char *ca_file;
    /** CA certificates as PEM text, used when ``ca_file`` is nullptr. */
    const char *ca_pem;
    /** Length of ``ca_pem`` (0 = NUL-terminated). */
    size_t ca_pem_len;
    /** Client certificate chain file (PEM) for mTLS; requires ``key_file``. */
    const char *cert_file;
    /** Client private key file (PEM) for mTLS; requires ``cert_file``. */
    const char *key_file;
    /** SNI host name (default: the target host). */
    const char *server_name;
    /** Skip server certificate verification. Testing only. */
    bool insecure_skip_verify;
  } tls;
} grpc_client_config;

/**
 * Result of a completed client RPC. Every pointer is owned by the struct;
 * release them with grpc_client_response_free().
 */
typedef struct grpc_client_response {
  /** ``grpc-status`` from the server, or GRPC_STATUS_UNKNOWN if none arrived. */
  grpc_status status;
  /** ``grpc-message`` from the server, or nullptr. */
  char *status_message;
  /** The (last) response message, or nullptr. Not used by server streaming. */
  grpc_message *message;
  /** Response headers (initial metadata), keys and values NUL-terminated. */
  grpc_metadata *headers;
  /** Number of entries in ``headers``. */
  size_t nb_headers;
  /** Trailing metadata, including ``grpc-status`` and ``grpc-message``. */
  grpc_metadata *trailers;
  /** Number of entries in ``trailers``. */
  size_t nb_trailers;
} grpc_client_response;

/**
 * Creates a client channel (or round-robin connection pool) and connects.
 * Blocks until connected (bounded by ``timeout_ms`` when set). With
 * ``targets`` it succeeds if at least one target is reachable.
 * Returns nullptr on failure and writes the error code to ``*out_err`` if non-null
 * (GRPC_ERR_INVALID for a bad config, GRPC_ERR_IO when no connection could be made).
 * Release with grpc_client_free(). On POSIX systems, if SIGPIPE still has its
 * default disposition (which kills the process when writing to a connection
 * the server closed), it is set to SIG_IGN; a handler the application installed
 * is kept.
 */
GRPC_API GRPC_NODISCARD grpc_client *grpc_client_new(const grpc_client_config *config,
                                                     grpc_err *out_err);

/** Closes all client connections and frees resources. Accepts nullptr. */
GRPC_API void grpc_client_free(grpc_client *client);

/**
 * Frees buffers owned by a grpc_client_response and zeroes it. Does not free
 * ``response`` itself. Accepts nullptr and an already freed (zeroed) response.
 */
GRPC_API void grpc_client_response_free(grpc_client_response *response);

/**
 * Executes a unary gRPC call on ``method`` (e.g. ``"/helloworld.Greeter/SayHello"``).
 * Honors configured retries and round-robin target selection. ``timeout_ms``
 * of 0 uses ``grpc_client_config.timeout_ms``. Blocks until the call completes.
 *
 * Returns GRPC_OK when the RPC completed at the transport level; the RPC
 * outcome is then in ``out->status``. ``out`` is filled even on failure and
 * must always be released with grpc_client_response_free().
 */
GRPC_API grpc_err grpc_client_unary(grpc_client *client, const char *method, const void *req_data,
                                    size_t req_len, const grpc_metadata *metadata,
                                    size_t nb_metadata, uint32_t timeout_ms,
                                    grpc_client_response *out);

/**
 * Executes a server-streaming gRPC call. Invokes ``on_message`` for each
 * response message as it arrives, and populates ``out`` with headers, trailers,
 * and final status. ``on_message`` runs on the calling thread and ``msg`` is
 * only valid during the callback. Retries work as for grpc_client_unary(), but
 * only until the first message reaches ``on_message``: after that the call is
 * committed and a failure is returned instead of retried, so ``on_message``
 * never sees a message twice.
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
 * ``metadata`` is sent with the request headers. ``timeout_ms`` of 0 uses
 * ``grpc_client_config.timeout_ms``. No retries are made. Returns nullptr on
 * failure and writes the error code to ``*out_err`` if non-null.
 *
 * Each connection carries one RPC at a time. With several targets an idle
 * connection is picked; if every connection already has an open stream, this
 * (and grpc_client_unary() / grpc_client_server_stream()) fails with
 * GRPC_ERR_STATE until a stream is finished or cancelled.
 */
GRPC_API GRPC_NODISCARD grpc_client_stream *
grpc_client_stream_open(grpc_client *client, const char *method, const grpc_metadata *metadata,
                        size_t nb_metadata, uint32_t timeout_ms, grpc_err *out_err);

/**
 * Sends one message on an open client/bidi stream, compressed per
 * ``grpc_client_config.compression``. Returns GRPC_ERR_STATE after
 * grpc_client_stream_close_send() and GRPC_ERR_CLOSED once the stream or
 * connection ended.
 */
GRPC_API grpc_err grpc_client_stream_send(grpc_client_stream *stream, const void *data, size_t len);

/**
 * Half-closes the client send direction (signals no more request messages).
 * Calling it again is a no-op.
 */
GRPC_API grpc_err grpc_client_stream_close_send(grpc_client_stream *stream);

/**
 * Receives the next response message from the server. On success, sets
 * ``*out_msg`` (caller frees with grpc_message_free()) or sets ``*out_msg`` to
 * nullptr when the server has finished sending. Blocks until a message
 * arrives or the stream ends. If the stream failed (e.g. GRPC_ERR_TIMEOUT or
 * GRPC_ERR_CLOSED), that error is returned with ``*out_msg`` set to nullptr.
 */
GRPC_API grpc_err grpc_client_stream_recv(grpc_client_stream *stream, grpc_message **out_msg);

/**
 * Half-closes the send side if still open, waits for the server to finish,
 * populates ``out`` with headers, trailers, and final status, and frees ``stream``.
 * A response message not yet read with grpc_client_stream_recv() is put in
 * ``out->message`` (the last one if several remain). Release ``out`` with
 * grpc_client_response_free().
 */
GRPC_API grpc_err grpc_client_stream_finish(grpc_client_stream *stream, grpc_client_response *out);

/** Cancels the stream (RST_STREAM CANCEL) and frees ``stream``. Accepts nullptr. */
GRPC_API void grpc_client_stream_cancel(grpc_client_stream *stream);
