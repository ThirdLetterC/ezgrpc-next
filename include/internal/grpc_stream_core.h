#pragma once

/* grpc_stream_core.h - transport-independent handling of one gRPC request stream
 *
 * HTTP/2 (internal_nghttp2_callbacks.c) and HTTP/3 (grpc_http3_server_callbacks.c) both deliver the same
 * events for a request: header fields, the end of the header block, body chunks, the end of the
 * body and the stream closing. These functions turn them into calls and messages, and produce the
 * response body, so both transports share one implementation of validation, routing, deadlines,
 * gRPC framing, compression, gRPC-Web and limits. Responses go out through session->ops.
 * Loop thread only.
 */
#include "core.h"

/* Records one request header field and parses the ones the server needs (:method, :scheme,
 * :path with its route lookup, content-type, grpc-encoding, grpc-accept-encoding, grpc-timeout,
 * te). name_ref/value_ref are the transport's buffers backing name and value; a reference is
 * taken on each (stream->ops->hdr_incref) and kept until stream_free(). Returns 0, or -1 when the
 * stream already holds CONFIG_STREAM_MAX_HEADERS fields (nothing is retained; the transport should
 * reset just that stream). */
int stream_core_on_header(grpc_session *session, grpc_stream *stream, const uint8_t *name,
                          size_t namelen, const uint8_t *value, size_t valuelen, void *name_ref,
                          void *value_ref);

/* Handles the end of the request header block. Ignores a repeated header block. Answers a CORS
 * preflight OPTIONS with 204, a non-gRPC content-type with 415, a request that is not POST over
 * http(s) with te: trailers (not required for gRPC-Web) with a stream reset, and an unknown or
 * disabled path with UNIMPLEMENTED unless the fallback route takes it; such streams are marked
 * discarding. Otherwise creates the call, arms its grpc-timeout deadline, and queues it for
 * dispatch when the request already ended (`end_stream`) or the route has an on_start handler. */
void stream_core_on_headers_end(grpc_session *session, grpc_stream *stream, bool end_stream);

/* Appends a chunk of request body to the stream's receive buffer. Ignored for streams without a
 * call or being discarded. A body growing past max_message_size aborts the call with
 * RESOURCE_EXHAUSTED and discards the stream (the connection stays up). Returns 0, or -1 when out
 * of memory. */
int stream_core_on_data(grpc_session *session, grpc_stream *stream, const uint8_t *data,
                        size_t len);

/* Parses the complete messages buffered so far and queues them on the call; at `end_stream` also
 * half-closes the call, or aborts it with DATA_LOSS if a partial message remains. Decodes
 * grpc-web-text at the end of the body. Errors end the call, never the connection. */
void stream_core_on_data_end(grpc_session *session, grpc_stream *stream, bool end_stream);

/* Handles the stream closing for any reason: detaches the call (cancelling it unless it
 * finished), then unlinks and frees the stream. */
void stream_core_on_close(grpc_session *session, grpc_stream *stream);

/* Copies up to `cap` bytes of the stream's queued response messages into `buf`: each message as
 * its 5-byte prefix and payload (cflag 0xFE messages are pre-framed grpc-web-text and copied as
 * is), continuing where the last call stopped. Frees finished messages and fires the call's
 * writable callback when backpressure clears. When the queue is empty sets *eof and clears
 * stream->is_data_active; *end_stream then says whether the body ends the stream (gRPC-Web, whose
 * trailers are in the body) or the stream stays open for trailers. Returns the bytes written. */
size_t stream_core_pull(grpc_stream *stream, uint8_t *buf, size_t cap, bool *eof, bool *end_stream);

/* After stream_core_pull() reported EOF without end_stream: submits the trailers that
 * call_finish_internal() parked while data was queued, if any. Returns 0, or nonzero if the
 * transport rejected them. */
int stream_core_flush_pending_trailers(grpc_session *session, grpc_stream *stream);
