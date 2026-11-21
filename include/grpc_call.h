#pragma once

/* grpc_call.h - server-side call API: read a request, answer it, finish it
 *
 * A grpc_call is one RPC on the server, handed to handlers registered with grpc_server_handle()
 * and friends (grpc_server.h) or found in a grpc_event (grpc_event.h). This header covers reading
 * the request and its metadata, per-call state, flow control, sending responses and finishing the
 * call, plus thread-safe ``*_async`` variants for answering from worker threads.
 */

#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_message.h"
#include "grpc_metadata.h"
#include "grpc_status.h"

/**
 * A gRPC call (an HTTP/2 stream). Everything about one request goes through
 * its call: reading metadata, sending responses and finishing it.
 *
 * Lifetime: a call handed to a handler or found in an event stays valid until
 * the handler returns (or until the next grpc_server_poll() for events).
 * Take a reference with grpc_call_ref() to keep it longer, e.g. to answer
 * from another thread, and drop it with grpc_call_unref().
 *
 * A call outlives its stream: once the client is gone, operations on it fail
 * with GRPC_ERR_CLOSED instead of crashing.
 *
 * Threads: every function here must be called from the thread running the
 * server, except grpc_call_ref(), grpc_call_unref() and the ``*_async``
 * functions (grpc_call_send_metadata_async(), grpc_call_add_trailers_async(),
 * grpc_call_send_async(), grpc_call_send_data_async(), grpc_call_reply_async()
 * and grpc_call_finish_async()).
 */
typedef struct grpc_call grpc_call;
typedef struct grpc_server grpc_server;

/* ---- the request ------------------------------------------------------ */

/**
 * The request of a method registered with grpc_server_handle(). It stays
 * valid as long as the call. It is nullptr for other methods.
 */
GRPC_API const grpc_message *grpc_call_request(const grpc_call *call);

/**
 * The method path, e.g. ``"/helloworld.Greeter/SayHello"``. Valid as long as
 * the call; nullptr if ``call`` is nullptr.
 */
GRPC_API const char *grpc_call_method(const grpc_call *call);

/**
 * The client address, e.g. ``"127.0.0.1:51234"`` or ``"[::1]:51234"``. Valid
 * as long as the call.
 */
GRPC_API const char *grpc_call_peer(const grpc_call *call);

/**
 * The verified TLS client certificate subject (e.g. ``"/CN=client"``), or
 * nullptr when TLS or client certificate authentication is not active. Always
 * nullptr with the rustls backend, which does not expose the subject.
 */
GRPC_API const char *grpc_call_peer_cert_subject(const grpc_call *call);

/**
 * Looks up request metadata by key (case-insensitive). Returns the value,
 * NUL-terminated and valid as long as the call, or nullptr if absent.
 * ``len`` may be nullptr.
 */
GRPC_API const char *grpc_call_metadata(const grpc_call *call, const char *key, size_t *len);

/**
 * Iterates over all request metadata values matching ``key`` (case-insensitive).
 * Set ``*index = 0`` before the first call. Each call returns the next matching
 * value and advances ``*index``, or returns nullptr when no more values exist.
 */
GRPC_API const char *grpc_call_metadata_next(const grpc_call *call, const char *key, size_t *index,
                                             size_t *len);

/** The number of request headers, including pseudo-headers such as ``:path``. */
GRPC_API size_t grpc_call_metadata_count(const grpc_call *call);

/**
 * Reads the request header at ``index`` into ``out``. The key and value point
 * into the call and stay valid as long as it. Returns false if ``index`` is
 * out of range or an argument is nullptr.
 */
GRPC_API bool grpc_call_metadata_at(const grpc_call *call, size_t index, grpc_metadata *out);

/**
 * Milliseconds left until the client's deadline (``grpc-timeout``), 0 once it
 * passed, or -1 without a deadline. The library ends calls that run past
 * their deadline with DEADLINE_EXCEEDED.
 */
GRPC_API int64_t grpc_call_deadline(const grpc_call *call);

/**
 * True once the call ended without the application finishing it: the client
 * cancelled or disconnected, the deadline passed, or a message was truncated.
 */
GRPC_API bool grpc_call_is_cancelled(const grpc_call *call);

/**
 * Why the call was cancelled: CANCELLED (by the client), DEADLINE_EXCEEDED,
 * UNAVAILABLE (connection lost) or DATA_LOSS. GRPC_STATUS_OK if it was not.
 */
GRPC_API grpc_status grpc_call_cancel_reason(const grpc_call *call);

/**
 * Parsed W3C Trace Context (``traceparent`` header:
 * ``"00-<32-hex-trace-id>-<16-hex-span-id>-<2-hex-flags>"``).
 */
typedef struct grpc_trace_context {
  /** 32 lowercase hex digits, NUL-terminated. */
  char trace_id[33];
  /** 16 lowercase hex digits, NUL-terminated. */
  char span_id[17];
  /** The trace flags byte (bit 0 is "sampled"). */
  uint8_t trace_flags;
  /** True when the fields hold a parsed or filled-in context. */
  bool valid;
} grpc_trace_context;

/**
 * Parses the ``traceparent`` request header into ``out``. Returns true if a
 * valid W3C traceparent header (version ``00``, lowercase hex, non-zero trace
 * and span ids) was present; otherwise ``out`` is zeroed and false is returned.
 */
GRPC_API bool grpc_call_trace_context(const grpc_call *call, grpc_trace_context *out);

/**
 * Formats a W3C ``traceparent`` header value into ``buf`` (needs at least 56
 * bytes: 55 characters plus the NUL). Returns GRPC_ERR_INVALID if ``ctx`` is
 * not ``valid``, its ids have the wrong length, or ``buf`` is too small.
 */
GRPC_API grpc_err grpc_traceparent_format(const grpc_trace_context *ctx, char *buf, size_t buf_len);

/** The server the call belongs to, or nullptr if ``call`` is nullptr. */
GRPC_API grpc_server *grpc_call_server(const grpc_call *call);

/* ---- per-call state ----------------------------------------------------- */

/**
 * Attaches application data to the call, e.g. the state of a streaming call.
 * ``free_fn`` (may be nullptr) is called on it when the call is released.
 * Replacing earlier data with a different pointer calls the previous
 * ``free_fn`` on the previous data.
 */
GRPC_API void grpc_call_set_userdata(grpc_call *call, void *data, void (*free_fn)(void *data));

/** Returns the data set with grpc_call_set_userdata(), or nullptr. */
GRPC_API void *grpc_call_userdata(const grpc_call *call);

/**
 * Calls ``fn`` on the server thread if the call is cancelled after this point
 * (see grpc_call_is_cancelled()). Useful for calls answered later. A call has
 * one cancel callback; this replaces any earlier one. For streaming methods
 * the ``on_cancel`` handler from registration is installed when the call
 * starts.
 */
GRPC_API void grpc_call_on_cancel(grpc_call *call, void (*fn)(grpc_call *call, void *userdata),
                                  void *userdata);

/**
 * Returns the number of response payload bytes currently queued for transmission
 * on this call's stream.
 */
GRPC_API size_t grpc_call_send_queue_bytes(const grpc_call *call);

/**
 * Returns true if the call's send queue is below ``grpc_config.max_send_queue_bytes``
 * (or if no send queue limit is configured). False once the call is finished or
 * closed.
 */
GRPC_API bool grpc_call_is_writable(const grpc_call *call);

/**
 * Registers a callback invoked on the server thread when the stream's send
 * queue drains below ``grpc_config.max_send_queue_bytes`` after experiencing
 * backpressure (e.g. after grpc_call_send() returned GRPC_ERR_LIMIT). Replaces
 * any earlier callback.
 */
GRPC_API void grpc_call_on_writable(grpc_call *call, void (*fn)(grpc_call *call, void *userdata),
                                    void *userdata);

/* ---- answering ---------------------------------------------------------- */

/**
 * Configures automatic compression for uncompressed response messages sent on
 * this call. Must be called before the first message is sent. Negotiates
 * against the client's ``grpc-accept-encoding`` header; if the client does not
 * advertise ``algorithm``, responses remain uncompressed. ``level`` is passed
 * to the codec. Returns GRPC_ERR_STATE once headers were sent and
 * GRPC_ERR_UNSUPPORTED if the library was built without ``algorithm``.
 */
GRPC_API grpc_err grpc_call_set_compression(grpc_call *call, grpc_compression_algorithm algorithm,
                                            int level);

/**
 * Sends initial metadata (response headers). Only before the first message.
 * The metadata is copied and sent with the first message, or with the status
 * if no message is sent. Returns GRPC_ERR_STATE once headers went out.
 */
GRPC_API grpc_err grpc_call_send_metadata(grpc_call *call, const grpc_metadata *metadata,
                                          size_t count);

/**
 * Sends a response message. Takes ownership of ``message``, also when it
 * fails. Call it several times to stream responses.
 *
 * Returns GRPC_ERR_LIMIT when ``grpc_config.max_send_queue_bytes`` would be
 * exceeded (see grpc_call_on_writable()), and GRPC_ERR_STATE when a
 * precompressed message uses a different encoding than the first message.
 */
GRPC_API grpc_err grpc_call_send(grpc_call *call, grpc_message *message);

/**
 * Sends a copy of ``len`` bytes of ``data`` as a response message. ``data``
 * may be nullptr when ``len`` is 0.
 */
GRPC_API grpc_err grpc_call_send_data(grpc_call *call, const void *data, size_t len);

/**
 * Sends ``message`` and finishes the call with GRPC_STATUS_OK (unary shortcut).
 * Takes ownership of ``message``, also on failure. If sending fails the call
 * is not finished.
 */
GRPC_API grpc_err grpc_call_reply(grpc_call *call, grpc_message *message);

/**
 * Sends a copy of ``len`` bytes of ``data`` and finishes the call with GRPC_STATUS_OK.
 */
GRPC_API grpc_err grpc_call_reply_data(grpc_call *call, const void *data, size_t len);

/**
 * Adds trailing metadata, sent by grpc_call_finish(). The metadata is copied;
 * may be called several times before finishing.
 */
GRPC_API grpc_err grpc_call_add_trailers(grpc_call *call, const grpc_metadata *metadata,
                                         size_t count);

/**
 * Ends the call with a status. ``message`` (may be nullptr) is sent as
 * ``grpc-message``; error statuses without one get the status name.
 * Every call must be finished exactly once, unless it is cancelled.
 */
GRPC_API grpc_err grpc_call_finish(grpc_call *call, grpc_status status, const char *message);

/* ---- references and threads --------------------------------------------- */

/** Takes a reference, keeping the call valid. Thread-safe. Returns ``call``. */
GRPC_API grpc_call *grpc_call_ref(grpc_call *call);

/**
 * Drops a reference taken with grpc_call_ref(); the last one frees the call.
 * Thread-safe.
 */
GRPC_API void grpc_call_unref(grpc_call *call);

/**
 * Thread-safe grpc_call_send_metadata(). Copies ``metadata`` and queues it on
 * the server thread. The caller must hold a reference to the call. The return
 * value only reports whether the operation was queued; errors of the deferred
 * operation itself are discarded.
 */
GRPC_API grpc_err grpc_call_send_metadata_async(grpc_call *call, const grpc_metadata *metadata,
                                                size_t count);

/**
 * Thread-safe grpc_call_add_trailers(). Copies ``metadata`` and queues it on
 * the server thread. The caller must hold a reference to the call. The return
 * value only reports whether the operation was queued; errors of the deferred
 * operation itself are discarded.
 */
GRPC_API grpc_err grpc_call_add_trailers_async(grpc_call *call, const grpc_metadata *metadata,
                                               size_t count);

/**
 * Thread-safe grpc_call_send(): queues the message for the server thread
 * and wakes it up. Takes ownership of ``message``, also on failure. The caller
 * must hold a reference to the call. The return value only reports whether the
 * message was queued; a later failure of the send itself is not reported.
 */
GRPC_API grpc_err grpc_call_send_async(grpc_call *call, grpc_message *message);

/**
 * Thread-safe grpc_call_send_data(): copies ``data`` and queues it like
 * grpc_call_send_async(). The caller must hold a reference.
 */
GRPC_API grpc_err grpc_call_send_data_async(grpc_call *call, const void *data, size_t len);

/**
 * Thread-safe grpc_call_reply(). Takes ownership of ``message``, also on
 * failure. The caller must hold a reference. Only queueing errors are returned.
 */
GRPC_API grpc_err grpc_call_reply_async(grpc_call *call, grpc_message *message);

/**
 * Thread-safe grpc_call_finish(). ``message`` is copied. The caller must hold
 * a reference. Only queueing errors are returned.
 */
GRPC_API grpc_err grpc_call_finish_async(grpc_call *call, grpc_status status, const char *message);
