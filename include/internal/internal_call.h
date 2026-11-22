#pragma once

/* internal_call.h - helpers shared by the call layer's files (grpc_call*.c)
 *
 * grpc_call.c owns a call's lifetime and header lists; grpc_call_response.c, grpc_call_access.c,
 * grpc_call_async.c and grpc_call_dispatch.c build on these. Not used outside the call layer.
 */
#include "core.h"

/* Frees every header of `list` and the array, and resets it to empty so it can be reused. */
void nv_list_free(nv_list *list);

/* Appends application metadata to `list`. A zero key_len or value_len means the string is
 * NUL-terminated. Rejects with GRPC_ERR_INVALID anything metadata_entry_valid() refuses: null,
 * empty or malformed keys, pseudo-headers and reserved names, and values with control bytes. On
 * error the entries already appended stay in the list. */
grpc_err nv_list_add_metadata(nv_list *list, const grpc_metadata *metadata, size_t count);

/* Copies `nva` (the array and every name and value) into a single grpc_malloc() block, so one
 * grpc_free() releases it. Names and values are not NUL-terminated in the copy. Returns nullptr
 * on allocation failure. */
grpc_nv *call_nva_dup(const grpc_nv *nva, size_t nvlen);

/* Frees the received messages that were not delivered to the application (the queue's buffer
 * is kept for later messages; call_free() releases it). */
void call_drop_pending(grpc_call *call);

/* Returns true if `alg` is identity or a codec compiled into this build. */
bool call_codec_supported(grpc_compression_algorithm alg);

/* Returns true if the client's grpc-accept-encoding header lists `alg`. */
bool call_client_accepts_encoding(const grpc_call *call, grpc_compression_algorithm alg);

/* Counts the call once in the server's ok/error statistics. */
void call_record_done_stat(grpc_call *call, grpc_status status);

/* Marks the call cancelled (once), records the statistics, runs the on_done interceptors and
 * queues the call so that dispatch delivers the cancellation to the application. */
void call_cancel(grpc_call *call, grpc_status reason);

/* Ends the call: sends grpc-status, grpc-message (defaulting to the status name when `message`
 * is null and the status is not OK) and the application's trailers. Depending on what was sent:
 *  - gRPC-Web: the trailers go in a final DATA frame flagged 0x80 (base64 for grpc-web-text);
 *  - no headers yet: one Trailers-Only HEADERS frame;
 *  - DATA still queued: the trailers are stored on the stream for the data provider to submit
 *    after the last message, so they cannot overtake it;
 *  - otherwise: a trailing HEADERS frame right away.
 * On success marks the call finished, disarms the deadline, schedules a flush and runs the
 * on_done interceptors. Does not check `finished`; callers do. Returns GRPC_ERR_CLOSED if the
 * stream is gone. */
grpc_err call_finish_internal(grpc_call *call, grpc_status status, const char *message);
