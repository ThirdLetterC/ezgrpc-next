#pragma once

/* grpc_status_details.h - gRPC rich error model (google.rpc.Status)
 *
 * Encodes and decodes the ``grpc-status-details-bin`` trailer, a Base64 google.rpc.Status
 * protobuf carrying a code, a message and ``google.protobuf.Any`` detail entries, and finishes
 * server calls with it. Strings and buffers returned here are allocated with grpc_malloc() and
 * released with grpc_free() or grpc_status_details_free().
 */

#include <stddef.h>
#include <stdint.h>
#include "grpc_call.h"
#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_status.h"

/** The trailer key carrying the encoded google.rpc.Status. */
#define GRPC_STATUS_DETAILS_TRAILER "grpc-status-details-bin"

/**
 * One ``google.protobuf.Any`` detail: a type URL (e.g.
 * ``"type.googleapis.com/google.rpc.ErrorInfo"``) and the serialized message.
 */
typedef struct grpc_status_detail_entry {
  /** NUL-terminated type URL; nullptr or ``""`` omits it. */
  const char *type_url;
  /** The serialized detail message. */
  const uint8_t *value;
  /** Length of ``value``. */
  size_t value_len;
} grpc_status_detail_entry;

/**
 * Decoded google.rpc.Status with all Any detail entries. Release with
 * grpc_status_details_free().
 */
typedef struct grpc_status_details {
  /** The status code (a grpc_status value). */
  int32_t code;
  /** The message, NUL-terminated, or nullptr if absent. */
  char *message;
  /** The detail entries; their strings and values are owned by the struct. */
  grpc_status_detail_entry *details;
  /** Number of entries in ``details``. */
  size_t nb_details;
} grpc_status_details;

/**
 * Encodes a google.rpc.Status protobuf message into Base64 for the
 * 'grpc-status-details-bin' trailing metadata.
 *
 * ``message`` may be nullptr. ``details`` must point to ``nb_details``
 * entries. Stores the string length in ``*out_len`` if it is non-null.
 * Caller must free the returned string with grpc_free.
 * Returns nullptr on error.
 */
GRPC_API GRPC_NODISCARD char *grpc_status_details_encode(int32_t code, const char *message,
                                                         const grpc_status_detail_entry *details,
                                                         size_t nb_details, size_t *out_len);

/**
 * Decodes a 'grpc-status-details-bin' base64 string back into status code,
 * message, and the first detail entry (if present).
 *
 * ``*out_message``, ``*out_type_url``, and ``*out_detail_value`` are allocated
 * with grpc_malloc() and must be freed with grpc_free(). Every output pointer
 * may be nullptr to skip that field; fields that are absent are set to nullptr
 * / 0. The type URL and value always come from the same (first) detail entry.
 * Decoding is lenient: it stops at a malformed field and keeps what was read.
 * Returns GRPC_ERR_INVALID if ``base64_str`` is nullptr or not Base64 (the
 * outputs are then left untouched).
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_status_details_decode(const char *base64_str,
                                                            int32_t *out_code, char **out_message,
                                                            char **out_type_url,
                                                            uint8_t **out_detail_value,
                                                            size_t *out_detail_len);

/**
 * Decodes a 'grpc-status-details-bin' base64 string with all Any detail entries.
 * Release the populated struct with grpc_status_details_free(). Unlike
 * grpc_status_details_decode(), input that is not a valid google.rpc.Status
 * fails with GRPC_ERR_INVALID: a truncated field, field number 0, a wire type
 * protobuf does not allow, or a field overrunning the input. Unknown fields
 * are skipped. On any error ``*out`` is freed and zeroed.
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_status_details_decode_all(const char *base64_str,
                                                                grpc_status_details *out);

/**
 * Frees buffers owned by a grpc_status_details struct and zeroes it. Does not
 * free ``details`` itself. Accepts nullptr.
 */
GRPC_API void grpc_status_details_free(grpc_status_details *details);

/**
 * Finishes a call with rich error details, sent as ``grpc-status-details-bin``
 * (the gRPC Rich Error Model). Trailers added with grpc_call_add_trailers()
 * are sent too. ``message`` is also sent as ``grpc-message``. ``details`` is
 * encoded before returning, so it need not outlive the call. Server thread
 * only, like grpc_call_finish().
 */
GRPC_API grpc_err grpc_call_finish_with_details(grpc_call *call, grpc_status status,
                                                const char *message,
                                                const grpc_status_detail_entry *details,
                                                size_t nb_details);

/**
 * Thread-safe grpc_call_finish_with_details(). The caller must hold a reference.
 * ``message`` and ``details`` are encoded/copied before returning. Only
 * queueing errors are returned; errors of the deferred finish are discarded.
 */
GRPC_API grpc_err grpc_call_finish_with_details_async(grpc_call *call, grpc_status status,
                                                      const char *message,
                                                      const grpc_status_detail_entry *details,
                                                      size_t nb_details);
