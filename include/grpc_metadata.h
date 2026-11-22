#pragma once

/* grpc_metadata.h - metadata entries and binary value encoding
 *
 * grpc_metadata is the key/value pair used for request headers, response headers and trailers on
 * both server and client. Binary values (keys ending in ``-bin``) travel Base64 encoded; the
 * helpers here convert them.
 */

#include "grpc_defs.h"

/**
 * A metadata entry (an HTTP/2 header): sent with
 * :c:func:`grpc_call_send_metadata()` and :c:func:`grpc_call_add_trailers()`,
 * and returned by :c:func:`grpc_call_metadata_at()`.
 *
 * When sending, a length of 0 means ``strlen()``. Keys must be lowercase.
 * Keys ending in ``-bin`` carry Base64 encoded binary values.
 */
typedef struct grpc_metadata {
  /** The header name. */
  const char *key;
  /** Length of ``key`` (0 = ``strlen()`` when sending). */
  size_t key_len;
  /** The header value. */
  const char *value;
  /** Length of ``value`` (0 = ``strlen()`` when sending). */
  size_t value_len;
} grpc_metadata;

/**
 * Decodes the Base64 value of a ``-bin`` key: the standard alphabet, padded
 * or unpadded (both are valid in gRPC). Any other character, including
 * whitespace, misplaced or excess padding, or a final group of a single
 * character makes the value malformed. Stores the decoded length in
 * ``*out_len`` if it is non-null. Returns a buffer to free with grpc_free(),
 * or nullptr if ``base64`` is nullptr, the value is malformed or allocation
 * fails.
 */
GRPC_API GRPC_NODISCARD uint8_t *grpc_metadata_decode_bin(const char *base64, size_t len,
                                                          size_t *out_len);

/**
 * Base64 encodes binary data for a ``-bin`` key (standard alphabet, padded).
 * Stores the string length in ``*out_len`` if it is non-null. Returns a
 * NUL-terminated string to free with grpc_free(), or nullptr if ``data`` is
 * nullptr or allocation fails.
 */
GRPC_API GRPC_NODISCARD char *grpc_metadata_encode_bin(const uint8_t *data, size_t len,
                                                       size_t *out_len);
