#pragma once

/* grpc_compression.h - message compression codecs
 *
 * Buffer and grpc_message helpers for the gRPC compression algorithms (gzip, deflate, and zstd /
 * snappy when built with them). Codecs that were not compiled in return GRPC_ERR_UNSUPPORTED.
 * Buffers returned through ``out`` are allocated with grpc_malloc() and must be released with
 * grpc_free(), never libc free(). Per-call automatic compression is configured with
 * grpc_call_set_compression() and the server/client configs.
 */

#include "grpc_defs.h"
#include "grpc_error.h"
#include "grpc_message.h"

/** Selects the codec's default compression level. */
#define GRPC_COMPRESSION_LEVEL_DEFAULT (-1)

/**
 * Compresses ``in_len`` bytes of ``in`` using the selected codec and level;
 * ``*out`` is freed with grpc_free(). ``in`` may be nullptr when ``in_len`` is
 * 0. Returns GRPC_ERR_UNSUPPORTED for codecs not built in and GRPC_ERR_INVALID
 * for an out-of-range level.
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_compress(grpc_compression_algorithm algorithm, int level,
                                               const uint8_t *in, size_t in_len, uint8_t **out,
                                               size_t *out_len);
/**
 * Decompresses using the selected codec; ``*out`` is freed with grpc_free().
 * The output size is unbounded: prefer grpc_decompress_limited() for
 * untrusted input.
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_decompress(grpc_compression_algorithm algorithm,
                                                 const uint8_t *in, size_t in_len, uint8_t **out,
                                                 size_t *out_len);
/**
 * Bounded decoding with any supported codec. Returns GRPC_ERR_LIMIT when output exceeds
 * ``max_output``. On failure, ``*out`` is nullptr and ``*out_len`` is zero; on
 * success ``*out`` is freed with grpc_free().
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_decompress_limited(grpc_compression_algorithm algorithm,
                                                         const uint8_t *in, size_t in_len,
                                                         uint8_t **out, size_t *out_len,
                                                         size_t max_output);
/**
 * Returns a compressed copy of ``msg`` (``cflag`` = 1, ``compression`` =
 * ``algorithm``) using ``algorithm`` and ``level``, or nullptr on failure.
 * Caller frees with grpc_message_free().
 */
GRPC_API GRPC_NODISCARD grpc_message *
grpc_message_compress(const grpc_message *msg, grpc_compression_algorithm algorithm, int level);
/**
 * Decompresses ``msg`` using ``algorithm`` into a new message with ``cflag`` = 0;
 * uncompressed messages (``cflag`` = 0) are duplicated. Returns nullptr on
 * failure. The output size is unbounded. Caller frees with grpc_message_free().
 */
GRPC_API GRPC_NODISCARD grpc_message *grpc_message_decompress(const grpc_message *msg,
                                                              grpc_compression_algorithm algorithm);

/**
 * Compresses data using gzip (RFC 1952).
 * Allocates ``*out`` with grpc_malloc(). The caller frees it with grpc_free().
 * Returns GRPC_OK on success, or a negative grpc_err on failure.
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_compress_gzip(const uint8_t *in, size_t in_len, uint8_t **out,
                                                    size_t *out_len);

/**
 * Decompresses gzip data (RFC 1952).
 * Allocates ``*out`` with grpc_malloc(). The caller frees it with grpc_free().
 * Returns GRPC_OK on success, or a negative grpc_err on failure.
 */
GRPC_API GRPC_NODISCARD grpc_err grpc_decompress_gzip(const uint8_t *in, size_t in_len,
                                                      uint8_t **out, size_t *out_len);

/**
 * Returns a new grpc_message with gzip-compressed payload and cflag = 1.
 * Caller frees with grpc_message_free.
 * Returns nullptr on failure.
 */
GRPC_API GRPC_NODISCARD grpc_message *grpc_message_compress_gzip(const grpc_message *msg);

/**
 * Decompresses a message (if msg->cflag == 1) using gzip and returns a new
 * grpc_message with uncompressed payload and cflag = 0.
 * If msg->cflag == 0, returns a duplicate of msg.
 * Caller frees with grpc_message_free.
 * Returns nullptr on decompression failure.
 */
GRPC_API GRPC_NODISCARD grpc_message *grpc_message_decompress_gzip(const grpc_message *msg);
