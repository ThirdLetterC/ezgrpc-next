/* grpc_compression.c - message compression codecs: gzip, deflate, zstd and snappy
 *
 * Each codec is compiled in only when its library is enabled (GRPC_ENABLE_ZLIB, _ZSTD,
 * _SNAPPY); otherwise the functions report GRPC_ERR_UNSUPPORTED (or failure) for it. All output
 * buffers come from the grpc allocator and are released with grpc_free(), never libc free().
 *
 * Decompression is bounded: grpc_decompress_limited() grows its output step by step and stops
 * with GRPC_ERR_LIMIT as soon as more than max_output bytes would be produced, so a small
 * compressed request cannot expand into an unbounded allocation. The server uses it with
 * max_message_size when parsing requests. Compression of messages writes straight into a
 * grpc_message sized by the codec's worst-case bound, avoiding a copy.
 */
#include <stdlib.h>
#include <string.h>
#include "core.h"
#include "grpc_compression.h"
#include "grpc_mem.h"

#if defined(GRPC_ENABLE_ZLIB)
#define ZLIB_CONST /* next_in is const: zlib never writes the input */
#include <zlib.h>
#endif
#if defined(GRPC_ENABLE_ZSTD)
#include <zstd.h>
#include <zstd_errors.h>
#endif
#if defined(GRPC_ENABLE_SNAPPY)
#include <snappy-c.h>
#endif

/* Compresses or decompresses with zlib, in gzip (RFC 1952) or zlib-wrapped deflate (RFC 1950)
 * framing depending on gzip. Compression sizes the buffer with deflateBound() and finishes in
 * one deflate(Z_FINISH) in practice. Decompression starts at twice the input (at least 256
 * bytes), doubles as needed and caps the buffer at max_output + 1 so that exceeding max_output
 * is detected and reported as GRPC_ERR_LIMIT. zlib counts sizes in uInt, so inputs and buffers
 * over UINT_MAX are rejected. Returns GRPC_OK with *out (grpc_free()) and *out_len set,
 * GRPC_ERR_INVALID (bad level or size), GRPC_ERR_NOMEM, GRPC_ERR_CODEC (corrupt or truncated
 * data, or no progress), or GRPC_ERR_UNSUPPORTED without zlib. Leaves *out untouched on failure.
 */
static grpc_err zlib_codec(bool compress, bool gzip, int level, const uint8_t *in, size_t in_len,
                           uint8_t **out, size_t *out_len, size_t max_output) {
#if defined(GRPC_ENABLE_ZLIB)
  if (in_len > UINT_MAX)
    return GRPC_ERR_INVALID;
  z_stream stream = {0};
  int window = gzip ? 15 + 16 : 15;
  int rc;
  if (compress) {
    if (level == GRPC_COMPRESSION_LEVEL_DEFAULT)
      level = Z_DEFAULT_COMPRESSION;
    if (level < Z_DEFAULT_COMPRESSION || level > Z_BEST_COMPRESSION)
      return GRPC_ERR_INVALID;
    rc = deflateInit2(&stream, level, Z_DEFLATED, window, 8, Z_DEFAULT_STRATEGY);
  } else {
    rc = inflateInit2(&stream, window);
  }
  if (rc != Z_OK)
    return GRPC_ERR_CODEC;
  size_t cap = compress ? (size_t)deflateBound(&stream, (uLong)in_len)
                        : (in_len < 256            ? 256
                           : in_len > UINT_MAX / 2 ? UINT_MAX
                                                   : in_len * 2);
  size_t max_cap = !compress && max_output < UINT_MAX ? max_output + 1 : UINT_MAX;
  if (!compress && cap > max_cap)
    cap = max_cap;
  if ((compress && cap < in_len) || cap > UINT_MAX) {
    (compress ? deflateEnd : inflateEnd)(&stream);
    return GRPC_ERR_INVALID;
  }
  uint8_t *buf = grpc_malloc(cap ? cap : 1);
  if (!buf) {
    (compress ? deflateEnd : inflateEnd)(&stream);
    return GRPC_ERR_NOMEM;
  }
  stream.next_in = (const Bytef *)in;
  stream.avail_in = (uInt)in_len;
  int status = Z_OK;
  do {
    stream.next_out = buf + stream.total_out;
    stream.avail_out = (uInt)(cap - stream.total_out);
    uLong before_in = stream.total_in, before_out = stream.total_out;
    status = compress ? deflate(&stream, Z_FINISH) : inflate(&stream, Z_NO_FLUSH);
    if (!compress && stream.total_out > max_output) {
      grpc_free(buf);
      inflateEnd(&stream);
      return GRPC_ERR_LIMIT;
    }
    if (status == Z_STREAM_END && !compress && stream.avail_in > 0) {
      /* more input after the end of the stream: further members of a multi-member gzip
       * stream (RFC 1952) decode into the same output, under the same limit; anything else is
       * corrupt input, and a zlib (deflate) stream has a single member */
      uLong total_in = stream.total_in, total_out = stream.total_out;
      if (!gzip || inflateReset(&stream) != Z_OK) {
        grpc_free(buf);
        inflateEnd(&stream);
        return GRPC_ERR_CODEC;
      }
      /* inflateReset() zeroes the totals this loop uses as the output position */
      stream.total_in = total_in;
      stream.total_out = total_out;
      continue;
    }
    if (status == Z_STREAM_END)
      break;
    if (status != Z_OK || (stream.total_in == before_in && stream.total_out == before_out)) {
      grpc_free(buf);
      (compress ? deflateEnd : inflateEnd)(&stream);
      return GRPC_ERR_CODEC;
    }
    if (stream.total_out == cap) {
      size_t next_cap = cap > SIZE_MAX / 2 ? 0 : cap * 2;
      if (next_cap > max_cap)
        next_cap = max_cap;
      if (next_cap <= cap || next_cap > UINT_MAX) {
        grpc_free(buf);
        (compress ? deflateEnd : inflateEnd)(&stream);
        return GRPC_ERR_CODEC;
      }
      uint8_t *next = grpc_realloc(buf, next_cap);
      if (!next) {
        grpc_free(buf);
        (compress ? deflateEnd : inflateEnd)(&stream);
        return GRPC_ERR_NOMEM;
      }
      buf = next;
      cap = next_cap;
    }
  } while (true);
  *out_len = (size_t)stream.total_out;
  *out = buf;
  (compress ? deflateEnd : inflateEnd)(&stream);
  return GRPC_OK;
#else
  (void)compress;
  (void)gzip;
  (void)level;
  (void)in;
  (void)in_len;
  (void)out;
  (void)out_len;
  (void)max_output;
  return GRPC_ERR_UNSUPPORTED;
#endif
}

/* Compresses in one shot into caller-owned storage of out_cap bytes, used by
 * grpc_message_compress() to avoid a temporary codec buffer followed by a payload copy; out_cap
 * should come from compression_bound(). Returns 0 with *out_len set on success, 1 for an invalid
 * level or size, 2 if zlib cannot be initialized, 4 if the codec fails (including output that
 * does not fit), and -1 for an unknown or compiled-out algorithm. */
static int compress_into(grpc_compression_algorithm algorithm, int level, const uint8_t *in,
                         size_t in_len, uint8_t *out, size_t out_cap, size_t *out_len) {
  if (algorithm == GRPC_COMPRESS_GZIP || algorithm == GRPC_COMPRESS_DEFLATE) {
#if defined(GRPC_ENABLE_ZLIB)
    if (in_len > UINT_MAX || out_cap > UINT_MAX)
      return 1;
    if (level == GRPC_COMPRESSION_LEVEL_DEFAULT)
      level = Z_DEFAULT_COMPRESSION;
    if (level < Z_DEFAULT_COMPRESSION || level > Z_BEST_COMPRESSION)
      return 1;
    z_stream stream = {0};
    int window = algorithm == GRPC_COMPRESS_GZIP ? 15 + 16 : 15;
    if (deflateInit2(&stream, level, Z_DEFLATED, window, 8, Z_DEFAULT_STRATEGY) != Z_OK)
      return 2;
    stream.next_in = (const Bytef *)in;
    stream.avail_in = (uInt)in_len;
    stream.next_out = out;
    stream.avail_out = (uInt)out_cap;
    int status = deflate(&stream, Z_FINISH);
    if (status == Z_STREAM_END)
      *out_len = (size_t)stream.total_out;
    deflateEnd(&stream);
    return status == Z_STREAM_END ? 0 : 4;
#else
    (void)level;
    (void)in;
    (void)in_len;
    (void)out;
    (void)out_cap;
    (void)out_len;
    return -1;
#endif
  }
  if (algorithm == GRPC_COMPRESS_ZSTD) {
#if defined(GRPC_ENABLE_ZSTD)
    if (level == GRPC_COMPRESSION_LEVEL_DEFAULT)
      level = ZSTD_CLEVEL_DEFAULT;
    if (level < ZSTD_minCLevel() || level > ZSTD_maxCLevel())
      return 1;
    size_t n = ZSTD_compress(out, out_cap, in, in_len, level);
    if (ZSTD_isError(n))
      return 4;
    *out_len = n;
    return 0;
#else
    (void)level;
    (void)in;
    (void)in_len;
    (void)out;
    (void)out_cap;
    (void)out_len;
    return -1;
#endif
  }
  if (algorithm == GRPC_COMPRESS_SNAPPY) {
#if defined(GRPC_ENABLE_SNAPPY)
    if (level != GRPC_COMPRESSION_LEVEL_DEFAULT)
      return 1;
    size_t n = out_cap;
    if (snappy_compress((const char *)in, in_len, (char *)out, &n) != SNAPPY_OK)
      return 4;
    *out_len = n;
    return 0;
#else
    (void)level;
    (void)in;
    (void)in_len;
    (void)out;
    (void)out_cap;
    (void)out_len;
    return -1;
#endif
  }
  return -1;
}

/* Returns the worst-case compressed size of len bytes for algorithm and level, or 0 when the
 * algorithm is unknown or compiled out, the level is invalid for it (snappy only accepts the
 * default), or the bound does not fit zlib's uInt. */
static size_t compression_bound(grpc_compression_algorithm algorithm, int level, size_t len) {
  (void)level; /* unused when every codec is compiled out */
  (void)len;
  switch (algorithm) {
  case GRPC_COMPRESS_GZIP:
  case GRPC_COMPRESS_DEFLATE:
#if defined(GRPC_ENABLE_ZLIB)
    if (len > UINT_MAX || level < Z_DEFAULT_COMPRESSION || level > Z_BEST_COMPRESSION)
      return 0;
    {
      z_stream stream = {0};
      int window = algorithm == GRPC_COMPRESS_GZIP ? 15 + 16 : 15;
      if (level == GRPC_COMPRESSION_LEVEL_DEFAULT)
        level = Z_DEFAULT_COMPRESSION;
      if (deflateInit2(&stream, level, Z_DEFLATED, window, 8, Z_DEFAULT_STRATEGY) != Z_OK)
        return 0;
      uLong bound = deflateBound(&stream, (uLong)len);
      deflateEnd(&stream);
      return bound <= UINT_MAX ? (size_t)bound : 0;
    }
#else
    return 0;
#endif
  case GRPC_COMPRESS_ZSTD:
#if defined(GRPC_ENABLE_ZSTD)
    if (level == GRPC_COMPRESSION_LEVEL_DEFAULT)
      level = ZSTD_CLEVEL_DEFAULT;
    if (level < ZSTD_minCLevel() || level > ZSTD_maxCLevel())
      return 0;
    return ZSTD_compressBound(len);
#else
    return 0;
#endif
  case GRPC_COMPRESS_SNAPPY:
#if defined(GRPC_ENABLE_SNAPPY)
    return level == GRPC_COMPRESSION_LEVEL_DEFAULT ? snappy_max_compressed_length(len) : 0;
#else
    return 0;
#endif
  default:
    return 0;
  }
}

/* Rejects nullptr out pointers and a nullptr input with a nonzero length; a nullptr empty input
 * is replaced by a dummy byte so the codecs get a valid pointer. zstd and snappy compress into a
 * buffer of their worst-case bound; gzip and deflate go through zlib_codec(). The default level
 * maps to each codec's own default; snappy accepts no other level. */
GRPC_API grpc_err grpc_compress(grpc_compression_algorithm algorithm, int level, const uint8_t *in,
                                size_t in_len, uint8_t **out, size_t *out_len) {
  if ((!in && in_len) || !out || !out_len)
    return GRPC_ERR_INVALID;
  static const uint8_t empty_input = 0;
  if (!in)
    in = &empty_input;
  *out = nullptr;
  *out_len = 0;
  switch (algorithm) {
  case GRPC_COMPRESS_GZIP:
  case GRPC_COMPRESS_DEFLATE:
    return zlib_codec(true, algorithm == GRPC_COMPRESS_GZIP, level, in, in_len, out, out_len,
                      SIZE_MAX);
  case GRPC_COMPRESS_ZSTD:
#if defined(GRPC_ENABLE_ZSTD)
  {
    if (level == GRPC_COMPRESSION_LEVEL_DEFAULT)
      level = ZSTD_CLEVEL_DEFAULT;
    if (level < ZSTD_minCLevel() || level > ZSTD_maxCLevel())
      return GRPC_ERR_INVALID;
    size_t cap = ZSTD_compressBound(in_len);
    uint8_t *buf = grpc_malloc(cap ? cap : 1);
    if (!buf)
      return GRPC_ERR_NOMEM;
    size_t n = ZSTD_compress(buf, cap, in, in_len, level);
    if (ZSTD_isError(n)) {
      grpc_free(buf);
      return GRPC_ERR_CODEC;
    }
    *out = buf;
    *out_len = n;
    return GRPC_OK;
  }
#else
    return GRPC_ERR_UNSUPPORTED;
#endif
  case GRPC_COMPRESS_SNAPPY:
#if defined(GRPC_ENABLE_SNAPPY)
  {
    if (level != GRPC_COMPRESSION_LEVEL_DEFAULT)
      return GRPC_ERR_INVALID;
    size_t cap = snappy_max_compressed_length(in_len);
    uint8_t *buf = grpc_malloc(cap ? cap : 1);
    if (!buf)
      return GRPC_ERR_NOMEM;
    size_t n = cap;
    if (snappy_compress((const char *)in, in_len, (char *)buf, &n) != SNAPPY_OK) {
      grpc_free(buf);
      return GRPC_ERR_CODEC;
    }
    *out = buf;
    *out_len = n;
    return GRPC_OK;
  }
#else
    return GRPC_ERR_UNSUPPORTED;
#endif
  default:
    return GRPC_ERR_UNSUPPORTED;
  }
}

/* zlib codecs go through zlib_codec(). zstd decompresses as a stream into a buffer that starts at
 * 4 KiB (or max_output + 1 if smaller) and doubles up to max_output + 1, so frames without a
 * declared content size are handled and the limit is checked after every step; truncated input
 * or a call that makes no progress is GRPC_ERR_CODEC. snappy reads the uncompressed length from
 * the header and checks it against max_output before allocating. */
GRPC_API grpc_err grpc_decompress_limited(grpc_compression_algorithm algorithm, const uint8_t *in,
                                          size_t in_len, uint8_t **out, size_t *out_len,
                                          size_t max_output) {
  if ((!in && in_len) || !out || !out_len)
    return GRPC_ERR_INVALID;
  static const uint8_t empty_input = 0;
  if (!in)
    in = &empty_input;
  *out = nullptr;
  *out_len = 0;
  if (algorithm == GRPC_COMPRESS_GZIP || algorithm == GRPC_COMPRESS_DEFLATE)
    return zlib_codec(false, algorithm == GRPC_COMPRESS_GZIP, 0, in, in_len, out, out_len,
                      max_output);
  if (algorithm == GRPC_COMPRESS_ZSTD) {
#if defined(GRPC_ENABLE_ZSTD)
    ZSTD_DCtx *ctx = ZSTD_createDCtx();
    if (!ctx)
      return GRPC_ERR_NOMEM;
    /* The decoder allocates the window a frame header asks for (up to 2^27 by default) before
     * producing output, so a few bytes could make it allocate 128 MiB whatever max_output is.
     * Bound the window by the output limit, but never below 2^21 (the window zstd uses at its
     * default level when the size is unknown, so ordinary streams still decode). */
    int window_log = 21;
    while (window_log < 27 && ((size_t)1 << window_log) < max_output)
      window_log++;
    if (ZSTD_isError(ZSTD_DCtx_setParameter(ctx, ZSTD_d_windowLogMax, window_log))) {
      ZSTD_freeDCtx(ctx);
      return GRPC_ERR_CODEC;
    }
    size_t max_cap = max_output < SIZE_MAX ? max_output + 1 : SIZE_MAX;
    size_t cap = max_cap < 4096 ? max_cap : 4096, used = 0;
    uint8_t *buf = grpc_malloc(cap);
    if (!buf) {
      ZSTD_freeDCtx(ctx);
      return GRPC_ERR_NOMEM;
    }
    ZSTD_inBuffer input = {in, in_len, 0};
    size_t remaining = 1;
    while (remaining != 0 || input.pos < input.size) {
      if (used == cap) {
        if (cap > SIZE_MAX / 2) {
          grpc_free(buf);
          ZSTD_freeDCtx(ctx);
          return GRPC_ERR_CODEC;
        }
        size_t next_cap = cap * 2;
        if (next_cap > max_cap)
          next_cap = max_cap;
        uint8_t *next = grpc_realloc(buf, next_cap);
        if (!next) {
          grpc_free(buf);
          ZSTD_freeDCtx(ctx);
          return GRPC_ERR_NOMEM;
        }
        buf = next;
        cap = next_cap;
      }
      ZSTD_outBuffer output = {buf + used, cap - used, 0};
      size_t before = input.pos;
      remaining = ZSTD_decompressStream(ctx, &output, &input);
      if (ZSTD_isError(remaining) &&
          ZSTD_getErrorCode(remaining) == ZSTD_error_frameParameter_windowTooLarge) {
        grpc_free(buf);
        ZSTD_freeDCtx(ctx);
        return GRPC_ERR_LIMIT; /* the frame needs more memory than the limit allows */
      }
      if (ZSTD_isError(remaining) || (before == input.pos && output.pos == 0)) {
        grpc_free(buf);
        ZSTD_freeDCtx(ctx);
        return GRPC_ERR_CODEC;
      }
      used += output.pos;
      if (used > max_output) {
        grpc_free(buf);
        ZSTD_freeDCtx(ctx);
        return GRPC_ERR_LIMIT;
      }
      if (input.pos == input.size && remaining != 0) {
        grpc_free(buf);
        ZSTD_freeDCtx(ctx);
        return GRPC_ERR_CODEC;
      }
    }
    ZSTD_freeDCtx(ctx);
    *out = buf;
    *out_len = used;
    return GRPC_OK;
#else
    return GRPC_ERR_UNSUPPORTED;
#endif
  }
  if (algorithm == GRPC_COMPRESS_SNAPPY) {
#if defined(GRPC_ENABLE_SNAPPY)
    size_t cap;
    if (snappy_uncompressed_length((const char *)in, in_len, &cap) != SNAPPY_OK)
      return GRPC_ERR_CODEC;
    if (cap > max_output)
      return GRPC_ERR_LIMIT;
    uint8_t *buf = grpc_malloc(cap ? cap : 1);
    if (!buf)
      return GRPC_ERR_NOMEM;
    if (snappy_uncompress((const char *)in, in_len, (char *)buf, &cap) != SNAPPY_OK) {
      grpc_free(buf);
      return GRPC_ERR_CODEC;
    }
    *out = buf;
    *out_len = cap;
    return GRPC_OK;
#else
    return GRPC_ERR_UNSUPPORTED;
#endif
  }
  return GRPC_ERR_UNSUPPORTED;
}

/* grpc_decompress_limited() without an output limit. */
GRPC_API grpc_err grpc_decompress(grpc_compression_algorithm algorithm, const uint8_t *in,
                                  size_t in_len, uint8_t **out, size_t *out_len) {
  return grpc_decompress_limited(algorithm, in, in_len, out, out_len, SIZE_MAX);
}

/* gzip at the default level through grpc_compress(). */
GRPC_API grpc_err grpc_compress_gzip(const uint8_t *in, size_t len, uint8_t **out,
                                     size_t *out_len) {
  return grpc_compress(GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT, in, len, out, out_len);
}
/* Unbounded gzip decompression through grpc_decompress(). */
GRPC_API grpc_err grpc_decompress_gzip(const uint8_t *in, size_t len, uint8_t **out,
                                       size_t *out_len) {
  return grpc_decompress(GRPC_COMPRESS_GZIP, in, len, out, out_len);
}

/* Allocates the result at the codec's worst-case bound and compresses straight into it with
 * compress_into(), then shrinks len to the compressed size (the allocation keeps its size). The
 * result has cflag 1 and records algorithm and level. Returns nullptr for an unsupported
 * algorithm or level, a bound over UINT32_MAX, or any failure. */
GRPC_API grpc_message *grpc_message_compress(const grpc_message *msg,
                                             grpc_compression_algorithm algorithm, int level) {
  if (!msg)
    return nullptr;
  size_t bound = compression_bound(algorithm, level, msg->len);
  if (bound == 0 || bound > UINT32_MAX)
    return nullptr;
  grpc_message *result = grpc_message_alloc(bound);
  if (result == nullptr)
    return nullptr;
  size_t len = 0;
  if (compress_into(algorithm, level, msg->data, msg->len, result->data, bound, &len) != 0) {
    grpc_message_free(result);
    return nullptr;
  }
  result->len = (uint32_t)len;
  result->cflag = 1;
  result->compression = algorithm;
  result->compression_level = level;
  return result;
}
/* Copies an uncompressed message (cflag 0); otherwise decompresses without an output limit into
 * a new message with cflag 0. Returns nullptr on failure. */
GRPC_API grpc_message *grpc_message_decompress(const grpc_message *msg,
                                               grpc_compression_algorithm algorithm) {
  if (!msg)
    return nullptr;
  if (!msg->cflag)
    return message_create(0, msg->data, msg->len);
  uint8_t *data = nullptr;
  size_t len = 0;
  if (grpc_decompress(algorithm, msg->data, msg->len, &data, &len) != 0)
    return nullptr;
  grpc_message *result = message_create(0, data, len);
  grpc_free(data);
  return result;
}
/* grpc_message_compress() with gzip at the default level. */
GRPC_API grpc_message *grpc_message_compress_gzip(const grpc_message *msg) {
  return grpc_message_compress(msg, GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT);
}
/* grpc_message_decompress() with gzip. */
GRPC_API grpc_message *grpc_message_decompress_gzip(const grpc_message *msg) {
  return grpc_message_decompress(msg, GRPC_COMPRESS_GZIP);
}
