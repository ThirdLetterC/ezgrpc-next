#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "grpc.h"
#include "test.h"

void setUp(void) {}
void tearDown(void) {}

#if defined(GRPC_ENABLE_ZLIB)
void test_gzip_compression_roundtrip(void) {
  const char *sample = "Hello World! This is a test message to verify RFC 1952 gzip compression "
                       "and decompression with zlib.";
  size_t sample_len = strlen(sample);

  uint8_t *compressed = nullptr;
  size_t comp_len = 0;
  int rc = grpc_compress_gzip((const uint8_t *)sample, sample_len, &compressed, &comp_len);
  TEST_ASSERT_EQUAL_INT(0, rc);
  TEST_ASSERT_NOT_NULL(compressed);
  TEST_ASSERT_TRUE(comp_len > 0);

  // Gzip magic header: 0x1f, 0x8b
  TEST_ASSERT_EQUAL_HEX8(0x1f, compressed[0]);
  TEST_ASSERT_EQUAL_HEX8(0x8b, compressed[1]);

  uint8_t *decompressed = nullptr;
  size_t decomp_len = 0;
  rc = grpc_decompress_gzip(compressed, comp_len, &decompressed, &decomp_len);
  TEST_ASSERT_EQUAL_INT(0, rc);
  TEST_ASSERT_NOT_NULL(decompressed);
  TEST_ASSERT_EQUAL_UINT64(sample_len, decomp_len);
  TEST_ASSERT_EQUAL_MEMORY(sample, decompressed, sample_len);

  grpc_free(compressed);
  grpc_free(decompressed);
}

void test_message_compress_decompress(void) {
  const char *msg_text = "Test grpc_message compression wrappers";
  grpc_message *orig = grpc_message_new(msg_text, strlen(msg_text));
  TEST_ASSERT_NOT_NULL(orig);
  TEST_ASSERT_EQUAL_UINT8(0, orig->cflag);

  grpc_message *comp = grpc_message_compress_gzip(orig);
  TEST_ASSERT_NOT_NULL(comp);
  TEST_ASSERT_EQUAL_UINT8(1, comp->cflag);

  grpc_message *decomp = grpc_message_decompress_gzip(comp);
  TEST_ASSERT_NOT_NULL(decomp);
  TEST_ASSERT_EQUAL_UINT8(0, decomp->cflag);
  TEST_ASSERT_EQUAL_UINT32(orig->len, decomp->len);
  TEST_ASSERT_EQUAL_MEMORY(orig->data, decomp->data, orig->len);

  grpc_message_free(orig);
  grpc_message_free(comp);
  grpc_message_free(decomp);
}
#endif

#if defined(GRPC_ENABLE_ZLIB) || defined(GRPC_ENABLE_ZSTD) || defined(GRPC_ENABLE_SNAPPY)
void test_configurable_codecs_roundtrip(void) {
  const char sample[] = "codec selection and level configuration round trip";
  const grpc_compression_algorithm algorithms[] = {
#if defined(GRPC_ENABLE_ZLIB)
      GRPC_COMPRESS_GZIP,
      GRPC_COMPRESS_DEFLATE,
#endif
#if defined(GRPC_ENABLE_ZSTD)
      GRPC_COMPRESS_ZSTD,
#endif
#if defined(GRPC_ENABLE_SNAPPY)
      GRPC_COMPRESS_SNAPPY,
#endif
  };
  for (size_t i = 0; i < sizeof(algorithms) / sizeof(algorithms[0]); ++i) {
    int level = algorithms[i] == GRPC_COMPRESS_SNAPPY ? GRPC_COMPRESSION_LEVEL_DEFAULT
                : algorithms[i] == GRPC_COMPRESS_ZSTD ? 3
                                                      : 6;
    uint8_t *packed = nullptr, *unpacked = nullptr;
    size_t packed_len = 0, unpacked_len = 0;
    TEST_ASSERT_EQUAL_INT(0, grpc_compress(algorithms[i], level, (const uint8_t *)sample,
                                           sizeof(sample) - 1, &packed, &packed_len));
    TEST_ASSERT_NOT_NULL(packed);
    TEST_ASSERT_EQUAL_INT(
        0, grpc_decompress(algorithms[i], packed, packed_len, &unpacked, &unpacked_len));
    TEST_ASSERT_EQUAL_UINT64(sizeof(sample) - 1, unpacked_len);
    TEST_ASSERT_EQUAL_MEMORY(sample, unpacked, unpacked_len);
    grpc_free(packed);
    grpc_free(unpacked);
  }
}
#endif

#if defined(GRPC_ENABLE_ZLIB) || defined(GRPC_ENABLE_ZSTD) || defined(GRPC_ENABLE_SNAPPY)
void test_decompression_limits(void) {
  const grpc_compression_algorithm algorithms[] = {
#if defined(GRPC_ENABLE_ZLIB)
      GRPC_COMPRESS_GZIP,
      GRPC_COMPRESS_DEFLATE,
#endif
#if defined(GRPC_ENABLE_ZSTD)
      GRPC_COMPRESS_ZSTD,
#endif
#if defined(GRPC_ENABLE_SNAPPY)
      GRPC_COMPRESS_SNAPPY,
#endif
  };
  uint8_t input[65536];
  memset(input, 'x', sizeof(input));
  const size_t limits[] = {
      0, 1, 255, 256, 4096, sizeof(input) - 1, sizeof(input), sizeof(input) + 1};
  for (size_t codec = 0; codec < sizeof(algorithms) / sizeof(algorithms[0]); codec++) {
    uint8_t *packed = nullptr;
    size_t packed_len = 0;
    TEST_ASSERT_EQUAL_INT(0, grpc_compress(algorithms[codec], GRPC_COMPRESSION_LEVEL_DEFAULT, input,
                                           sizeof(input), &packed, &packed_len));
    TEST_ASSERT_TRUE(packed_len < sizeof(input) / 2);
    for (size_t i = 0; i < sizeof(limits) / sizeof(limits[0]); i++) {
      uint8_t *output = nullptr;
      size_t size = 123;
      grpc_err rc =
          grpc_decompress_limited(algorithms[codec], packed, packed_len, &output, &size, limits[i]);
      if (limits[i] < sizeof(input)) {
        TEST_ASSERT_EQUAL_INT(GRPC_ERR_LIMIT, rc);
        TEST_ASSERT_NULL(output);
        TEST_ASSERT_EQUAL_UINT64(0, size);
      } else {
        TEST_ASSERT_EQUAL_INT(GRPC_OK, rc);
        TEST_ASSERT_EQUAL_UINT64(sizeof(input), size);
        TEST_ASSERT_EQUAL_MEMORY(input, output, size);
      }
      grpc_free(output);
    }
    uint8_t *output = nullptr;
    size_t size = 123;
    TEST_ASSERT_NOT_EQUAL(0, grpc_decompress_limited(algorithms[codec], packed, packed_len - 1,
                                                     &output, &size, sizeof(input)));
    TEST_ASSERT_NULL(output);
    TEST_ASSERT_EQUAL_UINT64(0, size);
    grpc_free(packed);
    TEST_ASSERT_EQUAL_INT(0, grpc_compress(algorithms[codec], GRPC_COMPRESSION_LEVEL_DEFAULT, input,
                                           0, &packed, &packed_len));
    TEST_ASSERT_EQUAL_INT(
        0, grpc_decompress_limited(algorithms[codec], packed, packed_len, &output, &size, 0));
    TEST_ASSERT_EQUAL_UINT64(0, size);
    grpc_free(output);
    grpc_free(packed);
    const uint8_t invalid[] = {0xff, 0xff, 0xff};
    TEST_ASSERT_NOT_EQUAL(0, grpc_decompress_limited(algorithms[codec], invalid, sizeof(invalid),
                                                     &output, &size, sizeof(input)));
    TEST_ASSERT_NULL(output);
    TEST_ASSERT_EQUAL_UINT64(0, size);
  }
}
#endif

void test_message_all_codecs_and_errors(void) {
  const char sample[] = "message compression across all enabled codecs and error paths";
  grpc_message *orig = grpc_message_new(sample, sizeof(sample) - 1);
  TEST_ASSERT_NOT_NULL(orig);

  /* Decompressing an uncompressed message duplicates it */
  grpc_message *dup = grpc_message_decompress(orig, GRPC_COMPRESS_GZIP);
  TEST_ASSERT_NOT_NULL(dup);
  TEST_ASSERT_TRUE(dup != orig);
  TEST_ASSERT_EQUAL_UINT8(0, dup->cflag);
  TEST_ASSERT_EQUAL_UINT32(orig->len, dup->len);
  TEST_ASSERT_EQUAL_MEMORY(orig->data, dup->data, orig->len);
  grpc_message_free(dup);

#if defined(GRPC_ENABLE_ZLIB) || defined(GRPC_ENABLE_ZSTD) || defined(GRPC_ENABLE_SNAPPY)
  /* only built with at least one codec: an empty initializer would be a zero-length array */
  const grpc_compression_algorithm algs[] = {
#if defined(GRPC_ENABLE_ZLIB)
      GRPC_COMPRESS_GZIP,
      GRPC_COMPRESS_DEFLATE,
#endif
#if defined(GRPC_ENABLE_ZSTD)
      GRPC_COMPRESS_ZSTD,
#endif
#if defined(GRPC_ENABLE_SNAPPY)
      GRPC_COMPRESS_SNAPPY,
#endif
  };
  for (size_t i = 0; i < sizeof(algs) / sizeof(algs[0]); i++) {
    grpc_message *c = grpc_message_compress(orig, algs[i], GRPC_COMPRESSION_LEVEL_DEFAULT);
    TEST_ASSERT_NOT_NULL(c);
    grpc_message *d = grpc_message_decompress(c, algs[i]);
    TEST_ASSERT_NOT_NULL(d);
    TEST_ASSERT_EQUAL_UINT32(orig->len, d->len);
    TEST_ASSERT_EQUAL_MEMORY(orig->data, d->data, orig->len);
    grpc_message_free(c);
    grpc_message_free(d);
  }
#endif
  /* codecs left out of the build are reported as unsupported */
  uint8_t *out = nullptr;
  size_t out_len = 0;
#if !defined(GRPC_ENABLE_ZLIB)
  TEST_ASSERT_NULL(grpc_message_compress(orig, GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT));
  TEST_ASSERT_NULL(
      grpc_message_compress(orig, GRPC_COMPRESS_DEFLATE, GRPC_COMPRESSION_LEVEL_DEFAULT));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_UNSUPPORTED,
                        grpc_compress(GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                      (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_NULL(out);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_UNSUPPORTED,
      grpc_decompress(GRPC_COMPRESS_DEFLATE, (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_NULL(out);
#endif
#if !defined(GRPC_ENABLE_ZSTD)
  TEST_ASSERT_NULL(grpc_message_compress(orig, GRPC_COMPRESS_ZSTD, GRPC_COMPRESSION_LEVEL_DEFAULT));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_UNSUPPORTED,
                        grpc_compress(GRPC_COMPRESS_ZSTD, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                      (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_NULL(out);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_UNSUPPORTED,
      grpc_decompress(GRPC_COMPRESS_ZSTD, (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_NULL(out);
#endif
#if !defined(GRPC_ENABLE_SNAPPY)
  TEST_ASSERT_NULL(
      grpc_message_compress(orig, GRPC_COMPRESS_SNAPPY, GRPC_COMPRESSION_LEVEL_DEFAULT));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_UNSUPPORTED,
                        grpc_compress(GRPC_COMPRESS_SNAPPY, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                      (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_NULL(out);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_UNSUPPORTED,
      grpc_decompress(GRPC_COMPRESS_SNAPPY, (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_NULL(out);
#endif
#if defined(GRPC_ENABLE_ZSTD)
  grpc_message *cz = grpc_message_compress(orig, GRPC_COMPRESS_ZSTD, 3);
  TEST_ASSERT_NOT_NULL(cz);
  grpc_message_free(cz);
  TEST_ASSERT_NULL(grpc_message_compress(orig, GRPC_COMPRESS_ZSTD, 9999));
#endif
#if defined(GRPC_ENABLE_SNAPPY)
  TEST_ASSERT_NULL(grpc_message_compress(orig, GRPC_COMPRESS_SNAPPY, 3));
#endif
  TEST_ASSERT_NULL(grpc_message_compress(orig, (grpc_compression_algorithm)99, -1));
  TEST_ASSERT_NULL(grpc_message_compress(nullptr, GRPC_COMPRESS_GZIP, -1));
  TEST_ASSERT_NULL(grpc_message_decompress(nullptr, GRPC_COMPRESS_GZIP));

#if defined(GRPC_ENABLE_ZLIB)
  TEST_ASSERT_NULL(grpc_message_compress(orig, GRPC_COMPRESS_GZIP, 99));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_compress(GRPC_COMPRESS_GZIP, 99,
                                                        (const uint8_t *)"a", 1, &out, &out_len));
#endif
#if defined(GRPC_ENABLE_ZSTD)
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_compress(GRPC_COMPRESS_ZSTD, 9999,
                                                        (const uint8_t *)"a", 1, &out, &out_len));
#endif
#if defined(GRPC_ENABLE_SNAPPY)
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_compress(GRPC_COMPRESS_SNAPPY, 3,
                                                        (const uint8_t *)"a", 1, &out, &out_len));
#endif
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_compress(GRPC_COMPRESS_GZIP, -1, nullptr, 5, &out, &out_len));
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_UNSUPPORTED,
      grpc_compress((grpc_compression_algorithm)99, -1, (const uint8_t *)"a", 1, &out, &out_len));
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_UNSUPPORTED,
      grpc_decompress((grpc_compression_algorithm)99, (const uint8_t *)"a", 1, &out, &out_len));
  grpc_message_free(orig);
}

#if defined(GRPC_ENABLE_ZLIB)
/* Bytes after the end of a gzip/deflate stream: more gzip members are decoded (RFC 1952), anything
 * else is rejected. */
void test_zlib_trailing_data_and_gzip_members(void) {
  uint8_t *a = nullptr, *b = nullptr, *out = nullptr;
  size_t alen = 0, blen = 0, out_len = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_compress(GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                               (const uint8_t *)"first-", 6, &a, &alen));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_compress(GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                               (const uint8_t *)"second", 6, &b, &blen));
  uint8_t *both = grpc_malloc(alen + blen + 4);
  TEST_ASSERT_NOT_NULL(both);
  memcpy(both, a, alen);
  memcpy(both + alen, b, blen);
  /* two members decode to both payloads */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_decompress_limited(GRPC_COMPRESS_GZIP, both, alen + blen,
                                                         &out, &out_len, 1024));
  TEST_ASSERT_EQUAL_UINT64(12, out_len);
  TEST_ASSERT_EQUAL_MEMORY("first-second", out, 12);
  grpc_free(out);
  out = nullptr;
  /* the limit covers all members together */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_LIMIT, grpc_decompress_limited(GRPC_COMPRESS_GZIP, both,
                                                                alen + blen, &out, &out_len, 8));
  /* garbage after a member is an error, not ignored */
  memcpy(both + alen, "XYZW", 4);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_CODEC, grpc_decompress_limited(GRPC_COMPRESS_GZIP, both, alen + 4,
                                                                &out, &out_len, 1024));
  grpc_free(a);
  grpc_free(b);
  /* a zlib (deflate) stream has a single member: trailing bytes are an error */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_compress(GRPC_COMPRESS_DEFLATE, GRPC_COMPRESSION_LEVEL_DEFAULT,
                                      (const uint8_t *)"only", 4, &a, &alen));
  memcpy(both, a, alen);
  memcpy(both + alen, "XY", 2);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_CODEC, grpc_decompress_limited(GRPC_COMPRESS_DEFLATE, both,
                                                                alen + 2, &out, &out_len, 1024));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_decompress_limited(GRPC_COMPRESS_DEFLATE, both, alen, &out, &out_len, 1024));
  TEST_ASSERT_EQUAL_MEMORY("only", out, 4);
  grpc_free(out);
  grpc_free(a);
  grpc_free(both);
}
#endif

#if defined(GRPC_ENABLE_ZSTD)
/* A zstd frame without a content size whose header asks for a 2^window_log window, holding one
 * raw block with the byte 'A'. */
static void zstd_frame(uint8_t out[10], int window_log) {
  const uint8_t frame[10] = {0x28,
                             0xB5,
                             0x2F,
                             0xFD,                              /* magic */
                             0x00,                              /* no content size, no checksum */
                             (uint8_t)((window_log - 10) << 3), /* window descriptor */
                             0x09,
                             0x00,
                             0x00, /* last block, raw, 1 byte */
                             'A'};
  memcpy(out, frame, sizeof(frame));
}

/* The decoder's window is bounded by the output limit: a 10-byte frame cannot make it allocate a
 * 128 MiB window whatever max_output is, while ordinary windows still decode. */
void test_zstd_window_bounded_by_limit(void) {
  uint8_t frame[10];
  uint8_t *out = nullptr;
  size_t out_len = 0;
  /* the default 2 MiB window of a stream of unknown size decodes under any limit */
  zstd_frame(frame, 21);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_decompress_limited(GRPC_COMPRESS_ZSTD, frame, sizeof(frame),
                                                         &out, &out_len, 100));
  TEST_ASSERT_EQUAL_UINT64(1, out_len);
  TEST_ASSERT_EQUAL_UINT8('A', out[0]);
  grpc_free(out);
  /* a window far beyond the limit is refused before it is allocated */
  zstd_frame(frame, 27);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_LIMIT,
      grpc_decompress_limited(GRPC_COMPRESS_ZSTD, frame, sizeof(frame), &out, &out_len, 4u << 20));
  TEST_ASSERT_NULL(out);
  zstd_frame(frame, 23);
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_LIMIT,
      grpc_decompress_limited(GRPC_COMPRESS_ZSTD, frame, sizeof(frame), &out, &out_len, 100));
  /* a limit that needs the window allows it */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_decompress_limited(GRPC_COMPRESS_ZSTD, frame, sizeof(frame),
                                                         &out, &out_len, 8u << 20));
  TEST_ASSERT_EQUAL_UINT64(1, out_len);
  grpc_free(out);
}
#endif

int main(void) {
  UNITY_BEGIN();
#if defined(GRPC_ENABLE_ZLIB)
  RUN_TEST(test_gzip_compression_roundtrip);
  RUN_TEST(test_message_compress_decompress);
#endif
#if defined(GRPC_ENABLE_ZLIB) || defined(GRPC_ENABLE_ZSTD) || defined(GRPC_ENABLE_SNAPPY)
  RUN_TEST(test_configurable_codecs_roundtrip);
  RUN_TEST(test_decompression_limits);
#endif
  RUN_TEST(test_message_all_codecs_and_errors);
#if defined(GRPC_ENABLE_ZSTD)
  RUN_TEST(test_zstd_window_bounded_by_limit);
#endif
#if defined(GRPC_ENABLE_ZLIB)
  RUN_TEST(test_zlib_trailing_data_and_gzip_members);
#endif
  return UNITY_END();
}
