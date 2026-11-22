#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "grpc.h"
#include "test.h"

void setUp(void) {}
void tearDown(void) {}

void test_metadata_base64_roundtrip(void) {
  const char *test_strings[] = {
      "", "f", "fo", "foo", "foob", "fooba", "foobar", "Hello, gRPC Binary Metadata World!",
  };

  for (size_t i = 0; i < sizeof(test_strings) / sizeof(test_strings[0]); i++) {
    const char *orig = test_strings[i];
    size_t orig_len = strlen(orig);

    size_t enc_len = 0;
    char *encoded = grpc_metadata_encode_bin((const uint8_t *)orig, orig_len, &enc_len);
    TEST_ASSERT_NOT_NULL(encoded);
    TEST_ASSERT_EQUAL_UINT64(enc_len, strlen(encoded));

    size_t dec_len = 0;
    uint8_t *decoded = grpc_metadata_decode_bin(encoded, enc_len, &dec_len);
    TEST_ASSERT_NOT_NULL(decoded);
    TEST_ASSERT_EQUAL_UINT64(orig_len, dec_len);
    if (orig_len > 0) {
      TEST_ASSERT_EQUAL_MEMORY(orig, decoded, orig_len);
    }

    grpc_free(encoded);
    grpc_free(decoded);
  }
}

void test_metadata_binary_random_bytes(void) {
  uint8_t raw[256];
  for (size_t i = 0; i < sizeof(raw); i++) {
    raw[i] = (uint8_t)(i & 0xFF);
  }

  size_t enc_len = 0;
  char *encoded = grpc_metadata_encode_bin(raw, sizeof(raw), &enc_len);
  TEST_ASSERT_NOT_NULL(encoded);

  size_t dec_len = 0;
  uint8_t *decoded = grpc_metadata_decode_bin(encoded, enc_len, &dec_len);
  TEST_ASSERT_NOT_NULL(decoded);
  TEST_ASSERT_EQUAL_UINT64(sizeof(raw), dec_len);
  TEST_ASSERT_EQUAL_MEMORY(raw, decoded, sizeof(raw));

  grpc_free(encoded);
  grpc_free(decoded);
}

/* Decodes `in` (its strlen) and checks the result is exactly `want_len` bytes of `want`. */
static void check_decode(const char *in, const char *want, size_t want_len) {
  size_t dec_len = 12345;
  uint8_t *decoded = grpc_metadata_decode_bin(in, strlen(in), &dec_len);
  TEST_ASSERT_NOT_NULL_MESSAGE(decoded, in);
  TEST_ASSERT_EQUAL_UINT64_MESSAGE(want_len, dec_len, in);
  if (want_len > 0)
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(want, decoded, want_len, in);
  grpc_free(decoded);
}

void test_metadata_encode_is_padded(void) {
  const struct {
    const char *in, *out;
  } cases[] = {{"f", "Zg=="}, {"fo", "Zm8="}, {"foo", "Zm9v"}, {"\xfb\xff", "+/8="}};
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    size_t enc_len = 0;
    char *enc =
        grpc_metadata_encode_bin((const uint8_t *)cases[i].in, strlen(cases[i].in), &enc_len);
    TEST_ASSERT_NOT_NULL(enc);
    TEST_ASSERT_EQUAL_STRING(cases[i].out, enc);
    TEST_ASSERT_EQUAL_UINT64(strlen(cases[i].out), enc_len);
    grpc_free(enc);
  }
}

/* padding is optional on decode (gRPC peers may omit it) */
void test_metadata_decode_unpadded(void) {
  check_decode("Zg", "f", 1);
  check_decode("Zm8", "fo", 2);
  check_decode("Zm9v", "foo", 3);
  check_decode("Zm9vYg", "foob", 4);
  check_decode("Zm9vYmE", "fooba", 5);
  check_decode("Zg=", "f", 1);
  check_decode("Zg==", "f", 1);
  check_decode("+/8", "\xfb\xff", 2);
  /* trailing padding, spaces and newlines are stripped */
  check_decode("Zm8= \n", "fo", 2);
  /* a lone final character carries only 6 bits: dropped */
  check_decode("Zm9vY", "foo", 3);
  check_decode("Z", "", 0);
}

/* characters outside the standard alphabet are skipped, not rejected */
void test_metadata_decode_skips_invalid(void) {
  check_decode("Zm 9v", "foo", 3);
  check_decode("Z!m@9#v", "foo", 3);
  check_decode("Zm9v\r\n\tYmFy", "foobar", 6);
  /* the URL-safe alphabet is not standard Base64: '-' and '_' are skipped too */
  check_decode("-_Zm9v", "foo", 3);
  /* nothing valid at all: an empty, non-null result */
  check_decode("!!!!", "", 0);
  check_decode("====", "", 0);
  /* an explicit length stops before the NUL */
  size_t dec_len = 0;
  uint8_t *decoded = grpc_metadata_decode_bin("Zm9vYmFy", 4, &dec_len);
  TEST_ASSERT_NOT_NULL(decoded);
  TEST_ASSERT_EQUAL_UINT64(3, dec_len);
  TEST_ASSERT_EQUAL_MEMORY("foo", decoded, 3);
  grpc_free(decoded);
  /* out_len may be null */
  decoded = grpc_metadata_decode_bin("Zm9v", 4, nullptr);
  TEST_ASSERT_NOT_NULL(decoded);
  TEST_ASSERT_EQUAL_MEMORY("foo", decoded, 3);
  grpc_free(decoded);
}

void test_metadata_null_handling(void) {
  TEST_ASSERT_NULL(grpc_metadata_encode_bin(nullptr, 0, nullptr));
  TEST_ASSERT_NULL(grpc_metadata_decode_bin(nullptr, 0, nullptr));
  TEST_ASSERT_NULL(grpc_call_metadata(nullptr, "test", nullptr));
  TEST_ASSERT_EQUAL_UINT64(0, (uint64_t)grpc_call_metadata_count(nullptr));
  grpc_metadata entry;
  TEST_ASSERT_FALSE(grpc_call_metadata_at(nullptr, 0, &entry));
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_metadata_base64_roundtrip);
  RUN_TEST(test_metadata_binary_random_bytes);
  RUN_TEST(test_metadata_encode_is_padded);
  RUN_TEST(test_metadata_decode_unpadded);
  RUN_TEST(test_metadata_decode_skips_invalid);
  RUN_TEST(test_metadata_null_handling);
  return UNITY_END();
}
