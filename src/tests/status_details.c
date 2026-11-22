#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "grpc.h"
#include "test.h"

void setUp(void) {}
void tearDown(void) {}

void test_status_details_roundtrip_with_any(void) {
  const char *msg = "Resource not found in database";
  const char *type_url = "type.googleapis.com/google.rpc.ErrorInfo";
  const uint8_t raw_payload[] = {0x08, 0x2A, 0x12, 0x04,
                                 0x74, 0x65, 0x73, 0x74}; // Sample inner proto bytes

  grpc_status_detail_entry detail = {
      .type_url = type_url,
      .value = raw_payload,
      .value_len = sizeof(raw_payload),
  };

  size_t enc_len = 0;
  char *b64 = grpc_status_details_encode((int32_t)GRPC_STATUS_NOT_FOUND, msg, &detail, 1, &enc_len);

  TEST_ASSERT_NOT_NULL(b64);
  TEST_ASSERT_TRUE(enc_len > 0);

  int32_t dec_code = 0;
  char *dec_msg = nullptr;
  char *dec_url = nullptr;
  uint8_t *dec_val = nullptr;
  size_t dec_vlen = 0;

  int rc = grpc_status_details_decode(b64, &dec_code, &dec_msg, &dec_url, &dec_val, &dec_vlen);

  TEST_ASSERT_EQUAL_INT(0, rc);
  TEST_ASSERT_EQUAL_INT32((int32_t)GRPC_STATUS_NOT_FOUND, dec_code);
  TEST_ASSERT_NOT_NULL(dec_msg);
  TEST_ASSERT_EQUAL_STRING(msg, dec_msg);
  TEST_ASSERT_NOT_NULL(dec_url);
  TEST_ASSERT_EQUAL_STRING(type_url, dec_url);
  TEST_ASSERT_EQUAL_UINT64(sizeof(raw_payload), dec_vlen);
  TEST_ASSERT_EQUAL_MEMORY(raw_payload, dec_val, sizeof(raw_payload));

  grpc_free(b64);
  grpc_free(dec_msg);
  grpc_free(dec_url);
  grpc_free(dec_val);
}

void test_status_details_basic_roundtrip(void) {
  const char *msg = "Invalid user argument";
  size_t enc_len = 0;
  char *b64 =
      grpc_status_details_encode((int32_t)GRPC_STATUS_INVALID_ARGUMENT, msg, nullptr, 0, &enc_len);

  TEST_ASSERT_NOT_NULL(b64);

  int32_t dec_code = 0;
  char *dec_msg = nullptr;

  int rc = grpc_status_details_decode(b64, &dec_code, &dec_msg, nullptr, nullptr, nullptr);
  TEST_ASSERT_EQUAL_INT(0, rc);
  TEST_ASSERT_EQUAL_INT32((int32_t)GRPC_STATUS_INVALID_ARGUMENT, dec_code);
  TEST_ASSERT_NOT_NULL(dec_msg);
  TEST_ASSERT_EQUAL_STRING(msg, dec_msg);

  grpc_free(b64);
  grpc_free(dec_msg);
}

void test_status_details_decode_all_multiple(void) {
  const uint8_t p1[] = {0x01, 0x02, 0x03};
  const uint8_t p2[] = {0x0A, 0x0B};
  grpc_status_detail_entry entries[] = {
      {.type_url = "type.googleapis.com/google.rpc.ErrorInfo",
       .value = p1,
       .value_len = sizeof(p1)},
      {.type_url = "type.googleapis.com/google.rpc.RetryInfo",
       .value = p2,
       .value_len = sizeof(p2)},
  };

  size_t enc_len = 0;
  char *b64 = grpc_status_details_encode((int32_t)GRPC_STATUS_UNAVAILABLE, "try again", entries, 2,
                                         &enc_len);
  TEST_ASSERT_NOT_NULL(b64);

  grpc_status_details decoded = {};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_status_details_decode_all(b64, &decoded));
  TEST_ASSERT_EQUAL_INT32((int32_t)GRPC_STATUS_UNAVAILABLE, decoded.code);
  TEST_ASSERT_EQUAL_STRING("try again", decoded.message);
  TEST_ASSERT_EQUAL_UINT64(2, decoded.nb_details);
  TEST_ASSERT_EQUAL_STRING("type.googleapis.com/google.rpc.ErrorInfo", decoded.details[0].type_url);
  TEST_ASSERT_EQUAL_UINT64(sizeof(p1), decoded.details[0].value_len);
  TEST_ASSERT_EQUAL_MEMORY(p1, decoded.details[0].value, sizeof(p1));
  TEST_ASSERT_EQUAL_STRING("type.googleapis.com/google.rpc.RetryInfo", decoded.details[1].type_url);
  TEST_ASSERT_EQUAL_UINT64(sizeof(p2), decoded.details[1].value_len);
  TEST_ASSERT_EQUAL_MEMORY(p2, decoded.details[1].value, sizeof(p2));

  grpc_status_details_free(&decoded);
  TEST_ASSERT_EQUAL_UINT64(0, decoded.nb_details);
  TEST_ASSERT_NULL(decoded.message);
  grpc_free(b64);
}

void test_status_details_unknown_fields_and_errors(void) {
  /* Proto with unknown varint (field 9, wire 0) and length-delimited (field 10, wire 2),
   * plus an Any (field 3) with unknown varint (field 5, wire 0) and len-delim (field 6, wire 2). */
  const uint8_t proto[] = {
      0x08, 0x05,             /* code = 5 */
      0x12, 0x02, 'o',  'k',  /* message = "ok" */
      0x48, 0x7F,             /* unknown field 9, varint 127 */
      0x52, 0x02, 'x',  'y',  /* unknown field 10, bytes "xy" */
      0x1A, 0x0E,             /* field 3: Any (14 bytes) */
      0x0A, 0x01, 'u',        /* Any.type_url = "u" */
      0x12, 0x01, 0x42,       /* Any.value = {0x42} */
      0x28, 0x01,             /* Any unknown varint field 5 */
      0x32, 0x02, 0x00, 0x01, /* Any unknown bytes field 6 */
      0x28, 0x02,             /* another varint */
  };
  size_t b64_len = 0;
  char *b64 = grpc_metadata_encode_bin(proto, sizeof(proto), &b64_len);
  TEST_ASSERT_NOT_NULL(b64);

  int32_t code = 0;
  char *msg = nullptr, *url = nullptr;
  uint8_t *val = nullptr;
  size_t vlen = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_status_details_decode(b64, &code, &msg, &url, &val, &vlen));
  TEST_ASSERT_EQUAL_INT32(5, code);
  TEST_ASSERT_EQUAL_STRING("ok", msg);
  /* the unknown fields inside the Any are skipped; its url and value survive */
  TEST_ASSERT_EQUAL_STRING("u", url);
  TEST_ASSERT_EQUAL_UINT64(1, vlen);
  TEST_ASSERT_NOT_NULL(val);
  TEST_ASSERT_EQUAL_HEX8(0x42, val[0]);
  grpc_free(msg);
  grpc_free(url);
  grpc_free(val);

  grpc_status_details all = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_status_details_decode_all(b64, &all));
  TEST_ASSERT_EQUAL_INT32(5, all.code);
  TEST_ASSERT_EQUAL_STRING("ok", all.message);
  TEST_ASSERT_EQUAL_UINT64(1, all.nb_details);
  TEST_ASSERT_EQUAL_STRING("u", all.details[0].type_url);
  TEST_ASSERT_EQUAL_UINT64(1, all.details[0].value_len);
  TEST_ASSERT_EQUAL_HEX8(0x42, all.details[0].value[0]);
  grpc_status_details_free(&all);
  grpc_free(b64);

  /* Invalid inputs & truncated length-delimited field */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_status_details_decode(nullptr, nullptr, nullptr,
                                                                     nullptr, nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_status_details_decode_all(nullptr, &all));
  const uint8_t trunc_msg[] = {0x12, 0x50, 'a'};
  char *b64_trunc = grpc_metadata_encode_bin(trunc_msg, sizeof(trunc_msg), &b64_len);
  TEST_ASSERT_NOT_NULL(b64_trunc);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_status_details_decode_all(b64_trunc, &all));
  grpc_free(b64_trunc);
  const uint8_t trunc_any[] = {0x1A, 0x50, 'a'};
  b64_trunc = grpc_metadata_encode_bin(trunc_any, sizeof(trunc_any), &b64_len);
  TEST_ASSERT_NOT_NULL(b64_trunc);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_status_details_decode_all(b64_trunc, &all));
  grpc_free(b64_trunc);
  grpc_status_details_free(nullptr);
}

/* Lengths near SIZE_MAX used to wrap `seek + len` past the bounds checks (out-of-bounds read,
 * and skips that moved backwards). Every decode must stay in bounds and terminate. */
void test_status_details_hostile_lengths(void) {
  /* 0xFF x9 + 0x01: a length of 2^64 - 1 */
#define HUGE_LEN 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01
  const uint8_t huge_msg[] = {0x12, HUGE_LEN, 'a', 'b'};
  const uint8_t huge_any[] = {0x1A, HUGE_LEN, 'a', 'b'};
  const uint8_t huge_skip[] = {0x52, HUGE_LEN, 0x08, 0x05};
  const uint8_t huge_in_any[] = {0x1A, 0x0C, 0x0A, HUGE_LEN, 'x'};
  const uint8_t huge_skip_in_any[] = {0x1A, 0x0C, 0x32, HUGE_LEN, 'x'};
  /* overlong varint (11 bytes, past 64 bits) for the code */
  const uint8_t overlong[] = {0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                              0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
#undef HUGE_LEN
  const struct {
    const uint8_t *data;
    size_t len;
  } cases[] = {
      {huge_msg, sizeof(huge_msg)},
      {huge_any, sizeof(huge_any)},
      {huge_skip, sizeof(huge_skip)},
      {huge_in_any, sizeof(huge_in_any)},
      {huge_skip_in_any, sizeof(huge_skip_in_any)},
      {overlong, sizeof(overlong)},
  };
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    size_t b64_len = 0;
    char *b64 = grpc_metadata_encode_bin(cases[i].data, cases[i].len, &b64_len);
    TEST_ASSERT_NOT_NULL(b64);
    int32_t code = 0;
    char *msg = nullptr, *url = nullptr;
    uint8_t *val = nullptr;
    size_t vlen = 0;
    grpc_err e1 = grpc_status_details_decode(b64, &code, &msg, &url, &val, &vlen);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, e1); /* lenient: skips what does not fit */
    TEST_ASSERT_NULL(msg);              /* nothing in these inputs is a complete message field */
    grpc_free(msg);
    grpc_free(url);
    grpc_free(val);
    grpc_status_details all = {0};
    grpc_err e2 = grpc_status_details_decode_all(b64, &all);
    (void)e2; /* strict: may reject; it only has to stay in bounds */
    grpc_status_details_free(&all);
    grpc_free(b64);
  }

  /* a repeated message field keeps the last value and does not leak the first */
  const uint8_t repeated[] = {0x12, 0x01, 'a', 0x12, 0x01, 'b'};
  size_t b64_len = 0;
  char *b64 = grpc_metadata_encode_bin(repeated, sizeof(repeated), &b64_len);
  TEST_ASSERT_NOT_NULL(b64);
  char *msg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_status_details_decode(b64, nullptr, &msg, nullptr, nullptr, nullptr));
  TEST_ASSERT_EQUAL_STRING("b", msg);
  grpc_free(msg);
  grpc_free(b64);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_status_details_roundtrip_with_any);
  RUN_TEST(test_status_details_basic_roundtrip);
  RUN_TEST(test_status_details_decode_all_multiple);
  RUN_TEST(test_status_details_unknown_fields_and_errors);
  RUN_TEST(test_status_details_hostile_lengths);
  return UNITY_END();
}
