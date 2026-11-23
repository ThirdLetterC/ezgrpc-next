/* unit_tests.c - unit tests of the OTLP metrics receiver's building blocks */
#include <math.h>
#include <string.h>

#include "test.h"

#include "otlp_arena.h"
#include "otlp_budget.h"
#include "otlp_fixtures.h"
#include "otlp_proto.h"
#include "otlp_queue.h"
#include "otlp_response.h"
#include "otlp_sink.h"
#include "otlp_stats.h"
#include "otlp_validate.h"
#include "otlp_wire.h"

static fx F;

void setUp(void) {
  fx_init(&F);
}

void tearDown(void) {
  fx_free(&F);
}

/* ---- budget and arena ---------------------------------------------------------------------- */

static void test_budget_limits_and_peak(void) {
  otlp_budget b;
  otlp_budget_init(&b, 100);
  TEST_ASSERT_TRUE(otlp_budget_reserve(&b, 60));
  TEST_ASSERT_FALSE(otlp_budget_reserve(&b, 41));
  TEST_ASSERT_TRUE(otlp_budget_reserve(&b, 40));
  TEST_ASSERT_EQUAL_size_t(100, otlp_budget_used(&b));
  otlp_budget_release(&b, 70);
  TEST_ASSERT_EQUAL_size_t(30, otlp_budget_used(&b));
  TEST_ASSERT_EQUAL_size_t(100, atomic_load(&b.peak));
  TEST_ASSERT_EQUAL_UINT64(1, atomic_load(&b.denied));
  TEST_ASSERT_FALSE(otlp_budget_reserve(&b, SIZE_MAX));
  otlp_budget_release(&b, 30);

  otlp_budget unlimited;
  otlp_budget_init(&unlimited, 0);
  TEST_ASSERT_TRUE(otlp_budget_reserve(&unlimited, SIZE_MAX / 2));
  otlp_budget_release(&unlimited, SIZE_MAX / 2);
}

static void test_arena_request_limit(void) {
  otlp_budget b;
  otlp_budget_init(&b, 0);
  otlp_arena a;
  otlp_arena_init(&a, &b, 0);
  otlp_arena_begin(&a, 64 * 1024, 100);
  TEST_ASSERT_NOT_NULL(otlp_arena_alloc(&a, 1000));
  TEST_ASSERT_NOT_NULL(otlp_arena_alloc(&a, 30000));
  TEST_ASSERT_NULL(otlp_arena_alloc(&a, 64 * 1024)); /* cannot fit in the request limit */
  TEST_ASSERT_EQUAL_INT(OTLP_ARENA_LIMIT, otlp_arena_status_get(&a));
  TEST_ASSERT_NULL(otlp_arena_alloc(&a, 8)); /* sticky until the next request */
  otlp_arena_end(&a);
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(&b)); /* keep_bytes = 0: nothing kept */
  otlp_arena_begin(&a, 64 * 1024, 100);
  TEST_ASSERT_NOT_NULL(otlp_arena_alloc(&a, 8));
  otlp_arena_destroy(&a);
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(&b));
}

static void test_arena_budget_and_alignment(void) {
  otlp_budget b;
  otlp_budget_init(&b, 40 * 1024);
  otlp_arena a;
  otlp_arena_init(&a, &b, 1 << 20);
  otlp_arena_begin(&a, SIZE_MAX, 0);
  for (int i = 0; i < 50; i++) {
    uint8_t *p = otlp_arena_alloc(&a, (size_t)i + 1);
    TEST_ASSERT_NOT_NULL(p);
    TEST_ASSERT_EQUAL_UINT64(0, (uintptr_t)p % alignof(max_align_t));
    memset(p, 0xab, (size_t)i + 1);
  }
  TEST_ASSERT_NULL(otlp_arena_alloc(&a, 64 * 1024)); /* over the shared budget */
  TEST_ASSERT_EQUAL_INT(OTLP_ARENA_NOMEM, otlp_arena_status_get(&a));
  otlp_arena_end(&a);
  /* the first chunk (16 KiB) is kept, and stays reserved */
  size_t kept = otlp_budget_used(&b);
  TEST_ASSERT_TRUE(kept > 0 && kept <= 40 * 1024);
  otlp_arena_begin(&a, SIZE_MAX, 0);
  TEST_ASSERT_NOT_NULL(otlp_arena_alloc(&a, 100));
  TEST_ASSERT_EQUAL_size_t(kept, otlp_budget_used(&b)); /* reused, nothing new reserved */
  otlp_arena_end(&a);
  otlp_arena_destroy(&a);
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(&b));
}

/* ---- wire pre-scan ------------------------------------------------------------------------- */

static void test_wire_accepts_valid_request(void) {
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 3)->base, &len);
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_OK, otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, len, 32));
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_OK, otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, 0, 32));
}

static void test_wire_rejects_bad_framing(void) {
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 2)->base, &len);
  /* every truncation of a valid request is cut inside some field */
  size_t malformed = 0;
  for (size_t cut = 1; cut < len; cut++) {
    if (otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, cut, 32) == OTLP_WIRE_MALFORMED)
      malformed++;
  }
  TEST_ASSERT_TRUE(malformed > 0);
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_MALFORMED,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, len - 1, 32));
  static const uint8_t field_zero[] = {0x00, 0x00};
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_MALFORMED,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, field_zero, 2, 32));
  static const uint8_t group[] = {0x0b, 0x0c}; /* start/end group of field 1 */
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_MALFORMED,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, group, 2, 32));
  static const uint8_t overrun[] = {0x0a, 0x05, 0x00};
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_MALFORMED,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, overrun, 3, 32));
  static const uint8_t long_varint[] = {0x08, 0xff, 0xff, 0xff, 0xff, 0xff,
                                        0xff, 0xff, 0xff, 0xff, 0x7f};
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_MALFORMED, otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR,
                                                             long_varint, sizeof(long_varint), 32));
}

static void test_wire_depth_limit(void) {
  /* the deep request nests: request 1, resource 2, scope 3, metric 4, gauge 5, point 6, kv 7,
   * any 8, then two levels (ArrayValue, AnyValue) per array */
  size_t len;
  uint8_t *data = fx_deep_request(10, &len);
  TEST_ASSERT_NOT_NULL(data);
  unsigned depth = 8 + 2 * 10;
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_OK,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, len, depth));
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_TOO_DEEP,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, len, depth - 1));
  /* and protobuf-c agrees the bytes are a valid request */
  ProtobufCMessage *m =
      protobuf_c_message_unpack(OTLP_PB_REQUEST_DESCRIPTOR, &F.arena.allocator, len, data);
  TEST_ASSERT_NOT_NULL(m);
  grpc_free(data);

  /* far deeper than any stack would survive in protobuf-c: refused without recursion */
  data = fx_deep_request(200000, &len);
  TEST_ASSERT_NOT_NULL(data);
  TEST_ASSERT_EQUAL_INT(OTLP_WIRE_TOO_DEEP,
                        otlp_wire_check(OTLP_PB_REQUEST_DESCRIPTOR, data, len, 64));
  grpc_free(data);
}

/* ---- decoding ------------------------------------------------------------------------------ */

static void test_decode_roundtrip_preserves_everything(void) {
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 4)->base, &len);
  otlp_arena a;
  otlp_arena_init(&a, nullptr, 0);
  otlp_arena_begin(&a, SIZE_MAX, len);
  otlp_pb_request *req = (otlp_pb_request *)protobuf_c_message_unpack(OTLP_PB_REQUEST_DESCRIPTOR,
                                                                      &a.allocator, len, data);
  TEST_ASSERT_NOT_NULL(req);
  /* re-encoding yields the same bytes: nothing was lost or transformed */
  size_t len2 = protobuf_c_message_get_packed_size(&req->base);
  TEST_ASSERT_EQUAL_size_t(len, len2);
  uint8_t *data2 = otlp_arena_alloc(&a, len2);
  protobuf_c_message_pack(&req->base, data2);
  TEST_ASSERT_EQUAL_MEMORY(data, data2, len);

  /* proto3 optional presence: HistogramDataPoint.sum is set to 0.0, max is unset */
  otlp_pb_metric *hist = req->resource_metrics[0]->scope_metrics[0]->metrics[2];
  TEST_ASSERT_EQUAL_INT(OTLP_KIND_HISTOGRAM, otlp_metric_kind_of(hist));
  otlp_pb_histogram_point *p = hist->histogram->data_points[0];
  TEST_ASSERT_EQUAL_INT(OPENTELEMETRY__PROTO__METRICS__V1__HISTOGRAM_DATA_POINT___SUM_SUM,
                        p->_sum_case);
  TEST_ASSERT_TRUE(p->sum == 0.0);
  TEST_ASSERT_EQUAL_INT(OPENTELEMETRY__PROTO__METRICS__V1__HISTOGRAM_DATA_POINT___MAX__NOT_SET,
                        p->_max_case);
  otlp_arena_destroy(&a);
}

static void test_decode_budget(void) {
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 50)->base, &len);
  otlp_arena a;
  otlp_arena_init(&a, nullptr, 0);
  otlp_arena_begin(&a, len / 2, len);
  TEST_ASSERT_NULL(protobuf_c_message_unpack(OTLP_PB_REQUEST_DESCRIPTOR, &a.allocator, len, data));
  TEST_ASSERT_EQUAL_INT(OTLP_ARENA_LIMIT, otlp_arena_status_get(&a));
  otlp_arena_end(&a);
  otlp_arena_begin(&a, 8 * len + 65536, len);
  TEST_ASSERT_NOT_NULL(
      protobuf_c_message_unpack(OTLP_PB_REQUEST_DESCRIPTOR, &a.allocator, len, data));
  TEST_ASSERT_EQUAL_INT(OTLP_ARENA_OK, otlp_arena_status_get(&a));
  otlp_arena_destroy(&a);
}

/* ---- validation ---------------------------------------------------------------------------- */

static void test_validate_counts_all_kinds(void) {
  otlp_counts c;
  otlp_validate_request(fx_all_kinds(&F, 7), &c);
  TEST_ASSERT_EQUAL_UINT64(1, c.resources);
  TEST_ASSERT_EQUAL_UINT64(1, c.scopes);
  TEST_ASSERT_EQUAL_UINT64(5, c.metrics);
  TEST_ASSERT_EQUAL_UINT64(35, c.points);
  TEST_ASSERT_EQUAL_UINT64(0, c.rejected);
  for (int k = OTLP_KIND_GAUGE; k < OTLP_KIND_COUNT; k++)
    TEST_ASSERT_EQUAL_UINT64(7, c.points_by_kind[k]);
  TEST_ASSERT_EQUAL_STRING("", c.reason);
}

static void test_validate_empty_request(void) {
  otlp_counts c;
  otlp_pb_request *req = fx_request(&F);
  otlp_validate_request(req, &c);
  TEST_ASSERT_EQUAL_UINT64(0, c.points);
  otlp_pb_resource_metrics *rm = fx_resource(&F, req, "svc");
  otlp_pb_scope_metrics *sm = fx_scope(&F, rm, "scope");
  fx_empty_metric(&F, sm, "no.data");
  otlp_validate_request(req, &c);
  TEST_ASSERT_EQUAL_UINT64(1, c.metrics);
  TEST_ASSERT_EQUAL_UINT64(0, c.points);
  TEST_ASSERT_EQUAL_UINT64(0, c.rejected);
}

/* Builds a request in which every rule is broken by a known number of points. */
static otlp_pb_request *invalid_request(uint64_t *expected_rejected, uint64_t *expected_points) {
  otlp_pb_request *req = fx_request(&F);
  otlp_pb_resource_metrics *rm = fx_resource(&F, req, "svc");
  otlp_pb_scope_metrics *sm = fx_scope(&F, rm, "scope");
  uint64_t rejected = 0;
  uint64_t points = 0;

  fx_gauge(&F, sm, "ok.gauge", 3);
  points += 3;

  otlp_pb_metric *m = fx_gauge(&F, sm, "", 4); /* empty name: all 4 */
  (void)m;
  rejected += 4;
  points += 4;

  m = fx_sum(&F, sm, "sum.unspecified", 2); /* temporality unspecified: both */
  m->sum->aggregation_temporality =
      OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_UNSPECIFIED;
  rejected += 2;
  points += 2;

  m = fx_gauge(&F, sm, "gauge.bad", 5);
  m->gauge->data_points[0]->time_unix_nano = 0; /* missing time */
  m->gauge->data_points[1]->value_case =
      OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE__NOT_SET; /* no value */
  m->gauge->data_points[2]->value_case =
      OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE__NOT_SET;
  m->gauge->data_points[2]->flags = 1;          /* ...but flagged NO_RECORDED_VALUE: valid */
  m->gauge->data_points[3]->time_unix_nano = 0; /* breaks two rules, counted once */
  m->gauge->data_points[3]->value_case =
      OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE__NOT_SET;
  rejected += 3;
  points += 5;

  m = fx_histogram(&F, sm, "hist.bad", 5);
  m->histogram->data_points[0]->count += 1;             /* buckets do not add up */
  m->histogram->data_points[1]->n_bucket_counts = 3;    /* wrong bucket count */
  m->histogram->data_points[2]->explicit_bounds[1] = 1; /* not strictly increasing */
  m->histogram->data_points[3]->explicit_bounds[0] = NAN;
  rejected += 4;
  points += 5;

  m = fx_exp_histogram(&F, sm, "exp.bad", 4);
  m->exponential_histogram->data_points[0]->scale = 21;
  m->exponential_histogram->data_points[1]->count = 0;
  m->exponential_histogram->data_points[2]->zero_threshold = -1;
  m->exponential_histogram->data_points[3]->positive->bucket_counts[0] = UINT64_MAX; /* overflow */
  rejected += 4;
  points += 4;

  m = fx_summary(&F, sm, "summary.bad", 3);
  m->summary->data_points[1]->quantile_values[2]->quantile = 1.5;
  rejected += 1;
  points += 3;

  /* a second resource whose only metric is entirely rejected */
  otlp_pb_resource_metrics *rm2 = fx_resource(&F, req, "svc2");
  otlp_pb_scope_metrics *sm2 = fx_scope(&F, rm2, "scope2");
  m = fx_histogram(&F, sm2, "hist.unspecified", 2);
  m->histogram->aggregation_temporality =
      OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_UNSPECIFIED;
  rejected += 2;
  points += 2;

  *expected_rejected = rejected;
  *expected_points = points;
  return req;
}

static void test_validate_exact_rejections(void) {
  uint64_t rejected, points;
  otlp_pb_request *req = invalid_request(&rejected, &points);
  otlp_counts c;
  otlp_validate_request(req, &c);
  TEST_ASSERT_EQUAL_UINT64(points, c.points);
  TEST_ASSERT_EQUAL_UINT64(rejected, c.rejected);
  TEST_ASSERT_EQUAL_STRING("metric name is empty (metric \"\")", c.reason);
}

static void test_filter_removes_exactly_the_rejected(void) {
  uint64_t rejected, points;
  otlp_pb_request *req = invalid_request(&rejected, &points);
  otlp_filter_request(req);
  otlp_counts c;
  otlp_validate_request(req, &c);
  TEST_ASSERT_EQUAL_UINT64(points - rejected, c.points);
  TEST_ASSERT_EQUAL_UINT64(0, c.rejected);
  /* the empty-name, unspecified-temporality and all-invalid exp.bad metrics are gone; so is the
   * second resource */
  TEST_ASSERT_EQUAL_size_t(1, req->n_resource_metrics);
  otlp_pb_scope_metrics *sm = req->resource_metrics[0]->scope_metrics[0];
  TEST_ASSERT_EQUAL_size_t(4, sm->n_metrics);
  TEST_ASSERT_EQUAL_STRING("ok.gauge", sm->metrics[0]->name);
  TEST_ASSERT_EQUAL_STRING("gauge.bad", sm->metrics[1]->name);
  TEST_ASSERT_EQUAL_size_t(2, sm->metrics[1]->gauge->n_data_points);
  /* survivors keep their order and identity */
  TEST_ASSERT_EQUAL_UINT64(1700000000000000000ull + 1000 + 2,
                           sm->metrics[1]->gauge->data_points[0]->time_unix_nano);

  /* the filtered request still encodes and decodes */
  size_t len;
  uint8_t *data = fx_pack(&F, &req->base, &len);
  TEST_ASSERT_NOT_NULL(
      protobuf_c_message_unpack(OTLP_PB_REQUEST_DESCRIPTOR, &F.arena.allocator, len, data));
}

static void test_filter_keeps_empty_envelopes(void) {
  otlp_pb_request *req = fx_request(&F);
  otlp_pb_resource_metrics *rm = fx_resource(&F, req, "svc");
  fx_scope(&F, rm, "empty.scope");
  fx_resource(&F, req, "empty");
  otlp_filter_request(req);
  TEST_ASSERT_EQUAL_size_t(2, req->n_resource_metrics);
  TEST_ASSERT_EQUAL_size_t(1, req->resource_metrics[0]->n_scope_metrics);
}

/* ---- responses ----------------------------------------------------------------------------- */

static void test_response_full_success_is_empty(void) {
  grpc_message *msg = otlp_response_new(0, nullptr);
  TEST_ASSERT_NOT_NULL(msg);
  TEST_ASSERT_EQUAL_UINT32(0, msg->len);
  grpc_message_free(msg);
}

static void test_response_partial_success(void) {
  char long_text[1000];
  memset(long_text, 'x', sizeof(long_text) - 1);
  long_text[sizeof(long_text) - 1] = '\0';
  grpc_message *msg = otlp_response_new(42, long_text);
  TEST_ASSERT_NOT_NULL(msg);
  otlp_pb_response *resp = (otlp_pb_response *)protobuf_c_message_unpack(
      &opentelemetry__proto__collector__metrics__v1__export_metrics_service_response__descriptor,
      &F.arena.allocator, msg->len, msg->data);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_NOT_NULL(resp->partial_success);
  TEST_ASSERT_EQUAL_INT64(42, resp->partial_success->rejected_data_points);
  TEST_ASSERT_EQUAL_size_t(OTLP_ERROR_MESSAGE_MAX, strlen(resp->partial_success->error_message));
  grpc_message_free(msg);

  /* a warning without rejections still sets partial_success */
  msg = otlp_response_new(0, "note");
  TEST_ASSERT_TRUE(msg->len > 0);
  grpc_message_free(msg);
}

static void test_retry_info_encoding(void) {
  uint8_t buf[32];
  size_t n = otlp_retry_info_encode(buf, 1000);
  static const uint8_t one_second[] = {0x0a, 0x02, 0x08, 0x01};
  TEST_ASSERT_EQUAL_size_t(sizeof(one_second), n);
  TEST_ASSERT_EQUAL_MEMORY(one_second, buf, n);
  n = otlp_retry_info_encode(buf, 250);
  /* nanos 250000000 = 0x0EE6B280 -> varint 80 e5 9a 77 */
  static const uint8_t quarter[] = {0x0a, 0x05, 0x10, 0x80, 0xe5, 0x9a, 0x77};
  TEST_ASSERT_EQUAL_size_t(sizeof(quarter), n);
  TEST_ASSERT_EQUAL_MEMORY(quarter, buf, n);
  n = otlp_retry_info_encode(buf, 0);
  static const uint8_t zero[] = {0x0a, 0x00};
  TEST_ASSERT_EQUAL_MEMORY(zero, buf, n);
}

/* ---- sinks ---------------------------------------------------------------------------------- */

static void test_memory_sink_store_and_dump(void) {
  otlp_sink *s = otlp_memory_sink_new(100, 0, OTLP_MEMORY_REJECT);
  TEST_ASSERT_NOT_NULL(s);
  const char *detail = nullptr;
  otlp_batch b1 = {.payload = (const uint8_t *)"aaaa", .payload_len = 4, .points = 2};
  otlp_batch b2 = {.payload = (const uint8_t *)"bbbbbb", .payload_len = 6, .points = 3};
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_ACCEPTED, otlp_sink_submit(s, &b1, nullptr, &detail));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_ACCEPTED, otlp_sink_submit(s, &b2, nullptr, &detail));
  uint8_t *out;
  size_t len, batches;
  TEST_ASSERT_TRUE(otlp_memory_sink_dump(s, 1000, &out, &len, &batches));
  TEST_ASSERT_EQUAL_size_t(10, len);
  TEST_ASSERT_EQUAL_size_t(2, batches);
  TEST_ASSERT_EQUAL_MEMORY("aaaabbbbbb", out, 10);
  grpc_free(out);
  TEST_ASSERT_FALSE(otlp_memory_sink_dump(s, 9, &out, &len, nullptr));

  otlp_batch big = {.payload = (const uint8_t *)"x", .payload_len = 101, .points = 1};
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_FAILED, otlp_sink_submit(s, &big, nullptr, &detail));
  uint8_t fill[95] = {0};
  otlp_batch b3 = {.payload = fill, .payload_len = 95, .points = 1};
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, otlp_sink_submit(s, &b3, nullptr, &detail));
  TEST_ASSERT_EQUAL_STRING("memory sink is full", detail);
  otlp_memory_sink_clear(s);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_ACCEPTED, otlp_sink_submit(s, &b3, nullptr, &detail));
  otlp_sink_shutdown(s);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, otlp_sink_submit(s, &b1, nullptr, &detail));
  otlp_sink_destroy(s);
}

static void test_memory_sink_evicts(void) {
  otlp_sink *s = otlp_memory_sink_new(10, 2, OTLP_MEMORY_EVICT);
  const char *detail;
  otlp_batch a = {.payload = (const uint8_t *)"aaaa", .payload_len = 4, .points = 1};
  otlp_batch b = {.payload = (const uint8_t *)"bbbb", .payload_len = 4, .points = 1};
  otlp_batch c = {.payload = (const uint8_t *)"cccc", .payload_len = 4, .points = 1};
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_ACCEPTED, otlp_sink_submit(s, &a, nullptr, &detail));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_ACCEPTED, otlp_sink_submit(s, &b, nullptr, &detail));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_ACCEPTED, otlp_sink_submit(s, &c, nullptr, &detail));
  uint8_t *out;
  size_t len;
  TEST_ASSERT_TRUE(otlp_memory_sink_dump(s, 100, &out, &len, nullptr));
  TEST_ASSERT_EQUAL_size_t(8, len);
  TEST_ASSERT_EQUAL_MEMORY("bbbbcccc", out, 8);
  grpc_free(out);
  otlp_strbuf json = {0};
  s->ops->stats_json(s, &json);
  TEST_ASSERT_NOT_NULL(strstr(json.data, "\"evicted_batches\":1"));
  otlp_strbuf_free(&json);
  otlp_sink_shutdown(s);
  otlp_sink_destroy(s);
}

typedef struct completion_log {
  int calls;
  otlp_sink_result last;
  char order[16];
} completion_log;

static void record_completion(void *ctx, otlp_sink_result result, const char *detail) {
  (void)detail;
  completion_log *log = ctx;
  log->last = result;
  log->order[log->calls++] = result == OTLP_SINK_ACCEPTED ? 'A' : 'R';
}

static void test_fault_sink_injection(void) {
  otlp_sink *inner = otlp_memory_sink_new(1000, 0, OTLP_MEMORY_REJECT);
  otlp_fault_config cfg = {.retryable_every = 3, .fail_every = 4};
  otlp_sink *s = otlp_fault_sink_new(inner, &cfg, nullptr);
  TEST_ASSERT_EQUAL_PTR(inner, otlp_sink_unwrap(s));
  otlp_batch b = {.payload = (const uint8_t *)"x", .payload_len = 1, .points = 1};
  const char *detail;
  otlp_sink_result expected[] = {OTLP_SINK_ACCEPTED, OTLP_SINK_ACCEPTED, OTLP_SINK_RETRYABLE,
                                 OTLP_SINK_FAILED,   OTLP_SINK_ACCEPTED, OTLP_SINK_RETRYABLE};
  for (size_t i = 0; i < 6; i++)
    TEST_ASSERT_EQUAL_INT(expected[i], otlp_sink_submit(s, &b, nullptr, &detail));
  otlp_sink_shutdown(s);
  otlp_sink_destroy(s);
}

static void test_fault_sink_gate(void) {
  otlp_budget budget;
  otlp_budget_init(&budget, 0);
  otlp_sink *inner = otlp_memory_sink_new(1000, 0, OTLP_MEMORY_REJECT);
  otlp_fault_config cfg = {.gate_closed = true, .gate_capacity = 2};
  otlp_sink *s = otlp_fault_sink_new(inner, &cfg, &budget);
  completion_log log = {0};
  otlp_sink_completion done = {.fn = record_completion, .ctx = &log};
  const char *detail;
  otlp_batch b1 = {.payload = (const uint8_t *)"one", .payload_len = 3, .points = 1};
  otlp_batch b2 = {.payload = (const uint8_t *)"two", .payload_len = 3, .points = 1};
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, otlp_sink_submit(s, &b1, &done, &detail));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, otlp_sink_submit(s, &b2, &done, &detail));
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, otlp_sink_submit(s, &b2, &done, &detail));
  TEST_ASSERT_EQUAL_size_t(2, otlp_fault_sink_held(s));
  TEST_ASSERT_EQUAL_size_t(6, otlp_budget_used(&budget)); /* held copies are reserved */
  TEST_ASSERT_EQUAL_INT(0, log.calls);
  otlp_fault_sink_set_gate(s, true);
  TEST_ASSERT_EQUAL_INT(2, log.calls);
  TEST_ASSERT_EQUAL_STRING("AA", log.order);
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(&budget));
  uint8_t *out;
  size_t len;
  TEST_ASSERT_TRUE(otlp_memory_sink_dump(s, 100, &out, &len, nullptr));
  TEST_ASSERT_EQUAL_MEMORY("onetwo", out, 6); /* in submission order */
  grpc_free(out);

  /* closed again: shutdown fails what is held */
  otlp_fault_sink_set_gate(s, false);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_PENDING, otlp_sink_submit(s, &b1, &done, &detail));
  otlp_sink_shutdown(s);
  TEST_ASSERT_EQUAL_INT(3, log.calls);
  TEST_ASSERT_EQUAL_INT(OTLP_SINK_RETRYABLE, log.last);
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(&budget));
  otlp_sink_destroy(s);
}

/* ---- queue and stats ------------------------------------------------------------------------ */

static void test_queue_bounded(void) {
  otlp_queue q;
  TEST_ASSERT_TRUE(otlp_queue_init(&q, 2));
  int a, b, c;
  TEST_ASSERT_TRUE(otlp_queue_push(&q, &a));
  TEST_ASSERT_TRUE(otlp_queue_push(&q, &b));
  TEST_ASSERT_FALSE(otlp_queue_push(&q, &c));
  TEST_ASSERT_EQUAL_PTR(&a, otlp_queue_pop(&q));
  TEST_ASSERT_TRUE(otlp_queue_push(&q, &c));
  otlp_queue_close(&q);
  TEST_ASSERT_FALSE(otlp_queue_push(&q, &a));
  TEST_ASSERT_EQUAL_PTR(&b, otlp_queue_pop(&q));
  TEST_ASSERT_EQUAL_PTR(&c, otlp_queue_pop(&q));
  TEST_ASSERT_NULL(otlp_queue_pop(&q));
  size_t depth, peak;
  otlp_queue_depth(&q, &depth, &peak);
  TEST_ASSERT_EQUAL_size_t(0, depth);
  TEST_ASSERT_EQUAL_size_t(2, peak);
  otlp_queue_destroy(&q);
}

static void test_stats_lookup_and_json(void) {
  otlp_stats st;
  otlp_stats_init(&st);
  OTLP_STAT_ADD(&st, points_accepted, 5);
  OTLP_STAT_ADD(&st, status[3], 2);
  otlp_stats_snapshot snap;
  otlp_stats_snapshot_get(&st, &snap);
  uint64_t v = 0;
  TEST_ASSERT_TRUE(otlp_stats_lookup(&snap, "points_accepted", &v));
  TEST_ASSERT_EQUAL_UINT64(5, v);
  TEST_ASSERT_FALSE(otlp_stats_lookup(&snap, "no_such_counter", &v));
  otlp_strbuf json = {0};
  otlp_stats_json(&snap, &json);
  TEST_ASSERT_FALSE(json.failed);
  TEST_ASSERT_NOT_NULL(strstr(json.data, "\"points_accepted\":5"));
  TEST_ASSERT_NOT_NULL(strstr(json.data, "\"status\":{\"3\":2}"));
  otlp_strbuf_free(&json);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_budget_limits_and_peak);
  RUN_TEST(test_arena_request_limit);
  RUN_TEST(test_arena_budget_and_alignment);
  RUN_TEST(test_wire_accepts_valid_request);
  RUN_TEST(test_wire_rejects_bad_framing);
  RUN_TEST(test_wire_depth_limit);
  RUN_TEST(test_decode_roundtrip_preserves_everything);
  RUN_TEST(test_decode_budget);
  RUN_TEST(test_validate_counts_all_kinds);
  RUN_TEST(test_validate_empty_request);
  RUN_TEST(test_validate_exact_rejections);
  RUN_TEST(test_filter_removes_exactly_the_rejected);
  RUN_TEST(test_filter_keeps_empty_envelopes);
  RUN_TEST(test_response_full_success_is_empty);
  RUN_TEST(test_response_partial_success);
  RUN_TEST(test_retry_info_encoding);
  RUN_TEST(test_memory_sink_store_and_dump);
  RUN_TEST(test_memory_sink_evicts);
  RUN_TEST(test_fault_sink_injection);
  RUN_TEST(test_fault_sink_gate);
  RUN_TEST(test_queue_bounded);
  RUN_TEST(test_stats_lookup_and_json);
  return UNITY_END();
}
