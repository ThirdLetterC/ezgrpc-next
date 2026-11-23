/* server_tests.c - the receiver end to end, in process, through the ezGRPC-next client
 *
 * Each test starts an app on an ephemeral port, talks to it with grpc_client (raw protobuf bytes
 * built with the generated types), and checks statuses, responses and counters. Tests that need
 * requests to stay in flight use the fault sink's gate, and wait for server-side conditions (the
 * Admin Wait method or the gate's held count), never for time.
 */
#include "otlp_threads.h"
#include <stdio.h>
#include <string.h>
#include <uv.h>

#include "test.h"

#include "grpc.h"
#include "otlp_app.h"
#include "otlp_fixtures.h"
#include "otlp_proto.h"
#include "otlp_response.h"
#include "otlp_validate.h"

static fx F;
static otlp_app *g_app;

void setUp(void) {
  fx_init(&F);
  g_app = nullptr;
}

void tearDown(void) {
  if (g_app != nullptr) {
    otlp_app_stop(g_app);
    (void)otlp_app_wait(g_app);
    g_app = nullptr;
  }
  fx_free(&F);
}

static otlp_config test_config(void) {
  otlp_config c;
  otlp_config_defaults(&c);
  c.host = "127.0.0.1";
  c.port = 0;
  c.log_level = GRPC_LOG_OFF;
  c.drain_ms = 2000;
  c.service.admin = true;
  c.sink = OTLP_SINK_KIND_MEMORY;
  return c;
}

static void start(const otlp_config *c) {
  TEST_ASSERT_EQUAL_INT(GRPC_OK, otlp_app_start(c, &g_app));
  TEST_ASSERT_NOT_NULL(g_app);
}

static grpc_client *connect_client(grpc_compression_algorithm compression) {
  grpc_err err;
  grpc_client *client = grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                              .port = otlp_app_port(g_app),
                                                              .timeout_ms = 10000,
                                                              .max_message_size = 128u << 20,
                                                              .compression = compression},
                                        &err);
  TEST_ASSERT_NOT_NULL(client);
  return client;
}

static grpc_status export_bytes(grpc_client *client, const uint8_t *data, size_t len,
                                const grpc_metadata *md, size_t nb_md, grpc_client_response *out) {
  *out = (grpc_client_response){0};
  grpc_err err = grpc_client_unary(client, OTLP_EXPORT_PATH, data, len, md, nb_md, 0, out);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  return out->status;
}

/* Decodes an ExportMetricsServiceResponse into the fixture arena. */
static otlp_pb_response *decode_response(const grpc_client_response *resp) {
  TEST_ASSERT_NOT_NULL(resp->message);
  otlp_pb_response *r = (otlp_pb_response *)protobuf_c_message_unpack(
      &opentelemetry__proto__collector__metrics__v1__export_metrics_service_response__descriptor,
      &F.arena.allocator, resp->message->len, resp->message->data);
  TEST_ASSERT_NOT_NULL(r);
  return r;
}

static otlp_stats_snapshot stats(void) {
  otlp_stats_snapshot snap;
  otlp_stats_snapshot_get(otlp_service_stats(otlp_app_service(g_app)), &snap);
  return snap;
}

/* The memory sink's content, validated: returns its point count. */
static uint64_t stored_points(void) {
  uint8_t *data;
  size_t len;
  TEST_ASSERT_TRUE(otlp_memory_sink_dump(otlp_service_sink(otlp_app_service(g_app)), SIZE_MAX,
                                         &data, &len, nullptr));
  otlp_pb_request *req = (otlp_pb_request *)protobuf_c_message_unpack(
      OTLP_PB_REQUEST_DESCRIPTOR, &F.arena.allocator, len, data);
  grpc_free(data);
  TEST_ASSERT_NOT_NULL(req);
  otlp_counts c;
  otlp_validate_request(req, &c);
  TEST_ASSERT_EQUAL_UINT64(0, c.rejected);
  return c.points;
}

/* ---- tests ------------------------------------------------------------------------------- */

static void test_full_success(void) {
  otlp_config c = test_config();
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 4)->base, &len);
  grpc_client_response resp;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, data, len, nullptr, 0, &resp));
  /* full success: an empty response, partial_success unset */
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(0, resp.message->len);
  otlp_pb_response *r = decode_response(&resp);
  TEST_ASSERT_NULL(r->partial_success);
  grpc_client_response_free(&resp);

  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(1, s.requests_ok);
  TEST_ASSERT_EQUAL_UINT64(20, s.points_accepted);
  TEST_ASSERT_EQUAL_UINT64(len, s.payload_bytes_stored);
  TEST_ASSERT_EQUAL_UINT64(20, stored_points());

  /* the stored batch is the request, byte for byte */
  uint8_t *dump;
  size_t dump_len;
  TEST_ASSERT_TRUE(otlp_memory_sink_dump(otlp_service_sink(otlp_app_service(g_app)), SIZE_MAX,
                                         &dump, &dump_len, nullptr));
  TEST_ASSERT_EQUAL_size_t(len, dump_len);
  TEST_ASSERT_EQUAL_MEMORY(data, dump, len);
  grpc_free(dump);
  grpc_client_free(client);
}

static void test_partial_success_exact_count(void) {
  otlp_config c = test_config();
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  otlp_pb_request *req = fx_request(&F);
  otlp_pb_scope_metrics *sm = fx_scope(&F, fx_resource(&F, req, "svc"), "scope");
  fx_gauge(&F, sm, "good", 6);
  otlp_pb_metric *bad = fx_histogram(&F, sm, "bad", 4);
  bad->histogram->data_points[1]->count = 1;
  bad->histogram->data_points[3]->count = 2;
  fx_gauge(&F, sm, "", 2);
  size_t len;
  uint8_t *data = fx_pack(&F, &req->base, &len);
  grpc_client_response resp;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, data, len, nullptr, 0, &resp));
  otlp_pb_response *r = decode_response(&resp);
  TEST_ASSERT_NOT_NULL(r->partial_success);
  TEST_ASSERT_EQUAL_INT64(4, r->partial_success->rejected_data_points);
  TEST_ASSERT_NOT_NULL(
      strstr(r->partial_success->error_message, "histogram bucket_counts do not add up to count"));
  grpc_client_response_free(&resp);

  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(1, s.requests_partial);
  TEST_ASSERT_EQUAL_UINT64(8, s.points_accepted);
  TEST_ASSERT_EQUAL_UINT64(4, s.points_rejected);
  /* the sink got exactly the accepted points */
  TEST_ASSERT_EQUAL_UINT64(8, stored_points());
  grpc_client_free(client);
}

static void test_all_rejected_and_empty_requests(void) {
  otlp_config c = test_config();
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  grpc_client_response resp;
  /* empty request: success, nothing stored */
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, nullptr, 0, nullptr, 0, &resp));
  TEST_ASSERT_NULL(decode_response(&resp)->partial_success);
  grpc_client_response_free(&resp);
  /* everything rejected: partial success with all of them */
  otlp_pb_request *req = fx_request(&F);
  fx_gauge(&F, fx_scope(&F, fx_resource(&F, req, "svc"), "scope"), "", 3);
  size_t len;
  uint8_t *data = fx_pack(&F, &req->base, &len);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, data, len, nullptr, 0, &resp));
  TEST_ASSERT_EQUAL_INT64(3, decode_response(&resp)->partial_success->rejected_data_points);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_UINT64(0, stored_points());
  TEST_ASSERT_EQUAL_UINT64(0, stats().payload_bytes_stored);
  grpc_client_free(client);
}

static void test_malformed_is_invalid_argument(void) {
  otlp_config c = test_config();
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  grpc_client_response resp;
  static const uint8_t garbage[] = {0x0a, 0x10, 0x01, 0x02};
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INVALID_ARGUMENT,
                        export_bytes(client, garbage, sizeof(garbage), nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  /* well framed, but a message field holding a varint: protobuf-c rejects it */
  static const uint8_t wrong_type[] = {0x08, 0x01};
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INVALID_ARGUMENT,
                        export_bytes(client, wrong_type, sizeof(wrong_type), nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  size_t len;
  uint8_t *deep = fx_deep_request(1000, &len);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INVALID_ARGUMENT,
                        export_bytes(client, deep, len, nullptr, 0, &resp));
  TEST_ASSERT_NOT_NULL(strstr(resp.status_message, "deeper"));
  grpc_client_response_free(&resp);
  grpc_free(deep);
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(2, s.malformed);
  TEST_ASSERT_EQUAL_UINT64(1, s.too_deep);
  TEST_ASSERT_EQUAL_UINT64(3, s.requests_failed);
  grpc_client_free(client);
}

static void test_compressed_request(void) {
  otlp_config c = test_config();
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_GZIP);
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 30)->base, &len);
  grpc_client_response resp;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, data, len, nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(1, s.encoding_gzip);
  TEST_ASSERT_EQUAL_UINT64(len, s.bytes_received); /* counted after decompression */
  TEST_ASSERT_EQUAL_UINT64(150, stored_points());
  grpc_client_free(client);
}

static void test_oversized_is_resource_exhausted(void) {
  otlp_config c = test_config();
  c.max_message_bytes = 64 * 1024;
  start(&c);
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 200)->base, &len);
  TEST_ASSERT_TRUE(len > 64 * 1024);
  grpc_client_response resp;
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED,
                        export_bytes(client, data, len, nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  /* compressed below the limit but decompressing above it: refused as well */
  client = connect_client(GRPC_COMPRESS_GZIP);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED,
                        export_bytes(client, data, len, nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  TEST_ASSERT_EQUAL_UINT64(0, stats().points_accepted);
}

/* ---- helpers for calls in flight -------------------------------------------------------- */

/* An Export on its own thread and client. Results are checked on the test thread (assertions
 * are only allowed there). */
typedef struct bg_call {
  uint16_t port;
  const uint8_t *data;
  size_t len;
  uint32_t timeout_ms;
  grpc_compression_algorithm compression;
  pthread_t thread;
  bool transport_ok;
  grpc_status status;
  int64_t rejected;
  bool retry_info; /* RetryInfo in grpc-status-details-bin */
  uint32_t retry_delay_ms;
  bool retry_trailer; /* google.rpc.retryinfo-bin */
  char message[128];
} bg_call;

static const char *trailer(const grpc_client_response *resp, const char *key) {
  for (size_t i = 0; i < resp->nb_trailers; i++) {
    if (strcmp(resp->trailers[i].key, key) == 0)
      return resp->trailers[i].value;
  }
  return nullptr;
}

/* Fills the result fields of `c` from a response (any thread). */
static void read_result(bg_call *c, const grpc_client_response *resp) {
  c->status = resp->status;
  if (resp->status_message != nullptr)
    snprintf(c->message, sizeof(c->message), "%s", resp->status_message);
  c->rejected = -1;
  if (resp->status == GRPC_STATUS_OK && resp->message != nullptr) {
    otlp_pb_response *r = (otlp_pb_response *)protobuf_c_message_unpack(
        &opentelemetry__proto__collector__metrics__v1__export_metrics_service_response__descriptor,
        nullptr, resp->message->len, resp->message->data);
    if (r != nullptr) {
      c->rejected = r->partial_success != nullptr ? r->partial_success->rejected_data_points : 0;
      protobuf_c_message_free_unpacked(&r->base, nullptr);
    }
  }
  const char *details = trailer(resp, GRPC_STATUS_DETAILS_TRAILER);
  grpc_status_details sd = {0};
  if (details != nullptr && grpc_status_details_decode_all(details, &sd) == GRPC_OK) {
    for (size_t i = 0; i < sd.nb_details; i++) {
      if (strcmp(sd.details[i].type_url, OTLP_RETRY_INFO_TYPE_URL) != 0)
        continue;
      /* RetryInfo{retry_delay{seconds, nanos}}: compare with our own encoding */
      for (uint32_t ms = 0; ms <= 60000; ms += 50) {
        uint8_t expect[32];
        size_t n = otlp_retry_info_encode(expect, ms);
        if (n == sd.details[i].value_len && memcmp(expect, sd.details[i].value, n) == 0) {
          c->retry_info = true;
          c->retry_delay_ms = ms;
        }
      }
    }
    grpc_status_details_free(&sd);
  }
  c->retry_trailer = trailer(resp, OTLP_RETRY_INFO_TRAILER) != nullptr;
}

static void *bg_main(void *arg) {
  bg_call *c = arg;
  grpc_err err;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1", .port = c->port, .timeout_ms = 30000, .compression = c->compression},
      &err);
  if (client == nullptr)
    return nullptr;
  grpc_client_response resp = {0};
  err = grpc_client_unary(client, OTLP_EXPORT_PATH, c->data, c->len, nullptr, 0, c->timeout_ms,
                          &resp);
  c->transport_ok = err == GRPC_OK || err == GRPC_ERR_TIMEOUT;
  if (err == GRPC_ERR_TIMEOUT && resp.status == GRPC_STATUS_UNKNOWN)
    resp.status = GRPC_STATUS_DEADLINE_EXCEEDED;
  read_result(c, &resp);
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  return nullptr;
}

static void bg_start(bg_call *c, const uint8_t *data, size_t len) {
  c->port = otlp_app_port(g_app);
  c->data = data;
  c->len = len;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&c->thread, nullptr, bg_main, c));
}

static void bg_join(bg_call *c) {
  TEST_ASSERT_EQUAL_INT(0, pthread_join(c->thread, nullptr));
  TEST_ASSERT_TRUE_MESSAGE(c->transport_ok, "the call failed at the transport level");
}

/* Blocks until the server's counter reaches `value` (Admin Wait: a server-side condition). */
static void wait_counter(const char *counter, uint64_t value) {
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  char body[96];
  int n = snprintf(body, sizeof(body), "%s %llu", counter, (unsigned long long)value);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/otlp_metrics.Admin/Wait", body,
                                                   (size_t)n, nullptr, 0, 20000, &resp));
  TEST_ASSERT_EQUAL_INT_MESSAGE(GRPC_STATUS_OK, resp.status, body);
  grpc_client_response_free(&resp);
  grpc_client_free(client);
}

static otlp_sink *app_sink(void) {
  return otlp_service_sink(otlp_app_service(g_app));
}

static uint8_t *small_request(size_t *len) {
  otlp_pb_request *req = fx_request(&F);
  fx_gauge(&F, fx_scope(&F, fx_resource(&F, req, "svc"), "scope"), "g", 2);
  return fx_pack(&F, &req->base, len);
}

/* ---- limits ------------------------------------------------------------------------------- */

static void test_decoded_budget_is_resource_exhausted(void) {
  otlp_config c = test_config();
  c.service.max_decoded_bytes = 32 * 1024;
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 40)->base, &len);
  TEST_ASSERT_TRUE(len < 512 * 1024); /* under the message limit, over the decoded budget */
  grpc_client_response resp;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED,
                        export_bytes(client, data, len, nullptr, 0, &resp));
  TEST_ASSERT_NULL(trailer(&resp, GRPC_STATUS_DETAILS_TRAILER)); /* not retryable */
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_UINT64(1, stats().too_large);
  /* a small request still fits */
  data = small_request(&len);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, data, len, nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_client_free(client);
}

static void test_authentication(void) {
  otlp_config c = test_config();
  c.service.auth_token = "t0ken";
  start(&c);
  grpc_client *client = connect_client(GRPC_COMPRESS_IDENTITY);
  size_t len;
  uint8_t *data = small_request(&len);
  grpc_client_response resp;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED,
                        export_bytes(client, data, len, nullptr, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_metadata wrong = {.key = "authorization", .value = "Bearer t0ke"};
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED,
                        export_bytes(client, data, len, &wrong, 1, &resp));
  grpc_client_response_free(&resp);
  grpc_metadata right = {.key = "authorization", .value = "Bearer t0ken"};
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, export_bytes(client, data, len, &right, 1, &resp));
  grpc_client_response_free(&resp);
  /* admin methods are protected too */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/otlp_metrics.Admin/Stats", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(1, s.requests); /* refused calls never reached the handler */
  TEST_ASSERT_EQUAL_UINT64(2, s.points_accepted);
  grpc_client_free(client);
}

/* Holds requests at the gate until the memory budget refuses one: the reservation bound. */
static void test_memory_limit_throttles(void) {
  otlp_config c = test_config();
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true, .gate_capacity = 1000};
  c.memory_limit = 256 * 1024;
  c.service.arena_keep_bytes = 0; /* so the decode arena returns everything it reserved */
  start(&c);
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 4)->base, &len);
  enum { MAX_CALLS = 128 };
  static bg_call calls[MAX_CALLS];
  memset(calls, 0, sizeof(calls));
  size_t n = 0;
  /* Each held request keeps its bytes and a copy at the gate; decoding needs more for a moment.
   * Whichever reservation fails first (request, decode arena or gate copy), the refusal is the
   * same retryable "memory limit reached". */
  while (n < MAX_CALLS) {
    bg_start(&calls[n], data, len);
    n++;
    wait_counter("requests", n);
    otlp_stats_snapshot s = stats();
    if (s.throttled_memory + s.sink_retryable > 0)
      break;
  }
  TEST_ASSERT_TRUE_MESSAGE(n >= 2 && n < MAX_CALLS, "the budget never refused a request");
  otlp_budget *budget = otlp_service_budget(otlp_app_service(g_app));
  TEST_ASSERT_TRUE(atomic_load(&budget->peak) <= budget->limit);
  TEST_ASSERT_EQUAL_size_t(n - 1, otlp_fault_sink_held(app_sink()));
  bg_join(&calls[n - 1]); /* refused at once */
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, calls[n - 1].status);
  TEST_ASSERT_TRUE(calls[n - 1].retry_info);
  TEST_ASSERT_EQUAL_UINT32(1000, calls[n - 1].retry_delay_ms);
  TEST_ASSERT_TRUE(calls[n - 1].retry_trailer);
  TEST_ASSERT_NOT_NULL(strstr(calls[n - 1].message, "memory limit reached"));

  otlp_fault_sink_set_gate(app_sink(), true);
  for (size_t i = 0; i + 1 < n; i++) {
    bg_join(&calls[i]);
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, calls[i].status);
  }
  wait_counter("requests_ok", n - 1);
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64((n - 1) * 20, s.points_accepted);
  TEST_ASSERT_EQUAL_UINT64((n - 1) * 20, stored_points());
  TEST_ASSERT_EQUAL_size_t(0, otlp_budget_used(budget)); /* every reservation released */
}

static void test_gate_capacity_is_retryable(void) {
  otlp_config c = test_config();
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true, .gate_capacity = 2};
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  bg_call calls[3] = {0};
  for (size_t i = 0; i < 3; i++) {
    bg_start(&calls[i], data, len);
    wait_counter("requests", i + 1);
  }
  bg_join(&calls[2]);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, calls[2].status);
  TEST_ASSERT_TRUE(calls[2].retry_info);
  TEST_ASSERT_NOT_NULL(strstr(calls[2].message, "fault gate is full"));
  otlp_fault_sink_set_gate(app_sink(), true);
  bg_join(&calls[0]);
  bg_join(&calls[1]);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, calls[0].status);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, calls[1].status);
  TEST_ASSERT_EQUAL_UINT64(4, stored_points());
}

/* ---- workers, queue, deadlines ------------------------------------------------------------ */

static void test_queue_full_is_retryable(void) {
  otlp_config c = test_config();
  c.service.workers = 1;
  c.service.queue_capacity = 2;
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true, .gate_blocks = true};
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  bg_call calls[4] = {0};
  bg_start(&calls[0], data, len);
  otlp_fault_sink_wait_held(app_sink(), 1); /* the only worker is stuck in the sink */
  for (size_t i = 1; i < 4; i++) {
    bg_start(&calls[i], data, len);
    wait_counter("requests", i + 1);
  }
  bg_join(&calls[3]); /* two queued, the fourth refused */
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, calls[3].status);
  TEST_ASSERT_TRUE(calls[3].retry_info);
  TEST_ASSERT_EQUAL_UINT64(1, stats().throttled_queue);
  otlp_fault_sink_set_gate(app_sink(), true);
  for (size_t i = 0; i < 3; i++) {
    bg_join(&calls[i]);
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, calls[i].status);
  }
  TEST_ASSERT_EQUAL_UINT64(6, stored_points());
  TEST_ASSERT_EQUAL_UINT64(0, stats().requests - stats().requests_ok - stats().requests_failed);
}

static void test_client_deadline_drops_batch(void) {
  otlp_config c = test_config();
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true};
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  bg_call call = {.timeout_ms = 200};
  bg_start(&call, data, len);
  bg_join(&call);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_DEADLINE_EXCEEDED, call.status);
  wait_counter("cancellations", 1); /* the server noticed */
  otlp_fault_sink_set_gate(app_sink(), true);
  wait_counter("requests_failed", 1);
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(1, s.cancelled);
  TEST_ASSERT_EQUAL_UINT64(0, s.points_accepted);
  TEST_ASSERT_EQUAL_UINT64(0, stored_points()); /* never stored for a client that gave up */
}

static void test_request_timeout_before_sink(void) {
  otlp_config c = test_config();
  c.service.workers = 1;
  c.service.request_timeout_ms = 100;
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true, .gate_blocks = true};
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  bg_call calls[2] = {0};
  bg_start(&calls[0], data, len);
  otlp_fault_sink_wait_held(app_sink(), 1);
  bg_start(&calls[1], data, len);
  wait_counter("requests", 2);
  /* let the queued request outlive the timeout: this waits for the clock itself */
  uv_sleep(150);
  otlp_fault_sink_set_gate(app_sink(), true);
  bg_join(&calls[0]);
  bg_join(&calls[1]);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, calls[0].status); /* already in the sink */
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_DEADLINE_EXCEEDED, calls[1].status);
  TEST_ASSERT_EQUAL_UINT64(1, stats().deadline_exceeded);
  TEST_ASSERT_EQUAL_UINT64(2, stored_points());
}

static void test_sink_failures(void) {
  otlp_config c = test_config();
  c.fault = true;
  c.fault_config = (otlp_fault_config){.retryable_every = 3, .fail_every = 2};
  c.service.retry_delay_ms = 250;
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  grpc_status expected[] = {GRPC_STATUS_OK, GRPC_STATUS_INTERNAL, GRPC_STATUS_UNAVAILABLE,
                            GRPC_STATUS_INTERNAL, GRPC_STATUS_OK};
  for (size_t i = 0; i < 5; i++) {
    bg_call call = {0};
    bg_start(&call, data, len);
    bg_join(&call);
    TEST_ASSERT_EQUAL_INT(expected[i], call.status);
    TEST_ASSERT_EQUAL_INT(expected[i] == GRPC_STATUS_UNAVAILABLE, call.retry_info);
    if (call.retry_info)
      TEST_ASSERT_EQUAL_UINT32(250, call.retry_delay_ms);
  }
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(2, s.sink_failed);
  TEST_ASSERT_EQUAL_UINT64(1, s.sink_retryable);
  TEST_ASSERT_EQUAL_UINT64(4, s.points_accepted);
  TEST_ASSERT_EQUAL_UINT64(6, s.points_failed);
  TEST_ASSERT_EQUAL_UINT64(4, stored_points()); /* only acknowledged data was stored */
}

/* ---- concurrency ---------------------------------------------------------------------------- */

enum { NB_THREADS = 8, CALLS_PER_THREAD = 150 };

typedef struct load_thread {
  uint16_t port;
  unsigned index;
  const uint8_t *payloads[3];
  size_t lens[3];
  pthread_t thread;
  /* results */
  uint64_t ok, partial, invalid, other, rejected;
} load_thread;

static void *load_main(void *arg) {
  load_thread *t = arg;
  grpc_err err;
  grpc_client *client =
      grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                            .port = t->port,
                                            .timeout_ms = 30000,
                                            .compression = t->index % 2 ? GRPC_COMPRESS_GZIP
                                                                        : GRPC_COMPRESS_IDENTITY},
                      &err);
  if (client == nullptr) {
    t->other = CALLS_PER_THREAD;
    return nullptr;
  }
  for (unsigned i = 0; i < CALLS_PER_THREAD; i++) {
    unsigned kind = (t->index + i) % 3;
    grpc_client_response resp = {0};
    err = grpc_client_unary(client, OTLP_EXPORT_PATH, t->payloads[kind], t->lens[kind], nullptr, 0,
                            0, &resp);
    bg_call r = {0};
    read_result(&r, &resp);
    if (err != GRPC_OK)
      t->other++;
    else if (r.status == GRPC_STATUS_OK && r.rejected == 0)
      t->ok++;
    else if (r.status == GRPC_STATUS_OK && r.rejected > 0) {
      t->partial++;
      t->rejected += (uint64_t)r.rejected;
    } else if (r.status == GRPC_STATUS_INVALID_ARGUMENT)
      t->invalid++;
    else
      t->other++;
    grpc_client_response_free(&resp);
  }
  grpc_client_free(client);
  return nullptr;
}

/* Many clients at once, valid, partially valid and malformed requests interleaved: every point
 * is accounted for exactly once and the sink holds exactly what was acknowledged. */
static void run_concurrent_accounting(unsigned loops, unsigned workers) {
  otlp_config c = test_config();
  c.loops = loops;
  c.service.workers = workers;
  c.memory_sink_bytes = (size_t)512 << 20;
  start(&c);
  size_t len_ok, len_partial;
  uint8_t *ok = fx_pack(&F, &fx_all_kinds(&F, 3)->base, &len_ok);          /* 15 points */
  otlp_pb_request *preq = fx_all_kinds(&F, 4);                             /* 20 points */
  preq->resource_metrics[0]->scope_metrics[0]->metrics[0]->name[0] = '\0'; /* -4: empty name */
  preq->resource_metrics[0]
      ->scope_metrics[0]
      ->metrics[4]
      ->summary->data_points[0]
      ->quantile_values[0]
      ->quantile = -1; /* -1 */
  uint8_t *partial = fx_pack(&F, &preq->base, &len_partial);
  static const uint8_t bad[] = {0x0a, 0x03, 0x12, 0x09};
  static load_thread threads[NB_THREADS];
  for (unsigned i = 0; i < NB_THREADS; i++) {
    threads[i] = (load_thread){.port = otlp_app_port(g_app),
                               .index = i,
                               .payloads = {ok, partial, bad},
                               .lens = {len_ok, len_partial, sizeof(bad)}};
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&threads[i].thread, nullptr, load_main, &threads[i]));
  }
  uint64_t ok_n = 0, partial_n = 0, invalid_n = 0, other_n = 0, rejected = 0;
  for (unsigned i = 0; i < NB_THREADS; i++) {
    TEST_ASSERT_EQUAL_INT(0, pthread_join(threads[i].thread, nullptr));
    ok_n += threads[i].ok;
    partial_n += threads[i].partial;
    invalid_n += threads[i].invalid;
    other_n += threads[i].other;
    rejected += threads[i].rejected;
  }
  uint64_t total = (uint64_t)NB_THREADS * CALLS_PER_THREAD;
  TEST_ASSERT_EQUAL_UINT64(0, other_n);
  TEST_ASSERT_EQUAL_UINT64(total, ok_n + partial_n + invalid_n);
  TEST_ASSERT_EQUAL_UINT64(partial_n * 5, rejected);
  uint64_t accepted = ok_n * 15 + partial_n * 15;
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(total, s.requests);
  TEST_ASSERT_EQUAL_UINT64(ok_n, s.requests_ok);
  TEST_ASSERT_EQUAL_UINT64(partial_n, s.requests_partial);
  TEST_ASSERT_EQUAL_UINT64(invalid_n, s.requests_failed);
  TEST_ASSERT_EQUAL_UINT64(accepted, s.points_accepted);
  TEST_ASSERT_EQUAL_UINT64(rejected, s.points_rejected);
  TEST_ASSERT_EQUAL_UINT64(s.points_received, s.points_accepted + s.points_rejected);
  TEST_ASSERT_EQUAL_UINT64(total / 2, s.encoding_gzip);
  TEST_ASSERT_EQUAL_UINT64(accepted, stored_points());
  otlp_budget *budget = otlp_service_budget(otlp_app_service(g_app));
  TEST_ASSERT_TRUE(atomic_load(&budget->peak) <= budget->limit);
}

static void test_concurrent_accounting_inline(void) {
  run_concurrent_accounting(1, 0);
}

static void test_concurrent_accounting_workers(void) {
  run_concurrent_accounting(1, 3);
}

static void test_concurrent_accounting_loops(void) {
#ifdef _WIN32
  TEST_IGNORE_MESSAGE("several loops need SO_REUSEPORT");
#else
  run_concurrent_accounting(4, 0);
#endif
}

static void test_concurrent_accounting_loops_and_workers(void) {
#ifdef _WIN32
  TEST_IGNORE_MESSAGE("several loops need SO_REUSEPORT");
#else
  run_concurrent_accounting(2, 2);
#endif
}

#ifdef OTLP_METRICS_WAL
#include "otlp_wal_format.h"

/* Concurrent clients, several loops and workers, the WAL sink: after a graceful stop the log holds
 * exactly the acknowledged points, in valid records. */
static void test_concurrent_accounting_wal(void) {
  uv_loop_t loop;
  uv_loop_init(&loop);
  uv_fs_t req;
  TEST_ASSERT_EQUAL_INT(0, uv_fs_mkdtemp(&loop, &req, "otlp-wal-server-XXXXXX", nullptr));
  char dir[256];
  snprintf(dir, sizeof(dir), "%s", req.path);
  uv_fs_req_cleanup(&req);

  otlp_config c = test_config();
#ifndef _WIN32
  c.loops = 2;
#endif
  c.service.workers = 2;
  c.sink = OTLP_SINK_KIND_WAL;
  c.wal_dir = dir;
  c.wal_sync = 0; /* page cache: the test checks accounting, not power-loss durability */
  c.wal_segment_bytes = 256 * 1024;
  start(&c);
  size_t len;
  uint8_t *data = fx_pack(&F, &fx_all_kinds(&F, 2)->base, &len); /* 10 points */
  static load_thread threads[NB_THREADS];
  static const uint8_t bad[] = {0x0a, 0x03, 0x12, 0x09};
  for (unsigned i = 0; i < NB_THREADS; i++) {
    threads[i] = (load_thread){.port = otlp_app_port(g_app),
                               .index = i,
                               .payloads = {data, data, bad},
                               .lens = {len, len, sizeof(bad)}};
    TEST_ASSERT_EQUAL_INT(0, pthread_create(&threads[i].thread, nullptr, load_main, &threads[i]));
  }
  uint64_t ok_n = 0;
  for (unsigned i = 0; i < NB_THREADS; i++) {
    TEST_ASSERT_EQUAL_INT(0, pthread_join(threads[i].thread, nullptr));
    TEST_ASSERT_EQUAL_UINT64(0, threads[i].other);
    ok_n += threads[i].ok;
  }
  otlp_stats_snapshot s = stats();
  TEST_ASSERT_EQUAL_UINT64(ok_n * 10, s.points_accepted);
  otlp_app_stop(g_app);
  TEST_ASSERT_EQUAL_INT(0, otlp_app_wait(g_app));
  g_app = nullptr;

  char **names;
  long n = otlp_wal_list_segments(dir, &names);
  TEST_ASSERT_TRUE(n >= 1);
  uint64_t records = 0, points = 0;
  for (long i = 0; i < n; i++) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, names[i]);
    otlp_wal_scan scan;
    otlp_wal_scan_segment(path, nullptr, nullptr, &scan);
    TEST_ASSERT_EQUAL_INT(OTLP_WAL_SCAN_CLEAN, scan.end);
    records += scan.records;
    points += scan.points;
    uv_fs_unlink(&loop, &req, path, nullptr);
    uv_fs_req_cleanup(&req);
  }
  otlp_wal_list_free(names, n);
  uv_fs_rmdir(&loop, &req, dir, nullptr);
  uv_fs_req_cleanup(&req);
  uv_loop_close(&loop);
  TEST_ASSERT_EQUAL_UINT64(ok_n, records);
  TEST_ASSERT_EQUAL_UINT64(ok_n * 10, points);
}
#endif

/* ---- shutdown ---------------------------------------------------------------------------------- */

typedef struct stopper {
  otlp_app *app;
  pthread_t thread;
  int status;
} stopper;

static void *stop_main(void *arg) {
  stopper *s = arg;
  otlp_app_stop(s->app);
  s->status = otlp_app_wait(s->app);
  return nullptr;
}

static void test_shutdown_fails_held_requests_as_retryable(void) {
  otlp_config c = test_config();
  c.drain_ms = 30000;
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true};
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  bg_call calls[3] = {0};
  for (size_t i = 0; i < 3; i++)
    bg_start(&calls[i], data, len);
  otlp_fault_sink_wait_held(app_sink(), 3);
  stopper st = {.app = g_app};
  g_app = nullptr; /* the stopper frees it */
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&st.thread, nullptr, stop_main, &st));
  TEST_ASSERT_EQUAL_INT(0, pthread_join(st.thread, nullptr));
  /* stopping quiesced the sink: the held batches were failed (not stored) and answered while the
   * server drained, so the drain completed */
  TEST_ASSERT_EQUAL_INT(0, st.status);
  for (size_t i = 0; i < 3; i++) {
    bg_join(&calls[i]);
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, calls[i].status);
    TEST_ASSERT_TRUE(calls[i].retry_info);
  }
}

typedef struct parked_wait {
  uint16_t port;
  pthread_t thread;
  grpc_status status;
} parked_wait;

static void *parked_wait_main(void *arg) {
  parked_wait *p = arg;
  grpc_err err;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = p->port, .timeout_ms = 60000}, &err);
  if (client == nullptr)
    return nullptr;
  grpc_client_response resp = {0};
  static const char body[] = "points_accepted 1000000000";
  if (grpc_client_unary(client, "/otlp_metrics.Admin/Wait", body, sizeof(body) - 1, nullptr, 0,
                        60000, &resp) == GRPC_OK)
    p->status = resp.status;
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  return nullptr;
}

/* A parked Admin/Wait call is an in-flight call: shutdown must answer it, not wait out the drain. */
static void test_shutdown_answers_parked_wait(void) {
  otlp_config c = test_config();
  c.drain_ms = 30000;
  start(&c);
  parked_wait p = {.port = otlp_app_port(g_app), .status = GRPC_STATUS_UNKNOWN};
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&p.thread, nullptr, parked_wait_main, &p));
  /* this Wait call is parked too, and counts: 2 means both are parked, in either order */
  wait_counter("admin_waits", 2);
  stopper st = {.app = g_app};
  g_app = nullptr;
  uint64_t t0 = uv_hrtime();
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&st.thread, nullptr, stop_main, &st));
  TEST_ASSERT_EQUAL_INT(0, pthread_join(st.thread, nullptr));
  TEST_ASSERT_EQUAL_INT(0, st.status); /* drained, not timed out */
  TEST_ASSERT_TRUE((uv_hrtime() - t0) / 1000000u < 20000);
  TEST_ASSERT_EQUAL_INT(0, pthread_join(p.thread, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, p.status);
}

static void test_shutdown_drains_requests_in_flight(void) {
  otlp_config c = test_config();
  c.drain_ms = 30000; /* the drain must outlast the request, which the test releases */
  c.service.workers = 1;
  c.fault = true;
  c.fault_config = (otlp_fault_config){.gate_closed = true, .gate_blocks = true};
  start(&c);
  size_t len;
  uint8_t *data = small_request(&len);
  bg_call call = {0};
  bg_start(&call, data, len);
  otlp_fault_sink_wait_held(app_sink(), 1);
  otlp_sink *sink = app_sink();
  stopper st = {.app = g_app};
  g_app = nullptr;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&st.thread, nullptr, stop_main, &st));
  otlp_fault_sink_set_gate(sink, true); /* the request completes while the app drains */
  TEST_ASSERT_EQUAL_INT(0, pthread_join(st.thread, nullptr));
  TEST_ASSERT_EQUAL_INT(0, st.status);
  bg_join(&call);
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, call.status);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_full_success);
  RUN_TEST(test_partial_success_exact_count);
  RUN_TEST(test_all_rejected_and_empty_requests);
  RUN_TEST(test_malformed_is_invalid_argument);
  RUN_TEST(test_compressed_request);
  RUN_TEST(test_oversized_is_resource_exhausted);
  RUN_TEST(test_decoded_budget_is_resource_exhausted);
  RUN_TEST(test_authentication);
  RUN_TEST(test_memory_limit_throttles);
  RUN_TEST(test_gate_capacity_is_retryable);
  RUN_TEST(test_queue_full_is_retryable);
  RUN_TEST(test_client_deadline_drops_batch);
  RUN_TEST(test_request_timeout_before_sink);
  RUN_TEST(test_sink_failures);
  RUN_TEST(test_concurrent_accounting_inline);
  RUN_TEST(test_concurrent_accounting_workers);
  RUN_TEST(test_concurrent_accounting_loops);
  RUN_TEST(test_concurrent_accounting_loops_and_workers);
#ifdef OTLP_METRICS_WAL
  RUN_TEST(test_concurrent_accounting_wal);
#endif
  RUN_TEST(test_shutdown_fails_held_requests_as_retryable);
  RUN_TEST(test_shutdown_drains_requests_in_flight);
  RUN_TEST(test_shutdown_answers_parked_wait);
  return UNITY_END();
}
