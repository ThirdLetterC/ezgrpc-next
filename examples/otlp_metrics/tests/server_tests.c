/* server_tests.c - the receiver end to end, in process, through the ezGRPC-next client
 *
 * Each test starts an app on an ephemeral port, talks to it with grpc_client (raw protobuf bytes
 * built with the generated types), and checks statuses, responses and counters. Tests that need
 * requests to stay in flight use the fault sink's gate, and wait for server-side conditions (the
 * Admin Wait method or the gate's held count), never for time.
 */
#include "otlp_threads.h"
#include <string.h>

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

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_full_success);
  RUN_TEST(test_partial_success_exact_count);
  RUN_TEST(test_all_rejected_and_empty_requests);
  RUN_TEST(test_malformed_is_invalid_argument);
  RUN_TEST(test_compressed_request);
  RUN_TEST(test_oversized_is_resource_exhausted);
  return UNITY_END();
}
