#include <nghttp2/nghttp2.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

static void *run_server_fn(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static bool bearer_validator(const char *token, [[maybe_unused]] void *userdata) {
  return strcmp(token, "valid-token") == 0;
}

/* ---- Handlers for comprehensive end-to-end integration tests ---------------- */

static void inspect_metadata_handler(grpc_call *call, const grpc_message *req,
                                     [[maybe_unused]] void *userdata) {
  TEST_ASSERT_NOT_NULL(grpc_call_request(call));
  TEST_ASSERT_NOT_NULL(grpc_call_peer(call));
  TEST_ASSERT_NOT_NULL(grpc_call_server(call));
  TEST_ASSERT_TRUE(grpc_call_metadata_count(call) >= 4);

  grpc_metadata first = {0};
  TEST_ASSERT_TRUE(grpc_call_metadata_at(call, 0, &first));
  TEST_ASSERT_FALSE(grpc_call_metadata_at(call, 9999, &first));

  /* Multi-value metadata iteration */
  size_t idx = 0, len = 0;
  int tag_count = 0;
  while (grpc_call_metadata_next(call, "x-tag", &idx, &len) != nullptr)
    tag_count++;

  /* Deadline check */
  int64_t rem = grpc_call_deadline(call);
  TEST_ASSERT_TRUE(rem > 0);

  /* Initial metadata + custom trailer */
  grpc_metadata init_md[] = {{.key = "x-init", .value = "yes"}};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_send_metadata(call, init_md, 1));
  grpc_metadata tr[] = {{.key = "x-tags-seen", .value = tag_count == 2 ? "2" : "0"}};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_add_trailers(call, tr, 1));

  /* Test per-call compression override */
  (void)grpc_call_set_compression(call, GRPC_COMPRESS_DEFLATE, 6);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_send_data(call, req->data, req->len));
  /* Setting compression or initial metadata after send returns GRPC_ERR_STATE */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE, grpc_call_set_compression(call, GRPC_COMPRESS_GZIP, -1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE, grpc_call_send_metadata(call, init_md, 1));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_finish(call, GRPC_STATUS_OK, nullptr));
}

static void rich_error_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                               [[maybe_unused]] void *userdata) {
  const uint8_t d1[] = {0x01, 0x02, 0x03};
  const uint8_t d2[] = {0x04, 0x05};
  grpc_status_detail_entry details[] = {
      {.type_url = "type.googleapis.com/google.rpc.ErrorInfo", .value = d1, .value_len = sizeof(d1)},
      {.type_url = "type.googleapis.com/google.rpc.RetryInfo", .value = d2, .value_len = sizeof(d2)},
  };
  /* Includes '%' and non-ASCII to exercise percent-encoding of grpc-message */
  (void)grpc_call_finish_with_details(call, GRPC_STATUS_INVALID_ARGUMENT, "100% bad\x01input",
                                      details, 2);
}

static void *async_worker_ok(void *arg) {
  grpc_call *call = arg;
  grpc_metadata md[] = {{.key = "x-async-md", .value = "1"}};
  grpc_metadata tr[] = {{.key = "x-async-tr", .value = "2"}};
  (void)grpc_call_send_metadata_async(call, md, 1);
  (void)grpc_call_send_data_async(call, "part1-", 6);
  (void)grpc_call_add_trailers_async(call, tr, 1);
  (void)grpc_call_reply_async(call, grpc_message_new("part2", 5));
  grpc_call_unref(call);
  return nullptr;
}

static void async_ok_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                             [[maybe_unused]] void *userdata) {
  grpc_call_ref(call);
  pthread_t th;
  if (pthread_create(&th, nullptr, async_worker_ok, call) == 0)
    pthread_detach(th);
  else
    grpc_call_unref(call);
}

static void *async_worker_err(void *arg) {
  grpc_call *call = arg;
  const uint8_t raw[] = {0xAA, 0xBB};
  grpc_status_detail_entry det = {
      .type_url = "type.googleapis.com/google.rpc.DebugInfo",
      .value = raw,
      .value_len = sizeof(raw),
  };
  (void)grpc_call_finish_with_details_async(call, GRPC_STATUS_FAILED_PRECONDITION, "async fail",
                                            &det, 1);
  grpc_call_unref(call);
  return nullptr;
}

static void async_err_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                              [[maybe_unused]] void *userdata) {
  grpc_call_ref(call);
  pthread_t th;
  if (pthread_create(&th, nullptr, async_worker_err, call) == 0)
    pthread_detach(th);
  else
    grpc_call_unref(call);
}

static void on_cancel_cb([[maybe_unused]] grpc_call *c, void *ud) {
  atomic_fetch_add((atomic_int *)ud, 1);
}

static void slow_cancel_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                void *userdata) {
  grpc_call_on_cancel(call, on_cancel_cb, userdata);
}

static void slow_deadline_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                  [[maybe_unused]] void *userdata) {
  /* Intentionally never finishes so server deadline timer fires DEADLINE_EXCEEDED */
  (void)call;
}

static void backpressure_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                 void *userdata) {
  int *hit_limit = userdata;
  char chunk[4096];
  memset(chunk, 'A', sizeof(chunk));
  for (int i = 0; i < 4; i++) {
    grpc_err err = grpc_call_send_data(call, chunk, sizeof(chunk));
    if (err == GRPC_ERR_LIMIT) {
      *hit_limit = 1;
      break;
    }
  }
  TEST_ASSERT_TRUE(grpc_call_send_queue_bytes(call) > 0);
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void large_echo_handler(grpc_call *call, const grpc_message *req,
                               [[maybe_unused]] void *userdata) {
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static grpc_status reject_recv_bad([[maybe_unused]] grpc_call *call, const grpc_message *msg,
                                   const char **err_msg, [[maybe_unused]] void *userdata) {
  if (msg->len == 3 && memcmp(msg->data, "BAD", 3) == 0) {
    *err_msg = "blocked by recv interceptor";
    return GRPC_STATUS_PERMISSION_DENIED;
  }
  return GRPC_STATUS_OK;
}

static grpc_status reject_send_bad([[maybe_unused]] grpc_call *call, const grpc_message *msg,
                                   const char **err_msg, [[maybe_unused]] void *userdata) {
  if (msg->len == 7 && memcmp(msg->data, "BADSEND", 7) == 0) {
    *err_msg = "blocked by send interceptor";
    return GRPC_STATUS_INTERNAL;
  }
  return GRPC_STATUS_OK;
}

/* ---- Integration Test 1: Metadata, Async, Rich Errors, Interceptors, Compression ---- */

void test_integration_full_server_and_client(void) {
  int bp_hit = 0;
  grpc_server *server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_message_size = 256 * 1024,
      .max_send_queue_bytes = 4096,
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Meta",
                                                    inspect_metadata_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/RichErr",
                                                    rich_error_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/AsyncOk",
                                                    async_ok_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/AsyncErr",
                                                    async_err_handler, nullptr));
  atomic_int cancel_hits = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Cancel",
                                                    slow_cancel_handler, &cancel_hits));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Deadline",
                                                    slow_deadline_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Backpressure",
                                                    backpressure_handler, &bp_hit));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/LargeEcho",
                                                    large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/auth.Svc/Secret",
                                                    large_echo_handler, nullptr));

  grpc_interceptor auth = grpc_interceptor_bearer_auth(bearer_validator, nullptr);
  auth.path_prefix = "/auth.Svc/";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(server, &auth));

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_add_interceptor(server, &(grpc_interceptor){
                                                       .name = "guard",
                                                       .path_prefix = "/int.Svc/LargeEcho",
                                                       .on_recv_message = reject_recv_bad,
                                                       .on_send_message = reject_send_bad,
                                                   }));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, server));

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(server),
          .timeout_ms = 5000,
          .compression = GRPC_COMPRESS_GZIP,
      },
      &err);
  TEST_ASSERT_NOT_NULL(client);

  /* 1. Metadata, repeated keys, deadline, initial md, trailers, deflate compression */
  grpc_metadata md[] = {
      {.key = "x-tag", .value = "alpha"},
      {.key = "x-tag", .value = "beta"},
  };
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Meta", "payload", 7, md, 2, 3000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_MEMORY("payload", resp.message->data, 7);
  TEST_ASSERT_TRUE(resp.nb_headers > 0);
  TEST_ASSERT_TRUE(resp.nb_trailers > 0);
  grpc_client_response_free(&resp);

  /* 2. Rich error details + percent-encoded grpc-message */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/RichErr", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INVALID_ARGUMENT, resp.status);
  const char *details_b64 = nullptr;
  for (size_t i = 0; i < resp.nb_trailers; i++) {
    if (strcmp(resp.trailers[i].key, GRPC_STATUS_DETAILS_TRAILER) == 0)
      details_b64 = resp.trailers[i].value;
  }
  TEST_ASSERT_NOT_NULL(details_b64);
  grpc_status_details decoded = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_status_details_decode_all(details_b64, &decoded));
  TEST_ASSERT_EQUAL_UINT64(2, decoded.nb_details);
  grpc_status_details_free(&decoded);
  grpc_client_response_free(&resp);

  /* 3. Worker-thread async reply & async finish_with_details */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/AsyncOk", "x", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/AsyncErr", "x", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_FAILED_PRECONDITION, resp.status);
  grpc_client_response_free(&resp);

  /* 4. Bearer auth: missing, malformed, wrong token, valid token */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "s", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);

  grpc_metadata bad_fmt[] = {{.key = "authorization", .value = "Basic abc"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "s", 1, bad_fmt, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);

  grpc_metadata bad_tok[] = {{.key = "authorization", .value = "Bearer wrong"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "s", 1, bad_tok, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED, resp.status);
  grpc_client_response_free(&resp);

  grpc_metadata good_tok[] = {{.key = "authorization", .value = "Bearer valid-token"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/auth.Svc/Secret", "ok", 2, good_tok, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* 5. Message-level recv & send interceptor rejections */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/LargeEcho", "BAD", 3, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_PERMISSION_DENIED, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/LargeEcho", "BADSEND", 7, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, resp.status);
  grpc_client_response_free(&resp);

  /* 6. Large multi-frame message (> 32 KiB) & unknown method (UNIMPLEMENTED) */
  char *big = malloc(48 * 1024);
  TEST_ASSERT_NOT_NULL(big);
  memset(big, 'Z', 48 * 1024);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK,
      grpc_client_unary(client, "/int.Svc/LargeEcho", big, 48 * 1024, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(48 * 1024, resp.message->len);
  grpc_client_response_free(&resp);
  free(big);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/no.Such/Method", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNIMPLEMENTED, resp.status);
  grpc_client_response_free(&resp);

  /* 7. Backpressure limit trigger */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Backpressure", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_INT(1, bp_hit);
  grpc_client_response_free(&resp);

  /* 8. Server-side deadline expiration (grpc-timeout: 50m) & client stream cancel */
  grpc_metadata short_dl[] = {{.key = "grpc-timeout", .value = "50m"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Deadline", "", 0, short_dl, 1, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_DEADLINE_EXCEEDED, resp.status);
  grpc_client_response_free(&resp);

  grpc_client_stream *cs_cancel =
      grpc_client_stream_open(client, "/int.Svc/Cancel", nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(cs_cancel);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs_cancel, "wait", 4));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(cs_cancel));
  uv_sleep(20);
  grpc_client_stream_cancel(cs_cancel);
  uv_sleep(30);
  TEST_ASSERT_TRUE(atomic_load(&cancel_hits) >= 1);

  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}

/* ---- Integration Test 2: Health Check & Watch + Reflection v1/v1alpha ------- */

void test_integration_health_watch_and_reflection(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/demo.Echo/Ping",
                                                    large_echo_handler, nullptr));
  const uint8_t fake_fdp[] = {0x0A, 0x0A, 'd', 'e', 'm', 'o', '.', 'p', 'r', 'o', 't', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_add_file_descriptor(
                                     server, "demo.proto", fake_fdp, sizeof(fake_fdp)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_add_file_descriptor(
                                     server, "demo.Echo", fake_fdp, sizeof(fake_fdp)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_set_status(server, "demo.Echo", GRPC_HEALTH_SERVING));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, server));

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server), .timeout_ms = 3000},
      &err);
  TEST_ASSERT_NOT_NULL(client);

  /* 1. Health Check for "" and "demo.Echo" */
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, GRPC_HEALTH_PATH_CHECK, "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(2, resp.message->len);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);

  /* HealthCheckRequest { service = "demo.Echo" } */
  const uint8_t hreq[] = {0x0A, 9, 'd', 'e', 'm', 'o', '.', 'E', 'c', 'h', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, GRPC_HEALTH_PATH_CHECK, hreq,
                                                   sizeof(hreq), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, resp.message->data[1]);
  grpc_client_response_free(&resp);

  /* 2. Health Watch: receive initial SERVING, update to NOT_SERVING, receive update, cancel */
  grpc_client_stream *watch =
      grpc_client_stream_open(client, GRPC_HEALTH_PATH_WATCH, nullptr, 0, 3000, &err);
  TEST_ASSERT_NOT_NULL(watch);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(watch, hreq, sizeof(hreq)));
  grpc_message *wmsg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(watch, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_SERVING, wmsg->data[1]);
  grpc_message_free(wmsg);

  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_health_set_status(server, "demo.Echo", GRPC_HEALTH_NOT_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(watch, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_EQUAL_UINT8(GRPC_HEALTH_NOT_SERVING, wmsg->data[1]);
  grpc_message_free(wmsg);
  grpc_client_stream_cancel(watch);

  /* 3. Reflection v1: list_services (field 7), file_by_filename (field 3), file_containing_symbol (field 4) */
  grpc_client_stream *ref =
      grpc_client_stream_open(client, GRPC_REFLECTION_V1_PATH, nullptr, 0, 3000, &err);
  TEST_ASSERT_NOT_NULL(ref);

  /* list_services = "" -> tag (7<<3)|2 = 0x3A, len 0 */
  const uint8_t req_list[] = {0x3A, 0x00};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_list, sizeof(req_list)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_TRUE(wmsg->len > 0);
  grpc_message_free(wmsg);

  /* file_by_filename = "demo.proto" -> tag (3<<3)|2 = 0x1A */
  const uint8_t req_file[] = {0x1A, 10, 'd', 'e', 'm', 'o', '.', 'p', 'r', 'o', 't', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_file, sizeof(req_file)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_TRUE(wmsg->len > 0);
  grpc_message_free(wmsg);

  /* file_containing_symbol = "demo.Echo" -> tag (4<<3)|2 = 0x22 */
  const uint8_t req_sym[] = {0x22, 9, 'd', 'e', 'm', 'o', '.', 'E', 'c', 'h', 'o'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_sym, sizeof(req_sym)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  TEST_ASSERT_TRUE(wmsg->len > 0);
  grpc_message_free(wmsg);

  /* missing symbol -> error_response */
  const uint8_t req_miss[] = {0x22, 4, 'n', 'o', 'p', 'e'};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(ref, req_miss, sizeof(req_miss)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(ref, &wmsg));
  TEST_ASSERT_NOT_NULL(wmsg);
  grpc_message_free(wmsg);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(ref, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}

/* ---- Integration Test 3: Poll-driven events, Keepalive/Idle, Max Conns ------ */

typedef struct poll_client_arg {
  uint16_t port;
  int ok;
} poll_client_arg;

static void *run_poll_client(void *arg) {
  poll_client_arg *a = arg;
  grpc_err err = GRPC_OK;
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = a->port, .timeout_ms = 2000}, &err);
  if (c == nullptr)
    return nullptr;
  grpc_client_response resp = {0};
  if (grpc_client_unary(c, "/poll.Svc/Echo", "ev", 2, nullptr, 0, 0, &resp) == GRPC_OK &&
      resp.status == GRPC_STATUS_OK && resp.message != nullptr && resp.message->len == 2) {
    a->ok = 1;
  }
  grpc_client_response_free(&resp);
  grpc_client_free(c);
  return nullptr;
}

void test_integration_poll_events_and_maintenance(void) {
  grpc_server *server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connections = 2,
      .keepalive_time_ms = 50,
      .keepalive_timeout_ms = 500,
      .max_connection_idle_ms = 120,
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_register_path(server, "/poll.Svc/Echo", nullptr));

  poll_client_arg carg = {.port = grpc_server_port(server), .ok = 0};
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_poll_client, &carg));

  int replied = 0;
  for (int iter = 0; iter < 50 && !replied; iter++) {
    int n = grpc_server_poll(server, 50);
    if (n > 0) {
      size_t count = 0;
      const grpc_event *evs = grpc_server_events(server, &count);
      for (size_t i = 0; i < count; i++) {
        if (evs[i].type == GRPC_EVENT_MESSAGE) {
          (void)grpc_call_reply_data(evs[i].call, evs[i].message->data, evs[i].message->len);
          replied = 1;
        }
      }
      grpc_server_flush(server);
    }
  }
  pthread_join(th, nullptr);
  TEST_ASSERT_EQUAL_INT(1, replied);
  TEST_ASSERT_EQUAL_INT(1, carg.ok);

  /* Exercise maintenance timer tick with an active idle client (PING + GOAWAY + close) */
  grpc_err cerr = GRPC_OK;
  grpc_client *idle_c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server)}, &cerr);
  TEST_ASSERT_NOT_NULL(idle_c);
  /* Open a 2nd & 3rd client to hit max_connections=2 rejection */
  grpc_client *c2 = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server)}, &cerr);
  grpc_client *c3 = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server)}, &cerr);
  for (int i = 0; i < 6; i++)
    (void)grpc_server_poll(server, 60);
  grpc_client_free(c3);
  grpc_client_free(c2);
  grpc_client_free(idle_c);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_drain(server, 100));
  grpc_server_free(server);
}

/* ---- Integration Test 4: gRPC-Web, gRPC-Web-Text, CORS OPTIONS, 415, IPv6 --- */

typedef struct raw_h2_buf {
  uint8_t *data;
  size_t len;
  size_t cap;
} raw_h2_buf;

static void raw_h2_send_all(int fd, const uint8_t *data, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = send(fd, (const char *)(data + off), len - off, 0);
    if (n <= 0)
      break;
    off += (size_t)n;
  }
}

typedef struct raw_h2_payload {
  const uint8_t *data;
  size_t len;
  size_t off;
} raw_h2_payload;

static ssize_t raw_h2_read_cb([[maybe_unused]] nghttp2_session *s, [[maybe_unused]] int32_t sid,
                              uint8_t *buf, size_t len, uint32_t *flags, nghttp2_data_source *src,
                              [[maybe_unused]] void *ud) {
  raw_h2_payload *p = src->ptr;
  size_t rem = p->len - p->off;
  size_t n = rem < len ? rem : len;
  if (n > 0) {
    memcpy(buf, p->data + p->off, n);
    p->off += n;
  }
  if (p->off == p->len)
    *flags = NGHTTP2_DATA_FLAG_EOF;
  return (ssize_t)n;
}

static int raw_h2_on_frame([[maybe_unused]] nghttp2_session *s, const nghttp2_frame *f, void *ud) {
  int *done = ud;
  if (f->hd.stream_id > 0 && (f->hd.flags & NGHTTP2_FLAG_END_STREAM))
    (*done)++;
  return 0;
}

static void run_raw_h2_requests(uint16_t port) {
#ifndef _WIN32
  int fd = socket(AF_INET6, SOCK_STREAM, 0);
  if (fd < 0)
    return;
  struct sockaddr_in6 sin6 = {0};
  sin6.sin6_family = AF_INET6;
  sin6.sin6_port = htons(port);
  inet_pton(AF_INET6, "::1", &sin6.sin6_addr);
  if (connect(fd, (struct sockaddr *)&sin6, sizeof(sin6)) != 0) {
    close(fd);
    return;
  }

  int streams_done = 0;
  nghttp2_session_callbacks *cbs = nullptr;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, raw_h2_on_frame);
  nghttp2_session *sess = nullptr;
  nghttp2_session_client_new(&sess, cbs, &streams_done);
  nghttp2_session_callbacks_del(cbs);
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, nullptr, 0);

  /* 1. CORS OPTIONS preflight */
  nghttp2_nv opt_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"OPTIONS", 7, 7, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, opt_hdrs, 4, nullptr, nullptr);

  /* 2. Unsupported content-type (415) */
  nghttp2_nv bad_ct[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"text/plain", 12, 10, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, bad_ct, 5, nullptr, nullptr);

  /* 3. gRPC-Web binary request */
  const uint8_t web_frame[] = {0x00, 0x00, 0x00, 0x00, 0x02, 'h', 'i'};
  raw_h2_payload p_web = {.data = web_frame, .len = sizeof(web_frame), .off = 0};
  nghttp2_data_provider dp_web = {.source.ptr = &p_web, .read_callback = raw_h2_read_cb};
  nghttp2_nv web_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc-web+proto", 12, 26,
       NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, web_hdrs, 5, &dp_web, nullptr);

  /* 4. gRPC-Web-Text (Base64) request */
  size_t b64_len = 0;
  char *b64 = grpc_metadata_encode_bin(web_frame, sizeof(web_frame), &b64_len);
  raw_h2_payload p_txt = {.data = (const uint8_t *)b64, .len = b64_len, .off = 0};
  nghttp2_data_provider dp_txt = {.source.ptr = &p_txt, .read_callback = raw_h2_read_cb};
  nghttp2_nv txt_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc-web-text", 12, 25,
       NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, txt_hdrs, 5, &dp_txt, nullptr);

  /* Flush outbound HTTP/2 frames */
  for (;;) {
    const uint8_t *out;
    ssize_t n = nghttp2_session_mem_send(sess, &out);
    if (n <= 0)
      break;
    raw_h2_send_all(fd, out, (size_t)n);
  }

  struct timeval tv = {.tv_sec = 0, .tv_usec = 200000};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  uint8_t rbuf[4096];
  for (int i = 0; i < 20 && streams_done < 4; i++) {
    ssize_t n = recv(fd, rbuf, sizeof(rbuf), 0);
    if (n <= 0)
      break;
    nghttp2_session_mem_recv(sess, rbuf, (size_t)n);
    for (;;) {
      const uint8_t *out;
      ssize_t wn = nghttp2_session_mem_send(sess, &out);
      if (wn <= 0)
        break;
      raw_h2_send_all(fd, out, (size_t)wn);
    }
  }
  TEST_ASSERT_TRUE(streams_done >= 4);

  /* 5. Rapid PINGs to trigger ping-strike ENHANCE_YOUR_CALM */
  static const uint8_t ping_bytes[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  for (int i = 0; i < 4; i++)
    nghttp2_submit_ping(sess, NGHTTP2_FLAG_NONE, ping_bytes);
  for (;;) {
    const uint8_t *out;
    ssize_t wn = nghttp2_session_mem_send(sess, &out);
    if (wn <= 0)
      break;
    raw_h2_send_all(fd, out, (size_t)wn);
  }
  (void)recv(fd, rbuf, sizeof(rbuf), 0);

  grpc_free(b64);
  nghttp2_session_del(sess);
  close(fd);
#else
  (void)port;
#endif
}

void test_integration_grpc_web_cors_ipv6_and_client_edge_cases(void) {
  grpc_server *server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv6_addr = "::1",
      .max_message_size = 1024,
      .max_ping_strikes = 1,
      .min_ping_interval_ms = 5000,
      .max_connection_age_ms = 5000,
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/web.Svc/Echo", large_echo_handler,
                                                    nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/web.Svc/Slow", slow_deadline_handler,
                                                    nullptr));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, server));
  uint16_t port = grpc_server_port(server);

  run_raw_h2_requests(port);

  /* Client over IPv6 target, codecs (deflate, zstd, snappy), max_message_size, timeout & errors */
  grpc_err err = GRPC_OK;
  TEST_ASSERT_NULL(grpc_client_new(nullptr, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  const char *bad_t[] = {"no-port"};
  TEST_ASSERT_NULL(
      grpc_client_new(&(grpc_client_config){.targets = bad_t, .nb_targets = 1}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  char ep6[64];
  snprintf(ep6, sizeof(ep6), "[::1]:%u", port);
  const char *t6[] = {ep6};

  const grpc_compression_algorithm algs[] = {
      GRPC_COMPRESS_DEFLATE,
#if defined(GRPC_ENABLE_ZSTD)
      GRPC_COMPRESS_ZSTD,
#endif
#if defined(GRPC_ENABLE_SNAPPY)
      GRPC_COMPRESS_SNAPPY,
#endif
  };
  for (size_t i = 0; i < sizeof(algs) / sizeof(algs[0]); i++) {
    grpc_client *c = grpc_client_new(
        &(grpc_client_config){
            .targets = t6,
            .nb_targets = 1,
            .compression = algs[i],
            .timeout_ms = 2000,
        },
        &err);
    TEST_ASSERT_NOT_NULL(c);
    grpc_client_response resp = {0};
    TEST_ASSERT_EQUAL_INT(
        GRPC_OK, grpc_client_unary(c, "/web.Svc/Echo", "codec-test", 10, nullptr, 0, 0, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    TEST_ASSERT_NOT_NULL(resp.message);
    TEST_ASSERT_EQUAL_MEMORY("codec-test", resp.message->data, 10);
    grpc_client_response_free(&resp);
    grpc_client_free(c);
  }

  /* Client timeout (GRPC_ERR_TIMEOUT) & oversized request (RESOURCE_EXHAUSTED) */
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){.targets = t6, .nb_targets = 1, .timeout_ms = 2000}, &err);
  TEST_ASSERT_NOT_NULL(c);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_TIMEOUT, grpc_client_unary(c, "/web.Svc/Slow", "", 0, nullptr, 0, 40, &resp));
  grpc_client_response_free(&resp);

  char huge[2048];
  memset(huge, 'X', sizeof(huge));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/web.Svc/Echo", huge, sizeof(huge), nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, resp.status);
  grpc_client_response_free(&resp);

  /* Stream EOF recv + post-close_send GRPC_ERR_STATE + null safety */
  TEST_ASSERT_NULL(grpc_client_stream_open(nullptr, "/a/b", nullptr, 0, 0, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_send(nullptr, "x", 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_close_send(nullptr));
  grpc_message *m = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_recv(nullptr, &m));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_client_stream_finish(nullptr, &resp));
  grpc_client_stream_cancel(nullptr);

  grpc_trace_context bad_tc = {0};
  char tp_buf[64];
  TEST_ASSERT_FALSE(grpc_call_trace_context(nullptr, &bad_tc));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_traceparent_format(&bad_tc, tp_buf, sizeof(tp_buf)));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_set_fallback(nullptr, nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_fallback(server, nullptr, nullptr));

  grpc_client_stream *st = grpc_client_stream_open(c, "/web.Svc/Echo", nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(st);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(st, "hi", 2));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(st));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE, grpc_client_stream_send(st, "no", 2));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(st, &m));
  TEST_ASSERT_NOT_NULL(m);
  grpc_message_free(m);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(st, &m));
  TEST_ASSERT_NULL(m);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(st, &resp));
  grpc_client_response_free(&resp);

  grpc_client_free(c);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_integration_full_server_and_client);
  RUN_TEST(test_integration_health_watch_and_reflection);
  RUN_TEST(test_integration_poll_events_and_maintenance);
  RUN_TEST(test_integration_grpc_web_cors_ipv6_and_client_edge_cases);
  return UNITY_END();
}
