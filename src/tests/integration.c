#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <nghttp2/nghttp2.h>
#include <signal.h>
#endif
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

  /* Deadline check + traceparent hex a-f check */
  int64_t rem = grpc_call_deadline(call);
  TEST_ASSERT_TRUE(rem > 0);
  grpc_trace_context tc = {0};
  TEST_ASSERT_TRUE(grpc_call_trace_context(call, &tc));
  TEST_ASSERT_EQUAL_UINT8(0xaf, tc.trace_flags);

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

static void writable_cb(grpc_call *call, void *userdata) {
  int *fired = userdata;
  *fired = 1;
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void backpressure_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                 void *userdata) {
  int *hit_limit = userdata;
  grpc_call_on_writable(call, writable_cb, hit_limit + 1);
  char chunk[4096];
  memset(chunk, 'A', sizeof(chunk));
  for (int i = 0; i < 4; i++) {
    grpc_err err = grpc_call_send_data(call, chunk, sizeof(chunk));
    if (err == GRPC_ERR_LIMIT) {
      hit_limit[0] = 1;
      break;
    }
  }
  TEST_ASSERT_TRUE(grpc_call_send_queue_bytes(call) > 0);
  if (!hit_limit[0])
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
  int bp_hit[2] = {0, 0};
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
                                                    slow_cancel_handler, (void *)&cancel_hits));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Deadline",
                                                    slow_deadline_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/int.Svc/Backpressure",
                                                    backpressure_handler, bp_hit));
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

  /* 1. Metadata, repeated keys, deadline, initial md, trailers, deflate compression, traceparent a-f */
  grpc_metadata md[] = {
      {.key = "x-tag", .value = "alpha"},
      {.key = "x-tag", .value = "beta"},
      {.key = "traceparent", .value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-af"},
  };
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Meta", "payload", 7, md, 3, 3000, &resp));
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

  /* 7. Backpressure limit trigger + on_writable callback firing */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/int.Svc/Backpressure", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_INT(1, bp_hit[0]);
  TEST_ASSERT_EQUAL_INT(1, bp_hit[1]);
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

#ifndef _WIN32
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

static ssize_t raw_h2_chunked_read_cb([[maybe_unused]] nghttp2_session *s,
                                      [[maybe_unused]] int32_t stream_id, uint8_t *buf, size_t len,
                                      uint32_t *flags, nghttp2_data_source *src,
                                      [[maybe_unused]] void *ud) {
  raw_h2_payload *p = src->ptr;
  size_t rem = p->len - p->off;
  /* Cap first chunk at 200 bytes so server parses msg1 (155B) and leaves 45B at recv_start=155 */
  size_t cap = p->off == 0 ? 200 : rem;
  size_t n = rem < cap ? rem : cap;
  if (n > len)
    n = len;
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
  if (f->hd.stream_id > 0 &&
      ((f->hd.flags & NGHTTP2_FLAG_END_STREAM) || f->hd.type == NGHTTP2_RST_STREAM))
    (*done)++;
  return 0;
}
#endif

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

  /* 5. gRPC-Web trailers-only (handler calls finish without send) */
  raw_h2_payload p_nobody = {.data = web_frame, .len = sizeof(web_frame), .off = 0};
  nghttp2_data_provider dp_nobody = {.source.ptr = &p_nobody, .read_callback = raw_h2_read_cb};
  nghttp2_nv nobody_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/NoBody", 5, 15, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc-web", 12, 20, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, nobody_hdrs, 5, &dp_nobody, nullptr);

  /* 6. gRPC-Web-Text invalid base64 -> INVALID_ARGUMENT */
  const uint8_t bad_b64[] = "!!!not-base64!!!";
  raw_h2_payload p_badb64 = {.data = bad_b64, .len = sizeof(bad_b64) - 1, .off = 0};
  nghttp2_data_provider dp_badb64 = {.source.ptr = &p_badb64, .read_callback = raw_h2_read_cb};
  nghttp2_submit_request(sess, nullptr, txt_hdrs, 5, &dp_badb64, nullptr);

  /* 7. gRPC-Web-Text truncated message -> DATA_LOSS */
  const uint8_t trunc_frame[] = {0x00, 0x00, 0x00, 0x00, 0x10, 'a'};
  size_t tb64_len = 0;
  char *tb64 = grpc_metadata_encode_bin(trunc_frame, sizeof(trunc_frame), &tb64_len);
  raw_h2_payload p_tb64 = {.data = (const uint8_t *)tb64, .len = tb64_len, .off = 0};
  nghttp2_data_provider dp_tb64 = {.source.ptr = &p_tb64, .read_callback = raw_h2_read_cb};
  nghttp2_submit_request(sess, nullptr, txt_hdrs, 5, &dp_tb64, nullptr);

  /* 8. Raw gRPC truncated message + grpc-encoding: identity -> DATA_LOSS */
  raw_h2_payload p_trunc = {.data = trunc_frame, .len = sizeof(trunc_frame), .off = 0};
  nghttp2_data_provider dp_trunc = {.source.ptr = &p_trunc, .read_callback = raw_h2_read_cb};
  nghttp2_nv id_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"grpc-encoding", (uint8_t *)"identity", 13, 8, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, id_hdrs, 7, &dp_trunc, nullptr);

  /* 9 & 10. Oversized gzip-compressed message (both gRPC and gRPC-Web-Text) -> RESOURCE_EXHAUSTED */
  char big_plain[2048];
  memset(big_plain, 'Z', sizeof(big_plain));
  uint8_t *zbuf = nullptr;
  size_t zlen = 0;
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_compress(GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT,
                             (const uint8_t *)big_plain, sizeof(big_plain), &zbuf, &zlen));
  uint8_t *zframe = grpc_malloc(5 + zlen);
  TEST_ASSERT_NOT_NULL(zframe);
  zframe[0] = 0x01;
  uint32_t zlen_be = htonl((uint32_t)zlen);
  memcpy(zframe + 1, &zlen_be, 4);
  memcpy(zframe + 5, zbuf, zlen);
  grpc_free(zbuf);

  raw_h2_payload p_zraw = {.data = zframe, .len = 5 + zlen, .off = 0};
  nghttp2_data_provider dp_zraw = {.source.ptr = &p_zraw, .read_callback = raw_h2_read_cb};
  nghttp2_nv gz_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"grpc-encoding", (uint8_t *)"gzip", 13, 4, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, gz_hdrs, 7, &dp_zraw, nullptr);

  size_t zb64_len = 0;
  char *zb64 = grpc_metadata_encode_bin(zframe, 5 + zlen, &zb64_len);
  raw_h2_payload p_zb64 = {.data = (const uint8_t *)zb64, .len = zb64_len, .off = 0};
  nghttp2_data_provider dp_zb64 = {.source.ptr = &p_zb64, .read_callback = raw_h2_read_cb};
  nghttp2_nv gz_txt_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc-web-text", 12, 25,
       NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"grpc-encoding", (uint8_t *)"gzip", 13, 4, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, gz_txt_hdrs, 6, &dp_zb64, nullptr);

  /* 10b. grpc-accept-encoding with trailing space not matching server's gzip */
  const uint8_t gz_req_frame[] = {0x00, 0x00, 0x00, 0x00, 0x04, 'g', 'z', 'i', 'p'};
  raw_h2_payload p_ae = {.data = gz_req_frame, .len = sizeof(gz_req_frame), .off = 0};
  nghttp2_data_provider dp_ae = {.source.ptr = &p_ae, .read_callback = raw_h2_read_cb};
  nghttp2_nv ae_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Codec", 5, 14, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"grpc-accept-encoding", (uint8_t *)"deflate ", 20, 8, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, ae_hdrs, 7, &dp_ae, nullptr);

  /* 10c. Headers-only END_STREAM (no DATA) on valid gRPC path -> half_closed + call_queue */
  nghttp2_nv empty_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/NoBody", 5, 15, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, empty_hdrs, 6, nullptr, nullptr);

  /* 10d. Invalid compressed payload (cflag=1, bad gzip) + trailing 2 bytes after msg (L27, L56) */
  const uint8_t bad_gz_frame[] = {0x01, 0x00, 0x00, 0x00, 0x03, 'b', 'a', 'd', 0x00, 0x00};
  raw_h2_payload p_bgz = {.data = bad_gz_frame, .len = sizeof(bad_gz_frame), .off = 0};
  nghttp2_data_provider dp_bgz = {.source.ptr = &p_bgz, .read_callback = raw_h2_read_cb};
  nghttp2_submit_request(sess, nullptr, gz_hdrs, 7, &dp_bgz, nullptr);

  /* 10e. Two-chunk DATA stream to trigger recv_data memmove compaction (recv_start > 0) */
  uint8_t compact_buf[310];
  compact_buf[0] = 0;
  uint32_t m150 = htonl(150);
  memcpy(compact_buf + 1, &m150, 4);
  memset(compact_buf + 5, 'A', 150);
  compact_buf[155] = 0;
  memcpy(compact_buf + 156, &m150, 4);
  memset(compact_buf + 160, 'B', 150);
  raw_h2_payload p_cmp = {.data = compact_buf, .len = sizeof(compact_buf), .off = 0};
  nghttp2_data_provider dp_cmp = {.source.ptr = &p_cmp, .read_callback = raw_h2_chunked_read_cb};
  nghttp2_nv cmp_hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/web.Svc/Echo", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, cmp_hdrs, 6, &dp_cmp, nullptr);

  /* Flush outbound HTTP/2 frames */
  for (;;) {
    const uint8_t *out;
    ssize_t n = nghttp2_session_mem_send(sess, &out);
    if (n <= 0)
      break;
    raw_h2_send_all(fd, out, (size_t)n);
  }

  struct timeval tv = {.tv_sec = 2, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  uint8_t rbuf[4096];
  for (int i = 0; i < 200 && streams_done < 14; i++) {
    ssize_t n = recv(fd, rbuf, sizeof(rbuf), 0);
    if (n < 0 && errno == EINTR)
      continue;
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
  TEST_ASSERT_TRUE(streams_done >= 12);

  /* 11. Too many headers (> 256) + Rapid PINGs to trigger ping-strike ENHANCE_YOUR_CALM */
  nghttp2_nv many_hdrs[265];
  many_hdrs[0] = (nghttp2_nv){(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE};
  many_hdrs[1] = (nghttp2_nv){(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE};
  many_hdrs[2] = (nghttp2_nv){(uint8_t *)":path", (uint8_t *)"/web.Svc/NoBody", 5, 15,
                              NGHTTP2_NV_FLAG_NONE};
  many_hdrs[3] = (nghttp2_nv){(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9,
                              NGHTTP2_NV_FLAG_NONE};
  many_hdrs[4] = (nghttp2_nv){(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16,
                              NGHTTP2_NV_FLAG_NONE};
  for (int h = 5; h < 265; h++) {
    many_hdrs[h] = (nghttp2_nv){(uint8_t *)"x-h", (uint8_t *)"1", 3, 1, NGHTTP2_NV_FLAG_NONE};
  }
  nghttp2_submit_request(sess, nullptr, many_hdrs, 265, nullptr, nullptr);
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
  grpc_free(tb64);
  grpc_free(zframe);
  grpc_free(zb64);
  nghttp2_session_del(sess);
  close(fd);
#else
  (void)port;
#endif
}

static atomic_int g_on_done_calls = 0;
static void test_on_done_cb([[maybe_unused]] grpc_call *call, [[maybe_unused]] grpc_status status,
                            [[maybe_unused]] uint64_t duration_ms, [[maybe_unused]] void *ud) {
  atomic_fetch_add(&g_on_done_calls, 1);
}

static atomic_int g_cancel_cb_fired = 0;
static void test_cancel_cb([[maybe_unused]] grpc_call *call, [[maybe_unused]] void *ud) {
  atomic_fetch_add(&g_cancel_cb_fired, 1);
}

static void cancel_watch_on_start(grpc_call *call, [[maybe_unused]] void *ud) {
  grpc_call_on_cancel(call, test_cancel_cb, nullptr);
}

static void no_body_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                            [[maybe_unused]] void *userdata) {
  (void)grpc_call_finish(call, GRPC_STATUS_OK, "empty");
  (void)grpc_call_finish(call, GRPC_STATUS_OK, "again");
}

static void codec_resp_handler(grpc_call *call, const grpc_message *req,
                               [[maybe_unused]] void *userdata) {
  if (req->len == 2 && memcmp(req->data, "id", 2) == 0) {
    (void)grpc_call_set_compression(call, GRPC_COMPRESS_IDENTITY, GRPC_COMPRESSION_LEVEL_DEFAULT);
  } else if (req->len == 4 && memcmp(req->data, "gzip", 4) == 0) {
    (void)grpc_call_set_compression(call, GRPC_COMPRESS_GZIP, GRPC_COMPRESSION_LEVEL_DEFAULT);
  }
#if defined(GRPC_ENABLE_ZSTD)
  else if (req->len == 4 && memcmp(req->data, "zstd", 4) == 0) {
    (void)grpc_call_set_compression(call, GRPC_COMPRESS_ZSTD, GRPC_COMPRESSION_LEVEL_DEFAULT);
  }
#endif
  (void)grpc_call_reply_data(call, req->data, req->len);
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
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/web.Svc/NoBody", no_body_handler,
                                                    nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/web.Svc/Codec", codec_resp_handler,
                                                    nullptr));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, server));
  uint16_t port = grpc_server_port(server);

  run_raw_h2_requests(port);

  /* Client over IPv6 target, codecs (deflate, zstd, snappy), max_message_size, timeout & errors */
  grpc_err err = GRPC_OK;
  TEST_ASSERT_NULL(grpc_client_new(nullptr, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  const char *bad_targets[] = {"no-port", "", "unix:", "[::1", "[::1]:0", "127.0.0.1:99999"};
  for (size_t i = 0; i < sizeof(bad_targets) / sizeof(bad_targets[0]); i++) {
    TEST_ASSERT_NULL(grpc_client_new(
        &(grpc_client_config){.targets = &bad_targets[i], .nb_targets = 1}, &err));
    TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  }

  /* Client TLS error paths: invalid CA/cert/key paths & TLS to plaintext server */
#if defined(GRPC_ENABLE_TLS)
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1",
                            .port = port,
                            .tls = {.enable = true, .ca_file = "/nonexistent/ca.pem"}},
      &err));
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1",
                            .port = port,
                            .tls = {.enable = true,
                                    .cert_file = "/nonexistent/c.pem",
                                    .key_file = "/nonexistent/k.pem"}},
      &err));
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1",
                            .port = port,
                            .timeout_ms = 500,
                            .tls = {.enable = true, .insecure_skip_verify = true}},
      &err));
#endif

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

  /* Identity, Gzip, and Zstd responses */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/web.Svc/Codec", "gzip", 4, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/web.Svc/Codec", "id", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Client max_message_size exceeded on compressed response -> GRPC_ERR_LIMIT */
  grpc_client *c_tiny = grpc_client_new(
      &(grpc_client_config){.targets = t6, .nb_targets = 1, .max_message_size = 8, .timeout_ms = 2000},
      &err);
  TEST_ASSERT_NOT_NULL(c_tiny);
  char msg64[64];
  memset(msg64, 'M', sizeof(msg64));
  grpc_err tiny_rc = grpc_client_unary(c_tiny, "/web.Svc/Echo", msg64, sizeof(msg64), nullptr, 0, 0,
                                       &resp);
  TEST_ASSERT_NOT_EQUAL_INT(GRPC_OK, tiny_rc);
  grpc_client_response_free(&resp);
  grpc_client_free(c_tiny);
#if defined(GRPC_ENABLE_ZSTD)
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/web.Svc/Codec", "zstd", 4, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
#endif
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_client_unary(c, "/web.Svc/Codec", "id", 2, nullptr, 0, 0, nullptr));

  /* Server edge-case validation & fallback disable */
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_register_path(nullptr, "/a/b", nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK,
      grpc_server_set_fallback(server, &(grpc_stream_handler){.on_message = no_body_handler},
                               nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_set_fallback(server, nullptr, nullptr));
#if defined(GRPC_ENABLE_TLS)
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0,
                     .tls = {.cert_file = "a.pem", .cert_pem = "pem"},
                     .log_level = GRPC_LOG_INFO},
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
#endif

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

static void *async_worker_finish_only(void *arg) {
  grpc_call *call = arg;
  (void)grpc_call_finish_async(call, GRPC_STATUS_ALREADY_EXISTS, "exists");
  grpc_call_unref(call);
  return nullptr;
}

static void async_finish_only_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                                      [[maybe_unused]] void *userdata) {
  grpc_call_ref(call);
  pthread_t th;
  if (pthread_create(&th, nullptr, async_worker_finish_only, call) == 0)
    pthread_detach(th);
  else
    grpc_call_unref(call);
}

static void comp_codecs_handler(grpc_call *call, const grpc_message *req,
                                [[maybe_unused]] void *userdata) {
  (void)grpc_call_set_compression(call, GRPC_COMPRESS_IDENTITY, -1);
#if defined(GRPC_ENABLE_ZSTD)
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_set_compression(call, GRPC_COMPRESS_ZSTD, 3));
#endif
#if defined(GRPC_ENABLE_SNAPPY)
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_set_compression(call, GRPC_COMPRESS_SNAPPY, -1));
#endif
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_UNSUPPORTED,
                        grpc_call_set_compression(call, (grpc_compression_algorithm)99, -1));
  grpc_metadata bad_md[] = {{.key = ":bad", .value = "v"}};
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_metadata(call, bad_md, 1));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_metadata_async(call, bad_md, 1));
  (void)grpc_call_reply(call, grpc_message_new(req->data, req->len));
}

void test_integration_maintenance_drain_and_edge_paths(void) {
  /* 1. Maintenance timer firing PING + max_connection_age GOAWAY + close */
  grpc_server *srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .keepalive_time_ms = 30,
      .keepalive_timeout_ms = 500,
      .max_connection_age_ms = 80,
      .max_connection_age_grace_ms = 40,
      .max_message_size = 5 * 1024 * 1024,
      .log_level = GRPC_LOG_TRACE,
  });
  TEST_ASSERT_NOT_NULL(srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv, "/m.Svc/AsyncFin",
                                                    async_finish_only_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv, "/m.Svc/Codecs",
                                                    comp_codecs_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv, "/m.Svc/Big",
                                                    large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv, "/m.Svc/Hold",
                                                    slow_deadline_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(srv));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, srv));

  /* Connect via DNS "localhost" + test async_finish_only, codecs, and 512 KiB big write */
  grpc_err err = GRPC_OK;
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){.host = "localhost", .port = grpc_server_port(srv), .timeout_ms = 3000},
      &err);
  TEST_ASSERT_NOT_NULL(c);

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/m.Svc/AsyncFin", "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_ALREADY_EXISTS, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/m.Svc/Codecs", "hello", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Health check with unknown varint + length-delimited fields before service name */
  const uint8_t hreq_unk[] = {0x10, 0x01, 0x1A, 0x02, 'x', 'y', 0x0A, 0x00};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, GRPC_HEALTH_PATH_CHECK, hreq_unk, sizeof(hreq_unk), nullptr, 0,
                                 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* 4 MiB payload to overflow loopback socket buffers and exercise async uv_write / on_sub_write / on_session_write */
  size_t big_sz = 4 * 1024 * 1024;
  char *big = malloc(big_sz);
  TEST_ASSERT_NOT_NULL(big);
  memset(big, 'M', big_sz);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/m.Svc/Big", big, big_sz, nullptr, 0, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32((uint32_t)big_sz, resp.message->len);
  grpc_client_response_free(&resp);
  free(big);

  /* Client max_message_size limit exceeded + invalid request metadata */
  grpc_client *c_small = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(srv),
          .max_message_size = 64,
          .timeout_ms = 2000,
      },
      &err);
  TEST_ASSERT_NOT_NULL(c_small);
  char med[256];
  memset(med, 'R', sizeof(med));
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_LIMIT,
      grpc_client_unary(c_small, "/m.Svc/Big", med, sizeof(med), nullptr, 0, 0, &resp));
  grpc_client_response_free(&resp);
  grpc_metadata null_key[] = {{.key = nullptr, .value = "v"}};
  TEST_ASSERT_EQUAL_INT(
      GRPC_ERR_INVALID,
      grpc_client_unary(c_small, "/m.Svc/Big", "a", 1, null_key, 1, 0, &resp));
  grpc_client_free(c_small);

  /* Wait for maintenance_timer (250ms) to send PING / GOAWAY and close */
  uv_sleep(380);
  grpc_client_free(c);

  /* Leave an active Health/Watch open across server shutdown to cover health_free watcher cleanup */
  grpc_client *c2 = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(srv), .timeout_ms = 2000},
      &err);
  if (c2 != nullptr) {
    grpc_client_stream *w =
        grpc_client_stream_open(c2, GRPC_HEALTH_PATH_WATCH, nullptr, 0, 2000, &err);
    if (w != nullptr) {
      (void)grpc_client_stream_send(w, "", 0);
      grpc_message *wm = nullptr;
      (void)grpc_client_stream_recv(w, &wm);
      grpc_message_free(wm);
      grpc_server_stop(srv);
      pthread_join(th, nullptr);
      /* Drain with active stream times out (GRPC_ERR_TIMEOUT) */
      TEST_ASSERT_EQUAL_INT(GRPC_ERR_TIMEOUT, grpc_server_drain(srv, 30));
      grpc_client_stream_cancel(w);
      grpc_client_free(c2);
      grpc_server_free(srv);
      goto uds_test;
    }
    grpc_client_free(c2);
  }
  grpc_server_stop(srv);
  pthread_join(th, nullptr);
  grpc_server_free(srv);

uds_test:;
#ifndef _WIN32
  /* 2. Unix target via targets[] ("unix:/path") + UDS max_connections=1 rejection */
  const char *upath = "/tmp/ezgrpc_uds_reject.sock";
  grpc_server *usrv = grpc_server_new(
      &(grpc_config){.unix_path = upath, .max_connections = 1, .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(usrv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(usrv, "/u.Svc/Echo", large_echo_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, usrv));

  const char *utargets[] = {"unix:///tmp/ezgrpc_uds_reject.sock"};
  grpc_client *uc1 = grpc_client_new(
      &(grpc_client_config){.targets = utargets, .nb_targets = 1, .timeout_ms = 2000}, &err);
  TEST_ASSERT_NOT_NULL(uc1);
  grpc_client *uc2 = grpc_client_new(
      &(grpc_client_config){.targets = utargets, .nb_targets = 1, .timeout_ms = 2000}, &err);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(uc1, "/u.Svc/Echo", "u", 1, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(uc2);
  grpc_client_free(uc1);
  grpc_server_stop(usrv);
  pthread_join(th, nullptr);
  grpc_server_free(usrv);
#endif

  /* 3. Server invalid IPv6 & invalid log level & keepalive-timeout + idle-GOAWAY */
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .ipv6_addr = "not-ipv6", .log_level = GRPC_LOG_OFF}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);
  TEST_ASSERT_NULL(grpc_server_new_ex(
      &(grpc_config){.port = 0, .log_level = (grpc_log_level)99}, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, err);

  /* Keepalive timeout + PING ACK: raw socket ignores PING (timeout), grpc_client ACKs PING */
  grpc_server *ka_srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .keepalive_time_ms = 20,
      .keepalive_timeout_ms = 50,
      .log_level = GRPC_LOG_DEBUG,
  });
  TEST_ASSERT_NOT_NULL(ka_srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(ka_srv, "/ka.Svc/Ping", no_body_handler,
                                                    nullptr));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, ka_srv));
#ifndef _WIN32
  grpc_client *ka_c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(ka_srv), .timeout_ms = 2000},
      &err);
  TEST_ASSERT_NOT_NULL(ka_c);
  int raw_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (raw_fd >= 0) {
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(grpc_server_port(ka_srv));
    inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
    if (connect(raw_fd, (struct sockaddr *)&sin, sizeof(sin)) == 0) {
      /* Wait for 250ms timer to send PING, let ka_c ACK it, then wait for raw_fd timeout */
      uv_sleep(280);
      (void)grpc_client_unary(ka_c, "/ka.Svc/Ping", "", 0, nullptr, 0, 1000, &resp);
      grpc_client_response_free(&resp);
      uv_sleep(280);
    }
    close(raw_fd);
  }
  grpc_client_free(ka_c);
#endif
  grpc_server_stop(ka_srv);
  pthread_join(th, nullptr);
  grpc_server_free(ka_srv);

  /* Idle timeout: client connects and stays idle -> server sends GOAWAY and closes */
  grpc_server *idle_srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connection_idle_ms = 30,
      .max_connection_age_grace_ms = 30,
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_status_details_decode_all(nullptr, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_handle(nullptr, "/a/b", no_body_handler, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_server_handle_stream(nullptr, "/a/b", &(grpc_stream_handler){0}, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_unregister(nullptr, "/a/b"));
  TEST_ASSERT_NOT_NULL(idle_srv);
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, idle_srv));
  grpc_client *ic = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(idle_srv)}, &err);
  TEST_ASSERT_NOT_NULL(ic);
  uv_sleep(560);
  grpc_client_free(ic);
  grpc_server_stop(idle_srv);
  pthread_join(th, nullptr);
  grpc_server_free(idle_srv);

  /* Max connection age + grace timeout: GOAWAY then close */
  grpc_server *age_srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connection_age_ms = 30,
      .max_connection_age_grace_ms = 30,
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(age_srv);
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, age_srv));
  grpc_client *ac = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(age_srv)}, &err);
  TEST_ASSERT_NOT_NULL(ac);
  uv_sleep(560);
  grpc_client_free(ac);
  grpc_server_stop(age_srv);
  pthread_join(th, nullptr);
  grpc_server_free(age_srv);

  /* Unreachable port with retries -> GRPC_ERR_IO */
  TEST_ASSERT_NULL(grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = 1,
          .max_retries = 1,
          .retry_backoff_ms = 5,
      },
      &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_IO, err);

  /* Poll-driven GRPC_EVENT_CANCEL when client cancels stream */
  grpc_server *pc_srv = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(pc_srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_register_path(pc_srv, "/pc.Svc/Hold", nullptr));
  grpc_client *pc_c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(pc_srv), .timeout_ms = 2000},
      &err);
  TEST_ASSERT_NOT_NULL(pc_c);
  grpc_client_stream *pc_st =
      grpc_client_stream_open(pc_c, "/pc.Svc/Hold", nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(pc_st);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(pc_st, "x", 1));
  int saw_msg = 0;
  for (int i = 0; i < 20 && !saw_msg; i++) {
    if (grpc_server_poll(pc_srv, 30) > 0) {
      size_t cnt = 0;
      const grpc_event *evs = grpc_server_events(pc_srv, &cnt);
      for (size_t j = 0; j < cnt; j++) {
        if (evs[j].type == GRPC_EVENT_MESSAGE) {
          saw_msg = 1;
        }
      }
    }
  }
  TEST_ASSERT_EQUAL_INT(1, saw_msg);
  grpc_client_stream_cancel(pc_st);
  int saw_cancel = 0;
  for (int i = 0; i < 20 && !saw_cancel; i++) {
    int n = grpc_server_poll(pc_srv, 30);
    if (n > 0) {
      size_t cnt = 0;
      const grpc_event *evs = grpc_server_events(pc_srv, &cnt);
      for (size_t j = 0; j < cnt; j++) {
        if (evs[j].type == GRPC_EVENT_CANCEL) {
          TEST_ASSERT_EQUAL_INT(GRPC_STATUS_CANCELLED, grpc_call_cancel_reason(evs[j].call));
          saw_cancel = 1;
        }
      }
    }
  }
  TEST_ASSERT_EQUAL_INT(1, saw_cancel);
  grpc_client_free(pc_c);
  grpc_server_free(pc_srv);

  /* Large client payload (4 MiB) to exercise async uv_write / on_sub_write + TCP max_connections */
  grpc_server *big_srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_connections = 1,
      .max_message_size = 8 * 1024 * 1024,
      .log_level = GRPC_LOG_INFO,
  });
  TEST_ASSERT_NOT_NULL(big_srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(big_srv, "/big.Svc/Echo", large_echo_handler,
                                                    nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle_stream(
                   big_srv, "/big.Svc/Hold",
                   &(grpc_stream_handler){.on_start = cancel_watch_on_start}, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(big_srv));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_channelz_enable(big_srv));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(big_srv));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_reflection_enable(nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_add_interceptor(
                            big_srv, &(grpc_interceptor){.on_done = test_on_done_cb}));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK,
      grpc_server_add_interceptor(big_srv, &(grpc_interceptor){.path_prefix = "/other.Svc/"}));
  grpc_interceptor ba_inc = grpc_interceptor_bearer_auth(bearer_validator, nullptr);
  ba_inc.path_prefix = "/big.Svc/Auth";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(big_srv, &ba_inc));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(big_srv, "/big.Svc/Auth", no_body_handler,
                                                    nullptr));
  grpc_interceptor rl_inc = grpc_interceptor_rate_limit(100.0, 2);
  rl_inc.path_prefix = "/big.Svc/RL";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(big_srv, &rl_inc));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(big_srv, "/big.Svc/RL", no_body_handler,
                                                    nullptr));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, big_srv));
  grpc_client *bc1 = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1",
                            .port = grpc_server_port(big_srv),
                            .max_message_size = 8 * 1024 * 1024,
                            .timeout_ms = 5000},
      &err);
  TEST_ASSERT_NOT_NULL(bc1);
  /* 2nd TCP connection while max_connections=1 -> triggers reject_connection() */
  grpc_client *bc2 = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(big_srv), .timeout_ms = 500},
      &err);
  grpc_client_free(bc2);

  size_t c_big_sz = 4 * 1024 * 1024;
  char *c_big_buf = grpc_malloc(c_big_sz);
  TEST_ASSERT_NOT_NULL(c_big_buf);
  memset(c_big_buf, 'A', c_big_sz);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(bc1, "/big.Svc/Echo", c_big_buf, c_big_sz, nullptr, 0, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32((uint32_t)c_big_sz, resp.message->len);
  grpc_client_response_free(&resp);
  grpc_free(c_big_buf);

  /* Channelz GetServers after >127 bytes transferred (multi-byte varints) */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, GRPC_CHANNELZ_PATH_GET_SERVERS, "", 0,
                                                   nullptr, 0, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Reflection with unknown protobuf fields (varint, len-delimited, fixed64) */
  grpc_client_stream *rf = grpc_client_stream_open(
      bc1, GRPC_REFLECTION_V1_PATH, nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(rf);
  const uint8_t unk_rf[] = {0xF8, 0x06, 0x01, 0xFA, 0x06, 0x01, 'A', 0xF9, 0x06, 0, 0, 0, 0, 0, 0, 0, 0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(rf, unk_rf, sizeof(unk_rf)));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(rf));
  grpc_message *rfm = nullptr;
  (void)grpc_client_stream_recv(rf, &rfm);
  grpc_message_free(rfm);
  (void)grpc_client_stream_finish(rf, &resp);
  grpc_client_response_free(&resp);

  /* Bearer auth with extra spaces + rate-limit refill after sleep + on_cancel callback */
  grpc_metadata sp_auth[] = {{.key = "authorization", .value = "Bearer   valid-token"}};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, "/big.Svc/Auth", "", 0, sp_auth, 1, 2000,
                                                   &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, "/big.Svc/RL", "", 0, nullptr, 0, 2000,
                                                   &resp));
  grpc_client_response_free(&resp);
  uv_sleep(40);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, "/big.Svc/RL", "", 0, nullptr, 0, 2000,
                                                   &resp));
  grpc_client_response_free(&resp);

  atomic_store(&g_cancel_cb_fired, 0);
  grpc_client_stream *hs = grpc_client_stream_open(bc1, "/big.Svc/Hold", nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(hs);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(hs, "x", 1));
  uv_sleep(20);
  grpc_client_stream_cancel(hs);
  uv_sleep(30);
  TEST_ASSERT_TRUE(atomic_load(&g_on_done_calls) > 0);

  /* Health check with >127-byte service name (multi-byte varint) + 2 concurrent watchers */
  uint8_t long_hc[140];
  long_hc[0] = 0x0A;
  long_hc[1] = 0x82; /* varint 130: (0x02 | 0x80), 0x01 */
  long_hc[2] = 0x01;
  memset(long_hc + 3, 'a', 130);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(bc1, GRPC_HEALTH_PATH_CHECK, long_hc, 133,
                                                   nullptr, 0, 2000, &resp));
  grpc_client_response_free(&resp);

  grpc_client_stream *w1 =
      grpc_client_stream_open(bc1, GRPC_HEALTH_PATH_WATCH, nullptr, 0, 2000, &err);
  TEST_ASSERT_NOT_NULL(w1);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(w1, "", 0));
  grpc_message *wm1 = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(w1, &wm1));
  grpc_message_free(wm1);
  grpc_client_stream_cancel(w1);

  /* Client retry when first subchannel's server shuts down mid-connection */
  grpc_server *srv_ok = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(srv_ok);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv_ok, "/big.Svc/Echo", large_echo_handler,
                                                    nullptr));
  pthread_t th_ok;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th_ok, nullptr, run_server_fn, srv_ok));
  char ep_a[64], ep_b[64];
  snprintf(ep_a, sizeof(ep_a), "127.0.0.1:%u", grpc_server_port(big_srv));
  snprintf(ep_b, sizeof(ep_b), "127.0.0.1:%u", grpc_server_port(srv_ok));
  const char *rr_targets[] = {ep_a, ep_b};
  grpc_client_free(bc1);
  grpc_client *rr = grpc_client_new(&(grpc_client_config){.targets = rr_targets,
                                                          .nb_targets = 2,
                                                          .max_retries = 2,
                                                          .retry_backoff_ms = 5,
                                                          .timeout_ms = 2000},
                                    &err);
  TEST_ASSERT_NOT_NULL(rr);
  /* Stop big_srv so first subchannel fails on send/recv and retries on srv_ok */
  grpc_server_stop(big_srv);
  pthread_join(th, nullptr);
  grpc_server_free(big_srv);
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(rr, "/big.Svc/Echo", "retry", 5, nullptr, 0, 2000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_client_free(rr);
  grpc_server_stop(srv_ok);
  pthread_join(th_ok, nullptr);
  grpc_server_free(srv_ok);
}

#ifndef _WIN32
static void burst_8mb_handler(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                              [[maybe_unused]] void *ud) {
  size_t sz = 8 * 1024 * 1024;
  char *buf = grpc_malloc(sz);
  if (buf != nullptr) {
    memset(buf, 'B', sz);
    (void)grpc_call_reply_data(call, buf, sz);
    grpc_free(buf);
  } else {
    (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, "oom");
  }
}

typedef struct slow_h2_srv_ctx {
  int listen_fd;
  bool abort_mid_write;
} slow_h2_srv_ctx;

static int slow_h2_on_frame(nghttp2_session *s, const nghttp2_frame *f, void *ud) {
  int *got_eos = ud;
  if (f->hd.stream_id > 0 && (f->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
    *got_eos = 1;
    nghttp2_nv resp_hdrs[] = {
        {(uint8_t *)":status", (uint8_t *)"200", 7, 3, NGHTTP2_NV_FLAG_NONE},
        {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
        {(uint8_t *)"grpc-status", (uint8_t *)"0", 11, 1, NGHTTP2_NV_FLAG_NONE},
    };
    nghttp2_submit_response(s, f->hd.stream_id, resp_hdrs, 3, nullptr);
  }
  return 0;
}

static void *slow_h2_server_thread(void *arg) {
  slow_h2_srv_ctx *ctx = arg;
  int cfd = accept(ctx->listen_fd, nullptr, nullptr);
  if (cfd < 0)
    return nullptr;

  int got_eos = 0;
  nghttp2_session_callbacks *cbs = nullptr;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, slow_h2_on_frame);
  nghttp2_session *sess = nullptr;
  nghttp2_session_server_new(&sess, cbs, &got_eos);
  nghttp2_session_callbacks_del(cbs);

  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024},
  };
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, iv, 1);
  nghttp2_session_set_local_window_size(sess, NGHTTP2_FLAG_NONE, 0, 16 * 1024 * 1024);
  for (;;) {
    const uint8_t *out;
    ssize_t n = nghttp2_session_mem_send(sess, &out);
    if (n <= 0)
      break;
    raw_h2_send_all(cfd, out, (size_t)n);
  }

  /* Pause so client's 8 MiB write fills the socket buffer and triggers async uv_write */
  uv_sleep(50);

  if (ctx->abort_mid_write) {
    struct linger lg = {.l_onoff = 1, .l_linger = 0};
    setsockopt(cfd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    nghttp2_session_del(sess);
    close(cfd);
    return nullptr;
  }

  struct timeval tv = {.tv_sec = 3, .tv_usec = 0};
  setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  uint8_t rbuf[32768];
  while (!got_eos) {
    ssize_t n = recv(cfd, rbuf, sizeof(rbuf), 0);
    if (n <= 0)
      break;
    nghttp2_session_mem_recv(sess, rbuf, (size_t)n);
    for (;;) {
      const uint8_t *out;
      ssize_t wn = nghttp2_session_mem_send(sess, &out);
      if (wn <= 0)
        break;
      raw_h2_send_all(cfd, out, (size_t)wn);
    }
  }
  for (;;) {
    const uint8_t *out;
    ssize_t wn = nghttp2_session_mem_send(sess, &out);
    if (wn <= 0)
      break;
    raw_h2_send_all(cfd, out, (size_t)wn);
  }
  nghttp2_session_del(sess);
  close(cfd);
  return nullptr;
}

void test_integration_async_write_backpressure(void) {
  /* 1. Server-side async uv_write / on_session_write via 8 MiB burst + small SO_RCVBUF */
  grpc_server *srv = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_message_size = 16 * 1024 * 1024,
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(srv);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv, "/bp.Svc/Burst", burst_8mb_handler,
                                                    nullptr));
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_fn, srv));

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  TEST_ASSERT_TRUE(fd >= 0);
  int rcvbuf = 4096;
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  struct sockaddr_in sin = {0};
  sin.sin_family = AF_INET;
  sin.sin_port = htons(grpc_server_port(srv));
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);
  TEST_ASSERT_EQUAL_INT(0, connect(fd, (struct sockaddr *)&sin, sizeof(sin)));

  int done = 0;
  nghttp2_session_callbacks *cbs = nullptr;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, raw_h2_on_frame);
  nghttp2_session *sess = nullptr;
  nghttp2_session_client_new(&sess, cbs, &done);
  nghttp2_session_callbacks_del(cbs);

  nghttp2_settings_entry iv[] = {{NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024}};
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, iv, 1);
  nghttp2_session_set_local_window_size(sess, NGHTTP2_FLAG_NONE, 0, 16 * 1024 * 1024);
  const uint8_t req_frame[] = {0, 0, 0, 0, 1, 'x'};
  raw_h2_payload p = {.data = req_frame, .len = sizeof(req_frame), .off = 0};
  nghttp2_data_provider dp = {.source.ptr = &p, .read_callback = raw_h2_read_cb};
  nghttp2_nv hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)"/bp.Svc/Burst", 5, 13, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)"localhost", 10, 9, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/grpc", 12, 16, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"te", (uint8_t *)"trailers", 2, 8, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_submit_request(sess, nullptr, hdrs, 6, &dp, nullptr);
  for (;;) {
    const uint8_t *out;
    ssize_t n = nghttp2_session_mem_send(sess, &out);
    if (n <= 0)
      break;
    raw_h2_send_all(fd, out, (size_t)n);
  }
  /* Pause so server's 8 MiB burst fills the tiny socket buffer -> uv_write + on_session_write */
  uv_sleep(50);
  struct timeval tv = {.tv_sec = 3, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  uint8_t rbuf[32768];
  while (!done) {
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
  TEST_ASSERT_EQUAL_INT(1, done);
  nghttp2_session_del(sess);
  close(fd);

  /* 1b. Server-side abort mid async uv_write (on_session_write status < 0) */
  int fd2 = socket(AF_INET, SOCK_STREAM, 0);
  if (fd2 >= 0) {
    setsockopt(fd2, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    if (connect(fd2, (struct sockaddr *)&sin, sizeof(sin)) == 0) {
      int done2 = 0;
      nghttp2_session_callbacks *cbs2 = nullptr;
      nghttp2_session_callbacks_new(&cbs2);
      nghttp2_session *sess2 = nullptr;
      nghttp2_session_client_new(&sess2, cbs2, &done2);
      nghttp2_session_callbacks_del(cbs2);
      nghttp2_submit_settings(sess2, NGHTTP2_FLAG_NONE, iv, 1);
      nghttp2_session_set_local_window_size(sess2, NGHTTP2_FLAG_NONE, 0, 16 * 1024 * 1024);
      raw_h2_payload p2 = {.data = req_frame, .len = sizeof(req_frame), .off = 0};
      nghttp2_data_provider dp2 = {.source.ptr = &p2, .read_callback = raw_h2_read_cb};
      nghttp2_submit_request(sess2, nullptr, hdrs, 6, &dp2, nullptr);
      for (;;) {
        const uint8_t *out;
        ssize_t n = nghttp2_session_mem_send(sess2, &out);
        if (n <= 0)
          break;
        raw_h2_send_all(fd2, out, (size_t)n);
      }
      uv_sleep(50);
      struct linger lg = {.l_onoff = 1, .l_linger = 0};
      setsockopt(fd2, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
      nghttp2_session_del(sess2);
    }
    close(fd2);
    uv_sleep(30);
  }

  grpc_server_stop(srv);
  pthread_join(th, nullptr);
  grpc_server_free(srv);

  /* 2. Client-side async uv_write / on_sub_write via 8 MiB upload to slow raw HTTP/2 server */
  int lfd = socket(AF_INET, SOCK_STREAM, 0);
  TEST_ASSERT_TRUE(lfd >= 0);
  int opt = 1;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  int lrcv = 4096;
  setsockopt(lfd, SOL_SOCKET, SO_RCVBUF, &lrcv, sizeof(lrcv));
  struct sockaddr_in lsin = {0};
  lsin.sin_family = AF_INET;
  lsin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  lsin.sin_port = 0;
  TEST_ASSERT_EQUAL_INT(0, bind(lfd, (struct sockaddr *)&lsin, sizeof(lsin)));
  TEST_ASSERT_EQUAL_INT(0, listen(lfd, 4));
  socklen_t slen = sizeof(lsin);
  TEST_ASSERT_EQUAL_INT(0, getsockname(lfd, (struct sockaddr *)&lsin, &slen));
  uint16_t lport = ntohs(lsin.sin_port);

  slow_h2_srv_ctx sctx = {.listen_fd = lfd};
  pthread_t sth;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&sth, nullptr, slow_h2_server_thread, &sctx));

  grpc_err err = GRPC_OK;
  grpc_client *c = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = lport, .timeout_ms = 5000}, &err);
  TEST_ASSERT_NOT_NULL(c);
  size_t big = 8 * 1024 * 1024;
  char *payload = grpc_malloc(big);
  TEST_ASSERT_NOT_NULL(payload);
  memset(payload, 'C', big);
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(c, "/bp.Svc/Upload", payload, big, nullptr, 0, 5000, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_free(payload);
  grpc_client_free(c);
  pthread_join(sth, nullptr);

  /* 3. Abort mid async uv_write on client (on_sub_write status < 0) */
  slow_h2_srv_ctx sctx2 = {.listen_fd = lfd, .abort_mid_write = true};
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&sth, nullptr, slow_h2_server_thread, &sctx2));
  grpc_client *c2 = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = lport, .timeout_ms = 2000}, &err);
  TEST_ASSERT_NOT_NULL(c2);
  payload = grpc_malloc(big);
  TEST_ASSERT_NOT_NULL(payload);
  memset(payload, 'D', big);
  (void)grpc_client_unary(c2, "/bp.Svc/Upload", payload, big, nullptr, 0, 2000, &resp);
  grpc_client_response_free(&resp);
  grpc_free(payload);
  grpc_client_free(c2);
  pthread_join(sth, nullptr);
  close(lfd);
}
#endif

int main(void) {
#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);
#endif
  UNITY_BEGIN();
  RUN_TEST(test_integration_full_server_and_client);
  RUN_TEST(test_integration_health_watch_and_reflection);
  RUN_TEST(test_integration_poll_events_and_maintenance);
  RUN_TEST(test_integration_grpc_web_cors_ipv6_and_client_edge_cases);
  RUN_TEST(test_integration_maintenance_drain_and_edge_paths);
#ifndef _WIN32
  RUN_TEST(test_integration_async_write_backpressure);
#endif
  return UNITY_END();
}
