#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "internal/grpc_threads.h"
#include "grpc.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

typedef struct server_thread_ctx {
  grpc_server *server;
  int recv_interceptor_hits;
  int send_interceptor_hits;
  int on_start_hits;
  int fallback_hits;
  char last_trace_id[33];
  char last_fallback_method[128];
} server_thread_ctx;

static void *run_server_thread(void *arg) {
  grpc_server *server = arg;
  (void)grpc_server_run(server);
  return nullptr;
}

static void echo_unary(grpc_call *call, const grpc_message *req, void *userdata) {
  server_thread_ctx *ctx = userdata;
  grpc_trace_context tc = {0};
  if (grpc_call_trace_context(call, &tc) && tc.valid)
    memcpy(ctx->last_trace_id, tc.trace_id, sizeof(ctx->last_trace_id));
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static void stream_three(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                         [[maybe_unused]] void *userdata) {
  for (int i = 0; i < 3; i++) {
    char msg[16];
    snprintf(msg, sizeof(msg), "item-%d", i);
    (void)grpc_call_send_data(call, msg, strlen(msg));
  }
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void fallback_on_start([[maybe_unused]] grpc_call *call, void *userdata) {
  server_thread_ctx *ctx = userdata;
  ctx->on_start_hits++;
}

static void fallback_on_message(grpc_call *call, const grpc_message *msg, void *userdata) {
  server_thread_ctx *ctx = userdata;
  ctx->fallback_hits++;
  const char *method = grpc_call_method(call);
  if (method != nullptr)
    snprintf(ctx->last_fallback_method, sizeof(ctx->last_fallback_method), "%s", method);
  (void)grpc_call_reply_data(call, msg->data, msg->len);
}

static grpc_status count_recv([[maybe_unused]] grpc_call *call,
                              [[maybe_unused]] const grpc_message *msg,
                              [[maybe_unused]] const char **err_msg, void *userdata) {
  server_thread_ctx *ctx = userdata;
  ctx->recv_interceptor_hits++;
  return GRPC_STATUS_OK;
}

static grpc_status count_send([[maybe_unused]] grpc_call *call,
                              [[maybe_unused]] const grpc_message *msg,
                              [[maybe_unused]] const char **err_msg, void *userdata) {
  server_thread_ctx *ctx = userdata;
  ctx->send_interceptor_hits++;
  return GRPC_STATUS_OK;
}

static void collect_stream_msg(const grpc_message *msg, void *userdata) {
  int *count = userdata;
  (*count)++;
  TEST_ASSERT_TRUE(msg->len >= 5);
}

void test_client_server_tcp_end_to_end(void) {
  server_thread_ctx ctx = {0};
  grpc_err err = GRPC_OK;
  grpc_server *server = grpc_server_new_ex(
      &(grpc_config){
          .port = 0,
          .ipv4_addr = "127.0.0.1",
          .default_compression = GRPC_COMPRESS_GZIP,
          .keepalive_time_ms = 5000,
          .max_connection_idle_ms = 30000,
          .max_send_queue_bytes = 65536,
          .log_level = GRPC_LOG_OFF,
      },
      &err);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  uint16_t port = grpc_server_port(server);
  TEST_ASSERT_TRUE(port > 0);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/test.Svc/Echo", echo_unary, &ctx));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/test.Svc/Stream", stream_three, &ctx));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_channelz_enable(server));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_set_fallback(
                   server,
                   &(grpc_stream_handler){.on_start = fallback_on_start,
                                          .on_message = fallback_on_message},
                   &ctx));

  /* Scoped message-level interceptor on /test.Svc/ */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_add_interceptor(server, &(grpc_interceptor){
                                                       .name = "msg_counter",
                                                       .path_prefix = "/test.Svc/",
                                                       .on_recv_message = count_recv,
                                                       .on_send_message = count_send,
                                                       .userdata = &ctx,
                                                   }));

  /* Rate limiter scoped to /test.Svc/Echo: burst of 2 */
  grpc_interceptor rl = grpc_interceptor_rate_limit(0.0, 2);
  rl.path_prefix = "/test.Svc/Echo";
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(server, &rl));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_thread, server));

  grpc_client *client = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = port, .timeout_ms = 3000}, &err);
  TEST_ASSERT_NOT_NULL(client);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);

  /* 1. Unary call with W3C traceparent */
  grpc_trace_context tc = {
      .trace_id = "4bf92f3577b34da6a3ce929d0e0e4736",
      .span_id = "00f067aa0ba902b7",
      .trace_flags = 1,
      .valid = true,
  };
  char tp[64];
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_traceparent_format(&tc, tp, sizeof(tp)));
  grpc_metadata md[] = {{.key = "traceparent", .value = tp}};

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/test.Svc/Echo", "hello", 5, md, 1, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(5, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY("hello", resp.message->data, 5);
  TEST_ASSERT_EQUAL_STRING("4bf92f3577b34da6a3ce929d0e0e4736", ctx.last_trace_id);
  grpc_client_response_free(&resp);

  /* 2. Second Echo succeeds (burst=2), third hits rate limiter -> RESOURCE_EXHAUSTED */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/test.Svc/Echo", "two", 3, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/test.Svc/Echo", "three", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_RESOURCE_EXHAUSTED, resp.status);
  grpc_client_response_free(&resp);

  /* 3. Server streaming */
  int streamed = 0;
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_server_stream(client, "/test.Svc/Stream", "go", 2, nullptr, 0, 0,
                                         collect_stream_msg, &streamed, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_INT(3, streamed);
  grpc_client_response_free(&resp);

  /* 4. Fallback handler for unregistered path */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/unknown.Proxy/Call", "proxy", 5, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_MEMORY("proxy", resp.message->data, 5);
  TEST_ASSERT_EQUAL_STRING("/unknown.Proxy/Call", ctx.last_fallback_method);
  TEST_ASSERT_TRUE(ctx.on_start_hits >= 1);
  TEST_ASSERT_EQUAL_INT(1, ctx.fallback_hits);
  grpc_client_response_free(&resp);

  /* 5. Channelz GetServers & GetServerSockets */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, GRPC_CHANNELZ_PATH_GET_SERVERS, "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_TRUE(resp.message->len > 0);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(
      GRPC_OK,
      grpc_client_unary(client, GRPC_CHANNELZ_PATH_GET_SERVER_SOCKETS, "", 0, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_TRUE(resp.message->len > 0);
  grpc_client_response_free(&resp);

  /* 6. Server stats */
  grpc_server_stats st = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_get_stats(server, &st));
  TEST_ASSERT_TRUE(st.total_connections >= 1);
  TEST_ASSERT_TRUE(st.total_calls >= 6);
  TEST_ASSERT_TRUE(st.calls_ok >= 5);
  TEST_ASSERT_TRUE(st.calls_error >= 1);
  TEST_ASSERT_TRUE(st.bytes_received > 0);
  TEST_ASSERT_TRUE(st.bytes_sent > 0);
  TEST_ASSERT_TRUE(ctx.recv_interceptor_hits >= 3);
  TEST_ASSERT_TRUE(ctx.send_interceptor_hits >= 5);

  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}

#ifndef _WIN32
void test_unix_domain_socket(void) {
  const char *sock_path = "/tmp/ezgrpc_next_test.sock";
  server_thread_ctx ctx = {0};
  grpc_err err = GRPC_OK;
  grpc_server *server = grpc_server_new_ex(
      &(grpc_config){.unix_path = sock_path, .log_level = GRPC_LOG_OFF}, &err);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/uds.Svc/Echo", echo_unary, &ctx));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_thread, server));

  grpc_client *client = grpc_client_new(
      &(grpc_client_config){.unix_path = sock_path, .timeout_ms = 3000}, &err);
  TEST_ASSERT_NOT_NULL(client);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/uds.Svc/Echo", "uds-ok", 6, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_UINT32(6, resp.message->len);
  TEST_ASSERT_EQUAL_MEMORY("uds-ok", resp.message->data, 6);
  grpc_client_response_free(&resp);

  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}
#endif

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_client_server_tcp_end_to_end);
#ifndef _WIN32
  RUN_TEST(test_unix_domain_socket);
#endif
  return UNITY_END();
}
