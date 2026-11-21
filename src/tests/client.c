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
      GRPC_OK, grpc_server_set_fallback(server,
                                        &(grpc_stream_handler){.on_start = fallback_on_start,
                                                               .on_message = fallback_on_message},
                                        &ctx));

  /* Scoped message-level interceptor on /test.Svc/ */
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_add_interceptor(server, &(grpc_interceptor){
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
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_unary(client, "/test.Svc/Echo", "hello", 5, md, 1, 0, &resp));
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
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_server_stream(client, "/test.Svc/Stream", "go", 2, nullptr, 0,
                                                  0, collect_stream_msg, &streamed, &resp));
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
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, GRPC_CHANNELZ_PATH_GET_SERVERS, "", 0,
                                                   nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_TRUE(resp.message->len > 0);
  grpc_client_response_free(&resp);

  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, GRPC_CHANNELZ_PATH_GET_SERVER_SOCKETS,
                                                   "", 0, nullptr, 0, 0, &resp));
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
  grpc_server *server =
      grpc_server_new_ex(&(grpc_config){.unix_path = sock_path, .log_level = GRPC_LOG_OFF}, &err);
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/uds.Svc/Echo", echo_unary, &ctx));

  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_thread, server));

  grpc_client *client =
      grpc_client_new(&(grpc_client_config){.unix_path = sock_path, .timeout_ms = 3000}, &err);
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

static void bidi_on_msg(grpc_call *call, const grpc_message *msg, [[maybe_unused]] void *userdata) {
  (void)grpc_call_send_data(call, msg->data, msg->len);
}

static void bidi_on_end(grpc_call *call, [[maybe_unused]] void *userdata) {
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void cstream_on_start(grpc_call *call, [[maybe_unused]] void *userdata) {
  int *sum = grpc_calloc(1, sizeof(*sum));
  grpc_call_set_userdata(call, sum, grpc_free);
}

static void cstream_on_msg(grpc_call *call, const grpc_message *msg,
                           [[maybe_unused]] void *userdata) {
  int *sum = grpc_call_userdata(call);
  *sum += (int)msg->len;
}

static void cstream_on_end(grpc_call *call, [[maybe_unused]] void *userdata) {
  int *sum = grpc_call_userdata(call);
  char out[32];
  int n = snprintf(out, sizeof(out), "%d", *sum);
  (void)grpc_call_reply_data(call, out, (size_t)n);
}

static void flaky_unary(grpc_call *call, const grpc_message *req, void *userdata) {
  int *attempts = userdata;
  (*attempts)++;
  if (*attempts == 1) {
    (void)grpc_call_finish(call, GRPC_STATUS_UNAVAILABLE, "transient error");
    return;
  }
  (void)grpc_call_reply_data(call, req->data, req->len);
}

/* Server-streaming handler that sends one message, then fails with UNAVAILABLE (a retryable
 * status) on every attempt. */
static void flaky_stream(grpc_call *call, [[maybe_unused]] const grpc_message *req,
                         void *userdata) {
  int *attempts = userdata;
  (*attempts)++;
  (void)grpc_call_send_data(call, "partial", 7);
  (void)grpc_call_finish(call, GRPC_STATUS_UNAVAILABLE, "failed after a message");
}

static void count_messages(const grpc_message *msg, void *userdata) {
  int *count = userdata;
  (void)msg;
  (*count)++;
}

static void count_hits_unary(grpc_call *call, const grpc_message *req, void *userdata) {
  int *hits = userdata;
  (*hits)++;
  (void)grpc_call_reply_data(call, req->data, req->len);
}

void test_client_streaming_bidi_retries_and_lb(void) {
  int flaky_attempts = 0;
  int flaky_stream_attempts = 0;
  int hits_a = 0, hits_b = 0;

  grpc_server *srv_a = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  grpc_server *srv_b = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(srv_a);
  TEST_ASSERT_NOT_NULL(srv_b);

  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle_stream(srv_a, "/stream.Svc/Bidi",
                                                  &(grpc_stream_handler){.on_message = bidi_on_msg,
                                                                         .on_end = bidi_on_end},
                                                  nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle_stream(srv_a, "/stream.Svc/Sum",
                                                           &(grpc_stream_handler){
                                                               .on_start = cstream_on_start,
                                                               .on_message = cstream_on_msg,
                                                               .on_end = cstream_on_end,
                                                           },
                                                           nullptr));
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_server_handle(srv_a, "/retry.Svc/Flaky", flaky_unary, &flaky_attempts));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(srv_a, "/retry.Svc/FlakyStream", flaky_stream,
                                                    &flaky_stream_attempts));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv_a, "/lb.Svc/Ping", count_hits_unary, &hits_a));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(srv_b, "/lb.Svc/Ping", count_hits_unary, &hits_b));

  pthread_t th_a, th_b;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th_a, nullptr, run_server_thread, srv_a));
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th_b, nullptr, run_server_thread, srv_b));

  /* 1. Client-streaming & Bidi-streaming + Retries against srv_a */
  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = grpc_server_port(srv_a),
          .timeout_ms = 3000,
          .max_retries = 2,
          .retry_backoff_ms = 10,
      },
      &err);
  TEST_ASSERT_NOT_NULL(client);

  /* Client streaming: send 3 chunks (2 + 3 + 4 = 9 bytes), close send, recv "9" */
  grpc_client_stream *cs =
      grpc_client_stream_open(client, "/stream.Svc/Sum", nullptr, 0, 3000, &err);
  TEST_ASSERT_NOT_NULL(cs);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs, "ab", 2));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs, "cde", 3));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(cs, "fghi", 4));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_close_send(cs));
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(cs, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  TEST_ASSERT_EQUAL_MEMORY("9", resp.message->data, 1);
  grpc_client_response_free(&resp);

  /* One RPC per connection: a second RPC while a stream is open fails with GRPC_ERR_STATE
   * instead of silently orphaning the open stream. */
  grpc_client_stream *busy =
      grpc_client_stream_open(client, "/stream.Svc/Sum", nullptr, 0, 3000, &err);
  TEST_ASSERT_NOT_NULL(busy);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(busy, "ab", 2));
  TEST_ASSERT_NULL(grpc_client_stream_open(client, "/stream.Svc/Bidi", nullptr, 0, 3000, &err));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE, err);
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_STATE,
                        grpc_client_unary(client, "/lb.Svc/Ping", "x", 1, nullptr, 0, 0, &resp));
  grpc_client_response_free(&resp);
  /* the first stream is unaffected */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(busy, "c", 1));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(busy, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_MEMORY("3", resp.message->data, 1);
  grpc_client_response_free(&resp);

  /* Bidi streaming: interleaved send & recv */
  grpc_client_stream *bs =
      grpc_client_stream_open(client, "/stream.Svc/Bidi", nullptr, 0, 3000, &err);
  TEST_ASSERT_NOT_NULL(bs);
  for (int i = 0; i < 3; i++) {
    char msg[16];
    int n = snprintf(msg, sizeof(msg), "ping-%d", i);
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_send(bs, msg, (size_t)n));
    grpc_message *reply = nullptr;
    TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_recv(bs, &reply));
    TEST_ASSERT_NOT_NULL(reply);
    TEST_ASSERT_EQUAL_UINT32((uint32_t)n, reply->len);
    TEST_ASSERT_EQUAL_MEMORY(msg, reply->data, (size_t)n);
    grpc_message_free(reply);
  }
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_stream_finish(bs, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);

  /* Retries: first attempt gets UNAVAILABLE, second succeeds */
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/retry.Svc/Flaky", "ok", 2, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_EQUAL_INT(2, flaky_attempts);
  grpc_client_response_free(&resp);

  /* A server stream that already delivered a message is committed: no retry, no duplicates */
  int seen = 0;
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_client_server_stream(client, "/retry.Svc/FlakyStream", "x", 1, nullptr,
                                                  0, 0, count_messages, &seen, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAVAILABLE, resp.status);
  TEST_ASSERT_EQUAL_INT(1, seen);
  TEST_ASSERT_EQUAL_INT(1, flaky_stream_attempts);
  grpc_client_response_free(&resp);
  grpc_client_free(client);

  /* 2. Round-robin load balancing across srv_a and srv_b */
  char ep_a[64], ep_b[64];
  snprintf(ep_a, sizeof(ep_a), "127.0.0.1:%u", grpc_server_port(srv_a));
  snprintf(ep_b, sizeof(ep_b), "127.0.0.1:%u", grpc_server_port(srv_b));
  const char *targets[] = {ep_a, ep_b};
  grpc_client *lb = grpc_client_new(
      &(grpc_client_config){.targets = targets, .nb_targets = 2, .timeout_ms = 3000}, &err);
  TEST_ASSERT_NOT_NULL(lb);
  for (int i = 0; i < 4; i++) {
    TEST_ASSERT_EQUAL_INT(GRPC_OK,
                          grpc_client_unary(lb, "/lb.Svc/Ping", "p", 1, nullptr, 0, 0, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    grpc_client_response_free(&resp);
  }
  TEST_ASSERT_EQUAL_INT(2, hits_a);
  TEST_ASSERT_EQUAL_INT(2, hits_b);
  grpc_client_free(lb);

  grpc_server_stop(srv_a);
  grpc_server_stop(srv_b);
  pthread_join(th_a, nullptr);
  pthread_join(th_b, nullptr);
  grpc_server_free(srv_a);
  grpc_server_free(srv_b);
}

/* Health and reflection parse protobuf from the client by hand. A length near SIZE_MAX used to
 * wrap `seek + len` past their bounds checks (out-of-bounds read) and make skips move backwards.
 * The server must answer every hostile request and keep serving. */
void test_hostile_protobuf_lengths(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_reflection_enable(server));
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, run_server_thread, server));

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                              .port = grpc_server_port(server),
                                                              .timeout_ms = 3000},
                                        &err);
  TEST_ASSERT_NOT_NULL(client);

#define HUGE_LEN 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 /* 2^64 - 1 */
  const uint8_t health_name[] = {0x0A, HUGE_LEN, 's', 'v', 'c'};
  const uint8_t health_skip[] = {0x12, HUGE_LEN, 0x0A, 0x01, 'x'};
  const uint8_t refl_symbol[] = {0x22, HUGE_LEN, 's', 'v', 'c'};
  const uint8_t refl_list[] = {0x3A, HUGE_LEN, 0x22, 0x01, 'x'};
  const uint8_t refl_skip[] = {0x12, HUGE_LEN, 0x22, 0x01, 'x'};
#undef HUGE_LEN
  const struct {
    const char *method;
    const uint8_t *data;
    size_t len;
  } cases[] = {
      {"/grpc.health.v1.Health/Check", health_name, sizeof(health_name)},
      {"/grpc.health.v1.Health/Check", health_skip, sizeof(health_skip)},
      {"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo", refl_symbol,
       sizeof(refl_symbol)},
      {"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo", refl_list, sizeof(refl_list)},
      {"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo", refl_skip, sizeof(refl_skip)},
  };
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    grpc_client_response resp = {0};
    TEST_ASSERT_EQUAL_INT_MESSAGE(GRPC_OK,
                                  grpc_client_unary(client, cases[i].method, cases[i].data,
                                                    cases[i].len, nullptr, 0, 0, &resp),
                                  cases[i].method);
    grpc_client_response_free(&resp);
  }

  /* Too many headers resets only that stream (it used to kill the whole connection) */
  grpc_metadata many[40];
  char keys[40][16];
  for (int i = 0; i < 40; i++) {
    snprintf(keys[i], sizeof(keys[i]), "x-key-%d", i);
    many[i] = (grpc_metadata){.key = keys[i], .value = "v"};
  }
  grpc_client_response big = {0};
  grpc_err big_err =
      grpc_client_unary(client, "/grpc.health.v1.Health/Check", "", 0, many, 40, 0, &big);
  TEST_ASSERT_TRUE(big_err != GRPC_OK || big.status != GRPC_STATUS_OK);
  grpc_client_response_free(&big);

  /* still serving, on the same connection */
  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/grpc.health.v1.Health/Check", "", 0,
                                                   nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  grpc_client_response_free(&resp);
  grpc_server_stats stats = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_get_stats(server, &stats));
  TEST_ASSERT_EQUAL_UINT64(1, stats.total_connections);

  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_client_server_tcp_end_to_end);
  RUN_TEST(test_client_streaming_bidi_retries_and_lb);
  RUN_TEST(test_hostile_protobuf_lengths);
#ifndef _WIN32
  RUN_TEST(test_unix_domain_socket);
#endif
  return UNITY_END();
}
