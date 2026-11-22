/* HTTP/3 with socket buffers that are often full: a preloaded shim (shims/udp_eagain.c) makes
 * every third UDP sendmsg() fail with EAGAIN. Server and client queue such datagrams with
 * uv_udp_send() instead of dropping them (which used to leave every one of them to QUIC loss
 * recovery), so a large request and response go through with nothing dropped.
 * Run from the directory holding test_cert.pem / test_key.pem. */
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "grpc.h"
#include "internal_client.h"
#include "internal_http3_server.h"
#include "test.h"

enum { BODY = 512 * 1024 };

static void echo(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *ud) {
  (void)grpc_call_reply_data(call, req->data, req->len);
}

static void *run_server(void *arg) {
  (void)grpc_server_run(arg);
  return nullptr;
}

void setUp(void) {}
void tearDown(void) {}

void test_http3_full_socket_buffers_queue_instead_of_drop(void) {
  grpc_server *server = grpc_server_new(&(grpc_config){
      .port = 0,
      .ipv4_addr = "127.0.0.1",
      .max_message_size = 4 * BODY,
      .tls = {.cert_file = "test_cert.pem", .key_file = "test_key.pem"},
      .http3 = {.enable = true},
      .log_level = GRPC_LOG_OFF,
  });
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/q.Svc/Echo", echo, nullptr));
  pthread_t thread;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&thread, nullptr, run_server, server));

  grpc_err err = GRPC_OK;
  grpc_client *client =
      grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                            .port = grpc_server_http3_port(server),
                                            .timeout_ms = 10000,
                                            .max_message_size = 4 * BODY,
                                            .transport = GRPC_TRANSPORT_HTTP3,
                                            .tls = {.enable = true, .insecure_skip_verify = true}},
                      &err);
  TEST_ASSERT_NOT_NULL(client);
  uint8_t *body = malloc(BODY);
  TEST_ASSERT_NOT_NULL(body);
  for (size_t i = 0; i < BODY; i++)
    body[i] = (uint8_t)(i * 31 + 7);
  for (int round = 0; round < 3; round++) {
    grpc_client_response resp = {0};
    TEST_ASSERT_EQUAL_INT(
        GRPC_OK, grpc_client_unary(client, "/q.Svc/Echo", body, BODY, nullptr, 0, 10000, &resp));
    TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
    TEST_ASSERT_NOT_NULL(resp.message);
    TEST_ASSERT_EQUAL_UINT32(BODY, resp.message->len);
    TEST_ASSERT_EQUAL_MEMORY(body, resp.message->data, BODY);
    grpc_client_response_free(&resp);
  }
  free(body);
  uint64_t client_queued = 0, client_dropped = 0, client_gso = 0;
  h3c_send_counters(&client->subs[0], &client_queued, &client_dropped, &client_gso);
  grpc_client_free(client);

  grpc_server_stop(server);
  pthread_join(thread, nullptr); /* the counters are the loop thread's */
  uint64_t server_queued = server->h3->stat_send_queued;
  uint64_t server_dropped = server->h3->stat_send_dropped;
  grpc_server_free(server);

  TEST_ASSERT_TRUE_MESSAGE(server_queued > 0, "the shim did not make the server queue");
  TEST_ASSERT_TRUE_MESSAGE(client_queued > 0, "the shim did not make the client queue");
  TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, server_dropped, "the server dropped datagrams");
  TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, client_dropped, "the client dropped datagrams");
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_http3_full_socket_buffers_queue_instead_of_drop);
  return UNITY_END();
}
