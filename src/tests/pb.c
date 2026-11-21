#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#ifndef _WIN32
#include <pthread.h>
#endif
#include "grpc.h"
#include "test_msg.pb-c.h"
#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

void test_pb_allocator(void) {
  ProtobufCAllocator *allocator = grpc_pb_get_allocator();
  TEST_ASSERT_NOT_NULL(allocator);
  TEST_ASSERT_NOT_NULL(allocator->alloc);
  TEST_ASSERT_NOT_NULL(allocator->free);

  // Allocate and free memory through allocator
  void *ptr = allocator->alloc(allocator->allocator_data, 128);
  TEST_ASSERT_NOT_NULL(ptr);
  memset(ptr, 0x42, 128);
  allocator->free(allocator->allocator_data, ptr);
}

void test_pb_pack_and_unpack(void) {
  Test__Echo echo = TEST__ECHO__INIT;
  echo.text = (char *)"Hello from grpc protobuf-c integration!";
  echo.number = 42;

  // Pack into grpc_message
  grpc_message *msg = grpc_pb_pack((ProtobufCMessage *)&echo);
  TEST_ASSERT_NOT_NULL(msg);
  TEST_ASSERT_EQUAL_UINT8(0, msg->cflag);
  TEST_ASSERT_GREATER_THAN_UINT32(0, msg->len);

  // Unpack from grpc_message
  Test__Echo *unpacked = GRPC_PB_UNPACK(Test__Echo, &test__echo__descriptor, msg);
  TEST_ASSERT_NOT_NULL(unpacked);
  TEST_ASSERT_EQUAL_STRING("Hello from grpc protobuf-c integration!", unpacked->text);
  TEST_ASSERT_EQUAL_INT32(42, unpacked->number);

  // Clean up
  GRPC_PB_FREE_UNPACKED(unpacked);
  grpc_message_free(msg);
}

void test_pb_send_null_safety(void) {
  Test__Echo echo = TEST__ECHO__INIT;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_pb(nullptr, &echo.base));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_send_pb(nullptr, nullptr));
}

void test_pb_null_safety(void) {
  TEST_ASSERT_NULL(grpc_pb_pack(nullptr));
  TEST_ASSERT_NULL(grpc_pb_unpack(nullptr, nullptr));
  // Freeing null should not crash
  grpc_pb_free_unpacked(nullptr);
}

#ifndef _WIN32
static void *pb_server_run(void *arg) {
  (void)grpc_server_run((grpc_server *)arg);
  return nullptr;
}

static void pb_echo_handler(grpc_call *call, const grpc_message *req_msg,
                            [[maybe_unused]] void *userdata) {
  Test__Echo *req = GRPC_PB_UNPACK(Test__Echo, &test__echo__descriptor, req_msg);
  TEST_ASSERT_NOT_NULL(req);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_call_reply_pb(call, &req->base));
  GRPC_PB_FREE_UNPACKED(req);
}

void test_pb_live_call(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_handle(server, "/test.Pb/Echo", pb_echo_handler, nullptr));
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, pb_server_run, server));

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){.host = "127.0.0.1", .port = grpc_server_port(server), .timeout_ms = 2000},
      &err);
  TEST_ASSERT_NOT_NULL(client);

  Test__Echo req = TEST__ECHO__INIT;
  req.text = (char *)"pb-live";
  req.number = 7;
  grpc_message *packed = grpc_pb_pack(&req.base);
  TEST_ASSERT_NOT_NULL(packed);

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(
      GRPC_OK, grpc_client_unary(client, "/test.Pb/Echo", packed->data, packed->len, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  Test__Echo *reply = GRPC_PB_UNPACK(Test__Echo, &test__echo__descriptor, resp.message);
  TEST_ASSERT_NOT_NULL(reply);
  TEST_ASSERT_EQUAL_STRING("pb-live", reply->text);
  TEST_ASSERT_EQUAL_INT32(7, reply->number);

  GRPC_PB_FREE_UNPACKED(reply);
  grpc_client_response_free(&resp);
  grpc_message_free(packed);
  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}
#endif

static void *pb_fail_malloc([[maybe_unused]] size_t sz) {
  return nullptr;
}

static void *pb_fail_calloc([[maybe_unused]] size_t n, [[maybe_unused]] size_t sz) {
  return nullptr;
}

static void *pb_fail_realloc([[maybe_unused]] void *p, [[maybe_unused]] size_t sz) {
  return nullptr;
}

/* a real no-op: the failing allocator hands out nothing, and anything freed while it is
 * installed came from the default allocator (_aligned_malloc on MSVC), not libc malloc */
static void pb_noop_free([[maybe_unused]] void *p) {}

static void *pb_fail_aligned([[maybe_unused]] size_t a, [[maybe_unused]] size_t sz) {
  return nullptr;
}

void test_pb_oom_and_errors(void) {
  Test__Echo req = TEST__ECHO__INIT;
  req.text = (char *)"oom";
  req.number = 1;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_reply_pb(nullptr, &req.base));

  grpc_allocator fail_alloc = {
      .malloc = pb_fail_malloc,
      .calloc = pb_fail_calloc,
      .realloc = pb_fail_realloc,
      .free = pb_noop_free,
      .aligned_alloc = pb_fail_aligned,
  };
  grpc_set_allocator(&fail_alloc);
  TEST_ASSERT_NULL(grpc_pb_pack(&req.base));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, grpc_call_send_pb((grpc_call *)&req, &req.base));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, grpc_call_reply_pb((grpc_call *)&req, &req.base));
  grpc_set_allocator(nullptr);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_pb_allocator);
  RUN_TEST(test_pb_pack_and_unpack);
  RUN_TEST(test_pb_send_null_safety);
  RUN_TEST(test_pb_null_safety);
  RUN_TEST(test_pb_oom_and_errors);
#ifndef _WIN32
  RUN_TEST(test_pb_live_call);
#endif
  return UNITY_END();
}
