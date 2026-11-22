#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdatomic.h>
#ifndef _WIN32
#include <pthread.h>
#endif
#include "grpc.h"
#include "test_msg.pb-c.h"
#include "test.h"

/* A forwarding allocator: every call goes to the allocator that was installed before it (so
 * blocks are always freed by the family that allocated them), except that allocations fail on a
 * thread that set fail_here. The live call fails only the server thread's allocations this way,
 * while the client on the test thread keeps working. */
static grpc_allocator saved_alloc;
#if defined(_MSC_VER) && !defined(__clang__)
#define PB_THREAD_LOCAL __declspec(thread)
#else
#define PB_THREAD_LOCAL _Thread_local /* thread_local is a keyword only from GCC 13 in C23 */
#endif
static PB_THREAD_LOCAL bool fail_here;

static void *fwd_malloc(size_t sz) {
  return fail_here ? nullptr : saved_alloc.malloc(sz);
}

static void *fwd_calloc(size_t n, size_t sz) {
  return fail_here ? nullptr : saved_alloc.calloc(n, sz);
}

static void *fwd_realloc(void *p, size_t sz) {
  return fail_here ? nullptr : saved_alloc.realloc(p, sz);
}

static void fwd_free(void *p) {
  saved_alloc.free(p);
}

static void *fwd_aligned(size_t a, size_t sz) {
  return fail_here ? nullptr : saved_alloc.aligned_alloc(a, sz);
}

static void install_failing_allocator(void) {
  saved_alloc = *grpc_get_allocator();
  grpc_set_allocator(&(grpc_allocator){.malloc = fwd_malloc,
                                       .calloc = fwd_calloc,
                                       .realloc = fwd_realloc,
                                       .free = fwd_free,
                                       .aligned_alloc = fwd_aligned});
}

void setUp(void) {}

/* runs even when an assertion failed between installing and restoring the allocator */
void tearDown(void) {
  fail_here = false;
  grpc_set_allocator(nullptr);
}

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

/* results of the handler, checked on the test thread */
static atomic_int handler_calls;
static atomic_bool handler_unpacked;
static atomic_int handler_send_oom = GRPC_OK;
static atomic_int handler_reply_oom = GRPC_OK;
static atomic_int handler_reply = GRPC_ERR_STATE;

static void pb_echo_handler(grpc_call *call, const grpc_message *req_msg,
                            [[maybe_unused]] void *userdata) {
  atomic_fetch_add(&handler_calls, 1);
  Test__Echo *req = GRPC_PB_UNPACK(Test__Echo, &test__echo__descriptor, req_msg);
  atomic_store(&handler_unpacked, req != nullptr);
  if (req == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_INTERNAL, "unpack failed");
    return;
  }
  /* a real call whose packing fails: neither function may send or finish */
  fail_here = true;
  atomic_store(&handler_send_oom, grpc_call_send_pb(call, &req->base));
  atomic_store(&handler_reply_oom, grpc_call_reply_pb(call, &req->base));
  fail_here = false;
  atomic_store(&handler_reply, grpc_call_reply_pb(call, &req->base));
  GRPC_PB_FREE_UNPACKED(req);
}

void test_pb_live_call(void) {
  install_failing_allocator();
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_server_handle(server, "/test.Pb/Echo", pb_echo_handler, nullptr));
  pthread_t th;
  TEST_ASSERT_EQUAL_INT(0, pthread_create(&th, nullptr, pb_server_run, server));

  grpc_err err = GRPC_OK;
  grpc_client *client = grpc_client_new(&(grpc_client_config){.host = "127.0.0.1",
                                                              .port = grpc_server_port(server),
                                                              .timeout_ms = 2000},
                                        &err);
  TEST_ASSERT_NOT_NULL(client);

  Test__Echo req = TEST__ECHO__INIT;
  req.text = (char *)"pb-live";
  req.number = 7;
  grpc_message *packed = grpc_pb_pack(&req.base);
  TEST_ASSERT_NOT_NULL(packed);

  grpc_client_response resp = {0};
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_client_unary(client, "/test.Pb/Echo", packed->data,
                                                   packed->len, nullptr, 0, 0, &resp));
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_OK, resp.status);
  TEST_ASSERT_NOT_NULL(resp.message);
  Test__Echo *reply = GRPC_PB_UNPACK(Test__Echo, &test__echo__descriptor, resp.message);
  TEST_ASSERT_NOT_NULL(reply);
  TEST_ASSERT_EQUAL_STRING("pb-live", reply->text);
  TEST_ASSERT_EQUAL_INT32(7, reply->number);
  TEST_ASSERT_EQUAL_INT(1, atomic_load(&handler_calls));
  TEST_ASSERT_TRUE(atomic_load(&handler_unpacked));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, atomic_load(&handler_send_oom));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_NOMEM, atomic_load(&handler_reply_oom));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, atomic_load(&handler_reply));

  GRPC_PB_FREE_UNPACKED(reply);
  grpc_client_response_free(&resp);
  grpc_message_free(packed);
  grpc_client_free(client);
  grpc_server_stop(server);
  pthread_join(th, nullptr);
  grpc_server_free(server);
}
#endif

void test_pb_oom_and_errors(void) {
  Test__Echo req = TEST__ECHO__INIT;
  req.text = (char *)"oom";
  req.number = 1;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_call_reply_pb(nullptr, &req.base));

  install_failing_allocator();
  fail_here = true;
  grpc_message *packed = grpc_pb_pack(&req.base);
  fail_here = false;
  TEST_ASSERT_NULL(packed);
  /* argument checks come before packing; send/reply on a real call that fails to pack are
   * covered by test_pb_live_call */
  fail_here = true;
  grpc_err send_err = grpc_call_send_pb(nullptr, &req.base);
  fail_here = false;
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, send_err);

  /* the same message packs once allocations succeed again */
  packed = grpc_pb_pack(&req.base);
  TEST_ASSERT_NOT_NULL(packed);
  grpc_message_free(packed);
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
