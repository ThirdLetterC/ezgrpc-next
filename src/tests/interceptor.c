#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "grpc.h"
#include "unity.h"

/* interceptors run on real calls; the Python integration test of
 * examples/python_client_c_server exercises on_call, rejection and on_done */

void setUp(void) {}

/* a failed assertion must not leave the failing allocator installed */
void tearDown(void) {
  grpc_set_allocator(nullptr);
}

static int s_freed = 0;

static grpc_status pass([[maybe_unused]] grpc_call *call, [[maybe_unused]] const char **message,
                        [[maybe_unused]] void *userdata) {
  return GRPC_STATUS_OK;
}

static void count_free([[maybe_unused]] void *userdata) {
  s_freed++;
}

static bool validator(const char *token, [[maybe_unused]] void *userdata) {
  return strcmp(token, "valid-secret-token") == 0;
}

void test_interceptor_registration(void) {
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);

  grpc_interceptor inc = {
      .name = "pass",
      .on_call = pass,
      .userdata = &s_freed,
      .userdata_free = count_free,
  };
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(server, &inc));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(server, &inc));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_server_add_interceptor(server, nullptr));

  /* the server owns the userdata */
  s_freed = 0;
  grpc_server_free(server);
  TEST_ASSERT_EQUAL_INT(2, s_freed);
}

void test_bearer_auth_interceptor(void) {
  grpc_interceptor auth = grpc_interceptor_bearer_auth(validator, nullptr);
  TEST_ASSERT_EQUAL_STRING("bearer_auth", auth.name);
  TEST_ASSERT_NOT_NULL(auth.on_call);
  TEST_ASSERT_NOT_NULL(auth.userdata);
  TEST_ASSERT_NOT_NULL(auth.userdata_free);

  /* freed with the server */
  grpc_server *server = grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_server_add_interceptor(server, &auth));
  grpc_server_free(server);
}

static void *fail_malloc([[maybe_unused]] size_t n) {
  return nullptr;
}
static void *fail_calloc([[maybe_unused]] size_t a, [[maybe_unused]] size_t b) {
  return nullptr;
}

/* The built-in interceptors fail closed: without their state (allocation failure) or with no
 * validator, they reject calls instead of silently letting everything through. */
void test_builtin_interceptors_fail_closed(void) {
  grpc_set_allocator(&(grpc_allocator){.malloc = fail_malloc, .calloc = fail_calloc});
  grpc_interceptor auth = grpc_interceptor_bearer_auth(validator, nullptr);
  grpc_interceptor rl = grpc_interceptor_rate_limit(100.0, 10);
  grpc_set_allocator(nullptr);
  TEST_ASSERT_NULL(auth.userdata);
  TEST_ASSERT_NULL(rl.userdata);

  /* with no state the hooks return before looking at the call */
  const char *msg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, auth.on_call(nullptr, &msg, auth.userdata));
  TEST_ASSERT_NOT_NULL(msg);
  msg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_INTERNAL, rl.on_call(nullptr, &msg, rl.userdata));
  TEST_ASSERT_NOT_NULL(msg);

  grpc_interceptor no_validator = grpc_interceptor_bearer_auth(nullptr, nullptr);
  TEST_ASSERT_NOT_NULL(no_validator.userdata);
  msg = nullptr;
  TEST_ASSERT_EQUAL_INT(GRPC_STATUS_UNAUTHENTICATED,
                        no_validator.on_call(nullptr, &msg, no_validator.userdata));
  no_validator.userdata_free(no_validator.userdata);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_interceptor_registration);
  RUN_TEST(test_bearer_auth_interceptor);
  RUN_TEST(test_builtin_interceptors_fail_closed);
  return UNITY_END();
}
