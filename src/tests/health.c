#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "grpc.h"
#include "test.h"

void setUp(void) {}
void tearDown(void) {}

/* an OS-assigned port, so concurrent runs and other tests never collide */
static grpc_server *new_server(void) {
  return grpc_server_new(
      &(grpc_config){.port = 0, .ipv4_addr = "127.0.0.1", .log_level = GRPC_LOG_OFF});
}

void test_health_status_get_set(void) {
  grpc_server *server = new_server();
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));

  /* everything is SERVING until told otherwise */
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, ""));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, "any.Service"));

  grpc_health_set_status(server, nullptr, GRPC_HEALTH_NOT_SERVING);
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_NOT_SERVING, grpc_health_get_status(server, nullptr));

  grpc_health_set_status(server, "helloworld.Greeter", GRPC_HEALTH_SERVING);
  grpc_health_set_status(server, "order.OrderService", GRPC_HEALTH_NOT_SERVING);
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, "helloworld.Greeter"));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_NOT_SERVING,
                        grpc_health_get_status(server, "order.OrderService"));
  grpc_health_set_status(server, "order.OrderService", GRPC_HEALTH_SERVING);
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, "order.OrderService"));

  grpc_server_free(server);
}

void test_health_is_per_server(void) {
  grpc_server *a = new_server();
  grpc_server *b = new_server();
  TEST_ASSERT_NOT_NULL(a);
  TEST_ASSERT_NOT_NULL(b);
  grpc_health_set_status(a, nullptr, GRPC_HEALTH_NOT_SERVING);
  grpc_health_set_status(b, nullptr, GRPC_HEALTH_SERVING);
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_NOT_SERVING, grpc_health_get_status(a, nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(b, nullptr));
  grpc_server_free(a);
  grpc_server_free(b);
}

void test_health_enable_twice(void) {
  grpc_server *server = new_server();
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_EXISTS, grpc_health_enable(server));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID, grpc_health_enable(nullptr));
  TEST_ASSERT_EQUAL_INT(GRPC_ERR_INVALID,
                        grpc_health_set_status(nullptr, "svc", GRPC_HEALTH_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_UNKNOWN, grpc_health_get_status(nullptr, "svc"));
  grpc_server_free(server);
}

/* Names are kept truncated to 127 bytes (MAX_SERVICE_NAME_LEN - 1), so they are compared on
 * their first 127 bytes: a long name set by the application is found again by the same long name
 * (it used to compare the truncated copy with the full name and never match). */
void test_health_long_service_name(void) {
  grpc_server *server = new_server();
  TEST_ASSERT_NOT_NULL(server);
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_enable(server));

  char long_name[200];
  memset(long_name, 'a', sizeof(long_name) - 1);
  long_name[sizeof(long_name) - 1] = '\0';
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_health_set_status(server, long_name, GRPC_HEALTH_NOT_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_NOT_SERVING, grpc_health_get_status(server, long_name));
  /* setting it again updates the same entry */
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_set_status(server, long_name, GRPC_HEALTH_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, long_name));
  TEST_ASSERT_EQUAL_INT(GRPC_OK,
                        grpc_health_set_status(server, long_name, GRPC_HEALTH_NOT_SERVING));

  /* documented limitation: two names that differ only after byte 127 share one entry */
  char other[200];
  memcpy(other, long_name, sizeof(other));
  other[127] = 'b';
  other[150] = 'c';
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_NOT_SERVING, grpc_health_get_status(server, other));
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_set_status(server, other, GRPC_HEALTH_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, long_name));

  /* a difference within the first 127 bytes is a different service */
  char distinct[200];
  memcpy(distinct, long_name, sizeof(distinct));
  distinct[126] = 'z';
  TEST_ASSERT_EQUAL_INT(GRPC_OK, grpc_health_set_status(server, distinct, GRPC_HEALTH_NOT_SERVING));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_NOT_SERVING, grpc_health_get_status(server, distinct));
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, long_name));

  /* the 127-byte prefix alone names the same entry as the long name */
  char prefix[128];
  memcpy(prefix, long_name, 127);
  prefix[127] = '\0';
  TEST_ASSERT_EQUAL_INT(GRPC_HEALTH_SERVING, grpc_health_get_status(server, prefix));

  grpc_server_free(server);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_health_status_get_set);
  RUN_TEST(test_health_is_per_server);
  RUN_TEST(test_health_enable_twice);
  RUN_TEST(test_health_long_service_name);
  return UNITY_END();
}
