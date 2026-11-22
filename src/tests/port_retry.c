/* With port 0 and both an IPv4 and an IPv6 address, the IPv4 listener picks the port and IPv6
 * must take the same one. When that port is already in use on IPv6, the server starts over with
 * a new port instead of failing. Run with the bind_v6_busy shim preloaded (Linux, CMake sets it),
 * which makes the first IPv6 bind fail with EADDRINUSE. */
#include <netinet/in.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>
#include "grpc.h"
#include "test.h"

void setUp(void) {}
void tearDown(void) {}

void test_port_zero_retries_when_ipv6_port_taken(void) {
  /* is there an IPv6 loopback at all? (a port-0 bind is not affected by the shim) */
  int probe = socket(AF_INET6, SOCK_STREAM, 0);
  struct sockaddr_in6 sin6 = {.sin6_family = AF_INET6, .sin6_addr = in6addr_loopback};
  bool has_v6 = probe >= 0 && bind(probe, (struct sockaddr *)&sin6, sizeof(sin6)) == 0;
  if (probe >= 0)
    close(probe);
  if (!has_v6)
    TEST_IGNORE_MESSAGE("no IPv6 loopback");

  grpc_err err = GRPC_OK;
  grpc_server *server = grpc_server_new_ex(
      &(grpc_config){
          .port = 0, .ipv4_addr = "127.0.0.1", .ipv6_addr = "::1", .log_level = GRPC_LOG_OFF},
      &err);
  TEST_ASSERT_NOT_NULL_MESSAGE(server, "a port taken on IPv6 made the dual-stack server fail");
  TEST_ASSERT_EQUAL_INT(GRPC_OK, err);
  TEST_ASSERT_TRUE(grpc_server_port(server) != 0);
  grpc_server_free(server);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_port_zero_retries_when_ipv6_port_taken);
  return UNITY_END();
}
