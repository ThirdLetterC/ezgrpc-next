/* The author disclaims copyright to this source code
 * and releases it into the public domain.
 *
 * A gRPC server reachable over HTTP/3 (QUIC, UDP) and HTTP/2 (TCP) on the same port number.
 * HTTP/2 responses carry `alt-svc: h3=":<port>"`, so HTTP/3-capable clients can switch.
 *
 *   ./http3_server.bin [port]
 *
 * TLS is mandatory for HTTP/3. The certificate and key come from TLS_CERT / TLS_KEY
 * (default server.crt / server.key in the working directory); the CMake build generates a
 * self-signed pair for CN=localhost next to the binary in generated/http3-example/.
 *
 * Methods (raw bytes, no protobuf):
 *   /example.Greeter/SayHello     unary: "Hello, <request>!"
 *   /example.Greeter/CountTo      server streaming: the request is a number n; replies 1..n
 *   /example.Greeter/Chat         bidirectional: echoes every message in upper case
 *   /grpc.health.v1.Health/Check  standard health checking
 *
 * Ctrl-C drains: no new calls are accepted, running calls get up to 5 seconds to finish.
 */
#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "grpc.h"

static grpc_server *server;

static void on_signal([[maybe_unused]] int sig) {
  grpc_server_stop(server); /* async-signal-safe */
}

static void say_hello(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *userdata) {
  char reply[300];
  int n = snprintf(reply, sizeof(reply), "Hello, %.*s!", (int)(req->len > 256 ? 256 : req->len),
                   (const char *)req->data);
  printf("SayHello from %s\n", grpc_call_peer(call));
  (void)grpc_call_reply_data(call, reply, (size_t)n);
}

static void count_to(grpc_call *call, const grpc_message *req, [[maybe_unused]] void *userdata) {
  char text[16] = {0};
  memcpy(text, req->data, req->len < sizeof(text) - 1 ? req->len : sizeof(text) - 1);
  long n = strtol(text, nullptr, 10);
  if (n < 1 || n > 1000) {
    (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT, "expected a number from 1 to 1000");
    return;
  }
  for (long i = 1; i <= n; i++) {
    char msg[16];
    int len = snprintf(msg, sizeof(msg), "%ld", i);
    (void)grpc_call_send_data(call, msg, (size_t)len);
  }
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void chat_on_message(grpc_call *call, const grpc_message *msg,
                            [[maybe_unused]] void *userdata) {
  char *upper = malloc(msg->len ? msg->len : 1);
  if (upper == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_RESOURCE_EXHAUSTED, nullptr);
    return;
  }
  for (uint32_t i = 0; i < msg->len; i++)
    upper[i] = (char)toupper(((const unsigned char *)msg->data)[i]);
  (void)grpc_call_send_data(call, upper, msg->len);
  free(upper);
}

static void chat_on_end(grpc_call *call, [[maybe_unused]] void *userdata) {
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

int main(int argc, char **argv) {
  if (!grpc_http3_available()) {
    fprintf(stderr, "this build of ezGRPC-next has no HTTP/3 support (-DGRPC_HTTP3=ON)\n");
    return 1;
  }
  uint16_t port = argc > 1 ? (uint16_t)atoi(argv[1]) : 50051;
  const char *cert = getenv("TLS_CERT") ? getenv("TLS_CERT") : "server.crt";
  const char *key = getenv("TLS_KEY") ? getenv("TLS_KEY") : "server.key";

  grpc_err err;
  server = grpc_server_new_ex(
      &(grpc_config){
          .port = port,
          .tls = {.cert_file = cert, .key_file = key},
          .http3 = {.enable = true, .advertise_alt_svc = true},
      },
      &err);
  if (server == nullptr) {
    fprintf(stderr, "cannot start the server: %s\n", grpc_strerror(err));
    return 1;
  }
  (void)grpc_server_handle(server, "/example.Greeter/SayHello", say_hello, nullptr);
  (void)grpc_server_handle(server, "/example.Greeter/CountTo", count_to, nullptr);
  (void)grpc_server_handle_stream(server, "/example.Greeter/Chat",
                                  &(grpc_stream_handler){
                                      .on_message = chat_on_message,
                                      .on_end = chat_on_end,
                                  },
                                  nullptr);
  (void)grpc_health_enable(server);

  printf("%s, %s\n", grpc_tls_backend_version(), grpc_http3_versions());
  printf("HTTP/2 on tcp/%u, HTTP/3 on udp/%u. Ctrl-C to stop.\n", grpc_server_port(server),
         grpc_server_http3_port(server));
  fflush(stdout);

  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  (void)grpc_server_run(server);

  printf("draining...\n");
  (void)grpc_server_drain(server, 5000);
  grpc_server_free(server);
  return 0;
}
