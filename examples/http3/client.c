/* The author disclaims copyright to this source code
 * and releases it into the public domain.
 *
 * Calls examples/http3/server.c over HTTP/3 (QUIC):
 *
 *   ./http3_client.bin [host:port] [ca.crt]
 *
 * host:port defaults to localhost:50051, the CA certificate to TLS_CERT or server.crt (the
 * server's self-signed certificate, which is valid for "localhost"). Exits with 0 when every
 * call succeeded.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "grpc.h"

static int failures;

/* Reports a failed call and counts it. */
static void check(bool ok, const char *what, grpc_err err, const grpc_client_response *resp) {
  if (ok)
    return;
  failures++;
  fprintf(stderr, "FAILED %s: %s, status %s%s%s\n", what, grpc_strerror(err),
          grpc_status_str(resp != nullptr ? resp->status : GRPC_STATUS_UNKNOWN),
          resp != nullptr && resp->status_message != nullptr ? ": " : "",
          resp != nullptr && resp->status_message != nullptr ? resp->status_message : "");
}

static void print_message(const grpc_message *msg, void *userdata) {
  int *count = userdata;
  (*count)++;
  printf("  %.*s\n", (int)msg->len, (const char *)msg->data);
}

int main(int argc, char **argv) {
  const char *target = "localhost:50051";
  const char *ca = getenv("TLS_CERT") ? getenv("TLS_CERT") : "server.crt";
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--tls") == 0)
      continue; /* accepted for the integration-test runner; HTTP/3 always uses TLS */
    if (strchr(argv[i], ':') != nullptr)
      target = argv[i];
    else
      ca = argv[i];
  }
  const char *colon = strrchr(target, ':');
  char host[256];
  snprintf(host, sizeof(host), "%.*s", (int)(colon - target), target);
  uint16_t port = (uint16_t)atoi(colon + 1);

  grpc_err err;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = host,
          .port = port,
          .timeout_ms = 5000,
          .transport = GRPC_TRANSPORT_HTTP3,
          .tls = {.enable = true, .ca_file = ca, .server_name = "localhost"},
      },
      &err);
  if (client == nullptr) {
    fprintf(stderr, "cannot connect to %s over HTTP/3: %s\n", target, grpc_strerror(err));
    return 1;
  }
  printf("connected to %s over HTTP/3 (%s)\n", target, grpc_http3_versions());

  /* unary */
  grpc_client_response resp = {0};
  err = grpc_client_unary(client, "/example.Greeter/SayHello", "QUIC", 4, nullptr, 0, 0, &resp);
  check(err == GRPC_OK && resp.status == GRPC_STATUS_OK && resp.message != nullptr, "SayHello", err,
        &resp);
  if (resp.message != nullptr)
    printf("SayHello: %.*s\n", (int)resp.message->len, (const char *)resp.message->data);
  grpc_client_response_free(&resp);

  /* server streaming */
  int count = 0;
  printf("CountTo 5:\n");
  err = grpc_client_server_stream(client, "/example.Greeter/CountTo", "5", 1, nullptr, 0, 0,
                                  print_message, &count, &resp);
  check(err == GRPC_OK && resp.status == GRPC_STATUS_OK && count == 5, "CountTo", err, &resp);
  grpc_client_response_free(&resp);

  /* bidirectional streaming */
  grpc_client_stream *chat =
      grpc_client_stream_open(client, "/example.Greeter/Chat", nullptr, 0, 0, &err);
  check(chat != nullptr, "Chat open", err, nullptr);
  if (chat != nullptr) {
    const char *lines[] = {"hello", "over", "quic"};
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
      (void)grpc_client_stream_send(chat, lines[i], strlen(lines[i]));
      grpc_message *reply = nullptr;
      err = grpc_client_stream_recv(chat, &reply);
      check(err == GRPC_OK && reply != nullptr, "Chat recv", err, nullptr);
      if (reply != nullptr) {
        printf("Chat: %s -> %.*s\n", lines[i], (int)reply->len, (const char *)reply->data);
        grpc_message_free(reply);
      }
    }
    err = grpc_client_stream_finish(chat, &resp);
    check(err == GRPC_OK && resp.status == GRPC_STATUS_OK, "Chat finish", err, &resp);
    grpc_client_response_free(&resp);
  }

  /* standard health check (empty request = the whole server) */
  err = grpc_client_unary(client, "/grpc.health.v1.Health/Check", "", 0, nullptr, 0, 0, &resp);
  check(err == GRPC_OK && resp.status == GRPC_STATUS_OK, "Health/Check", err, &resp);
  if (err == GRPC_OK)
    printf("Health/Check: OK\n");
  grpc_client_response_free(&resp);

  grpc_client_free(client);
  if (failures == 0)
    printf("all calls succeeded\n");
  else
    printf("%d call(s) failed\n", failures);
  return failures == 0 ? 0 : 1;
}
