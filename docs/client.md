# Client Guide

`ezGRPC-next` includes a lightweight HTTP/2 gRPC client (`include/grpc_client.h`) supporting TCP (IPv4, IPv6, hostnames) and Unix Domain Sockets / Windows named pipes.

## Connecting & Unary Calls

```c
#include <stdio.h>
#include "grpc.h"

int main(void) {
  grpc_err err;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = 19009,
          .timeout_ms = 5000,                  /* default per-call deadline */
          .compression = GRPC_COMPRESS_GZIP,   /* optional outgoing compression */
      },
      &err);
  if (client == nullptr) {
    fprintf(stderr, "connect failed: %s\n", grpc_strerror(err));
    return 1;
  }

  grpc_metadata req_md[] = {
      {.key = "authorization", .value = "Bearer secret"},
  };

  grpc_client_response resp = {0};
  err = grpc_client_unary(client, "/helloworld.Greeter/SayHello",
                          "hello", 5, req_md, 1, 0, &resp);
  if (err == GRPC_OK) {
    printf("grpc-status: %d (%s)\n", resp.status,
           resp.status_message ? resp.status_message : "ok");
    if (resp.message != nullptr)
      printf("body: %.*s\n", (int)resp.message->len, (const char *)resp.message->data);
  }

  grpc_client_response_free(&resp);
  grpc_client_free(client);
  return 0;
}
```

## Server-Streaming Calls

```c
static void on_item(const grpc_message *msg, void *userdata) {
  int *count = userdata;
  (*count)++;
  printf("item #%d: %.*s\n", *count, (int)msg->len, (const char *)msg->data);
}

int count = 0;
grpc_client_response resp = {0};
grpc_err err = grpc_client_server_stream(
    client, "/demo.Svc/CountUp", "", 0, nullptr, 0, 10000, on_item, &count, &resp);
grpc_client_response_free(&resp);
```

## Unix Domain Sockets

Set `.unix_path` instead of `.host`/`.port`:

```c
grpc_client *client = grpc_client_new(
    &(grpc_client_config){
        .unix_path = "/tmp/my_service.sock",
        .timeout_ms = 3000,
    },
    &err);
```

## Response Headers & Trailers

`grpc_client_response` exposes `resp.headers` / `resp.nb_headers` and `resp.trailers` / `resp.nb_trailers` as `grpc_metadata` arrays. Always call `grpc_client_response_free(&resp)` when done.
