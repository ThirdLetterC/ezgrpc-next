# Client Guide

`ezGRPC-next` includes an HTTP/2 gRPC client (`include/grpc_client.h`) supporting:
- Unary, server-streaming, client-streaming, and bidirectional streaming RPCs
- TCP (IPv4, IPv6, hostnames) and Unix Domain Sockets / Windows named pipes
- Client-side TLS & mTLS (CA file or PEM, SNI, client certificates)
- Automatic retries with exponential backoff on transient `UNAVAILABLE` / transport failures
- Round-robin client-side load balancing across multiple targets

## Unary Calls, TLS/mTLS & Retries

```c
#include <stdio.h>
#include "grpc.h"

int main(void) {
  grpc_err err;
  grpc_client *client = grpc_client_new(
      &(grpc_client_config){
          .host = "127.0.0.1",
          .port = 19009,
          .timeout_ms = 5000,
          .compression = GRPC_COMPRESS_GZIP,
          .max_retries = 3,
          .retry_backoff_ms = 50,
          .tls = {
              .enable = true,
              .ca_file = "ca.crt",
              .cert_file = "client.crt", /* optional mTLS client cert */
              .key_file = "client.key",
              .server_name = "localhost",
          },
      },
      &err);
  if (client == nullptr) {
    fprintf(stderr, "connect failed: %s\n", grpc_strerror(err));
    return 1;
  }

  grpc_metadata req_md[] = {{.key = "authorization", .value = "Bearer secret"}};
  grpc_client_response resp = {0};
  if (grpc_client_unary(client, "/helloworld.Greeter/SayHello",
                        "hello", 5, req_md, 1, 0, &resp) == GRPC_OK) {
    printf("status=%d body=%.*s\n", resp.status,
           resp.message ? (int)resp.message->len : 0,
           resp.message ? (const char *)resp.message->data : "");
  }
  grpc_client_response_free(&resp);
  grpc_client_free(client);
  return 0;
}
```

### HTTP/3

Set `.transport = GRPC_TRANSPORT_HTTP3` (with `tls.enable`) to call over QUIC instead of TCP; see [HTTP/3](http3.md).

## Server-Streaming Calls

```c
static void on_item(const grpc_message *msg, void *userdata) {
  int *count = userdata;
  (*count)++;
  printf("item #%d: %.*s\n", *count, (int)msg->len, (const char *)msg->data);
}

int count = 0;
grpc_client_response resp = {0};
(void)grpc_client_server_stream(
    client, "/demo.Svc/CountUp", "", 0, nullptr, 0, 10000, on_item, &count, &resp);
grpc_client_response_free(&resp);
```

## Client-Streaming & Bidirectional Streaming

```c
grpc_err err;
grpc_client_stream *stream =
    grpc_client_stream_open(client, "/demo.Svc/Chat", nullptr, 0, 5000, &err);

for (int i = 0; i < 3; i++) {
  (void)grpc_client_stream_send(stream, "ping", 4);
  grpc_message *reply = nullptr;
  if (grpc_client_stream_recv(stream, &reply) == GRPC_OK && reply != nullptr) {
    /* process reply->data / reply->len */
    grpc_message_free(reply);
  }
}

grpc_client_response resp = {0};
(void)grpc_client_stream_finish(stream, &resp); /* half-closes send & waits for trailers */
grpc_client_response_free(&resp);
```

For client-streaming, call `grpc_client_stream_send()` N times, then `grpc_client_stream_close_send()` and `grpc_client_stream_finish()` (which places the single reply in `resp.message`). Call `grpc_client_stream_cancel()` to abort early.

## Round-Robin Load Balancing & Unix Domain Sockets

```c
/* Round-robin pool across multiple endpoints ("host:port", "[ipv6]:port", or "unix:/path") */
const char *targets[] = {"127.0.0.1:19001", "127.0.0.1:19002"};
grpc_client *lb = grpc_client_new(
    &(grpc_client_config){
        .targets = targets,
        .nb_targets = 2,
        .max_retries = 2,
    },
    &err);

/* Or a single Unix Domain Socket */
grpc_client *uds = grpc_client_new(
    &(grpc_client_config){.unix_path = "/tmp/my_service.sock"}, &err);
```
