# Server & RPC Guide

## Registering RPC Handlers

### Unary & Server-Streaming (`grpc_server_handle`)

Called once the client finishes sending its single request message:

```c
/* Unary shortcut: sends payload and finishes with GRPC_STATUS_OK */
static void echo(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)userdata;
  (void)grpc_call_reply_data(call, req->data, req->len);
}

/* Server streaming: send multiple messages, then finish */
static void count_up(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  for (int i = 0; i < 5; i++) {
    char buf[16];
    int n = snprintf(buf, sizeof(buf), "msg-%d", i);
    (void)grpc_call_send_data(call, buf, (size_t)n);
  }
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

(void)grpc_server_handle(server, "/demo.Svc/Echo", echo, nullptr);
(void)grpc_server_handle(server, "/demo.Svc/CountUp", count_up, nullptr);
```

### Client-Streaming & Bidirectional Streaming (`grpc_server_handle_stream`)

Attach per-call state via `grpc_call_set_userdata()` (automatically freed when the call ends):

```c
static void bidi_start(grpc_call *call, void *userdata) {
  (void)userdata;
  size_t *count = grpc_calloc(1, sizeof(*count));
  grpc_call_set_userdata(call, count, grpc_free);
}

static void bidi_msg(grpc_call *call, const grpc_message *msg, void *userdata) {
  (void)userdata;
  size_t *count = grpc_call_userdata(call);
  (*count)++;
  (void)grpc_call_send_data(call, msg->data, msg->len);
}

static void bidi_end(grpc_call *call, void *userdata) {
  (void)userdata;
  (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}

static void bidi_cancel(grpc_call *call, void *userdata) {
  (void)call;
  (void)userdata;
}

grpc_stream_handler h = {
    .on_start = bidi_start,
    .on_message = bidi_msg,
    .on_end = bidi_end,
    .on_cancel = bidi_cancel,
};
(void)grpc_server_handle_stream(server, "/demo.Svc/Chat", &h, nullptr);
```

### Poll-Driven Mode (`grpc_server_register_path`)

Prefer polling over callbacks? Register a path for events and drive `grpc_server_poll()`:

```c
(void)grpc_server_register_path(server, "/demo.Svc/Raw", my_ctx);

while (running) {
  if (grpc_server_poll(server, 100) < 0)
    break;
  size_t n = 0;
  const grpc_event *events = grpc_server_events(server, &n);
  for (size_t i = 0; i < n; i++) {
    if (events[i].type == GRPC_EVENT_MESSAGE) {
      (void)grpc_call_reply_data(events[i].call, events[i].message->data, events[i].message->len);
    }
  }
}
```

### Fallback / Wildcard Handler

Handle any unregistered path (useful for proxies and dynamic gateways). `grpc_call_method(call)` returns the actual incoming `:path`:

```c
static void proxy_msg(grpc_call *call, const grpc_message *msg, void *userdata) {
  (void)userdata;
  const char *method = grpc_call_method(call); /* e.g. "/any.Service/Method" */
  (void)method;
  (void)grpc_call_reply_data(call, msg->data, msg->len);
}

(void)grpc_server_set_fallback(server, &(grpc_stream_handler){.on_message = proxy_msg}, nullptr);
```

---

## Metadata, Trailers & Rich Errors

```c
/* Single lookup (case-insensitive) */
size_t len = 0;
const char *ua = grpc_call_metadata(call, "user-agent", &len);

/* Multi-value lookup (repeated keys) */
size_t idx = 0;
const char *tag;
while ((tag = grpc_call_metadata_next(call, "x-tag", &idx, &len)) != nullptr) {
  /* process each x-tag header */
}

/* Initial response metadata (before first send) & trailing metadata */
grpc_metadata init_md[] = {{.key = "x-request-id", .value = "abc-123"}};
(void)grpc_call_send_metadata(call, init_md, 1);

grpc_metadata trailers[] = {{.key = "x-server-time", .value = "2ms"}};
(void)grpc_call_add_trailers(call, trailers, 1);
(void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
```

### Rich Error Model (`google.rpc.Status`)

```c
grpc_status_detail_entry details[] = {
    {.type_url = "type.googleapis.com/google.rpc.ErrorInfo", .value = buf1, .value_len = len1},
    {.type_url = "type.googleapis.com/google.rpc.RetryInfo", .value = buf2, .value_len = len2},
};
(void)grpc_call_finish_with_details(call, GRPC_STATUS_INVALID_ARGUMENT, "bad input", details, 2);
```

Decode all `Any` details from a `grpc-status-details-bin` Base64 string:

```c
grpc_status_details decoded = {0};
if (grpc_status_details_decode_all(b64_trailer, &decoded) == GRPC_OK) {
  for (size_t i = 0; i < decoded.nb_details; i++) {
    /* decoded.details[i].type_url, .value, .value_len */
  }
  grpc_status_details_free(&decoded);
}
```

---

## Transports: Ephemeral Ports, Unix Domain Sockets & gRPC-Web

- **Ephemeral port**: Set `.port = 0` and query the OS-assigned port with `grpc_server_port(server)`.
- **Unix Domain Sockets (or Windows Named Pipes)**:
  ```c
  grpc_server *server = grpc_server_new(&(grpc_config){
      .unix_path = "/tmp/my_service.sock",
  });
  ```
- **gRPC-Web**: Requests with `content-type: application/grpc-web` or `application/grpc-web-text` are handled automatically. Trailers are encoded inside the `0x80` DATA trailer frame (Base64-framed for `grpc-web-text`).

---

HTTP/3 (QUIC) is an additional transport on UDP next to TCP, enabled with `config.http3`; see [HTTP/3](http3.md).

## HTTP/2 Keepalive, Connection Limits, Backpressure & Graceful Drain

```c
grpc_server *server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .keepalive_time_ms = 30000,         /* send HTTP/2 PING after 30s idle */
    .keepalive_timeout_ms = 10000,      /* close if unacknowledged after 10s */
    .max_connection_idle_ms = 120000,   /* GOAWAY idle connections after 2m */
    .max_connection_age_ms = 600000,    /* rotate connections every 10m */
    .max_connection_age_grace_ms = 5000,
    .max_ping_strikes = 2,              /* ENHANCE_YOUR_CALM on ping abuse */
    .min_ping_interval_ms = 5000,
    .max_send_queue_bytes = 256 * 1024, /* per-stream backpressure limit */
});
```

### Streaming Backpressure

When `max_send_queue_bytes > 0`, `grpc_call_send()` returns `GRPC_ERR_LIMIT` if the stream's outbound queue would exceed the limit. Register `grpc_call_on_writable()` to resume streaming when the queue drains:

```c
static void resume_stream(grpc_call *call, void *userdata) {
  stream_ctx *ctx = userdata;
  while (grpc_call_is_writable(call) && ctx->remaining > 0) {
    if (grpc_call_send_data(call, ctx->chunk, ctx->chunk_len) == GRPC_ERR_LIMIT)
      return; /* on_writable will fire again once drained */
    ctx->remaining--;
  }
  if (ctx->remaining == 0)
    (void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
}
```

### Graceful Shutdown

Call `grpc_server_stop(server)` from a signal handler or another thread to exit `grpc_server_run()`, then drain active calls before freeing:

```c
(void)grpc_server_run(server);
/* Stop accepting new connections, send HTTP/2 GOAWAY, wait up to 5s for active calls */
grpc_err drain_err = grpc_server_drain(server, 5000);
grpc_server_free(server);
```
