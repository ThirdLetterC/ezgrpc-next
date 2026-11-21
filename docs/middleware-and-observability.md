# Middleware & Observability

## Interceptors (`include/grpc_interceptor.h`)

Interceptors run in registration order and support:
- `path_prefix`: optional method prefix filter (e.g. `"/helloworld.Greeter/"`; `nullptr` matches all methods)
- `on_call`: runs after headers arrive; returning anything other than `GRPC_STATUS_OK` rejects the call
- `on_recv_message`: runs before each inbound message is delivered to the handler
- `on_send_message`: runs before each outbound response message is queued
- `on_done`: runs once when the call finishes or is cancelled, with `duration_ms`

```c
static grpc_status audit_recv(grpc_call *call, const grpc_message *msg,
                              const char **err_msg, void *userdata) {
  (void)call;
  (void)userdata;
  if (msg->len == 0) {
    *err_msg = "empty payload rejected";
    return GRPC_STATUS_INVALID_ARGUMENT;
  }
  return GRPC_STATUS_OK;
}

static void log_done(grpc_call *call, grpc_status status, uint64_t duration_ms, void *userdata) {
  (void)userdata;
  printf("%s -> %d (%llu ms)\n", grpc_call_method(call), status, (unsigned long long)duration_ms);
}

(void)grpc_server_add_interceptor(server, &(grpc_interceptor){
    .name = "audit",
    .path_prefix = "/orders.OrderService/",
    .on_recv_message = audit_recv,
    .on_done = log_done,
});
```

### Built-in Bearer Auth & Rate Limiter

```c
static bool check_token(const char *token, void *userdata) {
  (void)userdata;
  return strcmp(token, "secret") == 0;
}

grpc_interceptor auth = grpc_interceptor_bearer_auth(check_token, nullptr);
(void)grpc_server_add_interceptor(server, &auth);

/* Token-bucket rate limiter: 100 req/s steady rate, burst of 20 */
grpc_interceptor rl = grpc_interceptor_rate_limit(100.0, 20);
rl.path_prefix = "/orders.OrderService/";
(void)grpc_server_add_interceptor(server, &rl);
```

---

## Health Checking (`Check` & `Watch`)

`grpc_health_enable(server)` registers both `/grpc.health.v1.Health/Check` and streaming `/grpc.health.v1.Health/Watch`. Updating a status from any thread pushes updates to all active `Watch` streams:

```c
(void)grpc_health_enable(server);
(void)grpc_health_set_status(server, "helloworld.Greeter", GRPC_HEALTH_SERVING);

/* Later, from any thread (e.g. when a dependency degrades): */
(void)grpc_health_set_status(server, "helloworld.Greeter", GRPC_HEALTH_NOT_SERVING);
```

---

## Server Reflection & Channelz

```c
/* Enables grpc.reflection.v1 and v1alpha ServerReflectionInfo */
(void)grpc_reflection_enable(server);
(void)grpc_reflection_add_file_descriptor(server, "helloworld.proto", desc_bytes, desc_len);

/* Enables /grpc.channelz.v1.Channelz/GetServers and GetServerSockets */
(void)grpc_channelz_enable(server);
```

---

## Runtime Metrics (`grpc_server_get_stats`)

Thread-safe snapshot of live counters for Prometheus/OpenTelemetry exporters:

```c
grpc_server_stats st;
if (grpc_server_get_stats(server, &st) == GRPC_OK) {
  printf("conns=%llu/%llu streams=%llu calls=%llu (ok=%llu err=%llu) rx=%lluB tx=%lluB\n",
         (unsigned long long)st.active_connections,
         (unsigned long long)st.total_connections,
         (unsigned long long)st.active_streams,
         (unsigned long long)st.total_calls,
         (unsigned long long)st.calls_ok,
         (unsigned long long)st.calls_error,
         (unsigned long long)st.bytes_received,
         (unsigned long long)st.bytes_sent);
}
```

---

## W3C Distributed Tracing (`traceparent`)

Parse incoming W3C `traceparent` headers and format them for downstream propagation:

```c
grpc_trace_context tc;
if (grpc_call_trace_context(call, &tc)) {
  char header[64];
  if (grpc_traceparent_format(&tc, header, sizeof(header)) == GRPC_OK) {
    /* propagate header to downstream grpc_client calls */
  }
}
```
