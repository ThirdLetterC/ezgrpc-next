# Architecture & Concurrency

## I/O Model

Each `grpc_server` owns one libuv event loop driven by `grpc_server_run()` or `grpc_server_poll()`:

- **Sockets**: Connections are libuv TCP streams (`uv_tcp_t`) or Unix Domain Sockets / Windows named pipes (`uv_pipe_t`). Incoming bytes are read into a shared loop buffer and fed directly to `nghttp2`.
- **Write batching**: `grpc_call_send()`, `grpc_call_reply()`, `grpc_call_finish()` and metadata calls only **queue** HTTP/2 frames. Everything queued for a connection is flushed in a single write at the end of the poll iteration. Call `grpc_server_flush()` if you need immediate transmission before the next poll.
- **In-memory TLS**: The TLS backend never touches sockets directly; ciphertext is fed and drained via memory buffers so batches are encrypted in large records and all 7 TLS backends share one I/O path.

---

## Scaling Across Cores

### Option 1: Multi-Loop (`SO_REUSEPORT`, Unix)

Create one `grpc_server` per thread bound to the same port. On Unix, listening sockets enable `SO_REUSEPORT` so the kernel distributes incoming connections across event loops with zero lock contention. The HTTP/3 (QUIC) UDP sockets do the same; see [HTTP/3](http3.md) for the one caveat (NAT rebinding).

### Option 2: Worker Thread Pool (Unix & Windows)

Run one `grpc_server` loop and offload CPU-heavy or blocking work to worker threads. Take a reference to `grpc_call` before handing it to a worker, then reply using the thread-safe `_async` APIs or `grpc_server_post()`:

```c
static void worker_job(void *arg) {
  grpc_call *call = arg;

  /* ... heavy computation on worker thread ... */
  grpc_metadata md[] = {{.key = "x-worker", .value = "pool-1"}};
  (void)grpc_call_send_metadata_async(call, md, 1);

  grpc_message *msg = grpc_message_new("computed", 8);
  (void)grpc_call_reply_async(call, msg);

  grpc_call_unref(call);
}

static void on_request(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  thread_pool *pool = userdata;
  grpc_call_ref(call);
  thread_pool_submit(pool, worker_job, call);
}
```

Available thread-safe call functions (caller must hold a `grpc_call_ref()`):
- `grpc_call_send_metadata_async()`
- `grpc_call_add_trailers_async()`
- `grpc_call_send_async()` / `grpc_call_send_data_async()`
- `grpc_call_reply_async()`
- `grpc_call_finish_async()` / `grpc_call_finish_with_details_async()`
- `grpc_server_post(server, fn, arg)` — runs arbitrary callback `fn(arg)` on the server's event-loop thread.

---

## Pluggable Allocators

Select a build-time allocator with `-DGRPC_ALLOCATOR=default|mimalloc|jemalloc`, or replace it at runtime before creating any server:

```c
grpc_allocator custom = {
    .malloc = my_malloc,
    .calloc = my_calloc,
    .realloc = my_realloc,
    .free = my_free,
    .aligned_alloc = my_aligned_alloc,
};
grpc_set_allocator(&custom); /* pass nullptr to restore the build default */
```

All buffers returned to the application (decoded metadata, compressed outputs, status details, etc.) are allocated through the active allocator and must be freed with `grpc_free()`.

---

## Logging

Each server has its own log threshold and `FILE *` output stream backed by [ulog](https://github.com/ThirdLetterC/ulog):

```c
grpc_server *server = grpc_server_new(&(grpc_config){
    .port = 50051,
    .log_level = GRPC_LOG_WARNING,
    .log_file = stderr,
});

/* Safe to call from any thread while the server is alive: */
(void)grpc_server_set_log_level(server, GRPC_LOG_DEBUG);
```

Levels: `GRPC_LOG_DEFAULT` (info), `GRPC_LOG_OFF`, `GRPC_LOG_ERROR`, `GRPC_LOG_WARNING`, `GRPC_LOG_INFO`, `GRPC_LOG_DEBUG`, `GRPC_LOG_TRACE` (requires `-DGRPC_TRACE=ON`).

---

## Performance

Measured on localhost with `ghz` sending unary `SayHello` calls to the Release example server (default libc allocator) on an Intel Core i9-9900K (server pinned to CPUs 0–7, client to CPUs 8–15; median of 3 runs of 300,000 calls after 5,000 warmup):

| Setup | Throughput |
|:---|---:|
| 1 loop, 16 connections × 32 concurrent calls | 83.4k req/s |
| 1 loop, 100 connections × 1 concurrent call | 57.9k req/s |
| 8 loops, 64 connections × 32 concurrent calls | 77.5k req/s |
