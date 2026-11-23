# ezGRPC-next

[![CI](https://github.com/ThirdLetterC/ezgrpc-next/actions/workflows/ci.yml/badge.svg)](https://github.com/ThirdLetterC/ezgrpc-next/actions/workflows/ci.yml)

**ezGRPC-next** is a lightweight, high-performance gRPC server and client library in C23 for Linux and Windows, built on [libuv](https://libuv.org/) and [nghttp2](https://nghttp2.org/). No hidden threads, no mandatory code generator, and no callback soup—you control scheduling, memory allocation, and core scaling.

---

## Highlights

- **Server & Client**: Full gRPC server (`grpc_server`) and lightweight client (`grpc_client`) over IPv4, IPv6, ephemeral ports, and Unix Domain Sockets / named pipes.
- **HTTP/2 and HTTP/3**: gRPC over HTTP/3 (QUIC, via ngtcp2 and nghttp3) on the server and the client, side by side with HTTP/2, with Alt-Svc advertisement and Retry address validation. Supported on every QUIC-capable TLS backend; see [HTTP/3](docs/http3.md).
- **All 4 RPC Modes & gRPC-Web**: Unary, server-streaming, client-streaming, bidirectional streaming, poll-driven events, catch-all fallback routing, and native `application/grpc-web` / `grpc-web-text`.
- **TLS, mTLS & Hot-Reload**: 7 TLS backends (OpenSSL, LibreSSL, BoringSSL, AWS-LC, wolfSSL, GnuTLS, rustls), file or in-memory PEMs, client cert verification, and zero-downtime `grpc_server_reload_tls()`.
- **Production HTTP/2 Controls**: Keepalive `PING`, idle & connection-age `GOAWAY`, ping-strike protection (`ENHANCE_YOUR_CALM`), per-stream send backpressure (`max_send_queue_bytes` / `on_writable`), and graceful `grpc_server_drain()`.
- **Middleware & Observability**: Scoped call & message interceptors, Bearer auth, token-bucket rate limiting, Health `Check` & `Watch`, Server Reflection (v1/v1alpha), Channelz, runtime stats, and W3C `traceparent` helpers.
- **Serialization & Compression**: Optional Protobuf-C and zero-copy FlatBuffers (`flatcc`) helpers; gzip, deflate, Zstandard, and Snappy codecs with automatic negotiation and bounded decompression.
- **Pluggable Allocators**: `libc`, `mimalloc`, `jemalloc`, or custom runtime allocators.

---

## Quick Build & Example

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Only a C compiler, CMake and OpenSSL headers need to be installed. nghttp2, libuv, zlib, zstd, Snappy and protobuf-c are downloaded as pinned, checksummed releases and built on the first configure. Pass `-DGRPC_DEPENDENCIES=SYSTEM` to use installed packages instead (see [Getting Started](docs/getting-started.md#third-party-libraries-bundled-by-default)).

```c
#include <string.h>
#include "grpc.h"

static void say_hello(grpc_call *call, const grpc_message *req, void *userdata) {
  (void)req;
  (void)userdata;
  const char reply[] = "Hello from ezGRPC-next!";
  (void)grpc_call_reply_data(call, reply, strlen(reply));
}

int main(void) {
  grpc_server *server = grpc_server_new(&(grpc_config){.port = 19009});
  if (server == nullptr)
    return 1;
  (void)grpc_health_enable(server);
  (void)grpc_reflection_enable(server);
  (void)grpc_server_handle(server, "/helloworld.Greeter/SayHello", say_hello, nullptr);
  (void)grpc_server_run(server);
  (void)grpc_server_drain(server, 5000);
  grpc_server_free(server);
  return 0;
}
```

---

## Documentation

Full guides and code examples live in [`docs/`](docs/README.md):

- [Getting Started & Building](docs/getting-started.md) — Linux/Windows builds, CMake options, `add_subdirectory`, `cmake --install` / `pkg-config`
- [Architecture & Concurrency](docs/architecture.md) — Event loop I/O, `SO_REUSEPORT`, worker-thread `_async` APIs, allocators, logging, benchmarks
- [Server & RPC Guide](docs/server-and-rpc.md) — All 4 RPC modes, poll API, fallback routing, metadata, rich errors, UDS, gRPC-Web, keepalive, backpressure, draining
- [Client Guide](docs/client.md) — Unary & server-streaming client over TCP and Unix Domain Sockets
- [Serialization & Compression](docs/serialization-and-compression.md) — Protobuf-C, FlatBuffers (`flatcc`), automatic & per-message codecs
- [TLS & mTLS](docs/tls.md) — 7 TLS backends, in-memory PEMs, client cert verification, zero-downtime hot-reload
- [Middleware & Observability](docs/middleware-and-observability.md) — Interceptors, Bearer auth, rate limiting, Health `Check`/`Watch`, Reflection, Channelz, Stats, W3C Tracing

---

## Examples & CI

- [`examples/single_threaded.c`](examples/single_threaded.c) & [`examples/multi_threaded.c`](examples/multi_threaded.c)
- [`examples/python_client_c_server/`](examples/python_client_c_server/) — Full Protobuf-C server + Python `grpcio` integration tests
- [`examples/flatbuffers/`](examples/flatbuffers/) — Zero-copy FlatBuffers (`flatcc`) server + Python client
- [`examples/http3/`](examples/http3/) — HTTP/3 (QUIC) server and client in C
- [`examples/otlp_metrics/`](examples/otlp_metrics/) — OpenTelemetry OTLP/gRPC metrics receiver: official protos via protobuf-c, partial success and RetryInfo, bounded memory, multi-loop and worker concurrency, null/memory/write-ahead-log sinks; tested with the official Python and Go SDK exporters, fuzzed, and benchmarked against grpc-go
- CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) tests Ubuntu 22.04/24.04 (GCC/Clang), Windows MSVC, `mimalloc`/`jemalloc`, AddressSanitizer, FlatBuffers, and all 7 TLS backends.

---

## Credits

ezGRPC-next is a fork of [EZgRPC2](https://github.com/mnyoshie/ezgrpc2) by M. N. Yoshie and contributors. See [AUTHORS](AUTHORS) for the people behind both projects and [LICENSE](LICENSE) for the license.
