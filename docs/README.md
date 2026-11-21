# ezGRPC-next Documentation

Welcome to the **ezGRPC-next** documentation.

## Guides

| Guide | Topics Covered |
|:---|:---|
| [Getting Started](getting-started.md) | Dependencies, Linux & Windows/MSVC builds, CMake options, `find_package` / `pkg-config`, and first server + client |
| [Architecture & Concurrency](architecture.md) | Event loop I/O model, multi-core `SO_REUSEPORT`, worker threads (`_async` APIs & `grpc_server_post`), allocators, logging, benchmarks |
| [Server & RPC Guide](server-and-rpc.md) | Unary/streaming handlers, poll mode, fallback routing, metadata, rich errors, UDS, gRPC-Web, keepalive, backpressure, graceful drain |
| [Client Guide](client.md) | `grpc_client` over TCP and Unix Domain Sockets, unary & server-streaming RPCs, metadata, timeouts, compression |
| [Serialization & Compression](serialization-and-compression.md) | Protobuf-C, FlatBuffers (`flatcc`), automatic & per-message compression (gzip, deflate, zstd, snappy), bounded decompression |
| [TLS & mTLS](tls.md) | 7 TLS backends, file & in-memory PEMs, client certificate verification (mTLS), zero-downtime hot-reload, native context access |
| [Middleware & Observability](middleware-and-observability.md) | Scoped & message-level interceptors, Bearer auth, rate limiting, Health `Check`/`Watch`, Reflection, Channelz, Stats, W3C Tracing |

## Runnable Examples

- [`examples/single_threaded.c`](../examples/single_threaded.c) — minimal raw-bytes server
- [`examples/multi_threaded.c`](../examples/multi_threaded.c) — server with a worker thread pool
- [`examples/python_client_c_server/`](../examples/python_client_c_server/) — full Protobuf-C Greeter server + Python `grpcio` integration suite
- [`examples/flatbuffers/`](../examples/flatbuffers/) — zero-copy FlatBuffers (`flatcc`) server + Python client
