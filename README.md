# ezGRPC-next

[![CI](https://github.com/ThirdLetterC/ezgrpc-next/actions/workflows/ci.yml/badge.svg)](https://github.com/ThirdLetterC/ezgrpc-next/actions/workflows/ci.yml)

**ezGRPC-next** is a lightweight, high-performance gRPC server library in C23 for Linux and Windows. It is built on [libuv](https://libuv.org/) and [nghttp2](https://nghttp2.org/), and exposes a simple poll-and-handle API: no hidden threads, no code generator, no callback soup. You decide how requests are scheduled, how memory is allocated, and how many cores to use.

---

## Features

- **Handler & poll APIs**: register unary/server-streaming (`grpc_server_handle()`), client/bidi-streaming (`grpc_server_handle_stream()` with `on_start`/`on_message`/`on_end`/`on_cancel`), or poll-driven (`grpc_server_register_path()` + `grpc_server_poll()`) methods.
- **All 4 RPC modes**: unary, server streaming, client streaming and bidirectional streaming.
- **TLS with ALPN & mTLS**: HTTP/2 over TLS 1.2 and 1.3 negotiating `h2`, from files or in-memory PEM buffers, with optional client certificate verification (`grpc_call_peer_cert_subject()`). Seven TLS libraries are supported: OpenSSL, LibreSSL, BoringSSL, AWS-LC, wolfSSL, GnuTLS and rustls (see [TLS backends](#tls-backends)).
- **Protobuf-C helpers**: optional [`protobuf-c`](https://github.com/protobuf-c/protobuf-c) integration (`GRPC_PB_UNPACK`, `grpc_call_send_pb`, `grpc_call_reply_pb`).
- **FlatBuffers helpers**: optional [`flatcc`](https://github.com/dvidelabs/flatcc) integration with verified, zero-copy reads (`GRPC_FB_UNPACK`, `grpc_call_send_fb`, `grpc_call_reply_fb`).
- **Interceptors**: automatic `on_call` / `on_done` hooks added via `grpc_server_add_interceptor()`. A built-in Bearer token interceptor is available (`grpc_interceptor_bearer_auth`).
- **Health checking**: standard `/grpc.health.v1.Health/Check` and streaming `/grpc.health.v1.Health/Watch` with per-service status.
- **Server reflection**: `grpc.reflection.v1` and `v1alpha` `ServerReflectionInfo`.
- **Metadata**: case-insensitive single and multi-value request headers (`grpc_call_metadata`, `grpc_call_metadata_next`), initial metadata, trailers, and Base64 `*-bin` values.
- **Deadlines**: `grpc-timeout` is parsed. Expired calls are ended with `DEADLINE_EXCEEDED` and cancelled.
- **Message compression**: gzip and deflate through zlib, plus optional Zstandard and Snappy codecs—both per-message and automatic negotiation via `grpc_config.default_compression` or `grpc_call_set_compression()`.
- **Rich error model**: `google.rpc.Status` with multiple `Any` details sent in `grpc-status-details-bin` (`grpc_call_finish_with_details`, `grpc_status_details_decode_all`).
- **Graceful shutdown**: `grpc_server_drain()` stops accepting new connections, sends HTTP/2 `GOAWAY`, and waits for in-flight calls.
- **Pluggable allocators**: libc `malloc`, `mimalloc`, `jemalloc`, or your own allocator set at runtime.

---

## Architecture

Register method handlers (or event-driven paths), then run the event loop with `grpc_server_run()` or step it manually with `grpc_server_poll()`:

```c
typedef struct grpc_event {
  grpc_event_type type;        // GRPC_EVENT_MESSAGE, GRPC_EVENT_END, GRPC_EVENT_CANCEL
  grpc_call *call;             // The call handle (valid until next poll; ref to keep)
  const grpc_message *message; // Payload for GRPC_EVENT_MESSAGE
  void *userdata;              // Pointer passed to grpc_server_register_path()
} grpc_event;
```

### I/O model

Each `grpc_server` owns one libuv loop, driven by `grpc_server_run()` or `grpc_server_poll()`:

- Connections are libuv TCP streams (`uv_tcp_t`). Received bytes go straight from a shared read buffer into nghttp2.
- `grpc_call_send()`, `grpc_call_reply()`, `grpc_call_finish()` and similar calls only **queue** frames. Everything queued for a connection (headers, data and trailers of all its streams) is flushed in a single write. If your next poll may be delayed, call `grpc_server_flush()`.
- Worker threads can safely answer calls with `grpc_call_ref()` + `grpc_call_send_async()` / `grpc_call_reply_async()` / `grpc_call_finish_async()`, or schedule arbitrary callbacks via `grpc_server_post()`.
- The TLS library never touches the socket: ciphertext is passed to and from it in memory, so each batch is encrypted with a single write call and every TLS library is driven the same way.

### Using more cores

A libuv loop runs on one thread. On Unix, create one server per thread on the same port to use more cores. The listening sockets use `SO_REUSEPORT`, so the kernel spreads incoming connections across the loops. Health and reflection state is shared and thread-safe. The example server does this when started with `THREADS=N`. On Windows, use one listening server with worker threads, as in `examples/multi_threaded.c`.

A single HTTP/2 connection always stays on one loop, so multiple loops only help when clients open multiple connections.

### Logging

Set the initial threshold and optional output stream in `grpc_config`, then
change the threshold at runtime with `grpc_server_set_log_level()`:

```c
grpc_config cfg = {
    .port = 50051,
    .log_level = GRPC_LOG_WARNING,
    .log_file = stderr,
};
grpc_server *server = grpc_server_new(&cfg);
/* Later, from any thread while the server is alive: */
grpc_server_set_log_level(server, GRPC_LOG_DEBUG);
```

Each server has its own threshold and output stream. `GRPC_LOG_DEFAULT`
selects info; `GRPC_LOG_OFF` disables server logs. Logging uses the
[ulog](https://github.com/thirdLetterC/ulog) backend.

### Performance

Measured on localhost with `ghz` sending unary `SayHello` calls to the Release example server, built with the default libc allocator. The server was pinned to CPUs 0–7 and the client to CPUs 8–15 on an Intel Core i9-9900K (8 cores / 16 threads). Each result is the median of three runs; each run warmed up with 5,000 calls, then sent 300,000 calls and excluded the first 5,000 from the reported rate.

| Setup | Before | After | Change |
|:---|---:|---:|---:|
| 1 loop, 16 connections × 32 concurrent calls | 83.4k req/s | 83.4k req/s | +0.1% |
| 1 loop, 100 connections × 1 concurrent call | 56.2k req/s | 57.9k req/s | +2.9% |
| 8 loops, 64 connections × 32 concurrent calls | 76.9k req/s | 77.5k req/s | +0.9% |

“Before” is the revision immediately before the receive-buffer, request-header, and message-compression optimizations; “After” includes those changes. Both used the same compiler, build options, server/client CPU affinity, payload (`{"name":"benchmark_user"}`), and `ghz` settings. The results are close to run-to-run variation, so treat them as updated reference measurements rather than a clear throughput gain. Results depend on hardware and client capacity; set `--connections` high enough to keep the server busy.

---

## Requirements & Dependencies

- **Platform**: Linux or Windows.
- **Compiler**: GCC 11+ or Clang 14+ with C23 support (`-std=c23`, or `-std=c2x` on older compilers), or MSVC 19.44+ on Windows (see below).
- **Build system**: CMake 3.20 or newer.
- **Logger dependency**: CMake fetches the pinned [ulog](https://github.com/ThirdLetterC/ulog) source at configure time. For offline or local development, set `FETCHCONTENT_SOURCE_DIR_ULOG` to an existing checkout.
- **Required libraries**:
  - `libuv` (event loop)
  - `libnghttp2` (HTTP/2)
  - `pthread` on Unix (native Windows synchronization is used on Windows)
- **Optional libraries** (detected automatically when installed):
  - A TLS library for TLS: OpenSSL by default, or one of the [other backends](#tls-backends)
  - `zlib` for gzip and deflate compression
  - `libzstd` for Zstandard compression
  - `libsnappy` for Snappy compression
  - `protobuf-c` for the Protobuf-C helpers
  - `flatcc` for the FlatBuffers helpers (no distribution package: build it with `scripts/build-flatcc.sh`)
  - `mimalloc` or `jemalloc` as the allocator (selected at build time)

On Debian / Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential pkg-config libuv1-dev libnghttp2-dev \
  zlib1g-dev libzstd-dev libsnappy-dev libssl-dev libprotobuf-c-dev protobuf-c-compiler \
  libmimalloc-dev libjemalloc-dev   # optional allocators
```

The Python integration tests also need `grpcio`, `grpcio-tools` and `protobuf` (`pip install grpcio grpcio-tools protobuf`).

---

## Building

The sources are laid out as:

| Directory | Contents |
|:---|:---|
| `include/` | All headers. Applications include `grpc.h` |
| `src/` | Library sources (`.c`) |
| `src/tests/` | Unit tests |
| `examples/` | Example servers and the Python integration tests |

Configure and build the static library, shared library and available examples:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Run the registered unit and integration tests with:

```bash
ctest --test-dir build --output-on-failure
```

The CMake targets are `grpc_static` and `grpc_shared`. The `grpc::grpc` and
`grpc::static` aliases select the static library; `grpc::shared` selects the
shared library. Each carries its required link dependencies.

Include `grpc.h` (or just `grpc_version.h`) and call `grpc_version()` to get the
linked library's version, currently `"1.0.0"`. The `GRPC_VERSION_STRING`,
`GRPC_VERSION_MAJOR`, `GRPC_VERSION_MINOR` and `GRPC_VERSION_PATCH` macros identify
the headers used at compile time. CMake generates these constants from the
project version and supplies the generated header's include path through its targets.

For a static-only optimized build, enable IPO/LTO with the compiler support check:

```bash
cmake -S . -B build-ipo -DGRPC_IPO=ON -DGRPC_BUILD_SHARED=OFF
cmake --build build-ipo --config Release --parallel
```

Include the same dependency/toolchain flags as your normal build, such as the
vcpkg toolchain on Windows.

IPO applies to Release, RelWithDebInfo and MinSizeRel. MSVC requires the
static-only setting because CMake's automatic DLL export scanner cannot read
`/GL` objects. GCC and Clang can also use IPO with the shared library enabled.

The project can be included with `add_subdirectory(path/to/ezgrpc-next)` and linked
through `grpc::grpc`. In that case, tests, examples and formatting targets
default to OFF, and the parent project's build type and directory compiler
options are preserved. Compiler requirements are attached to grpc targets.

### Windows with MSVC

Use Visual Studio 2022 17.14 (MSVC 19.44) or newer with the Desktop development
with C++ workload and Windows SDK. The build uses `/std:clatest` and
`/experimental:c11atomics`. Install the native dependencies with
[vcpkg](https://github.com/microsoft/vcpkg), then run these commands in Developer PowerShell for Visual Studio
(replace `C:/vcpkg` with your vcpkg directory):

```powershell
C:/vcpkg/vcpkg.exe install libuv:x64-windows nghttp2:x64-windows openssl:x64-windows zlib:x64-windows zstd:x64-windows snappy:x64-windows
cmake -S . -B build -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake `
  "-DGRPC_HOST_OPENSSL_EXECUTABLE=C:/Program Files/Git/usr/bin/openssl.exe" `
  -DGRPC_BUILD_INTEGRATION_TESTS=OFF
cmake --build build --config Release --parallel
ctest --test-dir build -C Release --output-on-failure
```

The static library is `grpc.lib`; the shared library is `grpc.dll` with import
library `grpc_dll.lib`. CMake targets carry the required dependencies, and vcpkg
copies dependency DLLs beside the built executables. pkg-config and libuuid are
not required. OpenSSL and `GRPC_TLS_BACKEND=none` are covered by Windows CI.
AddressSanitizer through `GRPC_ASAN` currently requires GCC or Clang.
The jemalloc allocator and rustls backend currently require Unix.

To run the example integration tests, install Python and `pip install grpcio`,
then configure with `-DGRPC_BUILD_INTEGRATION_TESTS=ON`. TLS unit tests also need
the `openssl` command; the commands above use the copy bundled with Git for
Windows (adjust its path if Git is installed elsewhere). Windows does not provide `SO_REUSEPORT` load
balancing; use one server per listening address/port (`THREADS=1` in the
Protobuf-C example), with worker threads as in `examples/multi_threaded.c`.

### Compression

The previous implementation provided gzip helpers only. Gzip and deflate both use zlib; deflate uses the zlib-wrapped DEFLATE stream used by gRPC. Zstandard (`zstd`) and Snappy (`snappy`) are detected at build time when their development libraries are installed. Set `-DGRPC_ZSTD=OFF` or `-DGRPC_SNAPPY=OFF` to disable either optional codec.

Compression remains explicit and per message. The server sends the corresponding `grpc-encoding` header when the first response message is compressed, and advertises the codecs enabled in that build. For example:

```c
grpc_message *response = grpc_message_new(payload, payload_len);
grpc_message *compressed = grpc_message_compress(
    response, GRPC_COMPRESS_ZSTD, 5);  // Zstandard level 5
grpc_message_free(response);
if (compressed != nullptr)
    grpc_call_send(call, compressed);   // call_send takes ownership
```

Use `GRPC_COMPRESS_GZIP` or `GRPC_COMPRESS_DEFLATE` with zlib levels `0` through `9`; use `GRPC_COMPRESSION_LEVEL_DEFAULT` for the library default. Zstandard accepts levels supported by the installed Zstandard library. Snappy has no level control and requires `GRPC_COMPRESSION_LEVEL_DEFAULT`. The legacy `grpc_message_compress_gzip()` and raw gzip functions remain available.

Inbound decompression is bounded by `grpc_config.max_message_size` (including the
5-byte gRPC message prefix), independently of the compressed size. Excessive
expansion ends the call with `RESOURCE_EXHAUSTED`; malformed compressed input is
rejected without growing buffers repeatedly. Raw-buffer consumers can use
`grpc_decompress_limited(..., max_output)`, which returns `5` on output-limit
exhaustion and leaves the output pointer null. The existing unbounded helpers
remain available for caller-managed trusted buffers.

Zstandard and Snappy are library-specific gRPC encodings. A peer must support the same encoding token to interoperate; standard gRPC implementations commonly support gzip and identity, while support for deflate, Zstandard and Snappy varies.

Add `-lprotobuf-c` when the Protobuf-C helpers are built, `-lflatccrt` when the FlatBuffers helpers are, and the allocator library (`-lmimalloc` or `-ljemalloc`) when one is selected.

### Build options

| CMake setting or command | Description |
|:---|:---|
| `cmake --build build --target check` | Builds and runs registered unit and integration tests |
| `-DGRPC_BUILD_SHARED=OFF` | Builds only the static library |
| `-DGRPC_IPO=ON` | Enables compiler-checked IPO/LTO in optimized configurations; MSVC also requires `GRPC_BUILD_SHARED=OFF` |
| `-DGRPC_ALLOCATOR=mimalloc` | Uses `mimalloc`; also accepts `jemalloc` or `default` |
| `-DGRPC_TLS_BACKEND=<backend>` | Selects `AUTO`, `openssl`, `libressl`, `boringssl`, `awslc`, `wolfssl`, `gnutls`, `rustls` or `none` |
| `-DGRPC_TLS_PREFIX=<dir>` | Searches `<dir>` for the selected TLS library |
| `-DGRPC_PROTOBUF_C=OFF` | Disables Protobuf-C helpers; `AUTO` detects them, `ON` requires them |
| `-DGRPC_FLATCC=OFF` | Disables FlatBuffers helpers; `AUTO` detects them, `ON` requires them |
| `-DGRPC_FLATCC_PREFIX=<dir>` | Searches `<dir>/bin`, `include` and `lib` for flatcc |
| `-DGRPC_ZLIB=OFF`, `-DGRPC_ZSTD=OFF`, `-DGRPC_SNAPPY=OFF` | Disable optional codecs; each accepts `AUTO`, `ON` or `OFF` |
| `-DGRPC_DEBUG_LOGS=ON` | Enables debug logging and debug symbols |
| `-DGRPC_TRACE=ON` | Enables trace logging of every HTTP/2 frame |
| `-DGRPC_ASAN=ON` | Builds with AddressSanitizer |
| `-DGRPC_BUILD_INTEGRATION_TESTS=OFF` | Skips Python client integration tests |
| `-DGRPC_BUILD_EXAMPLES=OFF` | Skips example server targets |
| `-DGRPC_BUILD_DEVELOPER_TARGETS=OFF` | Skips source formatting targets |
| `cmake --build build --target format` | Formats C sources with `clang-format` |
| `cmake --build build --target format-check` | Checks C formatting with `clang-format` |
| `cmake --build build --target clean` | Removes build output |

Feature values (`AUTO`, `ON`, `OFF`), TLS backend names and allocator names are
case-insensitive. An optional dependency set to `OFF` is not searched for.
The `check` target also works with examples disabled; embedded builds expose
it as `grpc_check`. To select tests directly, use `ctest --test-dir build -C Release
-L unit` or `-L integration`.

### TLS backends

Exactly one TLS library is compiled in, selected with `GRPC_TLS_BACKEND`. Use a separate build directory when switching backends. The versions listed are the ones the test suite has been run against.

| `GRPC_TLS_BACKEND` | Library | How to get it | Notes |
|:---|:---|:---|:---|
| `openssl` | OpenSSL 3.0 | `libssl-dev` | Default when installed |
| `libressl` | LibreSSL 4.3 | `scripts/build-tls-backend.sh libressl <dir>` | |
| `boringssl` | BoringSSL 0.20260929.0 | `scripts/build-tls-backend.sh boringssl <dir>` | |
| `awslc` | AWS-LC 5.10 | `scripts/build-tls-backend.sh awslc <dir>` | |
| `wolfssl` | wolfSSL 5.6 | `libwolfssl-dev` | Needs ALPN support (`--enable-alpn`). Without `--enable-aesni`, AES runs in software and bulk transfers are much slower |
| `gnutls` | GnuTLS 3.8 | `libgnutls28-dev` | |
| `rustls` | rustls via [rustls-ffi](https://github.com/rustls/rustls-ffi) 0.15 | `scripts/build-tls-backend.sh rustls <dir>` (needs a Rust toolchain) | Rejects clients that offer ALPN without `h2` |
| `none` | | | TLS functions fail at runtime |

LibreSSL, BoringSSL and AWS-LC install the same `openssl/*.h` headers as OpenSSL, so they are used from their own prefix. `scripts/build-tls-backend.sh` downloads a pinned, checksum-verified release, builds it, and installs it into a directory of your choice:

```bash
scripts/build-tls-backend.sh awslc "$HOME/tls/awslc"
cmake -S . -B build-awslc -DGRPC_TLS_BACKEND=awslc -DGRPC_TLS_PREFIX="$HOME/tls/awslc"
cmake --build build-awslc --parallel
```

The build fails if the headers it finds don't belong to the selected library, and the TLS unit test checks that the library linked in is the one requested. At runtime, `grpc_tls_backend()` and `grpc_tls_backend_version()` report the library in use.

All backends behave the same way: they load a PEM certificate chain and private key, check that the key matches the certificate, allow TLS 1.2 and 1.3 only, and select `h2` with ALPN. A client that offers no ALPN still connects. For settings ezGRPC-next doesn't expose, `grpc_tls_ctx_native()` returns the library's own context (`SSL_CTX *`, `WOLFSSL_CTX *`, `gnutls_certificate_credentials_t` or `const struct rustls_server_config *`).

---

## Quickstart

### 1. A complete server

```c
#include <stdio.h>
#include <string.h>
#include "grpc.h"

static void say_hello(grpc_call *call, const grpc_message *request, void *userdata) {
  (void)request;
  (void)userdata;
  const char reply[] = "Hello from ezGRPC-next!";
  (void)grpc_call_reply_data(call, reply, strlen(reply));
}

int main(void) {
  grpc_err err;
  grpc_server *server = grpc_server_new_ex(
      &(grpc_config){
          .port = 19009, /* or 0 for an OS-assigned ephemeral port */
          .log_level = GRPC_LOG_INFO,
      },
      &err);
  if (server == nullptr) {
    fprintf(stderr, "failed to start server: %s\n", grpc_strerror(err));
    return 1;
  }

  (void)grpc_health_enable(server);
  (void)grpc_reflection_enable(server);
  (void)grpc_server_handle(server, "/helloworld.Greeter/SayHello", say_hello, nullptr);

  (void)grpc_server_run(server);
  (void)grpc_server_drain(server, 5000);
  grpc_server_free(server);
  return 0;
}
```

Paths that are not registered are answered with `UNIMPLEMENTED` automatically.

### 2. Protobuf-C requests and responses

With `protobuf-c` installed, unpack requests and reply directly:

```c
#include "helloworld.pb-c.h"

static void say_hello_pb(grpc_call *call, const grpc_message *req_msg, void *userdata) {
  (void)userdata;
  Helloworld__HelloRequest *req = GRPC_PB_UNPACK(
      Helloworld__HelloRequest, &helloworld__hello_request__descriptor, req_msg);
  if (req == nullptr) {
    (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT, "bad protobuf");
    return;
  }

  Helloworld__HelloReply reply = HELLOWORLD__HELLO_REPLY__INIT;
  reply.message = "Hello, world!";
  (void)grpc_call_reply_pb(call, (const ProtobufCMessage *)&reply);
  GRPC_PB_FREE_UNPACKED(req);
}
```

### 3. FlatBuffers requests and responses

With [flatcc](https://github.com/dvidelabs/flatcc), generate C code for a schema with `flatcc -a greeter.fbs`. Requests are verified and read in place, without copying or unpacking:

```c
#include "greeter_builder.h"
#include "greeter_verifier.h"

flatcc_builder_t builder;
(void)grpc_fb_builder_init(&builder); /* once; uses the grpc allocator */

/* verifies the buffer and returns its root table, or nullptr if invalid */
greeter_HelloRequest_table_t req = GRPC_FB_UNPACK(greeter_HelloRequest, req_msg);
if (req == nullptr) {
  (void)grpc_call_finish(call, GRPC_STATUS_INVALID_ARGUMENT, nullptr);
  return;
}

const char *name = greeter_HelloRequest_name(req);
greeter_HelloReply_start_as_root(&builder);
greeter_HelloReply_message_create_str(&builder, name != nullptr ? name : "World");
greeter_HelloReply_end_as_root(&builder);

/* sends the finished buffer, resets the builder, and finishes with OK */
(void)grpc_call_reply_fb(call, &builder);
```

FlatBuffers services use the usual gRPC paths (`/greeter.Greeter/SayHello` for `rpc_service Greeter` in `namespace greeter`). flatcc requires `attribute "streaming";` in schemas that mark streaming methods. Message payloads (`grpc_message.data`) are aligned like `malloc()` memory, so buffers can be read where they are received.

### 4. Metadata, trailers and rich errors

Read request headers (including repeated keys) and end the call with custom trailers:

```c
size_t len = 0;
const char *agent = grpc_call_metadata(call, "user-agent", &len);

size_t idx = 0;
const char *val;
while ((val = grpc_call_metadata_next(call, "x-tag", &idx, &len)) != nullptr) {
  /* handle each x-tag header value */
}

grpc_metadata trailers[] = {
    {.key = "x-custom-reply", .value = "pong"},
};
(void)grpc_call_add_trailers(call, trailers, 1);
(void)grpc_call_finish(call, GRPC_STATUS_OK, nullptr);
```

Return an error with `google.rpc.Status` details, serialized into `grpc-status-details-bin`:

```c
grpc_status_detail_entry detail = {
    .type_url = "type.googleapis.com/google.rpc.ErrorInfo",
    .value = error_info_bytes, /* a serialized google.rpc.ErrorInfo */
    .value_len = error_info_len,
};

(void)grpc_call_finish_with_details(
    call, GRPC_STATUS_INVALID_ARGUMENT, "Invalid name provided", &detail, 1);
```

### 5. Bearer token authentication

```c
static bool validate_token(const char *token, void *userdata) {
  (void)userdata;
  return strcmp(token, "secret") == 0;
}

grpc_interceptor auth = grpc_interceptor_bearer_auth(validate_token, nullptr);
(void)grpc_server_add_interceptor(server, &auth);
```

---

## Complete Example & Integration Tests

[`examples/python_client_c_server/`](examples/python_client_c_server/) contains a full server and a Python test client:

- **`server.c`**: unary, server streaming, client streaming and bidirectional streaming handlers, trailers, rich errors, a logging interceptor, health checking and reflection. Gzip-compressed requests are decompressed by the library. Set `THREADS=N` to run N event loops on the same port.
- **`client.py`**: the official Python `grpcio` client, checking 9 scenarios over cleartext (`h2c`) and TLS.

[`examples/flatbuffers/`](examples/flatbuffers/) is a Greeter whose messages are FlatBuffers:

- **`server.c`**: unary and server-streaming methods built with flatcc, rejecting requests that fail verification.
- **`client.py`**: a `grpcio` client that encodes and decodes the messages with the official Python `flatbuffers` package, checking 6 scenarios. It needs `pip install flatbuffers`.

Run the tests:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

---

## Continuous Integration (CI)

Every push and pull request is tested with GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) on:

- **OS**: Ubuntu 24.04 and Ubuntu 22.04
- **Compilers**: GCC and Clang
- **Windows**: MSVC Debug and Release, static libraries and DLLs, OpenSSL and cleartext
- **Allocators**: libc `malloc`, `mimalloc` and `jemalloc`
- **Sanitizers**: AddressSanitizer builds (`GRPC_ASAN=ON`)
- **FlatBuffers**: flatcc built from source, with GCC, Clang and AddressSanitizer
- **TLS backends**: LibreSSL, BoringSSL, AWS-LC, wolfSSL, GnuTLS and rustls, in addition to OpenSSL
