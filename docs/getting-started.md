# Getting Started

## Requirements & Dependencies

- **Platform**: Linux or Windows.
- **Compiler**: GCC 11+ or Clang 14+ (`-std=c23` or `-std=c2x`), or MSVC 19.44+ (Visual Studio 2022 17.14+).
- **Build system**: CMake 3.20 or newer.
- **Logger dependency**: CMake fetches the pinned [ulog](https://github.com/ThirdLetterC/ulog) source at configure time. For offline builds, set `-DFETCHCONTENT_SOURCE_DIR_ULOG=/path/to/ulog`.
- **Required libraries**: `libuv`, `libnghttp2`, and `pthread` on Unix. They are downloaded and built by default (see below).
- **Optional libraries**:
  - TLS: OpenSSL (default), LibreSSL, BoringSSL, AWS-LC, wolfSSL, GnuTLS, or rustls. Always taken from the system or `GRPC_TLS_PREFIX`.
  - Compression: `zlib` (gzip/deflate), `libzstd`, `libsnappy`. Downloaded by default.
  - Serialization: `protobuf-c` (runtime downloaded by default; the `protoc-c` generator, needed only by its test and example, comes from the system) and `flatcc` (build it with `scripts/build-flatcc.sh`).
  - Allocators: `mimalloc`, `jemalloc`.

A fresh Linux machine needs only a compiler, CMake, and OpenSSL headers. A C++ compiler is also needed for Snappy, which is skipped without one:

```bash
sudo apt-get install -y build-essential cmake libssl-dev
```

### Third-party libraries: bundled by default

With `GRPC_DEPENDENCIES=BUNDLED` (the default), configure downloads pinned releases of nghttp2, libuv, zlib, zstd, Snappy and the protobuf-c runtime. Each archive comes from its GitHub release (or a listed mirror) and is checked against a SHA-256. Each library is built once, as a static position-independent library, into `build/_deps/bundled`. Nothing installed on the system can shadow these builds. They are linked into `libgrpc` and are not re-exported from `libgrpc.so`. `cmake --install` puts them in `<libdir>/ezgrpc-next/`, so the installed CMake package and `pkg-config --static` keep working.

The first configure takes about half a minute; later ones reuse the build. Archives are cached in `~/.cache/ezgrpc-next` (`%LOCALAPPDATA%\ezgrpc-next\cache` on Windows), which every build directory shares. Deleting `build/` therefore does not download them again.

| Option | Description |
|:---|:---|
| `-DGRPC_DEPENDENCIES=SYSTEM` | Use installed packages (CMake packages, then pkg-config, then a header/library search). The default under a vcpkg toolchain |
| `-DGRPC_SYSTEM_DEPENDENCIES="libuv;zlib"` | In bundled mode, take only these from the system: any of `nghttp2;libuv;zlib;zstd;snappy;protobuf-c;python` |
| `-DGRPC_DEPS_MIRRORS="https://mirror.example/ezgrpc"` | Base URLs tried before upstream. Each one serves the archive file names listed in `cmake/GrpcBundledDeps.cmake`. Also read from the `GRPC_DEPS_MIRRORS` environment variable |
| `-DGRPC_DEPS_CACHE_DIR=path` | Download cache (env `GRPC_DEPS_CACHE_DIR`). For an offline machine, copy the archives here |

If a download fails, configure warns and falls back to the system copy of that library. The pins (version, URLs, SHA-256) are in `cmake/GrpcBundledDeps.cmake`.

### Library checks and the configuration summary

Each candidate library, bundled or from the system, must compile and link a small test program before it is accepted. When the shared library is built on Linux, that program is linked as a shared object. This rejects a static archive built without `-fPIC` (for example an old `~/.local/lib/libprotobuf-c.a`) at configure time, instead of failing at the final `libgrpc.so` link. A rejected candidate does not stop the search: the next source is tried. If nothing fits, the error lists every rejected candidate with its reason (missing header, not PIC, link error) and the install command for common package managers.

The configuration summary ends with where each dependency came from:

```text
     dependencies   BUNDLED (GRPC_DEPENDENCIES)
       nghttp2        bundled 1.71.0
       libuv          bundled 1.53.0
       ...
       tls            system openssl 3.0.13 (/usr/lib/x86_64-linux-gnu/libssl.so)
       python         system (/usr/bin/python3)
```

### Using system packages instead (`-DGRPC_DEPENDENCIES=SYSTEM`)

Debian / Ubuntu (note `libnghttp2-dev`; the `nghttp2` package only has the command-line tools):

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential cmake pkg-config libuv1-dev libnghttp2-dev \
  zlib1g-dev libzstd-dev libsnappy-dev libssl-dev libprotobuf-c-dev protobuf-c-compiler \
  libmimalloc-dev libjemalloc-dev
```

### Python for the integration tests

The integration tests need `grpcio`, `grpcio-tools` and `protobuf` (`scripts/requirements-test.txt`). Configure picks the first interpreter that has them: `Python3_EXECUTABLE`, then `python3` and `python` on `PATH`. If none does, bundled mode creates `build/_deps/python-venv` and installs the requirements there. Otherwise the integration tests are skipped with a warning that names the interpreter and the `pip install` command; the build itself is not affected. To choose an interpreter: `-DPython3_EXECUTABLE=/path/to/python`.

---

## Building & Testing

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

### Windows with MSVC

Install native dependencies with [vcpkg](https://github.com/microsoft/vcpkg) and run in Developer PowerShell for Visual Studio:

```powershell
C:/vcpkg/vcpkg.exe install libuv:x64-windows nghttp2:x64-windows openssl:x64-windows zlib:x64-windows zstd:x64-windows snappy:x64-windows
cmake -S . -B build -A x64 `
  -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake `
  "-DGRPC_HOST_OPENSSL_EXECUTABLE=C:/Program Files/Git/usr/bin/openssl.exe" `
  -DGRPC_BUILD_INTEGRATION_TESTS=OFF
cmake --build build --config Release --parallel
ctest --test-dir build -C Release --output-on-failure
```

Static library: `grpc.lib`. Shared library: `grpc.dll` + import library `grpc_dll.lib`.

For HTTP/3, also install `"ngtcp2[openssl]:x64-windows" nghttp3:x64-windows` and configure with `-DGRPC_TLS_BACKEND=openssl -DGRPC_HTTP3=ON` (see [HTTP/3](http3.md#windows-msvc)).

### IPO / LTO

```bash
cmake -S . -B build-ipo -DGRPC_IPO=ON -DGRPC_BUILD_SHARED=OFF
cmake --build build-ipo --config Release --parallel
```

MSVC requires `-DGRPC_BUILD_SHARED=OFF` with IPO because CMake's automatic DLL export scanner cannot read `/GL` objects.

---

## Integrating into Your Project

### Option 1: `add_subdirectory`

```cmake
add_subdirectory(path/to/ezgrpc-next)
target_link_libraries(my_app PRIVATE grpc::grpc) # or grpc::static / grpc::shared
```

When embedded via `add_subdirectory()`, tests, examples, install, and formatting targets default to `OFF`, and your project's build type and compiler flags are preserved.

### Option 2: `cmake --install` + `find_package` or `pkg-config`

```bash
cmake --install build --prefix /usr/local
```

Then in CMake:

```cmake
find_package(ezGRPC-next REQUIRED)                  # or COMPONENTS shared to require grpc::shared
target_link_libraries(my_app PRIVATE grpc::grpc)    # = grpc::static; or grpc::shared
if(ezGRPC-next_HTTP3)                               # also ezGRPC-next_TLS_BACKEND, _FEATURES
  message(STATUS "built with HTTP/3")
endif()
```

The static target carries all of the library's dependencies, so nothing else needs to be found.

Or with `pkg-config` (`--static` adds the dependencies for linking `libgrpc.a`):

```bash
cc my_app.c $(pkg-config --cflags --libs ezgrpc-next)
cc my_app.c $(pkg-config --cflags ezgrpc-next) /usr/local/lib/libgrpc.a $(pkg-config --static --libs-only-l --libs-only-other ezgrpc-next | sed 's/-lgrpc//')
```

---

## CMake Presets

`CMakePresets.json` defines ready-made configurations. Each builds into `build/<preset>`:

| Preset | Configuration |
|:---|:---|
| `dev` | Debug, every feature (bundled dependencies), tests and examples |
| `system` | Debug like `dev`, with installed packages (`GRPC_DEPENDENCIES=SYSTEM`) |
| `release` | Release with IPO |
| `asan` | Debug under AddressSanitizer + UndefinedBehaviorSanitizer |
| `tsan` | Debug under ThreadSanitizer (run tests with `setarch -R` on kernels with high ASLR entropy) |
| `http3` | Debug with HTTP/3 against `.tls/openssl` (see [HTTP/3](http3.md)) |
| `minimal` | The library only: no TLS, codecs, serialization helpers, tests or examples |
| `msvc` | Windows only: Visual Studio + vcpkg (`VCPKG_ROOT`), as in CI |

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev          # or --preset unit for the unit tests only
```

Put personal presets in `CMakeUserPresets.json` (ignored by git).

---

## CMake Build Options

| Option / Target | Description |
|:---|:---|
| `cmake --build build --target check` | Builds and runs registered unit and integration tests |
| `-DGRPC_DEPENDENCIES=BUNDLED\|SYSTEM` | Download and build third-party libraries (default) or use installed ones; see [above](#third-party-libraries-bundled-by-default) |
| `-DGRPC_BUILD_SHARED=OFF` | Builds only the static library |
| `-DGRPC_IPO=ON` | Enables compiler-checked IPO/LTO in optimized builds |
| `-DGRPC_ALLOCATOR=<name>` | `default`, `mimalloc`, or `jemalloc` |
| `-DGRPC_TLS_BACKEND=<name>` | `AUTO`, `openssl`, `libressl`, `boringssl`, `awslc`, `wolfssl`, `gnutls`, `rustls`, or `none` |
| `-DGRPC_TLS_PREFIX=<dir>` | Search prefix for a custom TLS backend build |
| `-DGRPC_HTTP3=AUTO\|ON\|OFF` | HTTP/3 (QUIC) server and client; needs ngtcp2, nghttp3 and a QUIC-capable TLS backend (see [HTTP/3](http3.md)) |
| `-DGRPC_PROTOBUF_C=AUTO\|ON\|OFF` | Protobuf-C helper support |
| `-DGRPC_FLATCC=AUTO\|ON\|OFF` | FlatBuffers (`flatcc`) helper support |
| `-DGRPC_FLATCC_PREFIX=<dir>` | Search prefix for `flatcc` |
| `-DGRPC_ZLIB`, `-DGRPC_ZSTD`, `-DGRPC_SNAPPY` | Optional codecs (`AUTO`, `ON`, or `OFF`) |
| `-DGRPC_DEBUG_LOGS=ON` | Enables debug logging and debug symbols |
| `-DGRPC_TRACE=ON` | Enables per-frame HTTP/2 trace logging |
| `-DGRPC_ASAN=ON` | Builds with AddressSanitizer (GCC/Clang); same as `-DGRPC_SANITIZE=address` |
| `-DGRPC_SANITIZE=<list>` | Sanitizers for the library, tests and examples (GCC/Clang), e.g. `address;undefined` or `thread` |
| `-DGRPC_HARDENING=OFF` | Drops the library's hardening (on by default, GCC/Clang): stack protector, stack-clash protection, CET (`-fcf-protection`), zero-initialized locals, `_FORTIFY_SOURCE=3` in optimized builds, and RELRO / `BIND_NOW` / non-executable stack for the shared library. `_FORTIFY_SOURCE` is not passed on to programs that link the library |
| `-DGRPC_WERROR=ON` | Treats warnings in the library's sources as errors. The library builds with a strict set (`-Wextra -Wconversion -Wsign-conversion -Wshadow -Wcast-qual -Wcast-align=strict -Wformat=2 ...`) |
| `-DGRPC_BUILD_TESTS=OFF` | Skips unit tests |
| `-DGRPC_BUILD_INTEGRATION_TESTS=OFF` | Skips Python client integration tests |
| `-DGRPC_BUILD_EXAMPLES=OFF` | Skips example server targets |
| `-DGRPC_BUILD_DEVELOPER_TARGETS=OFF` | Skips `format` / `format-check` targets |
| `-DGRPC_INSTALL=OFF` | Skips CMake `install()` and `pkg-config` rules |

---

## First Server & Client

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
          .port = 19009, /* use 0 for an OS-assigned ephemeral port */
          .log_level = GRPC_LOG_INFO,
      },
      &err);
  if (server == nullptr) {
    fprintf(stderr, "startup failed: %s\n", grpc_strerror(err));
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

Include `grpc.h` (or `grpc_version.h`) and call `grpc_version()` to query the linked version at runtime, or use `GRPC_VERSION_STRING` / `GRPC_VERSION_MAJOR` / `GRPC_VERSION_MINOR` / `GRPC_VERSION_PATCH` at compile time.
