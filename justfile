# Common development commands for ezGRPC-next. Run `just` to list them.

set shell := ["bash", "-euo", "pipefail", "-c"]

build_dir := "build"
jobs := `nproc 2>/dev/null || echo 4`

# List available recipes
default:
    @just --list

# Configure the default build dir (extra args go to cmake, e.g. -DGRPC_TLS_BACKEND=none)
configure type="Debug" *args:
    cmake -S . -B {{build_dir}} -DCMAKE_BUILD_TYPE={{type}} -DCMAKE_EXPORT_COMPILE_COMMANDS=ON {{args}}

# Build everything (configures on first use)
build:
    @[ -f {{build_dir}}/CMakeCache.txt ] || just build_dir={{build_dir}} configure
    cmake --build {{build_dir}} --parallel {{jobs}}

# Build and run all tests, or those matching a regex: `just test tls`
test filter="": build
    ctest --test-dir {{build_dir}} --output-on-failure -j {{jobs}} {{ if filter != "" { "-R '" + filter + "'" } else { "" } }}

# Run unit tests only
unit: build
    ctest --test-dir {{build_dir}} --output-on-failure -j {{jobs}} -L unit

# Run integration tests only (needs python3 with grpcio, grpcio-tools, protobuf)
integration: build
    ctest --test-dir {{build_dir}} --output-on-failure -L integration

# Run one test binary directly from its working directory, e.g. `just run-test tls`
run-test name: build
    #!/usr/bin/env bash
    set -euo pipefail
    dir=$(ctest --test-dir {{build_dir}} -R '^{{name}}$' --show-only=json-v1 \
      | python3 -c 'import json,sys; t=json.load(sys.stdin)["tests"][0]; print(next((p["value"] for p in t.get("properties",[]) if p["name"]=="WORKING_DIRECTORY"), "."))')
    cd "$dir" && "$(realpath {{build_dir}})/test_{{name}}"

# Same as CI's `check` target: build everything and run the full suite
check: build
    cmake --build {{build_dir}} --target check --parallel {{jobs}}

# Build and test under AddressSanitizer, like CI (in build-asan)
asan:
    cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DGRPC_ASAN=ON
    cmake --build build-asan --parallel {{jobs}}
    ctest --test-dir build-asan --output-on-failure -j {{jobs}}

# Build and test under ThreadSanitizer (in build-tsan)
tsan:
    cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DGRPC_SANITIZE=thread
    cmake --build build-tsan --parallel {{jobs}}
    setarch -R ctest --test-dir build-tsan --output-on-failure -j {{jobs}}

# Optimized build with IPO (in build-release)
release:
    cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DGRPC_IPO=ON
    cmake --build build-release --parallel {{jobs}}
    ctest --test-dir build-release --output-on-failure -j {{jobs}}

# Build with a given allocator: default, mimalloc or jemalloc (in build-<allocator>)
allocator name:
    cmake -S . -B build-{{name}} -DCMAKE_BUILD_TYPE=Release -DGRPC_ALLOCATOR={{name}}
    cmake --build build-{{name}} --parallel {{jobs}}
    ctest --test-dir build-{{name}} --output-on-failure -j {{jobs}}

# Build and test with a TLS backend: openssl|wolfssl|gnutls|none (system), libressl|boringssl|awslc|rustls (fetched to .tls/)
tls backend:
    #!/usr/bin/env bash
    set -euo pipefail
    prefix=""
    case "{{backend}}" in
      libressl|boringssl|awslc|rustls)
        prefix="$PWD/.tls/{{backend}}"
        [ -d "$prefix" ] || scripts/build-tls-backend.sh {{backend}} "$prefix" .tls/src
        ;;
    esac
    cmake -S . -B build-{{backend}} -DCMAKE_BUILD_TYPE=Debug \
      -DGRPC_TLS_BACKEND={{backend}} ${prefix:+-DGRPC_TLS_PREFIX="$prefix"}
    cmake --build build-{{backend}} --parallel {{jobs}}
    ctest --test-dir build-{{backend}} --output-on-failure -j {{jobs}}

# Build and test with HTTP/3 on a TLS backend: openssl (3.5, built), boringssl, awslc, wolfssl, gnutls (deps fetched to .tls/)
http3 backend="openssl":
    #!/usr/bin/env bash
    set -euo pipefail
    prefix="$PWD/.tls/{{backend}}"
    case "{{backend}}" in
      openssl|boringssl|awslc)
        [ -f "$prefix/include/openssl/ssl.h" ] || scripts/build-tls-backend.sh {{backend}} "$prefix" .tls/src ;;
    esac
    [ -f "$prefix/lib/pkgconfig/libngtcp2.pc" ] || scripts/build-quic-deps.sh {{backend}} "$prefix" .tls/src
    cmake -S . -B build-h3-{{backend}} -DCMAKE_BUILD_TYPE=Debug -DGRPC_HTTP3=ON \
      -DGRPC_TLS_BACKEND={{backend}} -DGRPC_TLS_PREFIX="$prefix" -DOPENSSL_ROOT_DIR="$prefix"
    cmake --build build-h3-{{backend}} --parallel {{jobs}}
    ctest --test-dir build-h3-{{backend}} --output-on-failure -j {{jobs}}

# Build and test with FlatBuffers support (flatcc fetched into .tls/flatcc on first use)
flatcc:
    [ -d .tls/flatcc ] || scripts/build-flatcc.sh "$PWD/.tls/flatcc" .tls/src
    cmake -S . -B build-flatcc -DCMAKE_BUILD_TYPE=Debug -DGRPC_FLATCC=ON -DGRPC_FLATCC_PREFIX="$PWD/.tls/flatcc"
    cmake --build build-flatcc --parallel {{jobs}}
    ctest --test-dir build-flatcc --output-on-failure -j {{jobs}}

# Format sources in place with clang-format
format:
    @[ -f {{build_dir}}/CMakeCache.txt ] || just build_dir={{build_dir}} configure
    cmake --build {{build_dir}} --target format

# Fail if any source is not clang-formatted
format-check:
    @[ -f {{build_dir}}/CMakeCache.txt ] || just build_dir={{build_dir}} configure
    cmake --build {{build_dir}} --target format-check

# Run the single-threaded example server (extra args are passed through)
example-single *args: build
    {{build_dir}}/single_threaded.bin {{args}}

# Run the multi-threaded example server (extra args are passed through)
example-multi *args: build
    {{build_dir}}/multi_threaded.bin {{args}}

# Benchmark the allocators with ghz (needs ~/go/bin/ghz)
bench:
    ./bench_allocators.sh

# Remove all build directories (keeps fetched dependencies in .tls/)
clean:
    rm -rf build build-*

# Also remove fetched TLS backends and flatcc
distclean: clean
    rm -rf .tls
