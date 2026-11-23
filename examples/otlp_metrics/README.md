# OTLP/gRPC metrics receiver

An OpenTelemetry-compatible metrics ingestion server in C23 on the public ezGRPC-next API. It
serves `opentelemetry.proto.collector.metrics.v1.MetricsService/Export`, accepts what the
official OpenTelemetry SDKs and the Collector send (compressed or not), validates every data
point, answers with exact OTLP full/partial success or retryable/non-retryable errors, and hands
accepted data to a null, memory or write-ahead-log sink, all with bounded memory.

It is an example: application code lives here, and the library is used only through
`include/*.h`. Nothing in the library core was changed for it.

- [Building](#building) · [Running](#running) · [Protocol behavior](#protocol-behavior) ·
  [Architecture](#architecture) · [Sinks](#sinks) · [Testing](#testing) ·
  [Benchmarks](#benchmarks) · [Platforms](#platform-support) · [Limitations](#limitations-and-outstanding-work)

## Layout

| path | what |
|---|---|
| `src/` | the receiver: `otlp_service.c` (request path), `otlp_app.c` (loops, config, shutdown), `otlp_wire.c` (nesting pre-scan), `otlp_arena.c` (bounded decode arena), `otlp_validate.c` (rules, counting, filtering), `otlp_response.c`, sinks (`otlp_sink_basic.c`, `otlp_sink_fault.c`, `otlp_sink_wal.c`), `main.c` |
| `cmake/protoc_gen_c_shim.c` | lets stock protoc-c compile proto3 `optional` fields (see [code generation](#code-generation)) |
| `tests/` | C unit, end-to-end, WAL and fuzz tests |
| `tools/` | `otlp_gen` (deterministic request generator), `otlp_wal_dump` (log verifier/exporter) |
| `integration/` | Python SDK, WAL and Collector integration tests (official OpenTelemetry packages) |
| `go/` | Go SDK integration tests, the grpc-go comparison server, its validation package |
| `bench/` | benchmark harness and recorded results |

## Building

Requirements on top of the library's (a C23 compiler, CMake ≥ 3.20):

- **protobuf-c** support in the library (the default; the runtime is bundled) and the **protoc-c**
  code generator plus **protoc**. Any protoc-c version works, 1.4.1 (Ubuntu 24.04) included. On
  Debian/Ubuntu: `apt install protobuf-c-compiler protobuf-compiler`; macOS: `brew install
  protobuf-c`; Windows: `vcpkg install protobuf protobuf-c[tools]`.
- Network access on the first configure, to download opentelemetry-proto (or see `OTLP_PROTO_DIR`).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release            # the example is on by default
cmake --build build --target otlp_metrics_server otlp_gen
./build/otlp_metrics_server --help
```

| CMake option | default | |
|---|---|---|
| `GRPC_EXAMPLE_OTLP_METRICS` | `ON` | build this example (needs protobuf-c and protoc-c; skipped with a message otherwise) |
| `OTLP_METRICS_WAL` | `OFF` | the write-ahead-log sink and `otlp_wal_dump` |
| `OTLP_METRICS_FUZZ` | `OFF` | the libFuzzer target `otlp_fuzz_export` (Clang) |
| `OTLP_PROTO_DIR` | download | an extracted opentelemetry-proto v1.11.1 for offline builds |
| `OTLP_PYTHON_EXECUTABLE` | auto | Python with `integration/requirements.txt` (otherwise a venv is created when dependencies are bundled) |
| `OTLP_OTELCOL_EXECUTABLE` | none | `otelcol` binary for the Collector test (`integration/fetch-otelcol.sh`) |

### Code generation

The protocol comes from the official [opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto)
**v1.11.1** (tag commit `b3f75588eb23c5fca62264edd05d382de49beb1a`), downloaded at configure
time through the library's pinned, SHA-256-checked dependency cache
(`c0bdd2ff083665e0504a6574923ebe53c60c086ea11836c5aa543683f55a6c32`) and compiled unmodified:
`common`, `resource`, `metrics` and `collector/metrics` `.proto` files go through protoc +
protoc-c at build time.

No protobuf-c release (1.5.2 is the latest) declares support for proto3 `optional`, which
`HistogramDataPoint.sum/min/max` and `ExponentialHistogramDataPoint.sum/min/max` use, so protoc
refuses its output. Protoc already describes such a field as the only member of a *synthetic
oneof*, which protoc-c compiles like any oneof: the struct gets `_sum_case`, which records
presence. `cmake/protoc_gen_c_shim.c` runs the real plugin and appends
`supported_features = FEATURE_PROTO3_OPTIONAL` to its response; the `.proto` files and the
generator stay as published. Unit, Python and Go tests check that presence survives (a `sum` of
0.0 is present, an absent `max` stays absent).

## Running

```bash
./build/otlp_metrics_server --port 4317 --loops 4 --sink memory --admin
```

It prints `READY port=<n>` once listening (useful with `--port 0`), and stops gracefully on
SIGINT/SIGTERM (a second signal exits at once). Point any OTLP/gRPC exporter at it, e.g.
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://127.0.0.1:4317 OTEL_EXPORTER_OTLP_METRICS_PROTOCOL=grpc`.

| option | default | |
|---|---|---|
| `--host ADDR` / `--host6 ADDR` / `--port N` / `--unix PATH` | all addresses, 4317 | listeners |
| `--loops N` | 1 | event loops (grpc_servers) on one port, one thread each, `SO_REUSEPORT` (Unix) |
| `--workers N` | 0 | decode/submit on N worker threads instead of the loops |
| `--queue-capacity N` | 1024 | bounded queue in front of the workers |
| `--max-connections N` / `--max-streams N` | 256 / 64 | per loop / per connection |
| `--max-message-bytes N` | 4M | request limit, also after decompression |
| `--max-decoded-bytes N` | 32M | decoded-data budget per request |
| `--max-depth N` | 32 | deepest message nesting |
| `--memory-limit N` | 256M | memory the receiver keeps for requests (see [memory](#memory-bounds)) |
| `--request-timeout-ms N` | 10000 | longest wait before the sink (queue, gate) |
| `--retry-delay-ms N` | 1000 | `RetryInfo.retry_delay` of retryable errors |
| `--auth-token T` / `OTLP_AUTH_TOKEN` | none | require `authorization: Bearer T` |
| `--tls-cert F --tls-key F` | | serve TLS |
| `--sink null\|memory\|wal` | null | where accepted data goes ([sinks](#sinks)) |
| `--admin` | off | `/otlp_metrics.Admin/{Stats,Wait,Dump,Clear,Gate}` |
| `--drain-ms N` | 5000 | graceful shutdown budget |
| `--fault SPEC` | | fault injection for tests (`gate`, `gate-block`, `gate-capacity=N`, `retryable-every=N`, `fail-every=N`) |

Sizes take `K`/`M`/`G` suffixes. `--help` lists every option, including the sink settings.

## Protocol behavior

Following the [OTLP specification](https://github.com/open-telemetry/opentelemetry-proto/blob/v1.11.1/docs/specification.md)
of the pinned release:

| condition | gRPC status | retryable for OTLP clients |
|---|---|---|
| every point accepted (also: an empty request) | `OK`, `partial_success` **unset** | — |
| some or all points rejected by a [rule](#validation-rules) | `OK`, `partial_success{rejected_data_points: exact count, error_message}` | must not retry |
| not a valid `ExportMetricsServiceRequest`, or nesting deeper than `--max-depth` | `INVALID_ARGUMENT` | no |
| larger than `--max-message-bytes` (before or after decompression) | `RESOURCE_EXHAUSTED` (library) | no (no RetryInfo) |
| decodes to more than `--max-decoded-bytes` | `RESOURCE_EXHAUSTED` | no (no RetryInfo) |
| missing/wrong bearer token (`--auth-token`) | `UNAUTHENTICATED` (library interceptor) | no |
| memory budget, worker queue, sink full, sink I/O error, shutting down | `UNAVAILABLE` + RetryInfo | yes, after `retry_delay` |
| `--request-timeout-ms` passed before reaching the sink | `DEADLINE_EXCEEDED` | yes |
| client deadline passed / client cancelled | the library ends the call; the batch is **not** stored | yes |
| sink failed permanently | `INTERNAL` | no |

A request is acknowledged only after its sink accepted the data (for the WAL: after it was
written and, by default, fsynced). The response is encoded before the batch reaches the sink, so
answering cannot fail after acceptance.

**RetryInfo** is sent the way the specification describes, as a `google.rpc.RetryInfo` detail in
`grpc-status-details-bin` (read by the Go exporter and the Collector). The OpenTelemetry Python
exporter reads a separate `google.rpc.retryinfo-bin` trailer instead, so the same RetryInfo is
sent there too; without it, Python falls back to its exponential backoff. Both are covered by
tests (the Go test checks that a 1.5 s RetryInfo is honored over a 10 ms backoff; the Python test
sees the exporter retry after the configured 50 ms).

**Data handling.** A fully accepted request is stored byte for byte (unknown fields included).
When points are rejected, the accepted subset is re-encoded with protobuf-c from the decoded
request, minus the rejected points (metrics, scopes and resources emptied by the removal are
dropped; envelopes that arrived empty are kept). Metric semantics are not transformed. Strings
are not UTF-8-validated (the spec lets intermediaries pass them through).

### Validation rules

A point is rejected (and counted once, whatever rules it breaks) when:

- its metric has an empty name, or is a sum, histogram or exponential histogram with
  `AGGREGATION_TEMPORALITY_UNSPECIFIED` (all points of that metric);
- `time_unix_nano` is 0;
- a number point has no value and is not flagged `NO_RECORDED_VALUE`;
- a histogram point's `explicit_bounds` are not strictly increasing (or NaN), or it has buckets
  and `len(bucket_counts) != len(explicit_bounds) + 1`, or the bucket counts do not add up to
  `count` (unless flagged `NO_RECORDED_VALUE`);
- an exponential histogram point's scale is outside [-10, 20], its `zero_threshold` is negative
  or NaN, or `zero_count` plus all bucket counts does not equal `count` (overflow included);
- a summary quantile is outside [0, 1] or NaN.

A metric whose data is unset has no points and is kept. The grpc-go comparison server applies the
same rules (`go/otlpvalidate`); a test checks both servers agree.

## Architecture

```
            ┌──────────── loop thread (one per --loops, SO_REUSEPORT) ────────────┐
request ──► │ library: HTTP/2, size limit, decompression, auth interceptor         │
            │ on_export: admission ─► (inline) process ─► sink ─► answer           │
            └──────────┬──────────────────────────────────────────▲──────────────┘
                       │ --workers: bounded queue                 │ grpc_call_*_async()
                       ▼                                          │
            worker threads: process ─► sink ─────────────────────►┤
                                        │ PENDING                  │
                                        ▼                          │
                         sink thread (WAL writer): write, fsync ───┘
process = nesting pre-scan ─► protobuf-c decode into a bounded arena ─► validate & count
          ─► response ─► accepted payload (request bytes, or re-encoded subset)
```

- **Threads.** Each loop is a `grpc_server` on its own thread; handlers run there. With
  `--workers`, calls are referenced (`grpc_call_ref`) and queued; workers answer with the
  thread-safe `grpc_call_*_async()` functions. Sinks that defer (the WAL, the fault gate) complete
  on their own threads, also through the async API. Disk I/O never runs on a loop. Cancellation
  reaches jobs through `grpc_call_on_cancel()` (an atomic flag checked before the sink and by the
  WAL writer).
- **Decoding.** protobuf-c recurses per nested message without a limit, and the OTLP schema is
  recursive (`AnyValue` ↔ `ArrayValue`/`KeyValueList`), so `otlp_wire.c` first walks the encoding
  iteratively with the message descriptors and rejects nesting deeper than `--max-depth` (and
  framing errors). protobuf-c then decodes into a per-thread bump arena: no per-field malloc, the
  per-request budget is enforced on every allocation, and one chunk is kept between requests.
- **Shutdown.** On SIGINT/SIGTERM every loop stops accepting, new calls get `UNAVAILABLE`, and the
  sink *quiesces* (batches it cannot finish by itself are failed as retryable, so they are
  answered while connections are still open). Loops drain (`grpc_server_drain`, `--drain-ms`),
  then workers finish the queue, the sink shuts down (the WAL writes what is queued and syncs),
  and the servers are freed.

### Memory bounds

Everything the receiver itself keeps for a request is reserved from one budget
(`--memory-limit`) first: decode arena chunks, the request bytes of queued or deferred calls,
payload copies held by the fault gate and by the WAL queue. A reservation that does not fit is a
retryable `UNAVAILABLE`, so overload degrades into throttling. Independently of it:

- per request, decoded data ≤ `--max-decoded-bytes`, and messages ≤ `--max-message-bytes`;
- the worker queue holds ≤ `--queue-capacity` calls; the WAL queue ≤ `--wal-queue-bytes`;
- the memory sink holds ≤ `--memory-sink-bytes` (outside the budget: it is storage);
- the library buffers requests while they arrive: per stream up to `--max-message-bytes` (its
  HTTP/2 stream window), so in the worst case `loops × --max-connections × --max-streams ×
  --max-message-bytes`. Lower those for tight memory; this part is not covered by
  `--memory-limit` (see [limitations](#limitations-and-outstanding-work)).

`test_memory_limit_throttles` fills the budget deterministically and checks the peak never
exceeds it and that every reservation is returned.

## Sinks

The contract (`src/otlp_sink.h`): `submit()` may be called from any thread, borrows the batch for
the call only (a sink copies what it keeps), and answers ACCEPTED (stored), PENDING (it calls the
completion exactly once later), RETRYABLE or FAILED (not stored). `quiesce()`/`shutdown()` finish
or fail everything pending.

- **null**: accepts and counts. For measuring the receiver.
- **memory**: keeps payload copies up to `--memory-sink-bytes` (`--memory-sink-batches`),
  refusing (`--memory-policy reject`, retryable) or evicting the oldest (`evict`) when full.
  `Admin/Dump` returns everything stored as one `ExportMetricsServiceRequest` (concatenated
  encodings merge into one message), which the tests decode with the official bindings.
- **wal** (`-DOTLP_METRICS_WAL=ON`): durable, see `src/otlp_wal.h`. One writer thread appends
  every queued batch and syncs once per group (group commit), then acknowledges them. Segments
  `<first sequence>.wal`, CRC-32C framed records holding the accepted request. `--wal-sync fsync`
  (default: survives power loss) or `none` (survives a process crash). Full log, full queue, I/O
  errors: retryable; a failed write is truncated away, a failed sync drops the whole group (no
  duplicates on retry). Startup recovery truncates a torn tail and continues numbering.
  `otlp_wal_dump DIR [--export FILE]` verifies every CRC and payload and exports the log. The
  receiver never deletes the log: retention belongs to its consumer.
- **fault** (`--fault`): wraps the sink with deterministic failures and a gate that holds
  (or, `gate-block`, blocks workers on) batches until `Admin/Gate` opens it. Tests use it to keep
  requests in flight without sleeping.

## Admin methods

With `--admin` (and the bearer token, when set), raw-bytes methods under `/otlp_metrics.Admin/`:
`Stats` (JSON counters, memory, queue, sink), `Wait` (body `"<counter> <n>"`: answers once the
counter reaches n; the client deadline bounds it), `Dump` and `Clear` (memory sink), `Gate`
(`"open"`/`"close"`, fault sink). Counters satisfy `points_received = points_accepted +
points_rejected + points_failed` once traffic stops.

## Testing

```bash
cmake -S . -B build -DOTLP_METRICS_WAL=ON
cmake --build build
ctest --test-dir build -L otlp --output-on-failure
```

| test | what |
|---|---|
| `otlp_unit` | budget, arena, wire pre-scan (incl. 200 000-level nesting), decode round trip and presence, every validation rule with exact counts, filtering, responses, RetryInfo bytes, sinks, fault gate, queue, stats |
| `otlp_server` | in-process end to end through the ezGRPC-next client: success, exact partial success, empty/all-rejected, malformed, too deep, gzip, oversized (wire and decompressed), decoded budget, auth, memory-budget throttling, gate capacity, worker queue overflow, client deadline (not stored), request timeout, sink failure classes, accounting under 8 concurrent clients × 150 mixed requests with 1/4 loops × 0/2/3 workers (and the WAL), graceful shutdown |
| `otlp_wal` | CRC vectors, rotation, restart numbering, torn-tail recovery, bit flips, full log, queue bound, injected write failure, cancelled batches, drain on shutdown |
| `otlp_fuzz_replay` | the fuzz target over generated seeds and `tests/fuzz-regressions/` |
| `otlp_integration_python` | official Python SDK 1.45.2 exporter: every instrument kind × gzip/identity × cumulative/delta, auth, throttling + retry; official stubs: partial success, unknown fields preserved, malformed, oversized, RetryInfo in both places |
| `otlp_integration_wal` | Python SDK export, SIGKILL, log decoded with official bindings; torn tail + restart |
| `otlp_integration_go` | official Go SDK 1.47.0 exporter: every instrument kind × gzip/identity × cumulative/delta, RetryInfo honored, auth; summaries and optional presence byte-exact (`proto.Equal`); partial success; 16 concurrent clients; C vs grpc-go equivalence |
| `otlp_integration_collector` | SDK → Collector (`otelcol`, gzip and uncompressed exporters) → receiver, plus collector retries. **Needs** `OTLP_OTELCOL_EXECUTABLE`; skipped (not passed) otherwise |

Tests wait on server-side conditions (the READY line, `Admin/Wait`, the gate's held count),
never on time; the only deliberate clock wait is the one that lets `--request-timeout-ms`
expire.

**Sanitizers** (GCC/Clang, Linux): `-DGRPC_SANITIZE=address;undefined` and
`-DGRPC_SANITIZE=thread` apply to the library, the receiver and every test
(`UV_USE_IO_URING=0 setarch -R ctest ...` for TSan, as for the library's own tests).

**Fuzzing** (Clang): `-DOTLP_METRICS_FUZZ=ON -DGRPC_SANITIZE=address;undefined` builds
`otlp_fuzz_export` (libFuzzer, protobuf-c compiled in for coverage); seeds come from `otlp_gen`
(`otlp_fuzz_corpus` target, in `<build>/examples/otlp_metrics/fuzz-corpus`):

```bash
./otlp_fuzz_export -jobs=8 -workers=8 -max_total_time=600 -max_len=65536 corpus/
```


Campaign recorded for this work: 8 workers × 600 s, **29.7 million executions, no crash, leak,
timeout or invariant failure**; corpus 5 seeds → 9 376 inputs, 1 061 edges / 6 632 features. An
earlier run found undefined behavior in protobuf-c itself (see `tests/fuzz-regressions/`).

## Benchmarks

`bench/run_bench.py` (Linux) compares this receiver with `go/cmd/grpcgo-server`, a grpc-go
receiver with the same contract (full protobuf-go decode, the same validation rules and depth
limit, partial success, the same sink kinds; `TestComparisonServerEquivalence` checks they answer
alike). Load comes from [ghz](https://ghz.sh) with payloads from `otlp_gen`.

```bash
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DGRPC_IPO=ON -DOTLP_METRICS_WAL=ON
cmake --build build-bench --target otlp_metrics_server otlp_gen otlp_grpcgo_server
python3 examples/otlp_metrics/bench/run_bench.py --ez default=build-bench/otlp_metrics_server \
  --grpcgo build-bench/grpcgo-server --gen build-bench/otlp_gen \
  --proto-root build-bench/examples/otlp_metrics/opentelemetry-proto-1.11.1 \
  --wal-dir /tmp/otlp-bench-wal --duration 10 --reps 3 --out results/$(date -u +%Y%m%d-%H%M%S)
```

Method: a fresh server per run; 3 s warm-up; 10 s measured; 3 repetitions, interleaved; 64
concurrent calls over 16 connections. Server on physical cores 0-3 (logical 0-3, 8-11), ghz on
cores 4-7 (4-7, 12-15), so they never share a core; `sat-*` runs give the server one core (CPU 0)
and ghz the other seven. Requests/s and latencies come from ghz, accepted points/s from the
server's counters, CPU time and peak RSS from `/proc`. Medians, with min-max.

### Recorded results

[`bench/results/20261123-011435/`](bench/results/20261123-011435/): `summary.md`, 129 raw runs
(`runs/*.json`, with ghz's full report and server counters), `environment.json`, `run.log`. The
payload files (`workloads/*.bin`) are not committed; `workloads/workloads.json` holds the
`otlp_gen` arguments that recreate them byte for byte.

- Intel Core i9-9900K (8 cores / 16 threads), `powersave` governor, turbo on; Linux 7.0.0
  (Ubuntu 24.04); GCC 13.3, `-O3 -DNDEBUG`, IPO, bundled dependencies, glibc malloc (and
  mimalloc 2.x); Go 1.26.8, grpc-go 1.85.0; ghz built from source (`go version -m` in
  `environment.json`); source `2edb9b2`, opentelemetry-proto v1.11.1.
- Workloads: **small** = 1 resource, 10 metrics of all five kinds, 10 points, 2 061 bytes;
  **large** = 4 resources × 2 scopes × 25 metrics × 5 points, 1 000 points, 175 368 bytes;
  **partial** = 20 points of which 5 are invalid, 3 458 bytes (partial success every call).

Server at its limit (one core each, `sat-*`, null sink):

| workload | ezGRPC-next req/s | grpc-go req/s | ezGRPC-next CPU µs/req | grpc-go CPU µs/req | p99 ms (ez / go) |
|---|---:|---:|---:|---:|---:|
| small | **56 916** (569 k points/s) | 16 669 | 16.4 | 59.4 | 1.78 / 5.81 |
| large | **1 175** (1.18 M points/s) | 425 | 844 | 2 310 | 72.0 / 455.9 |
| partial | **42 955** (644 k accepted points/s) | 12 189 | 22.1 | 81.3 | 2.22 / 7.96 |

Four server cores (null sink), where ghz on four cores becomes the limit (small: ~38.7 k req/s):

| workload, config | req/s | server CPU cores | CPU µs/req | peak RSS MiB |
|---|---:|---:|---:|---:|
| small, ezGRPC-next 1 / 2 / 4 loops | 38 851 / 38 727 / 38 703 | 0.82 / 1.03 / 1.24 | 21.1 / 26.6 / 32.0 | 7 / 7 / 7 |
| small, grpc-go GOMAXPROCS 1 / 2 / 4 | 16 839 / 28 224 / 37 113 | 0.99 / 1.89 / 3.40 | 58.9 / 66.9 / 91.7 | 18 / 19 / 20 |
| large, ezGRPC-next 1 / 2 / 4 loops | 1 121 / 1 259 / 1 275 | 0.92 / 1.23 / 1.30 | 823 / 974 / 1 023 | 21 / 23 / 28 |
| large, grpc-go GOMAXPROCS 1 / 2 / 4 | 422 / 785 / 1 230 | 1.00 / 1.99 / 3.94 | 2 338 / 2 509 / 3 187 | 34 / 49 / 71 |

Other configurations (full tables in `summary.md`):

- **Sinks.** Memory sink (256 MiB, evicting), 4 loops: small 38 579 req/s at 31.1 µs/req, peak
  RSS 362 MiB; grpc-go's memory sink 37 900 req/s at 87.0 µs/req, 629 MiB. WAL with fsync
  (group commit), 4 loops, small: median **27 802 req/s** (278 k points/s), p99 4.26 ms; it is
  fsync-bound (groups of up to 63 batches, ~0.94 ms per `fdatasync`); one repetition ran at
  6 240 req/s because `fdatasync` averaged 3.9 ms during it (cause not determined).
- **Workers** (1 loop + 4 workers): same throughput as more loops under this client, at a higher
  CPU cost (small 32.0 µs/req vs 21.1 inline): the queue and the async answer cost more than they
  save when decoding takes microseconds. They are meant for keeping loops responsive with big
  requests or a blocking sink.
- **Allocator.** mimalloc vs glibc: +3.3 % req/s at saturation on small (58 821 vs 56 916) and
  partial (44 355 vs 42 955), none on large (1 175 both). The decode arena keeps the receiver's
  own allocations off the allocator; what remains is the library's per-call allocations.

What the numbers say, and what they do not:

- At saturation on one core the C receiver handles ~2.8-3.5× the requests of the grpc-go server,
  and uses 2.7-3.7× less CPU per request on every workload, with lower tail latency and memory.
- **Multi-loop scaling is not demonstrated by this setup.** With ghz on four cores as the client,
  every ezGRPC-next configuration hit the client's ceiling with 0.8-1.3 server cores busy, so
  adding loops could not raise throughput; it only lowered median latency and spread work
  (raising CPU per request, since each loop batches fewer calls per poll). A faster load
  generator, or several client machines, is needed to measure loop scaling; grpc-go, needing
  ~3.4 cores for the same rate, is the one whose scaling is visible here.
- "client non-OK" counts are calls in flight when ghz's duration ended; the receiver returned no
  error status in any run (CANCELLED from those cut-off calls excepted).
- The machine was not tuned (frequency scaling on, turbo on, a desktop OS); repetitions were
  interleaved to spread drift and min-max ranges are reported. A few light builds ran during
  part of the run. jemalloc was not measured (not installed).

## Platform support

| | Linux | Windows (MSVC, vcpkg) | notes |
|---|---|---|---|
| receiver, null/memory sinks, workers | ✓ tested | builds with the same CMake; not run in this work | needs `protobuf-c[tools]` and `protobuf` from vcpkg (`-DGRPC_PROTOBUF_C=ON`; the `msvc` preset turns it off) |
| `--loops` > 1 | ✓ (`SO_REUSEPORT`) | ✗ refused at startup | use `--workers` for parallelism |
| WAL sink | ✓ tested | portable code (libuv `uv_fs_*`, `FlushFileBuffers`); directory entries are not synced | |
| graceful stop | SIGINT/SIGTERM | Ctrl-C (SIGINT) | the Python harness terminates the process on Windows |
| C tests | ✓ | portable (pthreads shim, libuv) | |
| Python SDK / WAL tests | ✓ | should run (pure Python) | |
| Go SDK tests, grpc-go server | ✓ | should run | |
| Collector test | needs `otelcol` | needs `otelcol` | `fetch-otelcol.sh` is Linux/macOS |
| benchmarks | ✓ | ✗ | `taskset`, `/proc` |
| fuzzing | Clang/libFuzzer | ✗ (replay test runs everywhere) | |
| sanitizers | GCC/Clang ASan+UBSan, TSan | ✗ | |

Windows builds were not run in this work; the table states what the code is written for.

## Limitations and outstanding work

- **Transport buffering is outside `--memory-limit`.** The library buffers each stream's request
  up to `--max-message-bytes` before the handler runs; the bound is `loops ×
  --max-connections × --max-streams × --max-message-bytes`. A per-connection or global receive
  budget would need support in the library (e.g. a hook before a stream's body is buffered).
- **At-least-once.** As OTLP itself: a response lost after acceptance (client deadline or
  connection loss racing the answer) makes the client retry, so a batch can be stored twice.
- **WAL**: no compaction, retention or replay into another system; segments rotate only between
  write groups; a failed directory sync on rotation is not reported; one writer thread (a single
  disk stream) for all loops.
- **Collector test not run here**: running a downloaded Collector binary was not permitted in the
  environment this was developed in. The test and configuration are ready
  (`integration/fetch-otelcol.sh`, `OTLP_OTELCOL_EXECUTABLE`).
- **Windows** was not built or tested in this work (see the table above). jemalloc builds were
  not benchmarked (not installed on the benchmark machine).
- **protobuf-c**: proto3 `optional` relies on the shim until protobuf-c reports the feature
  itself; the fuzzer's finding in protobuf-c (`memcpy(NULL, p, 0)` for empty packed fields) is
  worth reporting upstream.
- **Not implemented**: OTLP/HTTP, traces/logs/profiles services, UTF-8 validation of strings,
  per-tenant limits, metrics about the receiver itself in OTLP form (the Admin JSON is the only
  self-telemetry).

