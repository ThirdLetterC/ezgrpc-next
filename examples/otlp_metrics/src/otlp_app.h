#pragma once

/* otlp_app.h - the receiver process: configuration, event loops, shutdown
 *
 * An app runs `loops` grpc_servers on the same port (SO_REUSEPORT spreads connections across them;
 * Unix only), each on its own thread, sharing one otlp_service. Stopping drains the loops
 * gracefully: new calls are refused, calls in flight finish (up to drain_ms), then the workers and
 * the sink are shut down and everything is freed.
 */

#include <stddef.h>
#include <stdint.h>

#include "otlp_service.h"

typedef enum otlp_sink_kind {
  OTLP_SINK_KIND_NULL = 0,
  OTLP_SINK_KIND_MEMORY,
  OTLP_SINK_KIND_WAL,
} otlp_sink_kind;

typedef struct otlp_config {
  /* transport */
  const char *host;           /* IPv4 address; default "0.0.0.0" */
  const char *host6;          /* IPv6 address; default none */
  uint16_t port;              /* default 4317; 0 = ephemeral */
  const char *unix_path;      /* listen on a Unix socket / named pipe instead */
  unsigned loops;             /* event loops (servers); default 1 */
  size_t max_connections;     /* default 256 */
  uint32_t max_streams;       /* concurrent calls per connection; default 64 */
  uint32_t max_message_bytes; /* request size limit, after decompression; default 4 MiB */
  const char *tls_cert;       /* PEM files enabling TLS */
  const char *tls_key;
  uint32_t drain_ms; /* graceful shutdown budget; default 5000 */
  int log_level;     /* grpc_log_level; default warning */

  /* processing */
  otlp_service_config service;
  size_t memory_limit; /* receiver memory budget in bytes; default 256 MiB */

  /* sink */
  otlp_sink_kind sink;
  size_t memory_sink_bytes;   /* default 64 MiB */
  size_t memory_sink_batches; /* 0 = unlimited */
  otlp_memory_full_policy memory_policy;
  const char *wal_dir;
  int wal_sync; /* otlp_wal_sync */
  size_t wal_segment_bytes;
  size_t wal_max_bytes;
  size_t wal_queue_bytes;

  /* fault injection (tests); active when `fault` is set */
  bool fault;
  otlp_fault_config fault_config;
} otlp_config;

/** Fills `config` with the defaults. */
void otlp_config_defaults(otlp_config *config);

/**
 * Parses command line options into `config` (which should hold the defaults). Returns 0 on
 * success, 1 after printing help, or -1 after printing an error to stderr. Strings point into
 * argv.
 */
int otlp_config_parse(otlp_config *config, int argc, char **argv);

typedef struct otlp_app otlp_app;

/** Creates the sink, service and servers, and starts serving on background threads. */
grpc_err otlp_app_start(const otlp_config *config, otlp_app **out);

/** The bound TCP port. */
uint16_t otlp_app_port(const otlp_app *app);

otlp_service *otlp_app_service(otlp_app *app);

/** Requests a graceful stop. Async-signal-safe and thread-safe. */
void otlp_app_stop(otlp_app *app);

/**
 * Waits until the app stopped (after otlp_app_stop()), drains, shuts down and frees everything.
 * Returns 0, or 1 when calls were still in flight after the drain timeout.
 */
int otlp_app_wait(otlp_app *app);
