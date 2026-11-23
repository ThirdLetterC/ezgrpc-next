#pragma once

/* otlp_stats.h - receiver counters
 *
 * Relaxed atomic counters updated by every loop and worker thread. A snapshot read while requests
 * are in flight is not consistent across counters; once traffic stops (or for counters updated
 * together under the acceptance invariant below) the numbers add up exactly:
 *
 *   requests = requests_ok + requests_partial + requests_failed + requests_in_flight
 *   points_received = points_accepted + points_rejected + points_failed (+ points in flight)
 */

#include <stdatomic.h>
#include <stdint.h>

#include "grpc.h"
#include "otlp_strbuf.h"

#define OTLP_STATS_COUNTERS(X)                                                                 \
  X(requests)             /* Export calls handed to the receiver */                            \
  X(requests_ok)          /* fully accepted */                                                 \
  X(requests_partial)     /* accepted with rejected data points (partial_success set) */       \
  X(requests_failed)      /* ended with an error status; nothing from them was stored */       \
  X(bytes_received)       /* request bytes after decompression */                              \
  X(points_received)      /* data points in requests that were decoded */                      \
  X(points_accepted)      /* data points stored by the sink and acknowledged */                \
  X(points_rejected)      /* invalid data points, reported as rejected_data_points */          \
  X(points_failed)        /* valid data points of requests that failed as a whole */           \
  X(payload_bytes_stored) /* bytes handed to the sink */                                       \
  X(malformed)            /* INVALID_ARGUMENT: not a valid ExportMetricsServiceRequest */      \
  X(too_deep)             /* INVALID_ARGUMENT: nesting exceeds --max-depth */                  \
  X(too_large)            /* RESOURCE_EXHAUSTED: decodes to more than --max-decoded-bytes */   \
  X(throttled_memory)     /* UNAVAILABLE: --memory-limit reached */                            \
  X(throttled_queue)      /* UNAVAILABLE: the worker queue is full */                          \
  X(sink_retryable)       /* UNAVAILABLE: the sink refused temporarily */                      \
  X(sink_failed)          /* INTERNAL: the sink failed permanently */                          \
  X(deadline_exceeded)    /* DEADLINE_EXCEEDED: --request-timeout-ms passed before the sink */ \
  X(cancelled)            /* the client cancelled (or its deadline passed) before the sink */  \
  X(shutting_down)        /* UNAVAILABLE: received while shutting down */                      \
  X(encoding_identity)    /* requests without grpc-encoding (or identity) */                   \
  X(encoding_gzip)        /* requests with grpc-encoding: gzip */                              \
  X(encoding_other)       /* requests with another grpc-encoding */

typedef struct otlp_stats {
#define OTLP_STATS_FIELD(name) atomic_uint_least64_t name;
  OTLP_STATS_COUNTERS(OTLP_STATS_FIELD)
#undef OTLP_STATS_FIELD
  /** Final statuses of Export calls answered by the receiver, by grpc_status. */
  atomic_uint_least64_t status[17];
} otlp_stats;

/** Plain copy of the counters. */
typedef struct otlp_stats_snapshot {
#define OTLP_STATS_FIELD(name) uint64_t name;
  OTLP_STATS_COUNTERS(OTLP_STATS_FIELD)
#undef OTLP_STATS_FIELD
  uint64_t status[17];
} otlp_stats_snapshot;

/** Adds `n` to a counter. */
#define OTLP_STAT_ADD(stats, name, n) \
  atomic_fetch_add_explicit(&(stats)->name, (uint64_t)(n), memory_order_relaxed)

void otlp_stats_init(otlp_stats *stats);

void otlp_stats_snapshot_get(const otlp_stats *stats, otlp_stats_snapshot *out);

/** The value of the counter called `name`, or false when there is none. */
bool otlp_stats_lookup(const otlp_stats_snapshot *snapshot, const char *name, uint64_t *value);

/** Appends the counters as JSON object members ("name": value, ...) without braces. */
void otlp_stats_json(const otlp_stats_snapshot *snapshot, otlp_strbuf *out);
