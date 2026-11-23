#pragma once

/* otlp_service.h - the OTLP metrics Export service
 *
 * One service is shared by every event loop (grpc_server) of the process: it owns the sink, the
 * memory budget accounting, the counters and the optional worker pool.
 *
 * Request path (per Export call):
 *   1. admission (loop thread): refuse while shutting down; with workers, or a sink that may defer,
 *      reserve the request's bytes from the memory budget and queue the call;
 *   2. processing (loop thread, or a worker): wire pre-scan (nesting depth), decode into a bounded
 *      arena, validate and count data points, encode the accepted subset when the sink stores data,
 *      prepare the response;
 *   3. the sink accepts, refuses or defers the batch;
 *   4. the call is answered (OK with an exact partial_success, or an error status with RetryInfo
 *      when retrying can help), counters are updated and reservations released.
 *
 * Threads: otlp_service_attach() runs before the servers serve. Handlers run on loop threads;
 * workers and sinks answer calls with the thread-safe grpc_call_*_async() functions.
 */

#include <stddef.h>
#include <stdint.h>

#include "grpc.h"
#include "otlp_budget.h"
#include "otlp_sink.h"
#include "otlp_stats.h"

typedef struct otlp_service_config {
  /** Per-request decoded-data budget in bytes (protobuf-c structures and strings). */
  size_t max_decoded_bytes;
  /** Deepest message nesting accepted (the request itself is depth 1). */
  unsigned max_depth;
  /** retry_delay of the RetryInfo sent with retryable errors. */
  uint32_t retry_delay_ms;
  /** Longest a request may wait (queue, gate) before reaching the sink; 0 = no limit. Past it the
   * request fails with DEADLINE_EXCEEDED, without being stored. */
  uint32_t request_timeout_ms;
  /** Worker threads that decode and submit off the event loops; 0 = on the loops. */
  unsigned workers;
  /** Bounded queue in front of the workers. */
  size_t queue_capacity;
  /** Decode arena chunk kept per thread between requests. */
  size_t arena_keep_bytes;
  /** Bearer token required on every call (copied); nullptr or "" = no authentication. */
  const char *auth_token;
  /** Register the /otlp_metrics.Admin/ methods (stats, waiting, memory-sink dump, fault gate). */
  bool admin;
} otlp_service_config;

typedef struct otlp_service otlp_service;

/**
 * Creates the service and starts its workers. Takes ownership of `sink` on success. `budget`
 * must outlive the service. Returns nullptr on failure (the sink is then still the caller's).
 */
otlp_service *otlp_service_new(const otlp_service_config *config, otlp_sink *sink,
                               otlp_budget *budget);

/** Registers the service's methods and interceptors on `server` (before it serves). */
grpc_err otlp_service_attach(otlp_service *service, grpc_server *server);

/**
 * New Export calls are refused with UNAVAILABLE from now on, and the sink quiesces (fails what it
 * holds but cannot finish by itself). Any thread except a signal handler; idempotent.
 */
void otlp_service_begin_shutdown(otlp_service *service);

/**
 * Lets the workers finish the queued calls and joins them, then shuts the sink down (completing
 * the batches it still holds). Call after the servers drained, before freeing them.
 */
void otlp_service_stop(otlp_service *service);

/** Frees the service and its sink. Call after the servers were freed. */
void otlp_service_free(otlp_service *service);

otlp_stats *otlp_service_stats(otlp_service *service);
otlp_sink *otlp_service_sink(otlp_service *service);
otlp_budget *otlp_service_budget(otlp_service *service);

/** Writes the full status document (counters, memory, queue, sink) as JSON into `out`. */
void otlp_service_stats_json(otlp_service *service, otlp_strbuf *out);
