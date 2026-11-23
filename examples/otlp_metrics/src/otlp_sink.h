#pragma once

/* otlp_sink.h - where accepted metric batches go
 *
 * A sink receives each batch the receiver is about to acknowledge. The receiver acknowledges a
 * request (OK, possibly with partial_success) only after its sink accepted the batch, and fails it
 * otherwise, so a client is never told that data was accepted when it was not.
 *
 * Contract of submit():
 *  - It may be called concurrently from any loop or worker thread.
 *  - `batch` and everything it points to are borrowed for the duration of the call only. A sink
 *    that keeps data copies it before returning.
 *  - The result says what happened to the batch:
 *      OTLP_SINK_ACCEPTED   stored (to the sink's guarantee: in memory, written, synced, ...);
 *                           the request is acknowledged now.
 *      OTLP_SINK_PENDING    the sink took `done` and will call it exactly once, from any thread,
 *                           with the final result (ACCEPTED, RETRYABLE or FAILED). Only sinks
 *                           with OTLP_SINK_MAY_DEFER return it.
 *      OTLP_SINK_RETRYABLE  not stored, for a temporary reason (full, I/O error, shutting down);
 *                           the client gets UNAVAILABLE with RetryInfo and may retry.
 *      OTLP_SINK_FAILED     not stored, and retrying would not help; the client gets INTERNAL.
 *    `done` is called only for OTLP_SINK_PENDING.
 *  - `detail` (in the completion) is a short reason for the client, valid during the call.
 *
 * quiesce() starts a shutdown while the event loops still drain: later submits return RETRYABLE,
 * and pending batches that cannot complete by themselves (held by a closed gate) are failed now,
 * so their calls are answered before the connections close. Batches that are making progress (a
 * WAL write) keep going. shutdown() then finishes or fails every pending batch (calling their
 * `done`) and joins the sink's threads. destroy() frees a shut-down sink.
 */

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "otlp_budget.h"
#include "otlp_strbuf.h"

typedef enum otlp_sink_result {
  OTLP_SINK_ACCEPTED = 0,
  OTLP_SINK_PENDING,
  OTLP_SINK_RETRYABLE,
  OTLP_SINK_FAILED,
} otlp_sink_result;

/** One request's accepted data. */
typedef struct otlp_batch {
  /** Serialized ExportMetricsServiceRequest with exactly the accepted data points; nullptr when
   * the sink does not set OTLP_SINK_NEEDS_PAYLOAD. */
  const uint8_t *payload;
  size_t payload_len;
  /** Accepted data points in the batch. */
  uint64_t points;
} otlp_batch;

/** Called once with the final result of a batch the sink returned OTLP_SINK_PENDING for. */
typedef void (*otlp_sink_done_fn)(void *ctx, otlp_sink_result result, const char *detail);

typedef struct otlp_sink_completion {
  otlp_sink_done_fn fn;
  void *ctx;
  /** Set (by another thread) once the client is gone: a sink may then skip storing the batch and
   * complete it as RETRYABLE. May be nullptr. */
  const atomic_bool *cancelled;
} otlp_sink_completion;

enum {
  /** The sink stores data: it needs `payload` (encoding it costs time, so sinks that do not store
   * anything leave this out). */
  OTLP_SINK_NEEDS_PAYLOAD = 1u << 0,
  /** submit() may return OTLP_SINK_PENDING. */
  OTLP_SINK_MAY_DEFER = 1u << 1,
};

typedef struct otlp_sink otlp_sink;

typedef struct otlp_sink_ops {
  const char *name;
  otlp_sink_result (*submit)(otlp_sink *sink, const otlp_batch *batch,
                             const otlp_sink_completion *done, const char **detail);
  /** Appends sink-specific JSON object members (without braces). Optional. */
  void (*stats_json)(otlp_sink *sink, otlp_strbuf *out);
  /** Optional; see above. Any thread, called once before shutdown(). */
  void (*quiesce)(otlp_sink *sink);
  void (*shutdown)(otlp_sink *sink);
  void (*destroy)(otlp_sink *sink);
} otlp_sink_ops;

struct otlp_sink {
  const otlp_sink_ops *ops;
  unsigned flags;
};

/**
 * Submits a batch; see the contract above. For results other than PENDING, `*detail` (may be
 * nullptr) receives a static reason or nullptr.
 */
static inline otlp_sink_result otlp_sink_submit(otlp_sink *sink, const otlp_batch *batch,
                                                const otlp_sink_completion *done,
                                                const char **detail) {
  const char *ignored = nullptr;
  return sink->ops->submit(sink, batch, done, detail != nullptr ? detail : &ignored);
}

static inline void otlp_sink_quiesce(otlp_sink *sink) {
  if (sink != nullptr && sink->ops->quiesce != nullptr)
    sink->ops->quiesce(sink);
}

static inline void otlp_sink_shutdown(otlp_sink *sink) {
  if (sink != nullptr && sink->ops->shutdown != nullptr)
    sink->ops->shutdown(sink);
}

static inline void otlp_sink_destroy(otlp_sink *sink) {
  if (sink != nullptr)
    sink->ops->destroy(sink);
}

/* ---- null: counts and drops ------------------------------------------------------------- */

/** A sink that accepts everything and keeps nothing (for measuring the receiver itself). */
otlp_sink *otlp_null_sink_new(void);

/* ---- memory: keeps batches in a bounded in-process store --------------------------------- */

typedef enum otlp_memory_full_policy {
  /** A batch that does not fit is refused (RETRYABLE): stored data is never dropped. */
  OTLP_MEMORY_REJECT = 0,
  /** The oldest batches are dropped to make room (counted as evicted). */
  OTLP_MEMORY_EVICT,
} otlp_memory_full_policy;

/**
 * A sink that keeps copies of the payloads, at most `max_bytes` of them and `max_batches` batches
 * (0 = no count limit). A batch larger than `max_bytes` is FAILED: it can never fit.
 */
otlp_sink *otlp_memory_sink_new(size_t max_bytes, size_t max_batches,
                                otlp_memory_full_policy policy);

/**
 * Copies the stored payloads, concatenated, into a new grpc_malloc buffer. Concatenated
 * ExportMetricsServiceRequest encodings decode as one request holding every batch's
 * resource_metrics in order. Fails (returns false) when they exceed `max_len` bytes or memory is
 * short. `*batches` (may be nullptr) receives the number of batches.
 */
bool otlp_memory_sink_dump(otlp_sink *sink, size_t max_len, uint8_t **out, size_t *out_len,
                           size_t *batches);

/** Drops every stored batch. */
void otlp_memory_sink_clear(otlp_sink *sink);

/* ---- fault injection: wraps another sink -------------------------------------------------- */

typedef struct otlp_fault_config {
  /** Every Nth submit (1-based count) fails RETRYABLE; 0 = never. */
  uint32_t retryable_every;
  /** Every Nth submit fails FAILED; 0 = never. Checked after retryable_every. */
  uint32_t fail_every;
  /** Start with the gate closed: batches are held (copied, PENDING) until the gate opens. */
  bool gate_closed;
  /** Most batches held at once by a closed gate; more are refused RETRYABLE. */
  size_t gate_capacity;
  /** A closed gate blocks the submitting thread instead of deferring the batch (a slow sink).
   * Only for worker threads: blocking an event loop stalls it. */
  bool gate_blocks;
} otlp_fault_config;

/**
 * Wraps `inner` (taking ownership) with injected failures and a gate. Held batches' payload
 * copies are reserved from `budget` (may be nullptr), like any memory kept for a request; a copy
 * that does not fit is refused RETRYABLE.
 */
otlp_sink *otlp_fault_sink_new(otlp_sink *inner, const otlp_fault_config *config,
                               otlp_budget *budget);

/**
 * Opens or closes the gate. Opening it forwards the held batches, in order, to the inner sink and
 * completes them. Safe from any thread.
 */
void otlp_fault_sink_set_gate(otlp_sink *sink, bool open);

/** Batches the gate holds (or blocks) now. */
size_t otlp_fault_sink_held(otlp_sink *sink);

/** Waits until the gate holds (or blocks) at least `count` batches. For tests; any thread. */
void otlp_fault_sink_wait_held(otlp_sink *sink, size_t count);

/** The sink wrapped by a fault sink (or `sink` itself when it is not one). */
otlp_sink *otlp_sink_unwrap(otlp_sink *sink);
