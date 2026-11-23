#pragma once

/* otlp_budget.h - a process-wide memory reservation counter
 *
 * Every buffer the receiver keeps on behalf of a request (the received message while it waits in a
 * queue, decoded data, payload copies held by a sink) is reserved here first. A reservation either
 * fits under the limit entirely or fails, so the total stays bounded no matter how many requests
 * arrive at once; a failed reservation is reported to the client as a retryable UNAVAILABLE.
 *
 * Thread-safe: every function may be called from any thread.
 */

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct otlp_budget {
  /** Upper bound of reserved bytes; 0 means unlimited. Fixed after otlp_budget_init(). */
  size_t limit;
  /** Bytes reserved now. */
  atomic_size_t used;
  /** Highest value `used` reached. */
  atomic_size_t peak;
  /** Reservations refused because they would exceed `limit`. */
  atomic_uint_least64_t denied;
} otlp_budget;

/** Initializes `budget` with `limit` bytes (0 = unlimited). */
void otlp_budget_init(otlp_budget *budget, size_t limit);

/** Reserves `bytes`. Returns false, reserving nothing, when that would exceed the limit. */
bool otlp_budget_reserve(otlp_budget *budget, size_t bytes);

/** Releases `bytes` reserved earlier with otlp_budget_reserve(). */
void otlp_budget_release(otlp_budget *budget, size_t bytes);

/** Bytes reserved now. */
size_t otlp_budget_used(const otlp_budget *budget);
