#pragma once

/* otlp_wal.h - the write-ahead-log sink (built with -DOTLP_METRICS_WAL=ON)
 *
 * Accepted batches are appended to segment files (format: otlp_wal_format.h) by one writer thread,
 * never by an event loop. submit() copies the payload into a bounded queue and returns PENDING;
 * the writer takes everything queued, writes it, syncs once (group commit) and only then
 * completes the batches as ACCEPTED, so the receiver acknowledges a request only once its data is
 * on disk (with OTLP_WAL_SYNC_FSYNC) or in the OS page cache (OTLP_WAL_SYNC_NONE).
 *
 * Refusals are retryable: queue full, log full (max_bytes), memory budget, shutting down, and I/O
 * errors (a failed write is truncated away; a failed sync fails the whole group). A batch whose
 * client already cancelled is not written.
 *
 * At startup the directory is recovered: segments are verified, a torn record at the end of the
 * last segment (a crash during a write) is truncated, and numbering continues after the last
 * valid record in a new segment. Segments rotate between write groups once they reach
 * segment_bytes, so one may exceed it by up to one group (at most queue_bytes). The log is never
 * deleted or compacted by the receiver: consumers own its retention (see otlp_wal_dump).
 */

#include <stddef.h>
#include <stdint.h>

#include "otlp_budget.h"
#include "otlp_sink.h"

typedef enum otlp_wal_sync {
  /** Acknowledge after write(): survives a process crash, not a power loss. */
  OTLP_WAL_SYNC_NONE = 0,
  /** Acknowledge after fdatasync() (FlushFileBuffers on Windows): survives a power loss. */
  OTLP_WAL_SYNC_FSYNC = 1,
} otlp_wal_sync;

typedef struct otlp_wal_config {
  /** Log directory; created (one level) when missing. */
  const char *dir;
  otlp_wal_sync sync;
  /** Rotate to a new segment once the current one reaches this size. */
  size_t segment_bytes;
  /** Bound on the total log size; batches that do not fit are refused (retryable). */
  size_t max_bytes;
  /** Bound on payload bytes waiting for the writer. */
  size_t queue_bytes;
  /** Test hook: every Nth write fails after writing part of its record (0 = never). */
  uint32_t fail_writes_every;
} otlp_wal_config;

/**
 * Opens (recovering) or creates the log and starts the writer. Queued payload copies are reserved
 * from `budget` (may be nullptr). Returns nullptr when the directory cannot be used, after printing
 * why to stderr.
 */
otlp_sink *otlp_wal_sink_new(const otlp_wal_config *config, otlp_budget *budget);
