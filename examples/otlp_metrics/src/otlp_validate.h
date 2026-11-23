#pragma once

/* otlp_validate.h - data point validation and counting for ExportMetricsServiceRequest
 *
 * The receiver accepts every data point except those that break a rule of the OTLP metrics data
 * model (see README.md, "Validation rules"); rejected points are reported to the client as a
 * partial success with their exact number. The rules look at one metric and one point at a time,
 * so a point is either accepted or rejected as a whole, and each rejected point is counted once
 * however many rules it breaks.
 *
 * The functions are pure (no allocation, no global state) and safe on any thread.
 */

#include <stddef.h>
#include <stdint.h>

#include "otlp_proto.h"

typedef enum otlp_metric_kind {
  OTLP_KIND_NONE = 0, /* the data oneof is not set: a metric without data points */
  OTLP_KIND_GAUGE,
  OTLP_KIND_SUM,
  OTLP_KIND_HISTOGRAM,
  OTLP_KIND_EXP_HISTOGRAM,
  OTLP_KIND_SUMMARY,
  OTLP_KIND_COUNT,
} otlp_metric_kind;

enum { OTLP_REASON_MAX = 192 };

typedef struct otlp_counts {
  uint64_t resources;
  uint64_t scopes;
  uint64_t metrics;
  /** Data points in the request, accepted or not. */
  uint64_t points;
  /** Data points that break a rule. */
  uint64_t rejected;
  /** Data points per metric kind. */
  uint64_t points_by_kind[OTLP_KIND_COUNT];
  /** Why the first rejected point was rejected, NUL-terminated; empty when none was. */
  char reason[OTLP_REASON_MAX];
} otlp_counts;

/** Counts the request and validates every data point into `*counts` (overwritten). */
void otlp_validate_request(const otlp_pb_request *request, otlp_counts *counts);

/**
 * Removes the rejected data points from `request`, in place, by compacting its pointer arrays.
 * Metrics whose points were all rejected are removed too, and so are scopes and resources left
 * empty by that; envelopes that were empty to begin with are kept as they were. Nothing is
 * allocated or freed (the request lives in an arena).
 */
void otlp_filter_request(otlp_pb_request *request);

/** The kind of `metric`. */
otlp_metric_kind otlp_metric_kind_of(const otlp_pb_metric *metric);

/** A lowercase name for `kind` ("gauge", "sum", ...). */
const char *otlp_metric_kind_name(otlp_metric_kind kind);
