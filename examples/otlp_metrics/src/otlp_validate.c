#include "otlp_validate.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

/* NO_RECORDED_VALUE: the point marks a gap (e.g. a stale series); its value fields are ignored */
#define FLAG_NO_RECORDED_VALUE \
  ((uint32_t)                  \
       OPENTELEMETRY__PROTO__METRICS__V1__DATA_POINT_FLAGS__DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK)

/* the scale range of the exponential histogram data model */
enum { EXP_SCALE_MIN = -10, EXP_SCALE_MAX = 20 };

otlp_metric_kind otlp_metric_kind_of(const otlp_pb_metric *metric) {
  switch (metric->data_case) {
  case OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE:
    return metric->gauge != nullptr ? OTLP_KIND_GAUGE : OTLP_KIND_NONE;
  case OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM:
    return metric->sum != nullptr ? OTLP_KIND_SUM : OTLP_KIND_NONE;
  case OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_HISTOGRAM:
    return metric->histogram != nullptr ? OTLP_KIND_HISTOGRAM : OTLP_KIND_NONE;
  case OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_EXPONENTIAL_HISTOGRAM:
    return metric->exponential_histogram != nullptr ? OTLP_KIND_EXP_HISTOGRAM : OTLP_KIND_NONE;
  case OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUMMARY:
    return metric->summary != nullptr ? OTLP_KIND_SUMMARY : OTLP_KIND_NONE;
  default:
    return OTLP_KIND_NONE;
  }
}

const char *otlp_metric_kind_name(otlp_metric_kind kind) {
  static const char *const names[OTLP_KIND_COUNT] = {
      [OTLP_KIND_NONE] = "none",
      [OTLP_KIND_GAUGE] = "gauge",
      [OTLP_KIND_SUM] = "sum",
      [OTLP_KIND_HISTOGRAM] = "histogram",
      [OTLP_KIND_EXP_HISTOGRAM] = "exponential_histogram",
      [OTLP_KIND_SUMMARY] = "summary",
  };
  return kind < OTLP_KIND_COUNT ? names[kind] : "unknown";
}

static const char *check_number(const otlp_pb_number_point *p);
static const char *check_histogram(const otlp_pb_histogram_point *p);
static const char *check_exp_histogram(const otlp_pb_exp_histogram_point *p);
static const char *check_summary(const otlp_pb_summary_point *p);

/* Expands `X(points, n, check)` for the typed point array of `metric` (of kind `kind`). */
#define WITH_POINTS(metric, kind, X)                                                          \
  switch (kind) {                                                                             \
  case OTLP_KIND_GAUGE:                                                                       \
    X((metric)->gauge->data_points, (metric)->gauge->n_data_points, check_number);            \
    break;                                                                                    \
  case OTLP_KIND_SUM:                                                                         \
    X((metric)->sum->data_points, (metric)->sum->n_data_points, check_number);                \
    break;                                                                                    \
  case OTLP_KIND_HISTOGRAM:                                                                   \
    X((metric)->histogram->data_points, (metric)->histogram->n_data_points, check_histogram); \
    break;                                                                                    \
  case OTLP_KIND_EXP_HISTOGRAM:                                                               \
    X((metric)->exponential_histogram->data_points,                                           \
      (metric)->exponential_histogram->n_data_points, check_exp_histogram);                   \
    break;                                                                                    \
  case OTLP_KIND_SUMMARY:                                                                     \
    X((metric)->summary->data_points, (metric)->summary->n_data_points, check_summary);       \
    break;                                                                                    \
  default:                                                                                    \
    break;                                                                                    \
  }

static size_t metric_point_count(const otlp_pb_metric *metric, otlp_metric_kind kind) {
  size_t count = 0;
#define COUNT(points, n, check) count = (n)
  WITH_POINTS(metric, kind, COUNT)
#undef COUNT
  return count;
}

static bool temporality_valid(int temporality) {
  return temporality ==
             OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_DELTA ||
         temporality ==
             OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_CUMULATIVE;
}

/* A rule that applies to every point of the metric, or nullptr. */
static const char *check_metric(const otlp_pb_metric *metric, otlp_metric_kind kind) {
  if (metric->name == nullptr || metric->name[0] == '\0')
    return "metric name is empty";
  switch (kind) {
  case OTLP_KIND_SUM:
    if (!temporality_valid((int)metric->sum->aggregation_temporality))
      return "sum aggregation temporality is unspecified";
    break;
  case OTLP_KIND_HISTOGRAM:
    if (!temporality_valid((int)metric->histogram->aggregation_temporality))
      return "histogram aggregation temporality is unspecified";
    break;
  case OTLP_KIND_EXP_HISTOGRAM:
    if (!temporality_valid((int)metric->exponential_histogram->aggregation_temporality))
      return "exponential histogram aggregation temporality is unspecified";
    break;
  default:
    break;
  }
  return nullptr;
}

/* a + b into *sum; false on overflow */
static bool add_u64(uint64_t a, uint64_t b, uint64_t *sum) {
  if (a > UINT64_MAX - b)
    return false;
  *sum = a + b;
  return true;
}

static const char *check_number(const otlp_pb_number_point *p) {
  if (p->time_unix_nano == 0)
    return "data point time_unix_nano is missing";
  if (p->value_case == OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE__NOT_SET &&
      (p->flags & FLAG_NO_RECORDED_VALUE) == 0)
    return "number data point has no value";
  return nullptr;
}

static const char *check_histogram(const otlp_pb_histogram_point *p) {
  if (p->time_unix_nano == 0)
    return "data point time_unix_nano is missing";
  for (size_t i = 0; i < p->n_explicit_bounds; i++) {
    if (isnan(p->explicit_bounds[i]) ||
        (i > 0 && !(p->explicit_bounds[i] > p->explicit_bounds[i - 1])))
      return "histogram explicit_bounds are not strictly increasing";
  }
  if (p->n_bucket_counts == 0)
    return nullptr;
  if (p->n_bucket_counts != p->n_explicit_bounds + 1)
    return "histogram bucket_counts must have one more entry than explicit_bounds";
  if ((p->flags & FLAG_NO_RECORDED_VALUE) != 0)
    return nullptr;
  uint64_t total = 0;
  for (size_t i = 0; i < p->n_bucket_counts; i++) {
    if (!add_u64(total, p->bucket_counts[i], &total))
      return "histogram bucket_counts do not add up to count";
  }
  if (total != p->count)
    return "histogram bucket_counts do not add up to count";
  return nullptr;
}

static bool add_buckets(const otlp_pb_exp_buckets *buckets, uint64_t *total) {
  if (buckets == nullptr)
    return true;
  for (size_t i = 0; i < buckets->n_bucket_counts; i++) {
    if (!add_u64(*total, buckets->bucket_counts[i], total))
      return false;
  }
  return true;
}

static const char *check_exp_histogram(const otlp_pb_exp_histogram_point *p) {
  if (p->time_unix_nano == 0)
    return "data point time_unix_nano is missing";
  if (p->scale < EXP_SCALE_MIN || p->scale > EXP_SCALE_MAX)
    return "exponential histogram scale is outside [-10, 20]";
  if (isnan(p->zero_threshold) || p->zero_threshold < 0)
    return "exponential histogram zero_threshold is negative";
  if ((p->flags & FLAG_NO_RECORDED_VALUE) != 0)
    return nullptr;
  uint64_t total = p->zero_count;
  if (!add_buckets(p->positive, &total) || !add_buckets(p->negative, &total) || total != p->count)
    return "exponential histogram bucket counts do not add up to count";
  return nullptr;
}

static const char *check_summary(const otlp_pb_summary_point *p) {
  if (p->time_unix_nano == 0)
    return "data point time_unix_nano is missing";
  for (size_t i = 0; i < p->n_quantile_values; i++) {
    const otlp_pb_quantile *q = p->quantile_values[i];
    if (q == nullptr || isnan(q->quantile) || q->quantile < 0 || q->quantile > 1)
      return "summary quantile is outside [0, 1]";
  }
  return nullptr;
}

static void note_reason(otlp_counts *counts, const otlp_pb_metric *metric, const char *reason) {
  if (counts->reason[0] != '\0')
    return;
  const char *name = metric->name != nullptr ? metric->name : "";
  /* the metric name is client data: bound it so the message stays short */
  (void)snprintf(counts->reason, sizeof(counts->reason), "%s (metric \"%.64s\")", reason, name);
}

void otlp_validate_request(const otlp_pb_request *request, otlp_counts *counts) {
  memset(counts, 0, sizeof(*counts));
  counts->resources = request->n_resource_metrics;
  for (size_t r = 0; r < request->n_resource_metrics; r++) {
    const otlp_pb_resource_metrics *rm = request->resource_metrics[r];
    counts->scopes += rm->n_scope_metrics;
    for (size_t s = 0; s < rm->n_scope_metrics; s++) {
      const otlp_pb_scope_metrics *sm = rm->scope_metrics[s];
      counts->metrics += sm->n_metrics;
      for (size_t m = 0; m < sm->n_metrics; m++) {
        const otlp_pb_metric *metric = sm->metrics[m];
        otlp_metric_kind kind = otlp_metric_kind_of(metric);
        size_t n = metric_point_count(metric, kind);
        counts->points += n;
        counts->points_by_kind[kind] += n;
        if (n == 0)
          continue;
        const char *reason = check_metric(metric, kind);
        if (reason != nullptr) {
          counts->rejected += n;
          note_reason(counts, metric, reason);
          continue;
        }
#define VALIDATE(points, count, check)     \
  for (size_t i = 0; i < (count); i++) {   \
    reason = check((points)[i]);           \
    if (reason != nullptr) {               \
      counts->rejected++;                  \
      note_reason(counts, metric, reason); \
    }                                      \
  }
        WITH_POINTS(metric, kind, VALIDATE)
#undef VALIDATE
      }
    }
  }
}

/* Drops the rejected points of `metric`. Returns true when it had points and has none left. */
static bool filter_metric(otlp_pb_metric *metric) {
  otlp_metric_kind kind = otlp_metric_kind_of(metric);
  if (metric_point_count(metric, kind) == 0)
    return false;
  bool drop_all = check_metric(metric, kind) != nullptr;
  size_t kept = 0;
#define COMPACT(points, count, check)               \
  for (size_t i = 0; i < (count); i++) {            \
    if (!drop_all && check((points)[i]) == nullptr) \
      (points)[kept++] = (points)[i];               \
  }                                                 \
  (count) = kept
  WITH_POINTS(metric, kind, COMPACT)
#undef COMPACT
  return kept == 0;
}

static bool filter_scope(otlp_pb_scope_metrics *sm) {
  if (sm->n_metrics == 0)
    return false;
  size_t kept = 0;
  for (size_t m = 0; m < sm->n_metrics; m++) {
    if (!filter_metric(sm->metrics[m]))
      sm->metrics[kept++] = sm->metrics[m];
  }
  sm->n_metrics = kept;
  return kept == 0;
}

static bool filter_resource(otlp_pb_resource_metrics *rm) {
  if (rm->n_scope_metrics == 0)
    return false;
  size_t kept = 0;
  for (size_t s = 0; s < rm->n_scope_metrics; s++) {
    if (!filter_scope(rm->scope_metrics[s]))
      rm->scope_metrics[kept++] = rm->scope_metrics[s];
  }
  rm->n_scope_metrics = kept;
  return kept == 0;
}

void otlp_filter_request(otlp_pb_request *request) {
  size_t kept = 0;
  for (size_t r = 0; r < request->n_resource_metrics; r++) {
    if (!filter_resource(request->resource_metrics[r]))
      request->resource_metrics[kept++] = request->resource_metrics[r];
  }
  request->n_resource_metrics = kept;
}
