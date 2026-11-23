/* otlp_gen - deterministic ExportMetricsServiceRequest generator (benchmarks, fuzz seeds)
 *
 *   otlp_gen --out FILE [--resources N] [--scopes N] [--metrics N] [--points N] [--attrs N]
 *            [--kinds gauge,sum,histogram,exp,summary] [--buckets N] [--seed N]
 *            [--invalid-every N]
 *
 * Writes one serialized request built with the protobuf-c bindings of the official protos:
 * `resources` x `scopes` x `metrics` metrics (kinds assigned round-robin from --kinds), each with
 * `points` data points carrying `attrs` attributes. Values come from a fixed-seed generator, so
 * the same arguments always give the same bytes. --invalid-every N makes every Nth data point
 * break a validation rule (for partial-success workloads). Prints a JSON summary (bytes, points,
 * expected rejected points) on stdout.
 */
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "otlp_arena.h"
#include "otlp_proto.h"
#include "otlp_strbuf.h"
#include "otlp_validate.h"

static otlp_arena g_arena;

static void *alloc0(size_t size) {
  void *p = otlp_arena_alloc(&g_arena, size);
  if (p == nullptr) {
    fputs("otlp_gen: out of memory\n", stderr);
    exit(1);
  }
  memset(p, 0, size);
  return p;
}

OTLP_PRINTF(1, 2) static char *str(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  char buf[128];
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  char *s = alloc0((size_t)n + 1);
  memcpy(s, buf, (size_t)n + 1);
  return s;
}

static uint64_t g_rng = 0x9E3779B97F4A7C15ull;

static uint64_t rnd(void) { /* xorshift64* */
  g_rng ^= g_rng >> 12;
  g_rng ^= g_rng << 25;
  g_rng ^= g_rng >> 27;
  return g_rng * 2685821657736338717ull;
}

typedef Opentelemetry__Proto__Common__V1__KeyValue kv_t;
typedef Opentelemetry__Proto__Common__V1__AnyValue any_t;

static kv_t **attributes(size_t n, const char *prefix, size_t *count) {
  kv_t **list = alloc0(n * sizeof(kv_t *) + 1);
  for (size_t i = 0; i < n; i++) {
    kv_t *kv = alloc0(sizeof(*kv));
    opentelemetry__proto__common__v1__key_value__init(kv);
    any_t *v = alloc0(sizeof(*v));
    opentelemetry__proto__common__v1__any_value__init(v);
    kv->key = str("%s.attr%zu", prefix, i);
    if (i % 3 == 2) {
      v->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_INT_VALUE;
      v->int_value = (int64_t)(rnd() % 1000);
    } else {
      v->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE;
      v->string_value = str("value-%" PRIu64, rnd() % 100);
    }
    kv->value = v;
    list[i] = kv;
  }
  *count = n;
  return list;
}

typedef struct shape {
  size_t resources, scopes, metrics, points, attrs, buckets;
  otlp_metric_kind kinds[8];
  size_t nb_kinds;
  size_t invalid_every;
} shape;

static size_t g_point_index;
static uint64_t g_expected_rejected;

/* true for the points to make invalid */
static bool spoil(const shape *s) {
  g_point_index++;
  if (s->invalid_every != 0 && g_point_index % s->invalid_every == 0) {
    g_expected_rejected++;
    return true;
  }
  return false;
}

#define T0 1700000000000000000ull

static otlp_pb_metric *metric(const shape *s, size_t index) {
  otlp_pb_metric *m = alloc0(sizeof(*m));
  opentelemetry__proto__metrics__v1__metric__init(m);
  otlp_metric_kind kind = s->kinds[index % s->nb_kinds];
  m->name = str("bench.%s.%zu", otlp_metric_kind_name(kind), index);
  m->unit = str("1");
  m->description = str("generated metric");
  size_t n = s->points;
  switch (kind) {
  case OTLP_KIND_GAUGE:
  case OTLP_KIND_SUM: {
    otlp_pb_number_point **pts = alloc0(n * sizeof(*pts) + 1);
    for (size_t i = 0; i < n; i++) {
      otlp_pb_number_point *p = alloc0(sizeof(*p));
      opentelemetry__proto__metrics__v1__number_data_point__init(p);
      p->attributes = attributes(s->attrs, "dp", &p->n_attributes);
      p->start_time_unix_nano = T0;
      p->time_unix_nano = spoil(s) ? 0 : T0 + i;
      p->value_case = OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE_AS_DOUBLE;
      p->as_double = (double)(rnd() % 100000) / 10.0;
      pts[i] = p;
    }
    if (kind == OTLP_KIND_GAUGE) {
      Opentelemetry__Proto__Metrics__V1__Gauge *g = alloc0(sizeof(*g));
      opentelemetry__proto__metrics__v1__gauge__init(g);
      g->data_points = pts;
      g->n_data_points = n;
      m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE;
      m->gauge = g;
    } else {
      Opentelemetry__Proto__Metrics__V1__Sum *sum = alloc0(sizeof(*sum));
      opentelemetry__proto__metrics__v1__sum__init(sum);
      sum->data_points = pts;
      sum->n_data_points = n;
      sum->is_monotonic = true;
      sum->aggregation_temporality =
          OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_CUMULATIVE;
      m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM;
      m->sum = sum;
    }
    break;
  }
  case OTLP_KIND_HISTOGRAM: {
    Opentelemetry__Proto__Metrics__V1__Histogram *h = alloc0(sizeof(*h));
    opentelemetry__proto__metrics__v1__histogram__init(h);
    h->aggregation_temporality =
        OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_DELTA;
    h->data_points = alloc0(n * sizeof(*h->data_points) + 1);
    h->n_data_points = n;
    for (size_t i = 0; i < n; i++) {
      otlp_pb_histogram_point *p = alloc0(sizeof(*p));
      opentelemetry__proto__metrics__v1__histogram_data_point__init(p);
      p->attributes = attributes(s->attrs, "dp", &p->n_attributes);
      p->start_time_unix_nano = T0;
      p->time_unix_nano = T0 + i;
      p->n_explicit_bounds = s->buckets;
      p->explicit_bounds = alloc0(s->buckets * sizeof(double) + 1);
      p->n_bucket_counts = s->buckets + 1;
      p->bucket_counts = alloc0((s->buckets + 1) * sizeof(uint64_t));
      uint64_t total = 0;
      for (size_t b = 0; b <= s->buckets; b++) {
        if (b < s->buckets)
          p->explicit_bounds[b] = (double)(1u << (b % 30));
        p->bucket_counts[b] = rnd() % 50;
        total += p->bucket_counts[b];
      }
      p->count = spoil(s) ? total + 1 : total;
      p->_sum_case = OPENTELEMETRY__PROTO__METRICS__V1__HISTOGRAM_DATA_POINT___SUM_SUM;
      p->sum = (double)(rnd() % 1000000);
      h->data_points[i] = p;
    }
    m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_HISTOGRAM;
    m->histogram = h;
    break;
  }
  case OTLP_KIND_EXP_HISTOGRAM: {
    Opentelemetry__Proto__Metrics__V1__ExponentialHistogram *h = alloc0(sizeof(*h));
    opentelemetry__proto__metrics__v1__exponential_histogram__init(h);
    h->aggregation_temporality =
        OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_DELTA;
    h->data_points = alloc0(n * sizeof(*h->data_points) + 1);
    h->n_data_points = n;
    for (size_t i = 0; i < n; i++) {
      otlp_pb_exp_histogram_point *p = alloc0(sizeof(*p));
      opentelemetry__proto__metrics__v1__exponential_histogram_data_point__init(p);
      p->attributes = attributes(s->attrs, "dp", &p->n_attributes);
      p->start_time_unix_nano = T0;
      p->time_unix_nano = T0 + i;
      p->scale = spoil(s) ? 42 : 4;
      otlp_pb_exp_buckets *pos = alloc0(sizeof(*pos));
      opentelemetry__proto__metrics__v1__exponential_histogram_data_point__buckets__init(pos);
      pos->offset = 3;
      pos->n_bucket_counts = s->buckets;
      pos->bucket_counts = alloc0(s->buckets * sizeof(uint64_t) + 1);
      uint64_t total = p->zero_count = rnd() % 5;
      for (size_t b = 0; b < s->buckets; b++) {
        pos->bucket_counts[b] = rnd() % 50;
        total += pos->bucket_counts[b];
      }
      p->positive = pos;
      p->count = total;
      h->data_points[i] = p;
    }
    m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_EXPONENTIAL_HISTOGRAM;
    m->exponential_histogram = h;
    break;
  }
  case OTLP_KIND_SUMMARY: {
    Opentelemetry__Proto__Metrics__V1__Summary *sm = alloc0(sizeof(*sm));
    opentelemetry__proto__metrics__v1__summary__init(sm);
    sm->data_points = alloc0(n * sizeof(*sm->data_points) + 1);
    sm->n_data_points = n;
    for (size_t i = 0; i < n; i++) {
      otlp_pb_summary_point *p = alloc0(sizeof(*p));
      opentelemetry__proto__metrics__v1__summary_data_point__init(p);
      p->attributes = attributes(s->attrs, "dp", &p->n_attributes);
      p->time_unix_nano = T0 + i;
      p->count = rnd() % 1000;
      p->sum = (double)(rnd() % 100000);
      static const double qs[] = {0.5, 0.9, 0.99};
      p->n_quantile_values = 3;
      p->quantile_values = alloc0(3 * sizeof(*p->quantile_values));
      bool bad = spoil(s);
      for (size_t q = 0; q < 3; q++) {
        otlp_pb_quantile *v = alloc0(sizeof(*v));
        opentelemetry__proto__metrics__v1__summary_data_point__value_at_quantile__init(v);
        v->quantile = bad && q == 0 ? 1.5 : qs[q];
        v->value = (double)(rnd() % 1000);
        p->quantile_values[q] = v;
      }
      sm->data_points[i] = p;
    }
    m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUMMARY;
    m->summary = sm;
    break;
  }
  default:
    break;
  }
  return m;
}

static bool parse_kinds(shape *s, const char *text) {
  char buf[128];
  if (strlen(text) >= sizeof(buf))
    return false;
  strcpy(buf, text);
  s->nb_kinds = 0;
  for (char *k = strtok(buf, ","); k != nullptr && s->nb_kinds < 8; k = strtok(nullptr, ",")) {
    if (strcmp(k, "gauge") == 0)
      s->kinds[s->nb_kinds++] = OTLP_KIND_GAUGE;
    else if (strcmp(k, "sum") == 0)
      s->kinds[s->nb_kinds++] = OTLP_KIND_SUM;
    else if (strcmp(k, "histogram") == 0)
      s->kinds[s->nb_kinds++] = OTLP_KIND_HISTOGRAM;
    else if (strcmp(k, "exp") == 0)
      s->kinds[s->nb_kinds++] = OTLP_KIND_EXP_HISTOGRAM;
    else if (strcmp(k, "summary") == 0)
      s->kinds[s->nb_kinds++] = OTLP_KIND_SUMMARY;
    else
      return false;
  }
  return s->nb_kinds > 0;
}

int main(int argc, char **argv) {
  shape s = {.resources = 1, .scopes = 1, .metrics = 10, .points = 1, .attrs = 3, .buckets = 8};
  parse_kinds(&s, "gauge,sum,histogram,exp,summary");
  const char *out = nullptr;
  for (int i = 1; i + 1 < argc; i += 2) {
    const char *k = argv[i], *v = argv[i + 1];
    size_t n = strtoull(v, nullptr, 10);
    if (strcmp(k, "--out") == 0)
      out = v;
    else if (strcmp(k, "--resources") == 0)
      s.resources = n;
    else if (strcmp(k, "--scopes") == 0)
      s.scopes = n;
    else if (strcmp(k, "--metrics") == 0)
      s.metrics = n;
    else if (strcmp(k, "--points") == 0)
      s.points = n;
    else if (strcmp(k, "--attrs") == 0)
      s.attrs = n;
    else if (strcmp(k, "--buckets") == 0)
      s.buckets = n;
    else if (strcmp(k, "--seed") == 0)
      g_rng = n != 0 ? n : 1;
    else if (strcmp(k, "--invalid-every") == 0)
      s.invalid_every = n;
    else if (strcmp(k, "--kinds") == 0) {
      if (!parse_kinds(&s, v)) {
        fprintf(stderr, "otlp_gen: bad --kinds %s\n", v);
        return 2;
      }
    } else {
      fprintf(stderr, "otlp_gen: unknown option %s\n", k);
      return 2;
    }
  }
  if (out == nullptr) {
    fprintf(stderr,
            "usage: %s --out FILE [--resources N] [--scopes N] [--metrics N] "
            "[--points N] [--attrs N] [--kinds LIST] [--buckets N] [--seed N] "
            "[--invalid-every N]\n",
            argv[0]);
    return 2;
  }
  otlp_arena_init(&g_arena, nullptr, 0);
  otlp_arena_begin(&g_arena, SIZE_MAX, 1 << 20);

  otlp_pb_request *req = alloc0(sizeof(*req));
  opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__init(req);
  req->n_resource_metrics = s.resources;
  req->resource_metrics = alloc0(s.resources * sizeof(otlp_pb_resource_metrics *) + 1);
  size_t metric_index = 0;
  for (size_t r = 0; r < s.resources; r++) {
    otlp_pb_resource_metrics *rm = alloc0(sizeof(*rm));
    opentelemetry__proto__metrics__v1__resource_metrics__init(rm);
    Opentelemetry__Proto__Resource__V1__Resource *res = alloc0(sizeof(*res));
    opentelemetry__proto__resource__v1__resource__init(res);
    res->attributes = attributes(s.attrs + 2, "resource", &res->n_attributes);
    res->attributes[0]->key = str("service.name");
    res->attributes[0]->value->value_case =
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE;
    res->attributes[0]->value->string_value = str("bench-service-%zu", r);
    rm->resource = res;
    rm->n_scope_metrics = s.scopes;
    rm->scope_metrics = alloc0(s.scopes * sizeof(otlp_pb_scope_metrics *) + 1);
    for (size_t sc = 0; sc < s.scopes; sc++) {
      otlp_pb_scope_metrics *sm = alloc0(sizeof(*sm));
      opentelemetry__proto__metrics__v1__scope_metrics__init(sm);
      Opentelemetry__Proto__Common__V1__InstrumentationScope *scope = alloc0(sizeof(*scope));
      opentelemetry__proto__common__v1__instrumentation_scope__init(scope);
      scope->name = str("bench.scope.%zu", sc);
      scope->version = str("1.0.0");
      sm->scope = scope;
      sm->n_metrics = s.metrics;
      sm->metrics = alloc0(s.metrics * sizeof(otlp_pb_metric *) + 1);
      for (size_t m = 0; m < s.metrics; m++)
        sm->metrics[m] = metric(&s, metric_index++);
      rm->scope_metrics[sc] = sm;
    }
    req->resource_metrics[r] = rm;
  }

  size_t len = protobuf_c_message_get_packed_size(&req->base);
  uint8_t *buf = alloc0(len + 1);
  len = protobuf_c_message_pack(&req->base, buf);
  FILE *f = fopen(out, "wb");
  if (f == nullptr || fwrite(buf, 1, len, f) != len || fclose(f) != 0) {
    perror(out);
    return 1;
  }
  otlp_counts counts;
  otlp_validate_request(req, &counts);
  printf("{\"file\":\"%s\",\"bytes\":%zu,\"resources\":%zu,\"scopes\":%zu,\"metrics\":%" PRIu64
         ",\"points\":%" PRIu64 ",\"rejected\":%" PRIu64 ",\"expected_rejected\":%" PRIu64 "}\n",
         out, len, s.resources, s.scopes, counts.metrics, counts.points, counts.rejected,
         g_expected_rejected);
  otlp_arena_destroy(&g_arena);
  return counts.rejected == g_expected_rejected ? 0 : 3;
}
