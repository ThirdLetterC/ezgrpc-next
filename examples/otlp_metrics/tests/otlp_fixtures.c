#include "otlp_fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "grpc_mem.h"

#define T0 1700000000000000000ull

void fx_init(fx *f) {
  otlp_arena_init(&f->arena, nullptr, 0);
  otlp_arena_begin(&f->arena, SIZE_MAX, 1 << 16);
}

void fx_free(fx *f) {
  otlp_arena_destroy(&f->arena);
}

static void *fx_alloc(fx *f, size_t size) {
  void *p = otlp_arena_alloc(&f->arena, size);
  if (p == nullptr)
    abort();
  memset(p, 0, size);
  return p;
}

static char *fx_str(fx *f, const char *s) {
  size_t len = strlen(s);
  char *p = fx_alloc(f, len + 1);
  memcpy(p, s, len + 1);
  return p;
}

/* Appends `item` to a pointer array (reallocated in the arena). */
#define APPEND(f, array, count, item)                              \
  do {                                                             \
    void *grown = fx_alloc((f), sizeof(*(array)) * ((count) + 1)); \
    if ((count) > 0)                                               \
      memcpy(grown, (array), sizeof(*(array)) * (count));          \
    (array) = grown;                                               \
    (array)[(count)++] = (item);                                   \
  } while (0)

typedef Opentelemetry__Proto__Common__V1__KeyValue kv_t;
typedef Opentelemetry__Proto__Common__V1__AnyValue any_t;

static any_t *any_string(fx *f, const char *s) {
  any_t *v = fx_alloc(f, sizeof(*v));
  opentelemetry__proto__common__v1__any_value__init(v);
  v->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE;
  v->string_value = fx_str(f, s);
  return v;
}

static kv_t *kv(fx *f, const char *key, any_t *value) {
  kv_t *k = fx_alloc(f, sizeof(*k));
  opentelemetry__proto__common__v1__key_value__init(k);
  k->key = fx_str(f, key);
  k->value = value;
  return k;
}

static kv_t *kv_string(fx *f, const char *key, const char *value) {
  return kv(f, key, any_string(f, value));
}

/* {"tags": ["a", 7, {"x": true}]}: exercises ArrayValue and KeyValueList */
static kv_t *kv_nested(fx *f) {
  any_t *arr = fx_alloc(f, sizeof(*arr));
  opentelemetry__proto__common__v1__any_value__init(arr);
  Opentelemetry__Proto__Common__V1__ArrayValue *av = fx_alloc(f, sizeof(*av));
  opentelemetry__proto__common__v1__array_value__init(av);
  arr->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_ARRAY_VALUE;
  arr->array_value = av;
  APPEND(f, av->values, av->n_values, any_string(f, "a"));
  any_t *i = fx_alloc(f, sizeof(*i));
  opentelemetry__proto__common__v1__any_value__init(i);
  i->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_INT_VALUE;
  i->int_value = 7;
  APPEND(f, av->values, av->n_values, i);
  any_t *b = fx_alloc(f, sizeof(*b));
  opentelemetry__proto__common__v1__any_value__init(b);
  b->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BOOL_VALUE;
  b->bool_value = true;
  any_t *map = fx_alloc(f, sizeof(*map));
  opentelemetry__proto__common__v1__any_value__init(map);
  Opentelemetry__Proto__Common__V1__KeyValueList *kvl = fx_alloc(f, sizeof(*kvl));
  opentelemetry__proto__common__v1__key_value_list__init(kvl);
  map->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_KVLIST_VALUE;
  map->kvlist_value = kvl;
  APPEND(f, kvl->values, kvl->n_values, kv(f, "x", b));
  APPEND(f, av->values, av->n_values, map);
  return kv(f, "tags", arr);
}

otlp_pb_request *fx_request(fx *f) {
  otlp_pb_request *r = fx_alloc(f, sizeof(*r));
  opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__init(r);
  return r;
}

otlp_pb_resource_metrics *fx_resource(fx *f, otlp_pb_request *req, const char *service_name) {
  otlp_pb_resource_metrics *rm = fx_alloc(f, sizeof(*rm));
  opentelemetry__proto__metrics__v1__resource_metrics__init(rm);
  Opentelemetry__Proto__Resource__V1__Resource *res = fx_alloc(f, sizeof(*res));
  opentelemetry__proto__resource__v1__resource__init(res);
  APPEND(f, res->attributes, res->n_attributes, kv_string(f, "service.name", service_name));
  rm->resource = res;
  rm->schema_url = fx_str(f, "https://opentelemetry.io/schemas/1.26.0");
  APPEND(f, req->resource_metrics, req->n_resource_metrics, rm);
  return rm;
}

otlp_pb_scope_metrics *fx_scope(fx *f, otlp_pb_resource_metrics *rm, const char *name) {
  otlp_pb_scope_metrics *sm = fx_alloc(f, sizeof(*sm));
  opentelemetry__proto__metrics__v1__scope_metrics__init(sm);
  Opentelemetry__Proto__Common__V1__InstrumentationScope *scope = fx_alloc(f, sizeof(*scope));
  opentelemetry__proto__common__v1__instrumentation_scope__init(scope);
  scope->name = fx_str(f, name);
  scope->version = fx_str(f, "1.0.0");
  APPEND(f, scope->attributes, scope->n_attributes, kv_string(f, "scope.attr", "s"));
  sm->scope = scope;
  APPEND(f, rm->scope_metrics, rm->n_scope_metrics, sm);
  return sm;
}

static otlp_pb_metric *new_metric(fx *f, otlp_pb_scope_metrics *sm, const char *name) {
  otlp_pb_metric *m = fx_alloc(f, sizeof(*m));
  opentelemetry__proto__metrics__v1__metric__init(m);
  m->name = fx_str(f, name);
  m->description = fx_str(f, "a test metric");
  m->unit = fx_str(f, "1");
  APPEND(f, m->metadata, m->n_metadata, kv_string(f, "meta.key", "meta.value"));
  APPEND(f, sm->metrics, sm->n_metrics, m);
  return m;
}

static Opentelemetry__Proto__Metrics__V1__Exemplar *exemplar(fx *f) {
  Opentelemetry__Proto__Metrics__V1__Exemplar *e = fx_alloc(f, sizeof(*e));
  opentelemetry__proto__metrics__v1__exemplar__init(e);
  e->time_unix_nano = T0;
  e->value_case = OPENTELEMETRY__PROTO__METRICS__V1__EXEMPLAR__VALUE_AS_DOUBLE;
  e->as_double = 1.5;
  static uint8_t trace_id[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  static uint8_t span_id[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  e->trace_id = (ProtobufCBinaryData){sizeof(trace_id), trace_id};
  e->span_id = (ProtobufCBinaryData){sizeof(span_id), span_id};
  APPEND(f, e->filtered_attributes, e->n_filtered_attributes, kv_string(f, "ex", "1"));
  return e;
}

static otlp_pb_number_point *number_point(fx *f, size_t i) {
  otlp_pb_number_point *p = fx_alloc(f, sizeof(*p));
  opentelemetry__proto__metrics__v1__number_data_point__init(p);
  p->start_time_unix_nano = T0;
  p->time_unix_nano = T0 + 1000 + i;
  if (i % 2 == 0) {
    p->value_case = OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE_AS_DOUBLE;
    p->as_double = 0.5 + (double)i;
  } else {
    p->value_case = OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE_AS_INT;
    p->as_int = (int64_t)i;
  }
  char host[32];
  snprintf(host, sizeof(host), "host-%zu", i);
  APPEND(f, p->attributes, p->n_attributes, kv_string(f, "host", host));
  APPEND(f, p->exemplars, p->n_exemplars, exemplar(f));
  return p;
}

otlp_pb_metric *fx_gauge(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points) {
  otlp_pb_metric *m = new_metric(f, sm, name);
  Opentelemetry__Proto__Metrics__V1__Gauge *g = fx_alloc(f, sizeof(*g));
  opentelemetry__proto__metrics__v1__gauge__init(g);
  for (size_t i = 0; i < points; i++)
    APPEND(f, g->data_points, g->n_data_points, number_point(f, i));
  m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE;
  m->gauge = g;
  return m;
}

otlp_pb_metric *fx_sum(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points) {
  otlp_pb_metric *m = new_metric(f, sm, name);
  Opentelemetry__Proto__Metrics__V1__Sum *s = fx_alloc(f, sizeof(*s));
  opentelemetry__proto__metrics__v1__sum__init(s);
  s->aggregation_temporality =
      OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_CUMULATIVE;
  s->is_monotonic = true;
  for (size_t i = 0; i < points; i++)
    APPEND(f, s->data_points, s->n_data_points, number_point(f, i));
  m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM;
  m->sum = s;
  return m;
}

otlp_pb_metric *fx_histogram(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points) {
  otlp_pb_metric *m = new_metric(f, sm, name);
  Opentelemetry__Proto__Metrics__V1__Histogram *h = fx_alloc(f, sizeof(*h));
  opentelemetry__proto__metrics__v1__histogram__init(h);
  h->aggregation_temporality =
      OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_DELTA;
  for (size_t i = 0; i < points; i++) {
    otlp_pb_histogram_point *p = fx_alloc(f, sizeof(*p));
    opentelemetry__proto__metrics__v1__histogram_data_point__init(p);
    p->start_time_unix_nano = T0;
    p->time_unix_nano = T0 + 1000 + i;
    p->n_explicit_bounds = 3;
    p->explicit_bounds = fx_alloc(f, 3 * sizeof(double));
    p->explicit_bounds[0] = 1;
    p->explicit_bounds[1] = 5;
    p->explicit_bounds[2] = 10;
    p->n_bucket_counts = 4;
    p->bucket_counts = fx_alloc(f, 4 * sizeof(uint64_t));
    p->bucket_counts[0] = 1;
    p->bucket_counts[1] = 2;
    p->bucket_counts[2] = 3;
    p->bucket_counts[3] = i;
    p->count = 6 + i;
    /* sum is optional: present, and zero, to check presence survives */
    p->_sum_case = OPENTELEMETRY__PROTO__METRICS__V1__HISTOGRAM_DATA_POINT___SUM_SUM;
    p->sum = 0.0;
    p->_min_case = OPENTELEMETRY__PROTO__METRICS__V1__HISTOGRAM_DATA_POINT___MIN_MIN;
    p->min = 0.25;
    APPEND(f, p->attributes, p->n_attributes, kv_nested(f));
    APPEND(f, p->exemplars, p->n_exemplars, exemplar(f));
    APPEND(f, h->data_points, h->n_data_points, p);
  }
  m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_HISTOGRAM;
  m->histogram = h;
  return m;
}

otlp_pb_metric *fx_exp_histogram(fx *f, otlp_pb_scope_metrics *sm, const char *name,
                                 size_t points) {
  otlp_pb_metric *m = new_metric(f, sm, name);
  Opentelemetry__Proto__Metrics__V1__ExponentialHistogram *h = fx_alloc(f, sizeof(*h));
  opentelemetry__proto__metrics__v1__exponential_histogram__init(h);
  h->aggregation_temporality =
      OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_CUMULATIVE;
  for (size_t i = 0; i < points; i++) {
    otlp_pb_exp_histogram_point *p = fx_alloc(f, sizeof(*p));
    opentelemetry__proto__metrics__v1__exponential_histogram_data_point__init(p);
    p->start_time_unix_nano = T0;
    p->time_unix_nano = T0 + 1000 + i;
    p->scale = 3;
    p->zero_count = 1;
    p->zero_threshold = 0.001;
    otlp_pb_exp_buckets *pos = fx_alloc(f, sizeof(*pos));
    opentelemetry__proto__metrics__v1__exponential_histogram_data_point__buckets__init(pos);
    pos->offset = -2;
    pos->n_bucket_counts = 3;
    pos->bucket_counts = fx_alloc(f, 3 * sizeof(uint64_t));
    pos->bucket_counts[0] = 4;
    pos->bucket_counts[1] = 0;
    pos->bucket_counts[2] = i;
    otlp_pb_exp_buckets *neg = fx_alloc(f, sizeof(*neg));
    opentelemetry__proto__metrics__v1__exponential_histogram_data_point__buckets__init(neg);
    neg->offset = 1;
    neg->n_bucket_counts = 1;
    neg->bucket_counts = fx_alloc(f, sizeof(uint64_t));
    neg->bucket_counts[0] = 2;
    p->positive = pos;
    p->negative = neg;
    p->count = 1 + 4 + i + 2;
    p->_sum_case = OPENTELEMETRY__PROTO__METRICS__V1__EXPONENTIAL_HISTOGRAM_DATA_POINT___SUM_SUM;
    p->sum = 12.5;
    p->_max_case = OPENTELEMETRY__PROTO__METRICS__V1__EXPONENTIAL_HISTOGRAM_DATA_POINT___MAX_MAX;
    p->max = 9;
    APPEND(f, p->attributes, p->n_attributes, kv_string(f, "route", "/"));
    APPEND(f, h->data_points, h->n_data_points, p);
  }
  m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_EXPONENTIAL_HISTOGRAM;
  m->exponential_histogram = h;
  return m;
}

otlp_pb_metric *fx_summary(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points) {
  otlp_pb_metric *m = new_metric(f, sm, name);
  Opentelemetry__Proto__Metrics__V1__Summary *s = fx_alloc(f, sizeof(*s));
  opentelemetry__proto__metrics__v1__summary__init(s);
  for (size_t i = 0; i < points; i++) {
    otlp_pb_summary_point *p = fx_alloc(f, sizeof(*p));
    opentelemetry__proto__metrics__v1__summary_data_point__init(p);
    p->start_time_unix_nano = T0;
    p->time_unix_nano = T0 + 1000 + i;
    p->count = 10 + i;
    p->sum = 100.0 + (double)i;
    static const double qs[] = {0.0, 0.5, 0.99, 1.0};
    for (size_t q = 0; q < 4; q++) {
      otlp_pb_quantile *v = fx_alloc(f, sizeof(*v));
      opentelemetry__proto__metrics__v1__summary_data_point__value_at_quantile__init(v);
      v->quantile = qs[q];
      v->value = (double)(q + 1);
      APPEND(f, p->quantile_values, p->n_quantile_values, v);
    }
    APPEND(f, s->data_points, s->n_data_points, p);
  }
  m->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUMMARY;
  m->summary = s;
  return m;
}

otlp_pb_metric *fx_empty_metric(fx *f, otlp_pb_scope_metrics *sm, const char *name) {
  return new_metric(f, sm, name);
}

otlp_pb_request *fx_all_kinds(fx *f, size_t points) {
  otlp_pb_request *req = fx_request(f);
  otlp_pb_resource_metrics *rm = fx_resource(f, req, "fixture-service");
  otlp_pb_scope_metrics *sm = fx_scope(f, rm, "fixture.scope");
  fx_gauge(f, sm, "fx.gauge", points);
  fx_sum(f, sm, "fx.sum", points);
  fx_histogram(f, sm, "fx.histogram", points);
  fx_exp_histogram(f, sm, "fx.exp_histogram", points);
  fx_summary(f, sm, "fx.summary", points);
  return req;
}

uint8_t *fx_pack(fx *f, const ProtobufCMessage *msg, size_t *len) {
  *len = protobuf_c_message_get_packed_size(msg);
  uint8_t *buf = fx_alloc(f, *len + 1);
  *len = protobuf_c_message_pack(msg, buf);
  return buf;
}

uint8_t *fx_pack_heap(const ProtobufCMessage *msg, size_t *len) {
  *len = protobuf_c_message_get_packed_size(msg);
  uint8_t *buf = grpc_malloc(*len + 1);
  if (buf != nullptr)
    *len = protobuf_c_message_pack(msg, buf);
  return buf;
}

static size_t varint(uint8_t *out, uint64_t v) {
  size_t n = 0;
  do {
    uint8_t b = v & 0x7f;
    v >>= 7;
    out[n++] = v ? (uint8_t)(b | 0x80) : b;
  } while (v);
  return n;
}

uint8_t *fx_deep_request(size_t depth, size_t *len) {
  /* innermost: AnyValue{string_value:"x"} = 0a 01 78. Each level wraps the AnyValue `inner` as
   * AnyValue{array_value: ArrayValue{values: [inner]}}: ArrayValue = 0a len inner,
   * AnyValue = 2a len ArrayValue. Then KeyValue{key:"k", value} inside NumberDataPoint attributes
   * (field 7) inside Gauge -> Metric -> ScopeMetrics -> ResourceMetrics -> request. Built from the
   * inside out into the end of a buffer. */
  size_t cap = 64 + depth * 24;
  uint8_t *buf = grpc_malloc(cap);
  if (buf == nullptr)
    return nullptr;
  size_t pos = cap;
  uint8_t tmp[10];
#define PREPEND_BYTES(bytes, n)      \
  do {                               \
    pos -= (n);                      \
    memcpy(buf + pos, (bytes), (n)); \
  } while (0)
  /* wraps everything from pos to the end in a length-delimited field with `tag` */
#define WRAP(tag)                  \
  do {                             \
    size_t inner = cap - pos;      \
    size_t n = varint(tmp, inner); \
    PREPEND_BYTES(tmp, n);         \
    uint8_t t = (tag);             \
    PREPEND_BYTES(&t, 1);          \
  } while (0)
  static const uint8_t leaf[] = {0x0a, 0x01, 'x'};
  PREPEND_BYTES(leaf, sizeof(leaf));
  for (size_t i = 0; i < depth; i++) {
    WRAP(0x0a); /* ArrayValue.values (field 1) */
    WRAP(0x2a); /* AnyValue.array_value (field 5) */
  }
  WRAP(0x12); /* KeyValue.value (field 2) */
  static const uint8_t key[] = {0x0a, 0x01, 'k'};
  PREPEND_BYTES(key, sizeof(key));
  WRAP(0x3a); /* NumberDataPoint.attributes (field 7) */
  /* time_unix_nano (field 3, fixed64) and as_int (field 6, sfixed64) */
  uint8_t fields[18] = {0x19};
  for (unsigned b = 0; b < 8; b++)
    fields[1 + b] = (uint8_t)(T0 >> (8 * b)); /* fixed64 is little-endian on the wire */
  fields[9] = 0x31;                           /* as_int = 0 */
  PREPEND_BYTES(fields, sizeof(fields));
  WRAP(0x0a); /* Gauge.data_points (field 1) */
  WRAP(0x2a); /* Metric.gauge (field 5) */
  static const uint8_t name[] = {0x0a, 0x04, 'd', 'e', 'e', 'p'};
  PREPEND_BYTES(name, sizeof(name));
  WRAP(0x12); /* ScopeMetrics.metrics (field 2) */
  WRAP(0x12); /* ResourceMetrics.scope_metrics (field 2) */
  WRAP(0x0a); /* ExportMetricsServiceRequest.resource_metrics (field 1) */
  *len = cap - pos;
  memmove(buf, buf + pos, *len);
  return buf;
#undef WRAP
#undef PREPEND_BYTES
}
