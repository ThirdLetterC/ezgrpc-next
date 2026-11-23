#pragma once

/* otlp_fixtures.h - builders for ExportMetricsServiceRequest test data
 *
 * Messages are built with the generated protobuf-c types in an arena (no budget) and serialized
 * with protobuf-c. Every builder makes valid data unless its name says otherwise; the invalid
 * variants each break exactly one validation rule so tests can predict rejected counts.
 */

#include <stddef.h>
#include <stdint.h>

#include "otlp_arena.h"
#include "otlp_proto.h"

typedef struct fx {
  otlp_arena arena;
} fx;

void fx_init(fx *f);
void fx_free(fx *f);

otlp_pb_request *fx_request(fx *f);
otlp_pb_resource_metrics *fx_resource(fx *f, otlp_pb_request *req, const char *service_name);
otlp_pb_scope_metrics *fx_scope(fx *f, otlp_pb_resource_metrics *rm, const char *name);

/* Valid metrics with `points` data points each. */
otlp_pb_metric *fx_gauge(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points);
otlp_pb_metric *fx_sum(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points);
otlp_pb_metric *fx_histogram(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points);
otlp_pb_metric *fx_exp_histogram(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points);
otlp_pb_metric *fx_summary(fx *f, otlp_pb_scope_metrics *sm, const char *name, size_t points);
/** A metric whose data oneof is unset (no points). */
otlp_pb_metric *fx_empty_metric(fx *f, otlp_pb_scope_metrics *sm, const char *name);

/** One resource and scope with every metric kind, `points` points each (5 * points in total),
 * attributes on every level and nested AnyValues, exemplars and metric metadata. */
otlp_pb_request *fx_all_kinds(fx *f, size_t points);

/** Serializes `msg` into a new arena buffer. */
uint8_t *fx_pack(fx *f, const ProtobufCMessage *msg, size_t *len);

/** Serializes into a grpc_malloc buffer the caller frees with grpc_free(). */
uint8_t *fx_pack_heap(const ProtobufCMessage *msg, size_t *len);

/**
 * A request whose attribute value nests `depth` ArrayValues inside each other (built directly as
 * wire bytes; protobuf-c could not encode it without recursing as deep). Caller frees with
 * grpc_free().
 */
uint8_t *fx_deep_request(size_t depth, size_t *len);
