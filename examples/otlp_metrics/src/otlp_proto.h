#pragma once

/* otlp_proto.h - short names for the protobuf-c bindings of the OpenTelemetry protos
 *
 * The bindings are generated at build time from the pinned upstream opentelemetry-proto release
 * (see CMakeLists.txt); nothing here changes them, it only shortens the generated names.
 */

#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb-c.h"

typedef Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsServiceRequest otlp_pb_request;
typedef Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsServiceResponse otlp_pb_response;
typedef Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsPartialSuccess
    otlp_pb_partial_success;
typedef Opentelemetry__Proto__Metrics__V1__ResourceMetrics otlp_pb_resource_metrics;
typedef Opentelemetry__Proto__Metrics__V1__ScopeMetrics otlp_pb_scope_metrics;
typedef Opentelemetry__Proto__Metrics__V1__Metric otlp_pb_metric;
typedef Opentelemetry__Proto__Metrics__V1__NumberDataPoint otlp_pb_number_point;
typedef Opentelemetry__Proto__Metrics__V1__HistogramDataPoint otlp_pb_histogram_point;
typedef Opentelemetry__Proto__Metrics__V1__ExponentialHistogramDataPoint
    otlp_pb_exp_histogram_point;
typedef Opentelemetry__Proto__Metrics__V1__ExponentialHistogramDataPoint__Buckets
    otlp_pb_exp_buckets;
typedef Opentelemetry__Proto__Metrics__V1__SummaryDataPoint otlp_pb_summary_point;
typedef Opentelemetry__Proto__Metrics__V1__SummaryDataPoint__ValueAtQuantile otlp_pb_quantile;

#define OTLP_PB_REQUEST_DESCRIPTOR \
  (&opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__descriptor)

/** The gRPC method path of OTLP metrics export. */
#define OTLP_EXPORT_PATH "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"
