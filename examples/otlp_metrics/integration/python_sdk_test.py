#!/usr/bin/env python3
"""Integration tests with the official OpenTelemetry Python SDK and OTLP/gRPC exporter.

    OTLP_SERVER=build/otlp_metrics_server python3 python_sdk_test.py

Every instrument kind is exported through OTLPMetricExporter, uncompressed and gzip, with
cumulative and delta temporality; what the receiver stored (memory sink) is read back and decoded
with the official opentelemetry-proto Python package. Further tests use the official generated
stubs for partial success, malformed, oversized, unauthenticated and throttled requests.
"""

import os
import sys
import unittest

import grpc
from google.rpc import error_details_pb2, status_pb2
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2, metrics_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2
from opentelemetry.proto.resource.v1 import resource_pb2
from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    MeterProvider,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    MetricExportResult,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import otlp_harness  # noqa: E402

SERVER = otlp_harness.server_binary()
EXPORT = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"


class RecordingExporter(OTLPMetricExporter):
    """The official exporter, remembering each export's result (force_flush hides failures)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.results = []

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        result = super().export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        self.results.append(result)
        return result


def export_all_kinds(target, compression, temporality, headers=None, service="sdk-test"):
    """Records every instrument kind once and exports it. Returns the exporter."""
    preferred = None
    if temporality == "delta":
        preferred = {
            Counter: AggregationTemporality.DELTA,
            UpDownCounter: AggregationTemporality.DELTA,
            Histogram: AggregationTemporality.DELTA,
            ObservableCounter: AggregationTemporality.DELTA,
            ObservableUpDownCounter: AggregationTemporality.DELTA,
            ObservableGauge: AggregationTemporality.DELTA,
        }
    exporter = RecordingExporter(
        endpoint=target,
        insecure=True,
        compression=compression,
        headers=headers,
        timeout=20,
        preferred_temporality=preferred,
    )
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=3_600_000)
    provider = MeterProvider(
        resource=Resource.create({"service.name": service, "deployment.environment": "test"}),
        metric_readers=[reader],
        views=[
            View(instrument_name="sdk.exp_histogram", aggregation=ExponentialBucketHistogramAggregation()),
        ],
    )
    meter = provider.get_meter("otlp.integration", "1.2.3")
    counter = meter.create_counter("sdk.counter", unit="1", description="a counter")
    for i in range(5):
        counter.add(i + 1, {"route": "/a"})
    counter.add(10, {"route": "/b"})
    meter.create_up_down_counter("sdk.updown").add(-3, {"k": "v"})
    hist = meter.create_histogram("sdk.histogram", unit="ms")
    for v in (1, 7, 25, 250, 1000):
        hist.record(v, {"route": "/a"})
    exp = meter.create_histogram("sdk.exp_histogram", unit="s")
    for v in (0.001, 0.5, 2.0, 1000.0):
        exp.record(v)
    meter.create_gauge("sdk.gauge").set(42.5, {"core": "0"})
    meter.create_observable_gauge("sdk.observable_gauge", callbacks=[lambda options: [_obs(3.25)]])
    meter.create_observable_counter("sdk.observable_counter", callbacks=[lambda options: [_obs(17)]])
    meter.create_observable_up_down_counter("sdk.observable_updown", callbacks=[lambda options: [_obs(-2)]])
    # shutdown collects and exports once (force_flush first would export the data twice)
    provider.shutdown()
    return exporter


def _obs(value):
    from opentelemetry.metrics import Observation

    return Observation(value, {"source": "callback"})


EXPECTED_KINDS = {
    "sdk.counter": "sum",
    "sdk.updown": "sum",
    "sdk.histogram": "histogram",
    "sdk.exp_histogram": "exponential_histogram",
    "sdk.gauge": "gauge",
    "sdk.observable_gauge": "gauge",
    "sdk.observable_counter": "sum",
    "sdk.observable_updown": "sum",
}


def find_metric(request, name):
    for rm in request.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == name:
                    return rm, sm, m
    raise AssertionError(f"metric {name} not stored")


class SdkExportTest(unittest.TestCase):
    def check_stored(self, server, temporality):
        stored = server.dump()
        self.assertEqual(otlp_harness.metric_kinds(stored), EXPECTED_KINDS)
        rm, sm, counter = find_metric(stored, "sdk.counter")
        attrs = {kv.key: kv.value.string_value for kv in rm.resource.attributes}
        self.assertEqual(attrs["service.name"], "sdk-test")
        self.assertEqual(sm.scope.name, "otlp.integration")
        self.assertEqual(sm.scope.version, "1.2.3")
        self.assertTrue(counter.sum.is_monotonic)
        expected_temporality = (
            metrics_pb2.AGGREGATION_TEMPORALITY_DELTA if temporality == "delta"
            else metrics_pb2.AGGREGATION_TEMPORALITY_CUMULATIVE
        )
        self.assertEqual(counter.sum.aggregation_temporality, expected_temporality)
        by_route = {p.attributes[0].value.string_value: p.as_int for p in counter.sum.data_points}
        self.assertEqual(by_route, {"/a": 15, "/b": 10})
        _, _, hist = find_metric(stored, "sdk.histogram")
        point = hist.histogram.data_points[0]
        self.assertEqual(point.count, 5)
        self.assertEqual(sum(point.bucket_counts), 5)
        self.assertEqual(len(point.bucket_counts), len(point.explicit_bounds) + 1)
        self.assertTrue(point.HasField("sum"))  # proto3 optional presence survived
        self.assertEqual(point.sum, 1283)
        self.assertEqual((point.min, point.max), (1, 1000))
        _, _, exp = find_metric(stored, "sdk.exp_histogram")
        ep = exp.exponential_histogram.data_points[0]
        self.assertEqual(ep.count, 4)
        self.assertEqual(ep.zero_count + sum(ep.positive.bucket_counts) + sum(ep.negative.bucket_counts), 4)
        _, _, gauge = find_metric(stored, "sdk.gauge")
        self.assertEqual(gauge.gauge.data_points[0].as_double, 42.5)
        _, _, og = find_metric(stored, "sdk.observable_gauge")
        self.assertEqual(og.gauge.data_points[0].as_double, 3.25)
        self.assertEqual(otlp_harness.count_points(stored), 9)
        return stored

    def run_export(self, compression, temporality):
        with otlp_harness.Server(SERVER, ["--sink", "memory"]) as server:
            exporter = export_all_kinds(server.target, compression, temporality)
            self.assertEqual(exporter.results, [MetricExportResult.SUCCESS])
            self.check_stored(server, temporality)
            stats = server.stats()["stats"]
            self.assertEqual(stats["requests_ok"], 1)
            self.assertEqual(stats["points_accepted"], 9)
            self.assertEqual(stats["points_rejected"], 0)
            gz = 1 if compression == grpc.Compression.Gzip else 0
            self.assertEqual(stats["encoding_gzip"], gz)
            self.assertEqual(stats["encoding_identity"], 1 - gz)

    def test_uncompressed_cumulative(self):
        self.run_export(grpc.Compression.NoCompression, "cumulative")

    def test_gzip_cumulative(self):
        self.run_export(grpc.Compression.Gzip, "cumulative")

    def test_uncompressed_delta(self):
        self.run_export(grpc.Compression.NoCompression, "delta")

    def test_gzip_delta(self):
        self.run_export(grpc.Compression.Gzip, "delta")

    def test_authentication(self):
        with otlp_harness.Server(SERVER, ["--sink", "memory", "--auth-token", "s3cret"]) as server:
            denied = export_all_kinds(server.target, None, "cumulative")
            self.assertEqual(denied.results, [MetricExportResult.FAILURE])
            wrong = export_all_kinds(server.target, None, "cumulative", headers={"authorization": "Bearer nope"})
            self.assertEqual(wrong.results, [MetricExportResult.FAILURE])
            ok = export_all_kinds(server.target, None, "cumulative", headers={"authorization": "Bearer s3cret"})
            self.assertEqual(ok.results, [MetricExportResult.SUCCESS])
            md = [("authorization", "Bearer s3cret")]
            stats = server.stats(metadata=md)["stats"]
            self.assertEqual(stats["points_accepted"], 9)
            # rejected calls never reached the receiver
            self.assertEqual(stats["requests"], 1)
            with self.assertRaises(grpc.RpcError) as cm:
                server.stats()
            self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAUTHENTICATED)

    def test_retry_after_throttling(self):
        # every second batch is refused as retryable; the exporter retries after RetryInfo's delay
        args = ["--sink", "memory", "--fault", "retryable-every=2", "--retry-delay-ms", "50"]
        with otlp_harness.Server(SERVER, args) as server:
            first = export_all_kinds(server.target, None, "cumulative", service="first")
            second = export_all_kinds(server.target, None, "cumulative", service="second")
            self.assertEqual(first.results, [MetricExportResult.SUCCESS])
            self.assertEqual(second.results, [MetricExportResult.SUCCESS])
            stats = server.stats()["stats"]
            self.assertEqual(stats["sink_retryable"], 1)
            self.assertEqual(stats["status"]["14"], 1)
            self.assertEqual(stats["points_accepted"], 18)
            self.assertEqual(stats["points_failed"], 9)
            # nothing acknowledged twice, nothing lost
            stored = server.dump()
            services = sorted(
                kv.value.string_value
                for rm in stored.resource_metrics
                for kv in rm.resource.attributes
                if kv.key == "service.name"
            )
            self.assertEqual(services, ["first", "second"])


def gauge_request(points, service="stub"):
    req = metrics_service_pb2.ExportMetricsServiceRequest()
    rm = req.resource_metrics.add()
    rm.resource.attributes.append(common_pb2.KeyValue(key="service.name", value=common_pb2.AnyValue(string_value=service)))
    sm = rm.scope_metrics.add()
    sm.scope.name = "stub"
    m = sm.metrics.add()
    m.name = "stub.gauge"
    for i in range(points):
        p = m.gauge.data_points.add()
        p.time_unix_nano = 1_700_000_000_000_000_000 + i
        p.as_double = float(i)
    return req


class StubTest(unittest.TestCase):
    """The official generated stub, for responses and errors the SDK does not expose."""

    @classmethod
    def setUpClass(cls):
        cls.server = otlp_harness.Server(SERVER, ["--sink", "memory", "--max-message-bytes", "256K"]).start()
        cls.stub = metrics_service_pb2_grpc.MetricsServiceStub(cls.server.channel())

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_full_success_leaves_partial_success_unset(self):
        resp = self.stub.Export(gauge_request(3), timeout=10)
        self.assertFalse(resp.HasField("partial_success"))

    def test_partial_success(self):
        req = gauge_request(4)
        metric = req.resource_metrics[0].scope_metrics[0].metrics.add()
        metric.name = "bad.histogram"
        metric.histogram.aggregation_temporality = metrics_pb2.AGGREGATION_TEMPORALITY_DELTA
        for count in (3, 4, 5):
            p = metric.histogram.data_points.add()
            p.time_unix_nano = 1
            p.count = count
            p.explicit_bounds.extend([1.0])
            p.bucket_counts.extend([1, 3])  # adds up to 4: only the second point is valid
        resp = self.stub.Export(req, timeout=10)
        self.assertTrue(resp.HasField("partial_success"))
        self.assertEqual(resp.partial_success.rejected_data_points, 2)
        self.assertIn("bucket_counts do not add up", resp.partial_success.error_message)

    def test_unknown_fields_are_preserved(self):
        self.server.admin("Clear")
        req = gauge_request(1, service="unknown-fields")
        raw = req.SerializeToString() + b"\xa2\x06\x03abc"  # field 100 (bytes): unknown to the schema
        call = self.server.channel().unary_unary(EXPORT, request_serializer=lambda b: b, response_deserializer=lambda b: b)
        self.assertEqual(call(raw, timeout=10), b"")
        self.assertEqual(self.server.admin("Dump"), raw)

    def test_malformed(self):
        call = self.server.channel().unary_unary(EXPORT, request_serializer=lambda b: b, response_deserializer=lambda b: b)
        with self.assertRaises(grpc.RpcError) as cm:
            call(b"\x0a\x7f\x00", timeout=10)
        self.assertEqual(cm.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)

    def test_oversized(self):
        with self.assertRaises(grpc.RpcError) as cm:
            self.stub.Export(gauge_request(20000), timeout=10)
        self.assertEqual(cm.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)

    def test_oversized_after_decompression(self):
        req = gauge_request(20000)
        self.assertGreater(req.ByteSize(), 256 * 1024)
        with self.assertRaises(grpc.RpcError) as cm:
            self.stub.Export(req, timeout=10, compression=grpc.Compression.Gzip)
        self.assertEqual(cm.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)


class ThrottleTest(unittest.TestCase):
    def test_retry_info_in_status_details(self):
        args = ["--sink", "memory", "--fault", "retryable-every=1", "--retry-delay-ms", "1500"]
        with otlp_harness.Server(SERVER, args) as server:
            stub = metrics_service_pb2_grpc.MetricsServiceStub(server.channel())
            with self.assertRaises(grpc.RpcError) as cm:
                stub.Export(gauge_request(1), timeout=10)
            err = cm.exception
            self.assertEqual(err.code(), grpc.StatusCode.UNAVAILABLE)
            trailers = dict(err.trailing_metadata())
            status = status_pb2.Status()
            status.ParseFromString(trailers["grpc-status-details-bin"])
            self.assertEqual(status.code, grpc.StatusCode.UNAVAILABLE.value[0])
            self.assertEqual(len(status.details), 1)
            info = error_details_pb2.RetryInfo()
            self.assertTrue(status.details[0].Unpack(info))
            self.assertEqual((info.retry_delay.seconds, info.retry_delay.nanos), (1, 500_000_000))
            alone = error_details_pb2.RetryInfo()
            alone.ParseFromString(trailers["google.rpc.retryinfo-bin"])
            self.assertEqual(alone, info)
            self.assertEqual(server.stats()["stats"]["points_accepted"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
