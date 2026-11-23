#!/usr/bin/env python3
"""Write-ahead-log sink with the official OpenTelemetry Python SDK exporter.

    OTLP_SERVER=build/otlp_metrics_server OTLP_WAL_DUMP=build/otlp_wal_dump python3 wal_test.py

What the server acknowledged must be in the log even when the server is killed right after
(fsync before acknowledging), the log must verify (CRC, decodable payloads) and decode with the
official opentelemetry-proto package, and a restarted server must recover it and continue.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import grpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from opentelemetry.sdk.metrics.export import MetricExportResult

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import otlp_harness  # noqa: E402
from python_sdk_test import EXPECTED_KINDS, export_all_kinds  # noqa: E402

SERVER = otlp_harness.server_binary()
WAL_DUMP = os.environ.get("OTLP_WAL_DUMP") or sys.exit("set OTLP_WAL_DUMP to the otlp_wal_dump binary")


def dump(wal_dir, tmp):
    out = os.path.join(tmp, "export.bin")
    proc = subprocess.run([WAL_DUMP, wal_dir, "--export", out], capture_output=True, text=True)
    req = metrics_service_pb2.ExportMetricsServiceRequest()
    with open(out, "rb") as f:
        req.ParseFromString(f.read())
    return proc.returncode, proc.stdout, req


def services(req):
    return [
        kv.value.string_value
        for rm in req.resource_metrics
        for kv in rm.resource.attributes
        if kv.key == "service.name"
    ]


class WalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="otlp-wal-it-")
        self.wal = os.path.join(self.tmp, "wal")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def server(self, *extra):
        return otlp_harness.Server(SERVER, ["--sink", "wal", "--wal-dir", self.wal, *extra])

    def test_acknowledged_data_survives_a_crash(self):
        for compression in (grpc.Compression.NoCompression, grpc.Compression.Gzip):
            server = self.server("--wal-sync", "fsync").start()
            try:
                exporter = export_all_kinds(server.target, compression, "cumulative", service=f"crash-{compression.name}")
                self.assertEqual(exporter.results, [MetricExportResult.SUCCESS])
                sink = server.stats()["sink"]
                self.assertEqual(sink["records"], 1)
                self.assertGreaterEqual(sink["syncs"], 1)
            finally:
                server.kill()  # SIGKILL: no drain, no final sync
        code, summary, req = dump(self.wal, self.tmp)
        self.assertEqual(code, 0, summary)
        self.assertIn("records=2 points=18", summary)
        self.assertIn("sequence_gaps=0", summary)
        self.assertEqual(services(req), ["crash-NoCompression", "crash-Gzip"])
        self.assertEqual(otlp_harness.metric_kinds(req), EXPECTED_KINDS)
        self.assertEqual(otlp_harness.count_points(req), 18)

    def test_restart_recovers_and_continues(self):
        with self.server() as server:
            self.assertEqual(export_all_kinds(server.target, None, "delta", service="first").results,
                             [MetricExportResult.SUCCESS])
        # a torn record at the end, as if the process died in the middle of a write
        segments = sorted(os.listdir(self.wal))
        with open(os.path.join(self.wal, segments[-1]), "ab") as f:
            f.write(b"OTRC\x10\x00\x00\x00partial")
        code, summary, _ = dump(self.wal, self.tmp)
        self.assertEqual(code, 1, summary)  # torn tail reported
        with self.server() as server:
            sink = server.stats()["sink"]
            self.assertEqual(sink["recovered_records"], 1)
            self.assertEqual(sink["truncated_bytes"], 15)
            self.assertEqual(export_all_kinds(server.target, None, "delta", service="second").results,
                             [MetricExportResult.SUCCESS])
        code, summary, req = dump(self.wal, self.tmp)
        self.assertEqual(code, 0, summary)
        self.assertIn("sequence_gaps=0", summary)
        self.assertEqual(services(req), ["first", "second"])

    def test_full_log_is_retryable(self):
        # room for a couple of small records: once full, batches are refused, retryably
        with self.server("--wal-max-bytes", "200") as server:
            stub_req = metrics_service_pb2.ExportMetricsServiceRequest()
            m = stub_req.resource_metrics.add().scope_metrics.add().metrics.add()
            m.name = "g"
            p = m.gauge.data_points.add()
            p.time_unix_nano = 1
            p.as_int = 1
            from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc

            stub = metrics_service_pb2_grpc.MetricsServiceStub(server.channel())
            for _ in range(10):
                try:
                    stub.Export(stub_req, timeout=10)
                except grpc.RpcError as err:
                    self.assertEqual(err.code(), grpc.StatusCode.UNAVAILABLE)
                    self.assertIn("write-ahead log is full", err.details())
                    break
            else:
                self.fail("the log never filled up")


if __name__ == "__main__":
    unittest.main(verbosity=2)
