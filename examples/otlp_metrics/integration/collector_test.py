#!/usr/bin/env python3
"""The OpenTelemetry Collector between the Python SDK and the receiver.

    OTLP_SERVER=build/otlp_metrics_server OTLP_OTELCOL=/path/to/otelcol python3 collector_test.py

SDK --OTLP/gRPC--> collector (otlp receiver) --two otlp exporters, gzip and none--> receiver.

The collector's own OTLP exporter (Go, exporterhelper with its queue and retries) is the client
here. Readiness is read from the collector's log ("Everything is ready"), and arrival is awaited
on the receiver with Admin/Wait: no sleeping. Exits 77 (skipped) when OTLP_OTELCOL is not set.
The core distribution ("otelcol") has every component the configuration uses.
"""

import os
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

import grpc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import otlp_harness  # noqa: E402
from python_sdk_test import EXPECTED_KINDS, export_all_kinds  # noqa: E402

SERVER = otlp_harness.server_binary()
OTELCOL = os.environ.get("OTLP_OTELCOL")
SKIP = 77
# component type of the OTLP/gRPC exporter ("otlp"; newer collectors also accept "otlp_grpc")
EXPORTER = os.environ.get("OTLP_OTELCOL_EXPORTER", "otlp")

CONFIG = """
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:{receiver_port}
exporters:
  {exporter}/identity:
    endpoint: 127.0.0.1:{server_port}
    compression: none
    tls:
      insecure: true
    retry_on_failure:
      enabled: true
      initial_interval: 5s
  {exporter}/gzip:
    endpoint: 127.0.0.1:{server_port}
    compression: gzip
    tls:
      insecure: true
    retry_on_failure:
      enabled: true
      initial_interval: 5s
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [{exporter}/identity, {exporter}/gzip]
  telemetry:
    metrics:
      level: none
    logs:
      level: info
"""


def free_port():
    # the collector needs a fixed port in its configuration; the kernel picks a free one
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Collector:
    def __init__(self, server_port, tmp):
        self.receiver_port = free_port()
        self.config = os.path.join(tmp, "collector.yaml")
        with open(self.config, "w") as f:
            f.write(CONFIG.format(receiver_port=self.receiver_port, server_port=server_port, exporter=EXPORTER))
        self.log = []
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen([OTELCOL, "--config", self.config], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.PIPE, text=True)
        ready = queue.Queue()

        def read():
            for line in self.proc.stderr:
                self.log.append(line)
                if "Everything is ready" in line:
                    ready.put(True)
            ready.put(False)

        threading.Thread(target=read, daemon=True).start()
        if not ready.get(timeout=60):
            raise RuntimeError("collector did not start:\n" + "".join(self.log))
        return self

    def stop(self):
        if self.proc is not None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=60)
            self.proc = None


class CollectorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="otlp-collector-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_pipeline(self, server_args, expect_retryable):
        with otlp_harness.Server(SERVER, ["--sink", "memory", *server_args]) as server:
            collector = Collector(server.port, self.tmp).start()
            try:
                exporter = export_all_kinds(f"127.0.0.1:{collector.receiver_port}", grpc.Compression.NoCompression,
                                            "cumulative", service="via-collector")
                self.assertEqual([r.name for r in exporter.results], ["SUCCESS"])
                # 9 points, forwarded by both exporters; Wait blocks on the receiver until they arrived
                stats = server.wait_for("points_accepted", 18, timeout=60)["stats"]
            finally:
                collector.stop()
            self.assertEqual(stats["points_accepted"], 18)
            self.assertGreaterEqual(stats["encoding_gzip"], 1)
            self.assertGreaterEqual(stats["encoding_identity"], 1)
            if expect_retryable:
                self.assertGreaterEqual(stats["sink_retryable"], 1)
            stored = server.dump()
            self.assertEqual(otlp_harness.metric_kinds(stored), EXPECTED_KINDS)
            self.assertEqual(otlp_harness.count_points(stored), 18)
            names = {
                kv.value.string_value
                for rm in stored.resource_metrics
                for kv in rm.resource.attributes
                if kv.key == "service.name"
            }
            self.assertEqual(names, {"via-collector"})

    def test_collector_forwards_compressed_and_uncompressed(self):
        self.run_pipeline([], expect_retryable=False)

    def test_collector_retries_after_throttling(self):
        # every second batch is refused as retryable (RetryInfo 200 ms); the collector retries it
        self.run_pipeline(["--fault", "retryable-every=2", "--retry-delay-ms", "200"], expect_retryable=True)


if __name__ == "__main__":
    if not OTELCOL:
        print("OTLP_OTELCOL is not set: skipping the collector test", file=sys.stderr)
        sys.exit(SKIP)
    unittest.main(verbosity=2)
