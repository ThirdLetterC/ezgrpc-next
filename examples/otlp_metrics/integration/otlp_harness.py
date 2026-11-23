"""Test harness for the OTLP metrics receiver: start it, talk to its admin methods, stop it.

The server prints "READY port=<n>" once it listens; start() reads that line instead of sleeping or
probing the port. Admin methods (/otlp_metrics.Admin/*) take and return raw bytes.
"""

import json
import os
import queue
import signal
import subprocess
import sys
import threading

import grpc

READY_TIMEOUT_S = 30


class Server:
    def __init__(self, binary, args=(), env=None):
        self.binary = binary
        self.args = list(args)
        self.env = env
        self.proc = None
        self.port = None
        self.stderr_lines = []
        self._channel = None
        self._stderr_thread = None

    def start(self):
        cmd = [self.binary, "--host", "127.0.0.1", "--port", "0", "--admin", *self.args]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        self._stderr_thread = threading.Thread(target=self._drain_stderr, args=(self.proc,), daemon=True)
        self._stderr_thread.start()
        # a thread reads the first line, so the wait below is bounded on every platform
        first_line = queue.Queue()
        threading.Thread(target=lambda: first_line.put(self.proc.stdout.readline()), daemon=True).start()
        try:
            line = first_line.get(timeout=READY_TIMEOUT_S)
        except queue.Empty:
            self.kill()
            raise RuntimeError("server did not report READY:\n" + self.stderr())
        text = line.decode().strip()
        if not text.startswith("READY port="):
            code = self.proc.wait(timeout=10)
            self._reap()
            raise RuntimeError(f"server exited ({code}) before READY: {text!r}\n{self.stderr()}")
        self.port = int(text.split("=", 1)[1])
        return self

    def _drain_stderr(self, proc):
        for raw in proc.stderr:
            self.stderr_lines.append(raw.decode(errors="replace"))

    def stderr(self):
        return "".join(self.stderr_lines)

    @property
    def target(self):
        return f"127.0.0.1:{self.port}"

    def channel(self):
        if self._channel is None:
            self._channel = grpc.insecure_channel(self.target)
        return self._channel

    def admin(self, method, body=b"", timeout=30, metadata=None):
        call = self.channel().unary_unary(
            f"/otlp_metrics.Admin/{method}",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        return call(body, timeout=timeout, metadata=metadata)

    def stats(self, metadata=None):
        return json.loads(self.admin("Stats", metadata=metadata))

    def wait_for(self, counter, value, timeout=30, metadata=None):
        """Blocks (server side) until stats[counter] >= value; returns the stats document."""
        return json.loads(self.admin("Wait", f"{counter} {value}".encode(), timeout=timeout, metadata=metadata))

    def dump(self, metadata=None):
        """The memory sink content as one ExportMetricsServiceRequest."""
        from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2

        req = metrics_service_pb2.ExportMetricsServiceRequest()
        req.ParseFromString(self.admin("Dump", metadata=metadata))
        return req

    def stop(self, expect_code=0):
        if self._channel is not None:
            self._channel.close()
            self._channel = None
        if self.proc is None:
            return
        if os.name == "nt":
            # no SIGTERM on Windows: the process is terminated, not drained
            self.proc.terminate()
            expect_code = None
        else:
            self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.kill()
            raise
        self._reap()
        if expect_code is not None and code != expect_code:
            raise RuntimeError(f"server exited with {code}:\n{self.stderr()}")
        return code

    def kill(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait()
            self._reap()

    def _reap(self):
        # the process has exited: the stderr reader reaches EOF, then both pipes are closed
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=10)
            self._stderr_thread = None
        self.proc.stdout.close()
        self.proc.stderr.close()
        self.proc = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            sys.stderr.write(self.stderr())
            self.kill()
        else:
            self.stop()


def server_binary():
    path = os.environ.get("OTLP_SERVER")
    if not path:
        raise SystemExit("set OTLP_SERVER to the otlp_metrics_server binary")
    return path


def metric_kinds(request):
    """{metric name: data kind} of every metric in an ExportMetricsServiceRequest."""
    kinds = {}
    for rm in request.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                kinds[m.name] = m.WhichOneof("data")
    return kinds


def count_points(request):
    n = 0
    for rm in request.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                kind = m.WhichOneof("data")
                if kind:
                    n += len(getattr(m, kind).data_points)
    return n
