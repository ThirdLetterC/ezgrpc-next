#!/usr/bin/env python3
"""Reproducible OTLP Export benchmarks: ezGRPC-next receiver vs the grpc-go comparison server.

    python3 run_bench.py --ez default=build-bench-default/otlp_metrics_server \
        --ez mimalloc=build-bench-mimalloc/otlp_metrics_server \
        --grpcgo build-bench-default/grpcgo-server --gen build-bench-default/otlp_gen \
        --proto-root build-bench-default/examples/otlp_metrics/opentelemetry-proto-1.11.1 \
        --out results/$(date +%Y%m%d-%H%M%S)

Linux only (taskset, /proc). Needs ghz and a Python with grpcio (integration/requirements.txt).

For every (server configuration, workload, repetition): a fresh server pinned to --server-cpus,
a warm-up ghz run, then counters and CPU time are read, a measured ghz run (pinned to
--client-cpus) follows, and counters, CPU time and memory are read again. Every raw measurement
is written to <out>/runs/*.json; <out>/environment.json records the machine, toolchain, build
flags and revisions; <out>/summary.md is generated from the raw files (medians over the
repetitions, with min-max). Nothing is estimated: requests/s and latencies come from ghz,
accepted points/s from the server's own counters, CPU and memory from /proc.
"""

import argparse
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import queue
import time

import grpc

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
CLK_TCK = os.sysconf("SC_CLK_TCK")
EXPORT_CALL = "opentelemetry.proto.collector.metrics.v1.MetricsService.Export"

# name -> otlp_gen arguments
WORKLOADS = {
    # a typical SDK export: one resource, ten metrics of every kind, one point each
    "small": ["--resources", "1", "--scopes", "1", "--metrics", "10", "--points", "1", "--attrs", "3"],
    # a collector batch: 4 resources x 2 scopes x 25 metrics x 5 points = 1000 points
    "large": ["--resources", "4", "--scopes", "2", "--metrics", "25", "--points", "5", "--attrs", "4"],
    # small, with every 4th point invalid: partial success on every request
    "partial": ["--resources", "1", "--scopes", "1", "--metrics", "10", "--points", "2", "--attrs", "3",
                "--invalid-every", "4"],
}


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def first_line(cmd):
    try:
        out = run(cmd)
        return (out.stdout or out.stderr).strip().splitlines()[0]
    except (OSError, IndexError):
        return None


def read_file(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def cmake_cache(build_dir, keys):
    out = {}
    cache = os.path.join(build_dir, "CMakeCache.txt")
    text = read_file(cache) or ""
    for key in keys:
        m = re.search(rf"^{re.escape(key)}:[A-Z]+=(.*)$", text, re.M)
        if m:
            out[key] = m.group(1)
    return out


def environment(args):
    cpu = {}
    for line in (read_file("/proc/cpuinfo") or "").splitlines():
        if line.startswith("model name") and "model" not in cpu:
            cpu["model"] = line.split(":", 1)[1].strip()
    cpu["logical_cpus"] = os.cpu_count()
    cpu["governor"] = read_file("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    cpu["intel_no_turbo"] = read_file("/sys/devices/system/cpu/intel_pstate/no_turbo")
    cpu["topology"] = run(["lscpu", "-e=CPU,CORE,SOCKET"]).stdout
    git = lambda *a: run(["git", "-C", REPO, *a]).stdout.strip()
    builds = {}
    for label, path in args.ez:
        bdir = os.path.dirname(os.path.abspath(path))
        builds[label] = cmake_cache(bdir, [
            "CMAKE_BUILD_TYPE", "CMAKE_C_COMPILER", "CMAKE_C_FLAGS", "CMAKE_C_FLAGS_RELEASE",
            "GRPC_ALLOCATOR", "GRPC_IPO", "GRPC_TLS_BACKEND", "GRPC_DEPENDENCIES", "GRPC_HARDENING",
            "OTLP_METRICS_WAL", "PROTOC_C_EXECUTABLE",
        ])
        builds[label]["binary"] = os.path.abspath(path)
        cc = builds[label].get("CMAKE_C_COMPILER")
        builds[label]["compiler_version"] = first_line([cc, "--version"]) if cc else None
    proto = read_file(os.path.join(args.proto_root, "VERSION")) if args.proto_root else None
    return {
        "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu": cpu,
        "memory_kb": next((l.split()[1] for l in (read_file("/proc/meminfo") or "").splitlines()
                           if l.startswith("MemTotal")), None),
        "os": {"platform": platform.platform(), "kernel": platform.release(),
               "os_release": read_file("/etc/os-release")},
        "source": {"revision": git("rev-parse", "HEAD"),
                   # tracked files only: results written into the tree are untracked
                   "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
                   "describe": git("describe", "--always", "--dirty")},
        "opentelemetry_proto": proto,
        "ez_builds": builds,
        "grpcgo": {"binary": os.path.abspath(args.grpcgo) if args.grpcgo else None,
                   "go": first_line(["go", "version"]),
                   "modules": run(["go", "version", "-m", args.grpcgo]).stdout if args.grpcgo else None},
        "ghz": {"binary": args.ghz, "build_info": run(["go", "version", "-m", args.ghz]).stdout},
        "python": sys.version,
        "grpcio": grpc.__version__,
        "settings": {"duration_s": args.duration, "warmup_s": args.warmup, "reps": args.reps,
                     "concurrency": args.concurrency, "connections": args.connections,
                     "server_cpus": args.server_cpus, "client_cpus": args.client_cpus},
    }


# ---- server processes ------------------------------------------------------------------------

class Server:
    def __init__(self, cmd, cpus, env=None):
        self.cmd = ["taskset", "-c", cpus, *cmd]
        self.env = env
        self.proc = None
        self.port = None
        self.stderr = []

    def start(self):
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        threading.Thread(target=lambda: self.stderr.extend(l.decode(errors="replace") for l in self.proc.stderr),
                         daemon=True).start()
        q = queue.Queue()
        threading.Thread(target=lambda: q.put(self.proc.stdout.readline()), daemon=True).start()
        line = q.get(timeout=30).decode().strip()
        if not line.startswith("READY port="):
            raise RuntimeError(f"server did not start: {line!r} {''.join(self.stderr)}")
        self.port = int(line.split("=", 1)[1])
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.port}")
        return self

    def stats(self):
        call = self.channel.unary_unary("/otlp_metrics.Admin/Stats", request_serializer=lambda b: b,
                                        response_deserializer=lambda b: b)
        return json.loads(call(b"", timeout=30))

    def cpu_seconds(self):
        # utime + stime of the whole process (all threads), from /proc/<pid>/stat
        fields = read_file(f"/proc/{self.proc.pid}/stat").rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / CLK_TCK

    def memory(self):
        out = {}
        for line in read_file(f"/proc/{self.proc.pid}/status").splitlines():
            if line.startswith(("VmHWM", "VmRSS")):
                out[line.split(":")[0]] = int(line.split()[1])
        return out

    def threads(self):
        return len(os.listdir(f"/proc/{self.proc.pid}/task"))

    def stop(self):
        self.channel.close()
        self.proc.send_signal(signal.SIGTERM)
        try:
            return self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait()


def ghz(args, port, payload, duration, client_cpus):
    cmd = ["taskset", "-c", client_cpus, args.ghz, "--insecure",
           "--proto", os.path.join(args.proto_root, "opentelemetry/proto/collector/metrics/v1/metrics_service.proto"),
           "-i", args.proto_root, "--call", EXPORT_CALL, "-B", payload,
           "-c", str(args.concurrency), "--connections", str(args.connections),
           "-z", f"{duration}s", "--cpus", str(len(cpu_list(client_cpus))), "-O", "json",
           f"127.0.0.1:{port}"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ghz failed: {err.decode()}")
    return json.loads(out)


def cpu_list(spec):
    out = []
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out += list(range(int(a), int(b or a) + 1))
    return out


def children_cpu():
    # CPU time of reaped child processes: each ghz run is reaped before the next reading, and the
    # server only after the measurement, so a delta around one ghz run is that run's client CPU
    t = os.times()
    return t.children_user + t.children_system


# ---- the matrix ----------------------------------------------------------------------------------

def configurations(args):
    configs = []
    ez_default = dict(args.ez)["default"] if "default" in dict(args.ez) else args.ez[0][1]
    common = ["--log-level", "error", "--max-message-bytes", "16M", "--max-decoded-bytes", "128M",
              "--memory-limit", "1G", "--max-streams", "256"]
    for loops in args.loops:
        configs.append({"name": f"ezgrpc-null-loops{loops}", "server": "ezgrpc", "allocator": "default",
                        "cmd": [ez_default, "--host", "127.0.0.1", "--port", "0", "--admin",
                                "--loops", str(loops), "--sink", "null", *common]})
    configs.append({"name": "ezgrpc-null-loops1-workers4", "server": "ezgrpc", "allocator": "default",
                    "cmd": [ez_default, "--host", "127.0.0.1", "--port", "0", "--admin", "--loops", "1",
                            "--workers", "4", "--sink", "null", *common]})
    configs.append({"name": "ezgrpc-memory-loops4", "server": "ezgrpc", "allocator": "default",
                    "cmd": [ez_default, "--host", "127.0.0.1", "--port", "0", "--admin", "--loops", "4",
                            "--sink", "memory", "--memory-sink-bytes", "256M", "--memory-policy", "evict", *common]})
    if args.wal_dir:
        # durability cost: every acknowledged batch written and fdatasync'ed (group commit)
        configs.append({"name": "ezgrpc-wal-fsync-loops4", "server": "ezgrpc", "allocator": "default",
                        "workloads": ["small"], "wal_dir": args.wal_dir,
                        "cmd": [ez_default, "--host", "127.0.0.1", "--port", "0", "--admin", "--loops", "4",
                                "--sink", "wal", "--wal-dir", args.wal_dir, "--wal-sync", "fsync",
                                "--wal-max-bytes", "8G", "--wal-queue-bytes", "256M", *common]})
    for label, path in args.ez:
        if label == "default":
            continue
        for loops in (1, 4):
            configs.append({"name": f"ezgrpc-{label}-null-loops{loops}", "server": "ezgrpc", "allocator": label,
                            "cmd": [path, "--host", "127.0.0.1", "--port", "0", "--admin",
                                    "--loops", str(loops), "--sink", "null", *common]})
    if args.grpcgo:
        for procs in args.loops:
            configs.append({"name": f"grpcgo-null-procs{procs}", "server": "grpc-go", "allocator": "go",
                            "env": {"GOMAXPROCS": str(procs)},
                            "cmd": [args.grpcgo, "-addr", "127.0.0.1:0", "-sink", "null",
                                    "-max-message-bytes", str(16 << 20), "-max-streams", "256"]})
        configs.append({"name": "grpcgo-memory-procs4", "server": "grpc-go", "allocator": "go",
                        "env": {"GOMAXPROCS": "4"},
                        "cmd": [args.grpcgo, "-addr", "127.0.0.1:0", "-sink", "memory",
                                "-memory-bytes", str(256 << 20), "-max-message-bytes", str(16 << 20),
                                "-max-streams", "256"]})
    # saturation: one physical core for the server, the other seven for the client, so the
    # server (not ghz) is the bottleneck
    if args.saturation:
        sat = {"server_cpus": "0", "client_cpus": "1-7,9-15"}
        configs.append({"name": "sat-ezgrpc-null-loops1", "server": "ezgrpc", "allocator": "default", **sat,
                        "cmd": [ez_default, "--host", "127.0.0.1", "--port", "0", "--admin", "--loops", "1",
                                "--sink", "null", *common]})
        for label, path in args.ez:
            if label != "default":
                configs.append({"name": f"sat-ezgrpc-{label}-null-loops1", "server": "ezgrpc",
                                "allocator": label, **sat,
                                "cmd": [path, "--host", "127.0.0.1", "--port", "0", "--admin", "--loops", "1",
                                        "--sink", "null", *common]})
        if args.grpcgo:
            configs.append({"name": "sat-grpcgo-null-procs1", "server": "grpc-go", "allocator": "go", **sat,
                            "env": {"GOMAXPROCS": "1"},
                            "cmd": [args.grpcgo, "-addr", "127.0.0.1:0", "-sink", "null",
                                    "-max-message-bytes", str(16 << 20), "-max-streams", "256"]})
    if args.only:
        configs = [c for c in configs if re.search(args.only, c["name"])]
    return configs


def measure(args, config, workload, meta, payload, rep):
    env = dict(os.environ, **config.get("env", {}))
    cpus = config.get("server_cpus", args.server_cpus)
    client_cpus = config.get("client_cpus", args.client_cpus)
    server = Server(config["cmd"], cpus, env).start()
    try:
        ghz(args, server.port, payload, args.warmup, client_cpus)
        before_stats = server.stats()["stats"]
        before_cpu = server.cpu_seconds()
        before_client = children_cpu()
        t0 = time.monotonic()
        report = ghz(args, server.port, payload, args.duration, client_cpus)
        wall = time.monotonic() - t0
        client_cpu = children_cpu() - before_client
        after_cpu = server.cpu_seconds()
        after = server.stats()
        mem = server.memory()
        threads = server.threads()
    finally:
        code = server.stop()
        if config.get("wal_dir"):
            shutil.rmtree(config["wal_dir"], ignore_errors=True)
    after_stats = after["stats"]
    delta = {k: after_stats[k] - before_stats.get(k, 0) for k in after_stats if isinstance(after_stats[k], int)}
    measured_s = report["total"] / 1e9
    ok = report.get("statusCodeDistribution", {}).get("OK", 0)
    lat = {f"p{int(d['percentage'])}": d["latency"] / 1e6 for d in report.get("latencyDistribution") or []}
    server_cpu = after_cpu - before_cpu
    return {
        "config": config["name"], "server": config["server"], "allocator": config["allocator"],
        "workload": workload, "workload_meta": meta, "rep": rep, "command": config["cmd"],
        "env": config.get("env", {}),
        "ghz": {k: report.get(k) for k in ("count", "total", "average", "fastest", "slowest", "rps",
                                           "statusCodeDistribution", "errorDistribution", "latencyDistribution")},
        "measured_s": measured_s,
        "wall_s": wall,
        "requests_ok": ok,
        "rps": report["rps"],
        "ok_rps": ok / measured_s,
        "latency_ms": {"average": report["average"] / 1e6, **lat},
        "server_counters_delta": delta,
        "points_accepted_per_s": delta.get("points_accepted", 0) / measured_s,
        "server_cpu_s": server_cpu,
        "server_cpu_cores": server_cpu / measured_s,
        "server_cpu_us_per_request": 1e6 * server_cpu / max(1, delta.get("requests", ok)),
        "client_cpu_s": client_cpu,
        "server_memory_kb": mem,
        "server_threads": threads,
        "server_exit_code": code,
        "server_sink": after.get("sink"),
        "server_memory_budget": after.get("memory"),
        # full counters, to tell server-side errors (status != 0) from client-side cancellations
        "server_stats_before": before_stats,
        "server_stats_after": after_stats,
        "server_status_delta": {k: v - before_stats.get("status", {}).get(k, 0)
                                for k, v in after_stats.get("status", {}).items()},
        "server_cpus": cpus,
        "client_cpus": client_cpus,
    }


def summarize(out_dir):
    runs = []
    for name in sorted(os.listdir(os.path.join(out_dir, "runs"))):
        with open(os.path.join(out_dir, "runs", name)) as f:
            runs.append(json.load(f))
    groups = {}
    for r in runs:
        groups.setdefault((r["workload"], r["config"]), []).append(r)

    def med(rs, key):
        vals = [key(r) for r in rs]
        return statistics.median(vals), min(vals), max(vals)

    lines = ["# OTLP Export benchmark summary", "",
             "Medians over the repetitions (min-max in brackets). Generated from runs/*.json by "
             "run_bench.py; see environment.json for the machine, builds and settings. "
             "\"client non-OK\" are calls in flight when ghz's duration ended (cancelled or cut off by "
             "ghz); \"server errors\" are error statuses the receiver returned (CANCELLED excluded).", ""]
    for workload in WORKLOADS:
        rows = [(cfg, rs) for (w, cfg), rs in groups.items() if w == workload]
        if not rows:
            continue
        meta = rows[0][1][0]["workload_meta"]
        lines += [f"## Workload `{workload}`: {meta['bytes']} bytes, {meta['points']} data points "
                  f"({meta['rejected']} rejected) per request", "",
                  "| configuration | req/s | accepted points/s | p50 ms | p90 ms | p99 ms | server CPU cores | "
                  "CPU µs/req | peak RSS MiB | client non-OK | server errors |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for cfg, rs in sorted(rows, key=lambda x: x[0]):
            rps = med(rs, lambda r: r["ok_rps"])
            pts = med(rs, lambda r: r["points_accepted_per_s"])
            p50 = med(rs, lambda r: r["latency_ms"].get("p50", float("nan")))[0]
            p90 = med(rs, lambda r: r["latency_ms"].get("p90", float("nan")))[0]
            p99 = med(rs, lambda r: r["latency_ms"].get("p99", float("nan")))[0]
            cores = med(rs, lambda r: r["server_cpu_cores"])[0]
            us = med(rs, lambda r: r["server_cpu_us_per_request"])[0]
            rss = med(rs, lambda r: r["server_memory_kb"].get("VmHWM", 0) / 1024)[0]
            errs = sum(r["ghz"]["count"] - r["requests_ok"] for r in rs)
            # statuses the server itself returned other than OK; CANCELLED (1) is excluded: those are
            # calls ghz cancelled when its duration ended (they match the `cancellations` counter)
            server_errs = sum(v for r in rs for k, v in r.get("server_status_delta", {}).items()
                              if k not in ("0", "1"))
            lines.append(f"| {cfg} | {rps[0]:,.0f} [{rps[1]:,.0f}-{rps[2]:,.0f}] | {pts[0]:,.0f} | "
                         f"{p50:.2f} | {p90:.2f} | {p99:.2f} | {cores:.2f} | {us:.1f} | {rss:.0f} | {errs} | "
                         f"{server_errs if rs[0]['server'] == 'ezgrpc' else 'n/a'} |")
        lines.append("")
    with open(os.path.join(out_dir, "summary.md"), "w") as f:
        f.write("\n".join(lines))
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ez", action="append", default=[], type=lambda s: tuple(s.split("=", 1)),
                   help="label=path of an otlp_metrics_server build (label 'default' is the baseline)")
    p.add_argument("--grpcgo", help="grpcgo-server binary")
    p.add_argument("--gen", required=True, help="otlp_gen binary")
    p.add_argument("--ghz", default=shutil.which("ghz") or os.path.expanduser("~/go/bin/ghz"))
    p.add_argument("--proto-root", required=True, help="extracted opentelemetry-proto release")
    p.add_argument("--out", required=True)
    p.add_argument("--duration", type=int, default=15)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--connections", type=int, default=16)
    p.add_argument("--loops", type=lambda s: [int(x) for x in s.split(",")], default=[1, 2, 4])
    p.add_argument("--server-cpus", default="0-3,8-11")
    p.add_argument("--client-cpus", default="4-7,12-15")
    p.add_argument("--saturation", action="store_true", default=True,
                   help="add single-core configurations with the client on the other cores")
    p.add_argument("--no-saturation", dest="saturation", action="store_false")
    p.add_argument("--workloads", default=",".join(WORKLOADS))
    p.add_argument("--wal-dir", help="directory for a WAL configuration (removed after each run)")
    p.add_argument("--only", help="regex: run only matching configurations")
    p.add_argument("--summarize-only", action="store_true")
    args = p.parse_args()
    if args.summarize_only:
        print(summarize(args.out))
        return
    if not args.ez:
        p.error("at least one --ez")
    os.makedirs(os.path.join(args.out, "runs"), exist_ok=True)
    os.makedirs(os.path.join(args.out, "workloads"), exist_ok=True)
    with open(os.path.join(args.out, "environment.json"), "w") as f:
        json.dump(environment(args), f, indent=2)

    payloads = {}
    for name in args.workloads.split(","):
        path = os.path.join(args.out, "workloads", f"{name}.bin")
        meta = json.loads(run([args.gen, "--out", path, *WORKLOADS[name]]).stdout)
        meta["generator_args"] = WORKLOADS[name]
        payloads[name] = (path, meta)
    with open(os.path.join(args.out, "workloads", "workloads.json"), "w") as f:
        json.dump({k: v[1] for k, v in payloads.items()}, f, indent=2)

    configs = configurations(args)
    total = args.reps * sum(1 for c in configs for w in payloads if w in c.get("workloads", [w]))
    n = 0
    # repetitions are interleaved (all configurations once, then again) to spread drift
    for rep in range(1, args.reps + 1):
        for workload, (path, meta) in payloads.items():
            for config in configs:
                if workload not in config.get("workloads", [workload]):
                    continue
                n += 1
                result = measure(args, config, workload, meta, path, rep)
                name = f"{workload}__{config['name']}__rep{rep}.json"
                with open(os.path.join(args.out, "runs", name), "w") as f:
                    json.dump(result, f, indent=2)
                print(f"[{n}/{total}] {workload} {config['name']} rep{rep}: {result['ok_rps']:,.0f} req/s, "
                      f"{result['points_accepted_per_s']:,.0f} points/s, p99 {result['latency_ms'].get('p99', 0):.2f} ms, "
                      f"cpu {result['server_cpu_cores']:.2f} cores", flush=True)
    print(summarize(args.out))


if __name__ == "__main__":
    main()
