import json
import os
import platform
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from behave import given, when, then
from influxdb_client import Point, WritePrecision


_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMG]?)\s*$", re.IGNORECASE)

def _size_to_bytes(value: str) -> int:
    m = _SIZE_RE.match(value)
    if not m:
        raise AssertionError(f"Invalid size format: {value!r} (expected e.g. 1K/1M/1G)")
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[suffix]
    return int(num * mult)

def _run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, text=True, capture_output=True)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    if p.returncode != 0:
        raise AssertionError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n{out}")
    return out

def _parse_sysbench_memory(output: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"raw": output}

    m = re.search(r"sysbench\s+([0-9][0-9A-Za-z\.\-\+~:]*)", output)
    if m:
        result["sysbench_version"] = m.group(1)

    m = re.search(r"([0-9.]+)\s+MiB\s+transferred\s+\(([0-9.]+)\s+MiB/sec\)", output)
    if m:
        result["mib_transferred"] = float(m.group(1))
        result["throughput_mib_s"] = float(m.group(2))

    m = re.search(r"total time:\s*([0-9.]+)s", output)
    if m:
        result["total_time_s"] = float(m.group(1))

    m = re.search(r"total number of events:\s*([0-9]+)", output)
    if m:
        result["events"] = int(m.group(1))

    m = re.search(r"events per second:\s*([0-9.]+)", output)
    if m:
        result["events_per_sec"] = float(m.group(1))

    m = re.search(
        r"Latency\s*\(ms\):\s*min:\s*([0-9.]+)\s*avg:\s*([0-9.]+)\s*max:\s*([0-9.]+)\s*95th percentile:\s*([0-9.]+)\s*sum:\s*([0-9.]+)",
        output,
    )
    if m:
        result["latency_ms"] = {
            "min": float(m.group(1)),
            "avg": float(m.group(2)),
            "max": float(m.group(3)),
            "p95": float(m.group(4)),
            "sum": float(m.group(5)),
        }

    if "throughput_mib_s" not in result:
        raise AssertionError(
            "Could not parse sysbench throughput from output. "
            "Maybe sysbench output format changed; check 'raw' in parsed result."
        )

    return result


@given("sysbench is installed")
def step_sysbench_installed(context):
    try:
        _run(["sysbench", "--version"])
    except Exception as e:
        raise AssertionError("sysbench not found. Add pkgs.sysbench to flake.nix") from e


@when('I run a sysbench memory benchmark with mode "{mode}", access mode "{access_mode}", block size "{block_size}", total size "{total_size}", threads {threads:d} and time limit {time_limit_s:d} seconds')
def step_run_sysbench_memory(context, mode, access_mode, block_size, total_size, threads, time_limit_s):
    if mode not in ("read", "write"):
        raise AssertionError(f"mode must be 'read' or 'write', got: {mode!r}")
    if access_mode not in ("seq", "rnd"):
        raise AssertionError(f"access_mode must be 'seq' or 'rnd', got: {access_mode!r}")

    cmd = [
        "sysbench", "memory",
        f"--memory-oper={mode}",
        f"--memory-access-mode={access_mode}",
        f"--memory-block-size={block_size}",
        f"--memory-total-size={total_size}",
        f"--threads={threads}",
        f"--time={time_limit_s}",
        "run",
    ]

    output = _run(cmd)
    parsed = _parse_sysbench_memory(output)

    context.memory_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "env_name": os.getenv("ENV_NAME"),
        "params": {
            "mode": mode,
            "access_mode": access_mode,
            "block_size": block_size,
            "total_size": total_size,
            "threads": threads,
            "time_limit_s": time_limit_s,
            "block_size_bytes": _size_to_bytes(block_size),
            "total_size_bytes": _size_to_bytes(total_size),
        },
        "result": parsed,
    }


@then('I store the memory benchmark result as "{report_path}"')
def step_store_memory_result(context, report_path):
    data = getattr(context, "memory_benchmark", None)
    if not data:
        raise AssertionError("No memory benchmark found in context (did the When step run?)")

    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


@then('I write the memory benchmark result to main influx measurement "{measurement}"')
def step_write_to_main_influx(context, measurement):
    data = getattr(context, "memory_benchmark", None)
    if not data:
        raise AssertionError("No memory benchmark found in context (did the When step run?)")

    # requires your existing features/environment.py to have set:
    # context.influxdb.main.write_api, context.influxdb.main.bucket, context.influxdb.main.org
    main = context.influxdb.main

    params = data["params"]
    res = data["result"]
    latency = res.get("latency_ms", {}) or {}

    p = (
        Point(measurement)
        .tag("host", data.get("host") or "")
        .tag("env_name", data.get("env_name") or "")
        .tag("mode", params["mode"])
        .tag("access_mode", params["access_mode"])
        .tag("block_size", params["block_size"])
        .tag("total_size", params["total_size"])
        .tag("threads", str(params["threads"]))
        .field("throughput_mib_s", float(res["throughput_mib_s"]))
        .field("mib_transferred", float(res.get("mib_transferred", 0.0)))
        .field("events", int(res.get("events", 0)))
        .field("events_per_sec", float(res.get("events_per_sec", 0.0)))
        .field("latency_avg_ms", float(latency.get("avg", 0.0)))
        .field("latency_p95_ms", float(latency.get("p95", 0.0)))
        .field("total_time_s", float(res.get("total_time_s", 0.0)))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )

    main.write_api.write(bucket=main.bucket, org=main.org, record=p)
