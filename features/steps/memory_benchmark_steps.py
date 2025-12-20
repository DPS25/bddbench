import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from behave import given, when, then

from utils import _run_on_sut, _sut_host_identifier, _size_to_bytes, write_json_report

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
        _run_on_sut(["sysbench", "--version"])
    except Exception as e:
        raise AssertionError("sysbench not found on SUT.Install sysbench there or add pkgs.sysbench to flake.nix") from e


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

    completed = _run_on_sut(cmd)
    output = (completed.stdout or "") + (("\n" + completed.stderr) if completed.stderr else "")
    parsed = _parse_sysbench_memory(output)

    context.memory_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": _sut_host_identifier(),
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

    write_json_report(report_path,data)
