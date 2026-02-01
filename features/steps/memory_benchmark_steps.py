import json
import os
import re
import socket
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from behave import when, then

from src.utils import (
    _run_on_sut,
    _size_to_bytes,
    store_sut_benchmark_result,
    write_json_report,   
)


def _guess_sut_host() -> Optional[str]:
    """
    Derive SUT host without requiring context.influxdb.

    Priority:
      1) INFLUXDB_SUT_URL -> hostname
      2) SUT_SSH -> host part of user@host
      3) None
    """
    sut_url = (os.getenv("INFLUXDB_SUT_URL") or "").strip()
    if sut_url:
        try:
            host = urlparse(sut_url).hostname
            if host:
                return host
        except Exception:
            pass

    sut_ssh = (os.getenv("SUT_SSH") or "").strip()
    if sut_ssh:
        # "user@host" or "host"
        host = sut_ssh.split("@")[-1].strip()
        return host or None

    return None


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


@when(
    'I run a sysbench memory benchmark with mode "{mode}", access mode "{access_mode}", '
    'block size "{block_size}", total size "{total_size}", threads {threads:d} and '
    "time limit {time_limit_s:d} seconds"
)
def step_run_sysbench_memory(context, mode, access_mode, block_size, total_size, threads, time_limit_s):
    if mode not in ("read", "write"):
        raise AssertionError(f"mode must be 'read' or 'write', got: {mode!r}")
    if access_mode not in ("seq", "rnd"):
        raise AssertionError(f"access_mode must be 'seq' or 'rnd', got: {access_mode!r}")

    cmd = [
        "sysbench",
        "memory",
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

    sut_host = _guess_sut_host() or socket.gethostname()

    context.memory_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        # ✅ 不依赖 context.influxdb（CI @memory 跑得过）
        "host": sut_host,
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
    """
    If Influx init was skipped for this feature (common for @memory),
    then context.influxdb does not exist. In that case, just write JSON report.

    If Influx is available, keep the existing behavior.
    """
    if not hasattr(context, "influxdb"):
        data = getattr(context, "memory_benchmark", None)
        if not isinstance(data, dict):
            raise AssertionError("No memory benchmark found in context (did the When step run?)")

        write_json_report(
            report_path,
            data,
            logger_=None,
            log_prefix="Stored memory benchmark result to ",
        )
        return

    store_sut_benchmark_result(
        context,
        report_path=report_path,
        context_attr="memory_benchmark",
        bench_type="memory",
        measurement="bddbench_memory_result",
    )
