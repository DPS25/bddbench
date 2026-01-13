import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from behave import when, then
from src.utils import (
    _run_on_sut, 
    _sut_host_identifier,
    store_sut_benchmark_result,
)

def _parse_sysbench_cpu(output: str) -> Dict[str, Any]:
    """
    Parse sysbench CPU output.

    Expected to handle output like:

        Number of threads: 4
        Prime numbers limit: 20000
        ...
        CPU speed:
            events per second:   3533.83
        ...
        total time:                          10.0003s
        total number of events:              35340
        ...
        Latency (ms):
             min: 0.10
             avg: 1.13
             max: 18.65
             95th percentile: 2.26
             sum: 39820.80
    """
    result: Dict[str, Any] = {"raw": output}

    m = re.search(r"sysbench\s+([0-9][0-9A-Za-z\.\-\+~:]*)", output)
    if m:
        result["sysbench_version"] = m.group(1)

    m = re.search(r"Number of threads:\s*([0-9]+)", output)
    if m:
        result["threads_reported"] = int(m.group(1))

    m = re.search(r"Prime numbers limit:\s*([0-9]+)", output)
    if m:
        result["max_prime_reported"] = int(m.group(1))

    m = re.search(r"events per second:\s*([0-9.]+)", output)
    if m:
        result["events_per_sec"] = float(m.group(1))

    m = re.search(r"total time:\s*([0-9.]+)s", output)
    if m:
        result["total_time_s"] = float(m.group(1))

    m = re.search(r"total number of events:\s*([0-9]+)", output)
    if m:
        result["total_events"] = int(m.group(1))

    m = re.search(
        r"Latency \(ms\):.*?min:\s*([0-9.]+).*?avg:\s*([0-9.]+).*?"
        r"max:\s*([0-9.]+).*?95th percentile:\s*([0-9.]+).*?sum:\s*([0-9.]+)",
        output,
        re.S,
    )
    if m:
        result["latency_ms"] = {
            "min": float(m.group(1)),
            "avg": float(m.group(2)),
            "max": float(m.group(3)),
            "p95": float(m.group(4)),
            "sum": float(m.group(5)),
        }

    if "events_per_sec" not in result:
        raise AssertionError(
            "Could not parse sysbench CPU 'events per second' from output. "
            "Maybe sysbench output format changed; check 'raw' in parsed result."
        )

    return result

@when(
    "I run a sysbench cpu benchmark with max prime {max_prime:d}, "
    "threads {threads:d} and time limit {time_limit_s:d} seconds"
)
def step_run_sysbench_cpu(context, max_prime: int, threads: int, time_limit_s: int):
    cmd = [
        "sysbench",
        "cpu",
        f"--cpu-max-prime={max_prime}",
        f"--threads={threads}",
        f"--time={time_limit_s}",
        "run",
    ]

    proc = _run_on_sut(cmd)
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    parsed = _parse_sysbench_cpu(output)

    context.cpu_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": _sut_host_identifier(),
        "env_name": os.getenv("ENV_NAME"),
        "params": {
            "max_prime": max_prime,
            "threads": threads,
            "time_limit_s": time_limit_s,
        },
        "result": parsed,
    }


@then('I store the cpu benchmark result as "{report_path}"')
def step_store_cpu_result(context, report_path: str):
    store_sut_benchmark_result(
        context,
        report_path=report_path,
        context_attr="cpu_benchmark",
        bench_type="cpu",
    )
