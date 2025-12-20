import json
import os
import platform
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from behave import given, when, then
from utils import _run_on_sut, _size_to_bytes, write_json_report

def _profile_to_fio_args(profile: str) -> List[str]:
    """
    Map logical profile names to fio --rw / mix options.
    """
    profile = profile.lower()

    if profile in ("seq-read", "sequential-read", "read"):
        return ["--rw=read"]
    if profile in ("seq-write", "sequential-write", "write"):
        return ["--rw=write"]
    if profile in ("rand-read", "random-read"):
        return ["--rw=randread"]
    if profile in ("rand-write", "random-write"):
        return ["--rw=randwrite"]
    if profile in ("rand-rw", "random-rw", "mixed"):
        # 70% read / 30% write as default
        return ["--rw=randrw", "--rwmixread=70"]

    raise AssertionError(f"Unknown fio profile: {profile!r}")


def _extract_fio_metrics(fio_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Summarize key metrics from fio JSON.

    We aggregate over all jobs:
      - read/write bandwidth (KiB/s)
      - read/write IOPS
      - mean completion latency (ns)
      - total bytes read/written
    """
    jobs = fio_json.get("jobs", [])
    if not jobs:
        raise AssertionError("fio JSON did not contain any jobs; check 'raw' output.")

    total_read_bw = 0.0
    total_write_bw = 0.0
    total_read_iops = 0.0
    total_write_iops = 0.0
    total_read_bytes = 0
    total_write_bytes = 0

    read_lat_sum = 0.0
    write_lat_sum = 0.0
    read_lat_count = 0
    write_lat_count = 0

    for job in jobs:
        read = job.get("read", {})
        write = job.get("write", {})

        total_read_bw += float(read.get("bw", 0.0))
        total_write_bw += float(write.get("bw", 0.0))
        total_read_iops += float(read.get("iops", 0.0))
        total_write_iops += float(write.get("iops", 0.0))

        total_read_bytes += int(read.get("bytes", 0))
        total_write_bytes += int(write.get("bytes", 0))

        clat_read = read.get("clat_ns") or {}
        clat_write = write.get("clat_ns") or {}

        if "mean" in clat_read:
            read_lat_sum += float(clat_read["mean"])
            read_lat_count += 1
        if "mean" in clat_write:
            write_lat_sum += float(clat_write["mean"])
            write_lat_count += 1

    metrics: Dict[str, Any] = {
        "jobs": len(jobs),
        "total_read_bytes": total_read_bytes,
        "total_write_bytes": total_write_bytes,
        "read_bw_kib_s": total_read_bw,
        "write_bw_kib_s": total_write_bw,
        "read_iops": total_read_iops,
        "write_iops": total_write_iops,
    }

    if read_lat_count:
        metrics["read_lat_ns_mean"] = read_lat_sum / read_lat_count
    if write_lat_count:
        metrics["write_lat_ns_mean"] = write_lat_sum / write_lat_count

    return metrics


# behave steps

@given("fio is installed")
def step_fio_installed(context) -> None:
    try:
        _run_on_sut(["fio", "--version"])
    
    except Exception as e:
        raise AssertionError(f"SUT command failed: {e}") from e


@when(
    'I run a fio storage benchmark with profile "{profile}", target directory "{target_dir}", '
    'file size "{file_size}", block size "{block_size}", jobs {jobs:d}, '
    "iodepth {iodepth:d} and time limit {time_limit_s:d} seconds"
)
def step_run_fio_storage_benchmark(
    context,
    profile: str,
    target_dir: str,
    file_size: str,
    block_size: str,
    jobs: int,
    iodepth: int,
    time_limit_s: int,
) -> None:
    """
    Build and run a fio command according to the Scenario Outline parameters
    and store parsed results in context.storage_benchmark.
    """

    _run_on_sut(["mkdir", "-p", target_dir])

    # Write JSON to a file on the SUT, then cat it back.
    out_file = f"{target_dir.rstrip('/')}/fio-result.json"

    cmd: List[str] = [
        "fio",
        "--name=storage-benchmark",
        f"--directory={target_dir}",
        f"--size={file_size}",
        f"--runtime={time_limit_s}",
        "--time_based",
        f"--numjobs={jobs}",
        f"--iodepth={iodepth}",
        f"--bs={block_size}",
        "--ioengine=libaio",
        "--direct=1",
        "--group_reporting",
        "--output-format=json",
        f"--output={out_file}",          # <-- CHANGED: JSON goes to file
        "--eta=never",
        "--status-interval=999999",
    ]
    cmd.extend(_profile_to_fio_args(profile))

    # Run fio on the SUT
    _run_on_sut(cmd)

    # Fetch JSON from the SUT file
    completed = _run_on_sut(["cat", out_file])

    raw = (completed.stdout or "") + (("\n" + completed.stderr) if completed.stderr else "")
    if not (completed.stdout or "").lstrip().startswith("{"):
        raise AssertionError(f"fio did not return JSON. Raw output:\n{raw}")

    try:
        fio_json = json.loads(completed.stdout)
    except json.JSONDecodeError as e:
        raise AssertionError(
            f"Failed to parse fio JSON output. Raw output:\n{raw}"
        ) from e

    metrics = _extract_fio_metrics(fio_json)

    context.storage_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "env_name": os.getenv("ENV_NAME"),
        "params": {
            "profile": profile,
            "target_dir": target_dir,
            "file_size": file_size,
            "block_size": block_size,
            "jobs": jobs,
            "iodepth": iodepth,
            "time_limit_s": time_limit_s,
            "file_size_bytes": _size_to_bytes(file_size),
            "block_size_bytes": _size_to_bytes(block_size),
        },
        "result": {
            "metrics": metrics,
            "raw": fio_json,
        },
    }


@then('I store the storage benchmark result as "{report_path}"')
def step_store_storage_result(context, report_path: str) -> None:
    data: Optional[Dict[str, Any]] = getattr(context, "storage_benchmark", None)
    if not data:
        raise AssertionError(
            "No storage benchmark found in context (did the When step run?)."
        )

    write_json_report(report_path,data)
