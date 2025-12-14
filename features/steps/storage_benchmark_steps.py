import json
import os
import platform
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from behave import given, when, then


# helpers

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMG]?)\s*$", re.IGNORECASE)


def _size_to_bytes(value: str) -> int:
    """
    Convert a size string like '4G', '1M', '4K' to a byte count.
    Used only for metadata in the report.
    """
    m = _SIZE_RE.match(value)
    if not m:
        raise AssertionError(f"Invalid size format: {value!r} (expected e.g. 1K/1M/1G)")
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[suffix]
    return int(num * mult)

def _get_sut_ssh_target() -> str:
    """
    Determine where to run fio (on the SUT) via SSH.

    Expected env vars (in .env, loaded by environment.py):
      - SUT_SSH: hostname or IP of the SUT (e.g. 192.168.8.34)
    """
    host = os.getenv("SUT_SSH")

    if not host:
        raise AssertionError(
            "SUT_SSH is not set. "
            "Add it to your .env so we know which SUT to run fio on."
        )

    return host


def _run_on_sut(cmd: list[str]) -> subprocess.CompletedProcess:
    """
    Run a command on the SUT via ssh and return the CompletedProcess.
    """
    ssh_target = _get_sut_ssh_target()
    ssh_cmd = ["ssh", ssh_target, "--"] + cmd
    return _run(ssh_cmd)


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    """
    Run a command, capture stdout/stderr, and raise AssertionError on non-zero exit.
    We keep stdout and stderr separate, since stdout is JSON for fio.
    """
    p = subprocess.run(cmd, text=True, capture_output=True)
    if p.returncode != 0:
        out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        raise AssertionError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n{out}")
    return p


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

    # Build fio command
    cmd: List[str] = [
        "fio",
        "--name=storage-benchmark",
        f"--directory={target_dir}",
        f"--size={file_size}",              # e.g. 4G
        f"--runtime={time_limit_s}",
        "--time_based",
        f"--numjobs={jobs}",
        f"--iodepth={iodepth}",
        f"--bs={block_size}",               # e.g. 4k / 1M
        "--ioengine=libaio",
        "--direct=1",                       # bypass page cache
        "--group_reporting",
        "--output-format=json",
        "--output=-",                       # write JSON to stdout
        "--status-interval=0",              # disable progress output
    ]
    cmd.extend(_profile_to_fio_args(profile))

    completed = _run_on_sut(cmd)

    if not completed.stdout.strip().startswith("{"):
        raise AssertionError(
            f"fio did not return JSON. Raw output:\n{completed.stdout}"
        )

    try:
        fio_json = json.loads(completed.stdout)
    except json.JSONDecodeError as e:
        raise AssertionError(
            "Failed to parse fio JSON output. "
            "Check that --output-format=json is supported and stdout is valid JSON."
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

    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
