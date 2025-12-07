#!/usr/bin/env python3
"""
Standalone storage stress tool.

Wraps `fio` with a common CLI contract so it can be orchestrated alongside
other stress tools.

Usage (examples):

  # random read/write, 10 minutes, 8 GiB file, default target dir
  python storage_stress.py \
    --duration 600 \
    --profile rand-rw \
    --file-size-gb 8 \
    --out-file reports/storage-randrw.json

  # sequential write on a specific directory
  python storage_stress.py \
    --duration 300 \
    --profile seq-write \
    --file-size-gb 4 \
    --target /var/lib/influxdb \
    --jobs 4 \
    --iodepth 16 \
    --block-size 256k \
    --out-file reports/storage-seqwrite.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# data structures 

@dataclass(frozen=True)
class StorageStressDefaults:
    target: str = "/var/lib/influxdb"
    file_size_gb: int = 8
    jobs: int = 4
    iodepth: int = 16
    block_size: str = "4k"

DEFAULTS = StorageStressDefaults()

@dataclass
class StorageStressConfig:
    duration: int                # seconds
    profile: str                 # seq-read, seq-write, rand-read, rand-write, rand-rw
    out_file: Path               # JSON report path

    target: str = DEFAULTS.target              # directory to place test file(s)
    file_size_gb: int = DEFAULTS.file_size_gb  # total file size per job
    jobs: int = DEFAULTS.jobs
    iodepth: int = DEFAULTS.iodepth
    block_size: str = DEFAULTS.block_size
    intensity: Optional[str] = None            # "low", "medium", "high": informational
    extra: Optional[Dict[str, Any]] = None     # arbitrary key, value pairs for metadata only



# fio command and parsing

def profile_to_fio_args(profile: str) -> List[str]:
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
        # 70% read / 30% write as  default
        return ["--rw=randrw", "--rwmixread=70"]

    raise ValueError(f"Unknown profile: {profile!r}")


def build_fio_command(cfg: StorageStressConfig) -> List[str]:
    """
    Build the fio command line based on the config.
    """
    directory = Path(cfg.target)

    cmd = [
        "fio",
        "--name=storage-stress",
        f"--directory={directory}",
        f"--size={cfg.file_size_gb}G",
        f"--runtime={cfg.duration}",
        "--time_based",
        f"--numjobs={cfg.jobs}",
        f"--iodepth={cfg.iodepth}",
        f"--bs={cfg.block_size}",
        "--ioengine=libaio",
        "--direct=1",          # bypass page cache, hit real storage
        "--group_reporting",
        "--output-format=json",
        "--output=-",          # write JSON to stdout
    ]

    cmd.extend(profile_to_fio_args(cfg.profile))
    return cmd


def run_fio(cfg: StorageStressConfig) -> Dict[str, Any]:
    """
    Run fio with the given configuration and return the parsed JSON output.
    Raises CalledProcessError if fio exits with non-zero status.
    """
    cmd = build_fio_command(cfg)

    # basic logging
    print(f"[storage-stress] Running: {' '.join(str(c) for c in cmd)}", file=sys.stderr)

    completed = subprocess.run( # run fio command
        cmd,
        check=True,
        text=True,
        capture_output=True,
    )
    # If fio prints diagnostics to stderr, forward them so CI logs show them
    if completed.stderr:
        print(completed.stderr, file=sys.stderr, end="")

    fio_json = json.loads(completed.stdout)
    return fio_json


def extract_metrics(fio_json: Dict[str, Any]) -> Dict[str, Any]:
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
        return {}

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


# CLI and main

def parse_extra(extra_list: Optional[List[str]]) -> Optional[Dict[str, str]]:
    """
    Parse --extra key=value,key2=value2 strings into a dict.
    This is *metadata only* for now, not passed to fio.
    """
    if not extra_list:
        return None

    result: Dict[str, str] = {}
    for item in extra_list:
        # allow comma-separated: "foo=bar,baz=qux"
        parts = [p.strip() for p in item.split(",") if p.strip()]
        for part in parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            result[key.strip()] = value.strip()
    return result or None


def parse_args(argv: Optional[List[str]] = None) -> StorageStressConfig:
    parser = argparse.ArgumentParser(
        description="Standalone storage stress tool wrapping fio."
    )

    parser.add_argument(
        "--duration",
        type=int,
        required=True,
        help="Duration of the stress test in seconds.",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Logical profile (e.g. seq-read, seq-write, rand-read, rand-write, rand-rw).",
    )
    parser.add_argument(
        "--out-file",
        required=True,
        help="Path to JSON report file.",
    )

    parser.add_argument(
        "--target",
        default=DEFAULTS.target,
        help="Target directory to place fio job files (default: /var/lib/influxdb).",
    )
    parser.add_argument(
        "--file-size-gb",
        type=int,
        default=DEFAULTS.file_size_gb,
        help="Size of the fio test file per job in GiB (default: 8).",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=DEFAULTS.jobs,
        help="Number of parallel fio jobs (default: 4).",
    )
    parser.add_argument(
        "--iodepth",
        type=int,
        default=DEFAULTS.iodepth,
        help="IO depth per job (default: 16).",
    )
    parser.add_argument(
        "--block-size",
        default=DEFAULTS.block_size,
        help="Block size for IO (default: 4k).",
    )
    parser.add_argument(
        "--intensity",
        default=None,
        help="Optional intensity label (e.g. low, medium, high) – currently informational.",
    )
    parser.add_argument(
        "--extra",
        action="append",
        help="Additional metadata as key=value or comma-separated list; "
             "can be passed multiple times.",
    )

    args = parser.parse_args(argv)

    extra = parse_extra(args.extra)

    return StorageStressConfig(
        duration=args.duration,
        profile=args.profile,
        out_file=Path(args.out_file),
        target=args.target,
        file_size_gb=args.file_size_gb,
        jobs=args.jobs,
        iodepth=args.iodepth,
        block_size=args.block_size,
        intensity=args.intensity,
        extra=extra,
    )


def main(argv: Optional[List[str]] = None) -> int:
    try:
        cfg = parse_args(argv)
        start_time = datetime.now(timezone.utc).isoformat()

        fio_json = run_fio(cfg)

        end_time = datetime.now(timezone.utc).isoformat()
        metrics = extract_metrics(fio_json)

        report = {
            "stress_type": "storage",
            "tool": "fio",
            "start_time": start_time,
            "end_time": end_time,
            "parameters": asdict(cfg),
            "metrics": metrics,
            # include raw fio JSON for later analysis if needed
            "raw": fio_json,
        }

        cfg.out_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.out_file.write_text(json.dumps(report, indent=2))
        print(f"[storage-stress] Wrote report to {cfg.out_file}", file=sys.stderr)
        return 0

    except FileNotFoundError as e:
        print(f"[storage-stress] ERROR: {e}. Is `fio` installed and in PATH?", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as e:
        print(f"[storage-stress] ERROR: fio exited with code {e.returncode}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr, end="")
        return e.returncode
    except Exception as e:
        print(f"[storage-stress] ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
