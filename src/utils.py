import os
import platform
import subprocess
import logging
import re
import time
import json
import math
import random
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Any, Dict


from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from behave.runner import Context

logger = logging.getLogger("bddbench.utils")

# =====================================================================
# VM benchmarks
# =====================================================================

def _run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    logger.debug("Running command: %s", " ".join(cmd))
    p = subprocess.run(cmd, text=True, capture_output=True)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    if p.returncode != 0:
        raise AssertionError(
            f"Command failed ({p.returncode}): {' '.join(cmd)}\n{out}"
        )
    return p


def _get_sut_ssh_target() -> Optional[str]:
    return os.getenv("SUT_SSH")


def _run_on_sut(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    target = _get_sut_ssh_target()
    if target:
        ssh_cmd = ["ssh", target, "--", *cmd]
        logger.debug("Running on SUT (%s): %s", target, " ".join(cmd))
        return _run(ssh_cmd)
    else:
        logger.debug("Running locally (no SUT_SSH): %s", " ".join(cmd))
        return _run(cmd)


def _sut_host_identifier() -> str:
    target = _get_sut_ssh_target()
    if target:
        return target
    return platform.node()

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMG]?)\s*$", re.IGNORECASE)


def _size_to_bytes(value: str) -> int:
    m = _SIZE_RE.match(value)
    if not m:
        raise AssertionError(
            f"Invalid size format: {value!r} (expected e.g. 1K/1M/1G)"
        )
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[suffix]
    return int(num * mult)


# =====================================================================
# Generic Helpers for all influx benchmarks
# =====================================================================

def influx_precision_from_str(p: str) -> WritePrecision:
    """
    used in influx_write and influx_multi_bucket
    maps a String on WritePrecision
    """
    p_lower = p.lower()
    if p_lower == "ns":
        return WritePrecision.NS
    if p_lower == "ms":
        return WritePrecision.MS
    if p_lower == "s":
        return WritePrecision.S
    raise ValueError(f"Unsupported precision: {p}")


def base_timestamp_for_precision(precision: WritePrecision) -> int:
    """
    used in influx_write and influx_multi_bucket
    gives a timestamp for the given precision
    """
    if precision == WritePrecision.NS:
        return time.time_ns()
    if precision == WritePrecision.MS:
        return int(time.time() * 1000)
    return int(time.time())


def build_benchmark_point(
    measurement: str,
    base_ts: int,
    idx: int,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    precision: WritePrecision,
) -> Point:
    """
    used in influx_write and influx_multi_bucket
    creates a Point for the @write benchmarks
    """

    device_id = idx % max(1, tag_cardinality)

    if time_ordering == "out_of_order":
        if precision == WritePrecision.NS:
            jitter = random.randint(-1_000_000_000, 1_000_000_000)
        elif precision == WritePrecision.MS:
            jitter = random.randint(-1000, 1000)
        else:
            jitter = random.randint(-1, 1)
        ts = base_ts + jitter
    else:
        ts = base_ts + idx

    p = Point(measurement).tag("device_id", f"dev-{device_id}")

    p = p.field("value", float(idx)).field("seq", idx)

    if point_complexity == "high":
        p = (
            p.field("aux1", float(idx % 100))
             .field("aux2", math.sin(idx))
             .field("aux3", math.cos(idx))
        )

    p = p.time(ts, precision)
    return p

def scenario_id_from_outfile(outfile: str, prefixes: Sequence[str]) -> str:
    """
    used in: 
    influx_write, 
    influx_query, 
    influx_multi_bucket, 
    influx_delete, 
    influx_user

    extracts a scenario_id from a report-filename
    """
    base = os.path.basename(outfile)
    for prefix in prefixes:
        if base.startswith(prefix) and base.endswith(".json"):
            return base[len(prefix):-len(".json")]
    return ""

def main_influx_is_configured(context: Context) -> bool:
    """
    used in:
    influx_query
    influx_write
    influx_multi
    influx_delete

    checks if context.influxdb.main is configured logically
    """
    influxdb = getattr(context, "influxdb", None)
    if influxdb is None:
        return False
    main = getattr(influxdb, "main", None)
    if main is None:
        return False

    return bool(
        getattr(main, "url", None)
        and getattr(main, "token", None)
        and getattr(main, "org", None)
        and getattr(main, "bucket", None)
    )


def get_main_influx_write_api(
    context: Context,
    create_client_if_missing: bool = True,
) -> Tuple[Optional[InfluxDBClient], Optional[Any]]:
    """
    used in:
    influx_query
    influx_write
    influx_multi_bucket
    influx_delete
    influx_user

    returns a pair for the main InfluxDB instance configured via context.influxdb.main
    optionally creates a client if non exists yet
    """
    main = getattr(context.influxdb, "main", None)

    if main is None:
        return None, None

    client = getattr(main, "client", None)
    write_api = getattr(main, "write_api", None)

    if client is not None and write_api is not None:
        return client, write_api

    if not create_client_if_missing:
        return None, None

    if not main_influx_is_configured(context):
        return None, None

    client = InfluxDBClient(url=main.url, token=main.token, org=main.org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    return client, write_api

def build_write_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
) -> Point:
    """
    used in:
    influx_write

    builds the Point for exporting a generic write benchmark result to the main InfluxDB
    """
    latency_stats = summary.get("latency_stats", {}) or {}
    throughput = summary.get("throughput", {}) or {}

    return (
        Point("bddbench_write_result")
        # tags
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("precision", str(meta.get("precision", "")))
        .tag("point_complexity", str(meta.get("point_complexity", "")))
        .tag("time_ordering", str(meta.get("time_ordering", "")))
        .tag("sut_bucket", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        # fields
        .field("total_points", int(meta.get("total_points", 0)))
        .field("total_batches", int(meta.get("total_batches", 0)))
        .field("total_duration_s", float(meta.get("total_duration_s", 0.0)))
        .field(
            "throughput_points_per_s",
            float(throughput.get("points_per_s") or 0.0),
        )
        .field("error_rate", float(summary.get("error_rate", 0.0)))
        .field("errors_count", int(summary.get("errors_count", 0)))
        .field("latency_min_s", float(latency_stats.get("min") or 0.0))
        .field("latency_max_s", float(latency_stats.get("max") or 0.0))
        .field("latency_avg_s", float(latency_stats.get("avg") or 0.0))
        .field("latency_median_s", float(latency_stats.get("median") or 0.0))
    )

def build_multi_write_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
) -> Point:
    """
    used in:
      influx_multi_bucket

    builds the Point for exporting a multi-bucket write benchmark summary to the main InfluxDB.
    """
    latency_stats = summary.get("latency_stats", {}) or {}
    throughput = summary.get("throughput", {}) or {}

    return (
        Point("bddbench_multi_write_result")
        # tags
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("bucket_prefix", str(meta.get("bucket_prefix", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("precision", str(meta.get("precision", "")))
        .tag("point_complexity", str(meta.get("point_complexity", "")))
        .tag("time_ordering", str(meta.get("time_ordering", "")))
        .tag("sut_bucket_pattern", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        # fields
        .field("bucket_count", int(meta.get("bucket_count", 0)))
        .field("total_points", int(meta.get("total_points", 0)))
        .field("total_duration_s", float(meta.get("total_duration_s", 0.0)))
        .field(
            "throughput_points_per_s",
            float(throughput.get("points_per_s") or 0.0),
        )
        .field("error_rate", float(summary.get("error_rate", 0.0)))
        .field("errors_count", int(summary.get("errors_count", 0)))
        .field("latency_min_s", float(latency_stats.get("min") or 0.0))
        .field("latency_max_s", float(latency_stats.get("max") or 0.0))
        .field("latency_avg_s", float(latency_stats.get("avg") or 0.0))
        .field("latency_median_s", float(latency_stats.get("median") or 0.0))
    )

def build_query_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    total_runs: int,
    errors_count: int,
) -> Point:
    """
    used in:
      influx_query

    builds the Point for exporting a generic query benchmark summary
    to the main InfluxDB.
    """
    ttf_stats = summary.get("time_to_first_result_s", {}) or {}
    total_time_stats = summary.get("total_time_s", {}) or {}
    bytes_stats = summary.get("bytes_returned", {}) or {}
    rows_stats = summary.get("rows_returned", {}) or {}
    throughput = summary.get("throughput", {}) or {}
    error_rate = float(summary.get("error_rate", 0.0))

    def _f(d: Dict[str, Any], key: str) -> float:
        v = d.get(key)
        return float(v) if v is not None else 0.0

    throughput_bytes_per_s = float(throughput.get("bytes_per_s") or 0.0)
    throughput_rows_per_s = float(throughput.get("rows_per_s") or 0.0)

    return (
        Point("bddbench_query_result")
        # tags
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("time_range", str(meta.get("time_range", "")))
        .tag("query_type", str(meta.get("query_type", "")))
        .tag("result_size", str(meta.get("result_size", "")))
        .tag("output_format", str(meta.get("output_format", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("sut_bucket", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        # fields
        .field("total_runs", int(total_runs))
        .field("errors_count", int(errors_count))
        .field("error_rate", error_rate)
        .field("ttf_min_s", _f(ttf_stats, "min"))
        .field("ttf_max_s", _f(ttf_stats, "max"))
        .field("ttf_avg_s", _f(ttf_stats, "avg"))
        .field("ttf_median_s", _f(ttf_stats, "median"))
        .field("total_time_min_s", _f(total_time_stats, "min"))
        .field("total_time_max_s", _f(total_time_stats, "max"))
        .field("total_time_avg_s", _f(total_time_stats, "avg"))
        .field("total_time_median_s", _f(total_time_stats, "median"))
        .field("bytes_min", _f(bytes_stats, "min"))
        .field("bytes_max", _f(bytes_stats, "max"))
        .field("bytes_avg", _f(bytes_stats, "avg"))
        .field("bytes_median", _f(bytes_stats, "median"))
        .field("rows_min", _f(rows_stats, "min"))
        .field("rows_max", _f(rows_stats, "max"))
        .field("rows_avg", _f(rows_stats, "avg"))
        .field("rows_median", _f(rows_stats, "median"))
        .field("throughput_bytes_per_s", throughput_bytes_per_s)
        .field("throughput_rows_per_s", throughput_rows_per_s)
    )

def build_delete_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    metrics: Any,
    scenario_id: str,
) -> Point:
    """
    used in:
      influx_delete

    builds the Point("bddbench_delete_result") for exporting a generic
    delete benchmark summary to the main InfluxDB.
    """
    points_before = int(
        getattr(metrics, "points_before", summary.get("points_before", 0)) or 0
    )
    points_after = int(
        getattr(metrics, "points_after", summary.get("points_after", 0)) or 0
    )
    deleted_points = points_before - points_after
    expected_points = int(
        meta.get("expected_points", summary.get("expected_points", 0)) or 0
    )
    latency_s = float(
        getattr(metrics, "latency_s", summary.get("latency_s", 0.0)) or 0.0
    )
    status_code = int(
        getattr(metrics, "status_code", summary.get("status_code", 0)) or 0
    )
    ok = bool(getattr(metrics, "ok", points_after == 0))

    return (
        Point("bddbench_delete_result")
        # tags
        .tag("measurement", str(meta.get("measurement", "")))
        .tag("sut_bucket", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        # fields
        .field("points_before", points_before)
        .field("points_after", points_after)
        .field("deleted_points", deleted_points)
        .field("expected_points", expected_points)
        .field("latency_s", latency_s)
        .field("status_code", status_code)
        .field("ok", ok)
    )

def write_json_report(
    outfile: str,
    data: Any,
    logger_: Optional[logging.Logger] = None,
    log_prefix: str = "",
) -> None:
    """
    used in:
    influx_query
    influx_write
    influx_delete
    influx_multi_bucket
    influx_user

    saves JSON reports
    """
    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    if logger_ is not None:
        if log_prefix:
            logger_.info("%s%s", log_prefix, outfile)
        else:
            logger_.info("Stored JSON report to %s", outfile)
