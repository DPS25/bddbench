import os
import platform
import subprocess
import logging
import re
import time
import json
import math
import random
from datetime import datetime, timezone
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
    return  re.search(r"\d+\.\d+\.\d+\.\d+", os.getenv("INFLUXDB_SUT_URL")).group()

def _run_on_sut(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    target = _get_sut_ssh_target()
    if target:
        ssh_cmd = ["ssh", f'nixos@{target}', "--", *cmd]
        logger.debug("Running on SUT (%s): %s", target, " ".join(cmd))
        return _run(ssh_cmd)
    else:
        logger.debug("Running locally (no SUT_SSH): %s", " ".join(cmd))
        return _run(cmd)

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

def write_sut_benchmark_to_main_influx(
    context: Context,
    *,
    bench_type: str,
    data: Dict[str, Any],
    measurement: str = "sut_host_benchmark",
    strict: bool = False,
    logger_: Optional[logging.Logger] = None,
) -> None:
    """
    Writes a SUT Host-Benchmark result into Influx

    expected data format:
      {
        "timestamp_utc": "...",
        "host": "...",
        "env_name": "...",
        "params": {...},
        "result": {...},
      }
    """
    log = logger_ or logger

    client, write_api = get_main_influx_write_api(context, create_client_if_missing=True)
    if client is None or write_api is None:
        log.info("MAIN InfluxDB not configured -> skip SUT %s export", bench_type)
        return

    main = context.influxdb.main
    bucket = getattr(main, "bucket", None)
    org = getattr(main, "org", None)
    if not bucket or not org:
        log.info("MAIN InfluxDB bucket/org misses -> skip SUT %s export", bench_type)
        return

    point = generate_base_point(context, measurement).tag("bench_type", str(bench_type))

    if data.get("host"):
        point = point.tag("host", str(data["host"]))
    if data.get("env_name"):
        point = point.tag("env_name", str(data["env_name"]))

    params = data.get("params") or {}
    if isinstance(params, dict):
        for k, v in params.items():
            if v is None:
                continue
            if isinstance(v, (str, int, float, bool)):
                point = point.tag(f"param_{k}", str(v))

    fields: Dict[str, Any] = {}

    def _walk(obj: Any, prefix: str = "") -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "raw":
                    continue
                key = f"{prefix}_{k}" if prefix else str(k)
                _walk(v, key)
        elif isinstance(obj, (int, float, bool)):
            if prefix:
                fields[prefix] = obj
        else:
            return

    _walk(data.get("result") or {})

    for k, v in fields.items():
        point = point.field(k, v)

    ts = data.get("timestamp_utc")
    if isinstance(ts, str) and ts.strip():
        iso = ts.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        point = point.time(dt, WritePrecision.NS)

    write_to_influx(
        write_api=write_api,
        bucket=bucket,
        org=org,
        record=point,
        logger_=log,
        strict=strict,
        success_msg=f"Exported SUT {bench_type} benchmark to MAIN Influx",
        failure_prefix=f"SUT {bench_type} export failed",
    )


def store_sut_benchmark_result(
    context: Context,
    *,
    report_path: str,
    context_attr: str,
    bench_type: str,
    measurement: str = "sut_host_benchmark",
    strict: bool = False,
) -> None:
    """
    writes data report and gets data from context.<context_attr>
    """
    data = getattr(context, context_attr, None)
    if not data:
        raise AssertionError(
            f"No {bench_type} benchmark found in context.{context_attr} (did the When step run?)"
        )

    write_json_report(report_path, data)
    write_sut_benchmark_to_main_influx(
        context,
        bench_type=bench_type,
        data=data,
        measurement=measurement,
        strict=strict,
    )

# =====================================================================
# Generic Helpers for all influx benchmarks
# =====================================================================
from influxdb_client.rest import ApiException

def write_to_influx(
    *,
    write_api: Any,
    bucket: str,
    org: str,
    record: Any,
    logger_: Optional[logging.Logger] = None,
    strict: bool = False,
    success_msg: str | None = None,
    failure_prefix: str = "Influx write failed",
) -> None:
    try:
        write_api.write(bucket=bucket, org=org, record=record)
        if logger_ and success_msg:
            logger_.info(success_msg)
    except ApiException as exc:
        msg = f"{failure_prefix}: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        if logger_:
            logger_.warning(msg)
    except Exception as exc:
        msg = f"{failure_prefix}: {exc}"
        if strict:
            raise
        if logger_:
            logger_.warning(msg)


def generate_base_point(context: Context, measurement: str) -> Point:
    point = Point(measurement)
    return add_tags(context, point, {})


def add_tags(context: Context, point: Point, extra_tags: Dict[str, str]) -> Point:
    sut = context.influxdb.sut
    (point.tag("sut_version", str(getattr(sut, "version", "")))
     .tag("sut_commit", str(getattr(sut, "commit", "")))
     .tag("sut_bucket", str(getattr(sut, "bucket", "")))
     .tag("sut_org", str(getattr(sut, "org", "")))
     .tag("sut_influx_url", str(getattr(sut, "url", "")))
     .tag("sut_host", str(getattr(sut, "host", ""))))
    for k, v in extra_tags.items():
        point = point.tag(k, v)
    return point

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
    run_id: Optional[str] = None,
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
    if run_id:
        p = p.tag("run_id", str(run_id))

    p = p.field("value", float(idx)).field("seq", idx)

    # Feature files currently use low/medium/high. Keep backwards compatibility:
    # - medium: one extra field
    # - high:   several extra fields
    pc = (point_complexity or "").lower()
    if pc in {"medium", "high"}:
        p = p.field("aux1", float(idx % 100))
    if pc == "high":
        p = p.field("aux2", math.sin(idx)).field("aux3", math.cos(idx))

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
