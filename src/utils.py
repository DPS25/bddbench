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
from influxdb_client.rest import ApiException
from behave.runner import Context

logger = logging.getLogger("bddbench.utils")

# =====================================================================
# VM / SUT helpers
# =====================================================================

def _run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    logger.debug("Running command: %s", " ".join(cmd))
    p = subprocess.run(cmd, text=True, capture_output=True)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    if p.returncode != 0:
        raise AssertionError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n{out}")
    return p


_IP_RE = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b")


def _get_sut_ssh_target() -> Optional[str]:
    """
    Returns SSH target used to run commands on SUT.

    Priority:
      1) SUT_SSH env (e.g. "nixos@192.168.1.10" or "192.168.1.10")
      2) parse IP from INFLUXDB_SUT_URL and assume "nixos@<ip>"
      3) None (run locally)
    """
    t = (os.getenv("SUT_SSH") or "").strip()
    if t:
        return t

    url = (os.getenv("INFLUXDB_SUT_URL") or "").strip()
    if not url:
        return None

    m = _IP_RE.search(url)
    if not m:
        return None

    ip = m.group(1)
    return f"nixos@{ip}"


def _run_on_sut(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    target = _get_sut_ssh_target()
    if target:
        ssh_cmd = ["ssh", target, "--", *cmd]
        logger.debug("Running on SUT (%s): %s", target, " ".join(cmd))
        return _run(ssh_cmd)

    logger.debug("Running locally (no SUT SSH target): %s", " ".join(cmd))
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
        raise AssertionError(f"Invalid size format: {value!r} (expected e.g. 1K/1M/1G)")
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[suffix]
    return int(num * mult)


# =====================================================================
# Generic helpers for all Influx benchmarks
# =====================================================================

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


def _env_tag(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def generate_base_point(context: Context, measurement: str) -> Point:
    """
    Create Point(measurement) with standard tags (SUT + BDD meta).
    """
    point = Point(measurement)
    return add_tags(context, point, {})



def add_tags(context: Context, point: Point, extra_tags: Dict[str, str]) -> Point:
    sut = context.influxdb.sut

    # core SUT tags
    point = (
        point
        .tag("sut_version", str(getattr(sut, "version", "")))
        .tag("sut_commit", str(getattr(sut, "commit", "")))
        .tag("sut_bucket", str(getattr(sut, "bucket", "")))
        .tag("sut_org", str(getattr(sut, "org", "")))
        .tag("sut_influx_url", str(getattr(sut, "url", "")))
        .tag("sut_host", str(getattr(sut, "host", "")))
    )

    # pipeline / regression tags (optional but highly recommended)
    env_name = os.getenv("ENV_NAME") or ""
    git_sha = os.getenv("BDD_GIT_SHA") or ""
    git_ref = os.getenv("BDD_GIT_REF") or ""
    pipeline_id = os.getenv("BDD_PIPELINE_ID") or ""
    suite_run_id = os.getenv("BDD_SUITE_RUN_ID") or ""

    if env_name:
        point = point.tag("env_name", env_name)
    if git_sha:
        point = point.tag("git_sha", git_sha)
    if git_ref:
        point = point.tag("git_ref", git_ref)
    if pipeline_id:
        point = point.tag("pipeline_id", pipeline_id)
    if suite_run_id:
        point = point.tag("suite_run_id", suite_run_id)

    # scenario run id from behave context (set in before_scenario)
    run_id = getattr(context, "run_id", None) or getattr(context, "scenario_run_id", None)
    if run_id:
        point = point.tag("scenario_run_id", str(run_id))

    # user provided extra tags
    for k, v in (extra_tags or {}).items():
        if v is None:
            continue
        s = str(v)
        if s != "":
            point = point.tag(k, s)

    return point




def influx_precision_from_str(p: str) -> WritePrecision:
    p_lower = (p or "").lower()
    if p_lower == "ns":
        return WritePrecision.NS
    if p_lower == "ms":
        return WritePrecision.MS
    if p_lower == "s":
        return WritePrecision.S
    raise ValueError(f"Unsupported precision: {p}")


def base_timestamp_for_precision(precision: WritePrecision) -> int:
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

    pc = (point_complexity or "").lower()
    if pc in {"medium", "high"}:
        p = p.field("aux1", float(idx % 100))
    if pc == "high":
        p = p.field("aux2", math.sin(idx)).field("aux3", math.cos(idx))

    p = p.time(ts, precision)
    return p


def scenario_id_from_outfile(outfile: str, prefixes: Sequence[str]) -> str:
    base = os.path.basename(outfile)
    for prefix in prefixes:
        if base.startswith(prefix) and base.endswith(".json"):
            return base[len(prefix):-len(".json")]
    return ""


def main_influx_is_configured(context: Context) -> bool:
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
    main = getattr(getattr(context, "influxdb", None), "main", None)
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

    # cache back
    main.client = client
    main.write_api = write_api

    return client, write_api


def write_json_report(
    outfile: str,
    data: Any,
    logger_: Optional[logging.Logger] = None,
    log_prefix: str = "",
) -> None:
    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    if logger_ is not None:
        if log_prefix:
            logger_.info("%s%s", log_prefix, outfile)
        else:
            logger_.info("Stored JSON report to %s", outfile)


# =====================================================================
# SUT host benchmark export (keep your colleague's functionality)
# =====================================================================

def write_sut_benchmark_to_main_influx(
    context: Context,
    *,
    bench_type: str,
    data: Dict[str, Any],
    measurement: str = "sut_host_benchmark",
    strict: bool = False,
    logger_: Optional[logging.Logger] = None,
) -> None:
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

