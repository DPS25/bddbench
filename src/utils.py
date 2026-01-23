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
    """
    Defensive: in VM-local checks (INFLUXDB_SKIP_SUT=1), context.influxdb.sut may
    be missing or not initialized. Still return a valid Point with empty tags.
    """
    influxdb = getattr(context, "influxdb", None)
    sut = getattr(influxdb, "sut", None) if influxdb else None
    return (
        Point(measurement)
        .tag("sut_bucket", str(getattr(sut, "bucket", "")) if sut else "")
        .tag("sut_org", str(getattr(sut, "org", "")) if sut else "")
        .tag("sut_influx_url", str(getattr(sut, "url", "")) if sut else "")
    )


def influx_precision_from_str(p: str) -> WritePrecision:
    p_lower = p.lower()
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
    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    if logger_ is not None:
        if log_prefix:
            logger_.info("%s%s", log_prefix, outfile)
        else:
            logger_.info("Stored JSON report to %s", outfile)

