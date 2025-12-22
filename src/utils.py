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
from influxdb_client.rest import ApiException
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


def generate_base_point(
    *,
    context: Context,
    measurement: str,
    scenario_id: str,
) -> Point:
    """Create a base Point with common tags used across all benchmark result exports."""
    p = Point(measurement).tag("scenario_id", scenario_id or "")

    # SUT metadata (where the benchmark ran)
    sut = getattr(getattr(context, "influxdb", None), "sut", None)
    if sut is not None:
        if getattr(sut, "bucket", None):
            p.tag("sut_bucket", str(sut.bucket))
        if getattr(sut, "org", None):
            p.tag("sut_org", str(sut.org))
        if getattr(sut, "url", None):
            p.tag("sut_influx_url", str(sut.url))

    return p


def export_point_to_main_influx(
    *,
    context: Context,
    point: Point,
    bench_label: str,
    logger_: logging.Logger,
) -> None:
    """Write a pre-built Point into MAIN Influx (if configured).

    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning.
    """
    if not main_influx_is_configured(context):
        logger_.info("MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = os.getenv("INFLUXDB_EXPORT_STRICT", "0").strip().lower() in ("1", "true", "yes")

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger_.info("MAIN write_api missing – skipping export")
        return

    try:
        write_api.write(bucket=main.bucket, org=main.org, record=point)
        logger_.info("Exported %s result to MAIN Influx", bench_label)
    except ApiException as exc:
        msg = f"MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger_.warning(msg)
    except Exception as exc:
        msg = f"MAIN export failed: {exc}"
        if strict:
            raise
        logger_.warning(msg)



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