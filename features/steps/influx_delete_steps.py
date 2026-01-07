import logging
import os
import time
from dataclasses import dataclass, asdict
from typing import Dict, Any

from behave import when, then
from behave.runner import Context
from influxdb_client import Point
from influxdb_client.rest import ApiException

from src.utils import (
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
    write_to_influx, generate_base_point,
)

logger = logging.getLogger("bddbench.influx_delete_steps")


# ---------- Datatype ----------


@dataclass
class DeleteRunMetrics:
    """Simple container for a single delete operation."""
    latency_s: float
    status_code: int
    ok: bool
    points_before: int
    points_after: int


# ---------- Helpers ----------


def _count_points_for_measurement(
    context: Context,
    measurement: str,
) -> int:
    """
    Counts all points in the SUT bucket for the given measurement.

    Uses the SUT query_api from context.influxdb.sut and counts over all time
    (range(start: 0)).
    """
    bucket = context.influxdb.sut.bucket
    org = context.influxdb.sut.org
    query_api = context.influxdb.sut.query_api

    flux = f"""
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["_field"] == "value")
  |> count()
"""
    # NOTE: query_api returns tables -> sum their counts
    tables = query_api.query(org=org, query=flux)
    total = 0
    for table in tables:
        for record in table.records:
            v = record.get_value()
            try:
                total += int(v)
            except (TypeError, ValueError):
                pass
    return total

def build_delete_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    context: Context
) -> Point:

    p = generate_base_point(context=context, measurement="bddbench_delete_result")
    return (
        p
        .tag("measurement", str(meta.get("measurement", "")))
        .tag("scenario_id", scenario_id or "")
        .field("points_before", int(summary.get("points_before", 0)))
        .field("points_after", int(summary.get("points_after", 0)))
        .field("deleted_points", int(summary.get("deleted_points", 0)))
        .field("latency_s", float(summary.get("latency_s", 0.0)))
        .field("ok", bool(summary.get("ok", False)))
        .field("status_code", int(summary.get("status_code", 0)))
    )

def _export_delete_result_to_main_influx(
    context: Context,
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    outfile: str,
) -> None:
    """
    Sends a compact summary of the delete benchmark result to the 'main' InfluxDB.

    Uses context.influxdb.main.* from environment.py.
    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning.
    """
    if not main_influx_is_configured(context):
        logger.info(" MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = bool(getattr(context.influxdb, "export_strict", False))

    client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info(" MAIN write_api missing – skipping export")
        return

    scenario_id = scenario_id_from_outfile(outfile, prefixes=("delete-",))

    p = build_delete_export_point(
        meta=meta,
        summary=summary,
        scenario_id=scenario_id,
        context=context
    )
    try:
        write_api.write(bucket=main.bucket, org=main.org, record=p)
        logger.info(" Exported delete result to main Influx")
    except ApiException as exc:
        msg = f" MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger.warning(msg)
    except Exception as exc:
        msg = f" MAIN export failed: {exc}"
        if strict:
            raise
        logger.warning(msg)


# ---------- Scenario Steps ----------


@when('I delete all points for measurement "{measurement}" in the SUT bucket')
def step_delete_measurement(context: Context, measurement: str) -> None:
    """
    Deletes all points for a given measurement from the SUT bucket
    and records delete latency and before/after counts.

    This is meant to be run after a write-benchmark scenario so it can
    optionally look at context.write_benchmark_meta["total_points"].
    """
    sut = context.influxdb.sut

    if not getattr(sut, "client", None):
        raise RuntimeError("SUT InfluxDB client is not configured on context.influxdb.sut")

    bucket = sut.bucket
    org = sut.org
    client = sut.client

    points_before = _count_points_for_measurement(context, measurement)

    expected_points = None
    wb_meta = getattr(context, "write_benchmark_meta", None)
    if isinstance(wb_meta, dict) and wb_meta.get("measurement") == measurement:
        expected_points = wb_meta.get("total_points")

    delete_api = client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"
    predicate = f'_measurement="{measurement}"'

    ok = True
    status_code = 204
    t0 = time.perf_counter()
    logger.debug(f"t0 = {t0:.6f}s – starting delete for measurement={measurement}")
    try:
        delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket, org=org)
    except Exception as exc:
        logger.info(f" delete for measurement={measurement} failed: {exc}")
        ok = False
        status_code = 500
    t1 = time.perf_counter()
    logger.debug(f"t1 = {t1:.6f}s – delete completed for measurement={measurement}")
    latency_s = t1 - t0
    logger.debug("latency_s = " + str(latency_s))
    points_after = _count_points_for_measurement(context, measurement)

    metrics = DeleteRunMetrics(
        latency_s=latency_s,
        status_code=status_code,
        ok=ok,
        points_before=points_before,
        points_after=points_after,
    )

    context.delete_metrics = metrics
    context.delete_benchmark_meta = {
        "measurement": measurement,
        "bucket": bucket,
        "org": org,
        "sut_url": sut.url,
        "expected_points": expected_points,
    }
    context.delete_summary = {
        "latency_s": latency_s,
        "points_before": points_before,
        "points_after": points_after,
        "deleted_points": points_before - points_after,
        "expected_points": expected_points,
        "ok": ok,
        "status_code": status_code,
    }

    logger.info(
        f" measurement={measurement} "
        f"before={points_before}, after={points_after}, "
        f"latency={latency_s:.6f}s, ok={ok}"
    )


@then("the delete duration shall be <= {max_ms:d} ms")
def step_check_delete_latency(context: Context, max_ms: int) -> None:
    """
    Delete latency check that no longer influences pass/fail
    """
    metrics: DeleteRunMetrics = getattr(context, "delete_metrics", None)
    if metrics is None:
        raise AssertionError("No delete metrics recorded on context.delete_metrics")

    # hard check --> will fail if criteria isn't fulfilled
    latency_ms = metrics.latency_s * 1000.0
    if latency_ms > max_ms:
        raise AssertionError(
            f"Delete latency too high: {latency_ms:.2f} ms (max {max_ms} ms)"
        )



@then('no points for measurement "{measurement}" shall remain in the SUT bucket')
def step_ensure_no_points_remain(context: Context, measurement: str) -> None:
    """
    Verifies that the measurement is completely deleted in the SUT bucket
    """
    points_after = _count_points_for_measurement(context, measurement)
    if points_after != 0:
        raise AssertionError(
            f"Expected 0 points for measurement={measurement} after delete, "
            f"but found {points_after}"
        )


@then('I store the generic delete benchmark result as "{outfile}"')
def step_store_delete_result(context: Context, outfile: str) -> None:
    """
    Persists the delete benchmark result to a JSON file and optionally
    exports a compact summary to the main InfluxDB
    """
    metrics: DeleteRunMetrics = getattr(context, "delete_metrics", None)
    meta: Dict[str, Any] = getattr(context, "delete_benchmark_meta", {})
    summary: Dict[str, Any] = getattr(context, "delete_summary", {})

    if metrics is None:
        raise AssertionError("No delete metrics recorded on context.delete_metrics")

    result: Dict[str, Any] = {
        "meta": meta,
        "summary": summary,
        "run": asdict(metrics),
        "created_at_epoch_s": time.time(),
    }

    write_json_report(
        outfile,
        result,
        logger_=logger,
        log_prefix="Stored generic delete benchmark result to ",
    )

    _export_delete_result_to_main_influx(
        context=context,
        meta=meta,
        summary=summary,
        outfile=outfile,
    )
