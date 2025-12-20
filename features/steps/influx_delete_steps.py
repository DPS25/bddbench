import logging
import os
import time
from dataclasses import dataclass, asdict
from typing import Dict, Any

from behave import when, then
from behave.runner import Context
from influxdb_client.rest import ApiException

from utils import (
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
    build_delete_export_point,
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
  |> count()
"""
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


def _export_delete_result_to_main_influx(
    context: Context,
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    metrics: DeleteRunMetrics,
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
        logger.info("[delete-bench] MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = os.getenv("INFLUXDB_EXPORT_STRICT", "0").strip().lower() in ("1", "true", "yes")

    client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info("[delete-bench] MAIN write_api missing – skipping export")
        return

    scenario_id = scenario_id_from_outfile(outfile, prefixes=("delete-",))

    p = build_delete_export_point(
        meta=meta,
        summary=summary,
        metrics=metrics,
        scenario_id=scenario_id,
    )

    try:
        write_api.write(bucket=main.bucket, org=main.org, record=p)
        logger.info("[delete-bench] Exported delete result to main Influx")
    except ApiException as exc:
        msg = f"[delete-bench] MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger.warning(msg)
    except Exception as exc:
        msg = f"[delete-bench] MAIN export failed: {exc}"
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

    t0 = time.perf_counter()
    ok = True
    status_code = 204
    try:
        delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket, org=org)
    except Exception as exc:
        logger.info(f"[delete-bench] delete for measurement={measurement} failed: {exc}")
        ok = False
        status_code = 500
    t1 = time.perf_counter()
    latency_s = t1 - t0

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
    }

    logger.info(
        f"[delete-bench] measurement={measurement} "
        f"before={points_before}, after={points_after}, "
        f"latency={latency_s:.6f}s, ok={ok}"
    )


@then("the delete duration shall be <= {max_ms:d} ms")
def step_check_delete_latency(context: Context, max_ms: int) -> None:
    metrics: DeleteRunMetrics = getattr(context, "delete_metrics", None)
    if metrics is None:
        raise AssertionError("No delete metrics recorded on context.delete_metrics")

    latency_ms = metrics.latency_s * 1000.0
    logger.info(
        f"[delete-bench] measured delete latency: {latency_ms:.2f} ms "
        f"(max_ms from feature: {max_ms} ms – ignored for pass/fail)"
    )


@then('no points for measurement "{measurement}" shall remain in the SUT bucket')
def step_ensure_no_points_remain(context: Context, measurement: str) -> None:
    points_after = _count_points_for_measurement(context, measurement)
    if points_after != 0:
        raise AssertionError(
            f"Expected 0 points for measurement={measurement} after delete, "
            f"but found {points_after}"
        )


@then('I store the generic delete benchmark result as "{outfile}"')
def step_store_delete_result(context: Context, outfile: str) -> None:
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
        metrics=metrics,
        outfile=outfile,
    )
