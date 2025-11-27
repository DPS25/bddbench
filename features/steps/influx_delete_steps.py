import logging
import os
import time
import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any

from behave import when, then
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


logger = logging.getLogger(f"bddbench.influx_delete_steps")


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

    # Count across all time; count() returns grouped counts, we sum them.
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
            # count() puts the count into _value
            v = record.get_value()
            try:
                total += int(v)
            except (TypeError, ValueError):
                pass
    return total


def _export_delete_result_to_main_influx(
    meta: Dict[str, Any],
    metrics: DeleteRunMetrics,
    outfile: str,
) -> None:
    """
    Sends a compact summary of the delete benchmark result to the 'main' InfluxDB.

    Expects MAIN_INFLUX_URL, MAIN_INFLUX_TOKEN, MAIN_INFLUX_ORG, MAIN_INFLUX_BUCKET
    to be set in the environment. If not set, the export is skipped.

    This mirrors the style of _export_write_result_to_main_influx / query export.
    """
    main_url = os.getenv("MAIN_INFLUX_URL")
    main_token = os.getenv("MAIN_INFLUX_TOKEN")
    main_org = os.getenv("MAIN_INFLUX_ORG")
    main_bucket = os.getenv("MAIN_INFLUX_BUCKET")

    if not main_url or not main_token or not main_org or not main_bucket:
        logging.info("[delete-bench] MAIN_INFLUX_* not fully set – skipping export to main Influx")
        return

    scenario_id = None
    base_name = os.path.basename(outfile)
    if base_name.startswith("delete-") and base_name.endswith(".json"):
        scenario_id = base_name[len("delete-"):-len(".json")]

    client = InfluxDBClient(url=main_url, token=main_token, org=main_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    p = (
        Point("bddbench_delete_result")
        # tags
        .tag("measurement", str(meta.get("measurement", "")))
        .tag("sut_bucket", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        # fields
        .field("points_before", int(metrics.points_before))
        .field("points_after", int(metrics.points_after))
        .field("deleted_points", int(metrics.points_before - metrics.points_after))
        .field("expected_points", int(meta.get("expected_points", 0)))
        .field("latency_s", float(metrics.latency_s))
        .field("status_code", int(metrics.status_code))
        .field("ok", bool(metrics.ok))
    )

    write_api.write(bucket=main_bucket, org=main_org, record=p)
    client.close()

    logging.info("[delete-bench] Exported delete result to main Influx")







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

    # Count how many points exist before delete
    points_before = _count_points_for_measurement(context, measurement)

    # Optional: expected points from write benchmark (if available)
    expected_points = None
    wb_meta = getattr(context, "write_benchmark_meta", None)
    if isinstance(wb_meta, dict) and wb_meta.get("measurement") == measurement:
        expected_points = wb_meta.get("total_points")

    # Perform delete over a wide time range for that measurement
    delete_api = client.delete_api()
    # full-time range: everything from epoch to far future
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"
    predicate = f'_measurement="{measurement}"'

    t0 = time.perf_counter()
    ok = True
    status_code = 204  # Influx delete API has no HTTP code in the client; assume 204 on success
    try:
        delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket, org=org)
    except Exception as exc:
        logging.info(f"[delete-bench] delete for measurement={measurement} failed: {exc}")
        ok = False
        status_code = 500
    t1 = time.perf_counter()
    latency_s = t1 - t0

    # Count again after delete
    points_after = _count_points_for_measurement(context, measurement)

    metrics = DeleteRunMetrics(
        latency_s=latency_s,
        status_code=status_code,
        ok=ok,
        points_before=points_before,
        points_after=points_after,
    )

    # Store on context for later steps
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

    logging.info(
        f"[delete-bench] measurement={measurement} "
        f"before={points_before}, after={points_after}, "
        f"latency={latency_s:.6f}s, ok={ok}"
    )





@then("the delete duration shall be <= {max_ms:d} ms")
def step_check_delete_latency(context: Context, max_ms: int) -> None:
    """
    Simple SLA-like check on the delete latency.
    """
    metrics: DeleteRunMetrics = getattr(context, "delete_metrics", None)
    if metrics is None:
        raise AssertionError("No delete metrics recorded on context.delete_metrics")

    latency_ms = metrics.latency_s * 1000.0
    if latency_ms > max_ms:
        raise AssertionError(
            f"delete latency {latency_ms:.2f} ms exceeded limit {max_ms} ms"
        )


@then('no points for measurement "{measurement}" shall remain in the SUT bucket')
def step_ensure_no_points_remain(context: Context, measurement: str) -> None:
    """
    Verifies that the measurement is completely deleted in the SUT bucket.
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
    exports a compact summary to the main InfluxDB.
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

    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    logging.info("=== Generic Delete Benchmark Result ===")
    logging.info(json.dumps(result, indent=2))

    _export_delete_result_to_main_influx(meta, metrics, outfile)
