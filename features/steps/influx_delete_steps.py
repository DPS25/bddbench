import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any, Optional

from behave import given, when, then
from behave.runner import Context
from influxdb_client import Point
from influxdb_client.rest import ApiException

from src.utils import (
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
    generate_base_point,
)

logger = logging.getLogger("bddbench.influx_delete_steps")


def _load_json_file(p: str) -> dict:
    fp = Path(p)
    if not fp.exists():
        raise FileNotFoundError(
            f"Missing required context file: {fp}. "
            f"Run the write benchmark first so it generates this file "
            f"(e.g. behave -t=write), then rerun the delete scenario."
        )
    with fp.open("r", encoding="utf-8") as f:
        return json.load(f)


@given('I load the write benchmark context from "{infile}"')
def step_load_write_context(context: Context, infile: str) -> None:
    """
    Loads the JSON created by the write step:
      Then I store the write benchmark context as "reports/write-context-<id>.json"

    Populates `context.write_benchmark_meta` so delete steps can delete smoke/load separately via run_id.
    """
    data = _load_json_file(infile)
    meta = data.get("write_benchmark_meta") or data.get("meta")
    if not isinstance(meta, dict):
        raise AssertionError(f"{infile} does not contain 'write_benchmark_meta' (dict)")
    context.write_benchmark_meta = meta
    logger.info(
        f"Loaded write benchmark context from {infile} "
        f"(measurement={meta.get('measurement')}, run_id={meta.get('run_id')})"
    )


# ---------- Datatype ----------


@dataclass
class DeleteRunMetrics:
    latency_s: float
    status_code: int
    ok: bool
    points_before: int
    points_after: int


# ---------- Helpers ----------


def _count_points_for_measurement(
    context: Context,
    measurement: str,
    run_id: Optional[str] = None,
) -> int:
    """
    Counts points in the SUT bucket for the given measurement, optionally filtered by run_id (tag).
    """
    bucket = context.influxdb.sut.bucket
    org = context.influxdb.sut.org
    query_api = context.influxdb.sut.query_api

    run_id_filter = f'  |> filter(fn: (r) => r["run_id"] == "{run_id}")\n' if run_id else ""

    flux = f"""
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
{run_id_filter}  |> filter(fn: (r) => r["_field"] == "value")
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


def build_delete_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    context: Context,
) -> Point:
    p = generate_base_point(context=context, measurement="bddbench_delete_result")
    return (
        p
        .tag("measurement", str(meta.get("measurement", "")))
        .tag("run_id", str(meta.get("run_id", "")))
        .tag("scenario_id", scenario_id or "")
        .field("points_before", int(summary.get("points_before", 0)))
        .field("points_after", int(summary.get("points_after", 0)))
        .field("deleted_points", int(summary.get("deleted_points", 0) or 0))
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
    """
    if not main_influx_is_configured(context):
        logger.info(" MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info(" MAIN write_api missing – skipping export")
        return

    scenario_id = scenario_id_from_outfile(outfile, prefixes=("delete-",))
    p = build_delete_export_point(meta=meta, summary=summary, scenario_id=scenario_id, context=context)

    try:
        write_api.write(bucket=main.bucket, org=main.org, record=p)
        logger.info(" Exported delete result to main Influx")
    except ApiException as exc:
        logger.info(f" Failed to export delete result to main Influx: {exc}")
    except Exception as exc:
        logger.info(f" Failed to export delete result to main Influx: {exc}")


# ---------- Steps ----------


@when("I delete ALL points in the SUT bucket")
def step_delete_bucket_all(context: Context) -> None:
    """
    Deletes everything from the SUT bucket (bucket wipe).
    """
    sut = context.influxdb.sut
    if not getattr(sut, "client", None):
        raise RuntimeError("SUT InfluxDB client is not configured on context.influxdb.sut")

    delete_api = sut.client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"

    t0 = time.perf_counter()
    logger.debug(f"t0 = {t0:.6f}s – starting BUCKET-WIPE delete for bucket={sut.bucket}")
    try:
        delete_api.delete(start=start, stop=stop, predicate="", bucket=sut.bucket, org=sut.org)
        ok = True
        status_code = 204
    except Exception as exc:
        logger.info(f" bucket-wipe delete failed: {exc}")
        ok = False
        status_code = 500

    t1 = time.perf_counter()
    latency_s = t1 - t0
    logger.debug(f"t1 = {t1:.6f}s – bucket-wipe delete completed (latency_s={latency_s:.6f})")

    context.delete_metrics = DeleteRunMetrics(
        latency_s=latency_s,
        status_code=status_code,
        ok=ok,
        points_before=-1,
        points_after=-1,
    )
    context.delete_benchmark_meta = {
        "measurement": "",
        "run_id": "",
        "bucket": sut.bucket,
        "org": sut.org,
        "sut_url": sut.url,
        "expected_points": None,
    }
    context.delete_summary = {
        "latency_s": latency_s,
        "points_before": -1,
        "points_after": -1,
        "deleted_points": None,
        "expected_points": None,
        "ok": ok,
        "status_code": status_code,
    }


@when('I delete all points for measurement "{measurement}" in the SUT bucket')
def step_delete_measurement(context: Context, measurement: str) -> None:
    """
    Deletes points for (measurement + run_id) from the SUT bucket.

    This step intentionally REFUSES to run without a loaded write-context (run_id),
    to avoid accidentally deleting multiple runs (e.g. smoke + load) at once.
    """
    sut = context.influxdb.sut
    if not getattr(sut, "client", None):
        raise RuntimeError("SUT InfluxDB client is not configured on context.influxdb.sut")

    bucket = sut.bucket
    org = sut.org
    client = sut.client

    wb_meta = getattr(context, "write_benchmark_meta", None)
    if not isinstance(wb_meta, dict):
        raise AssertionError(
            "No write benchmark context loaded. "
            "This delete benchmark requires a write-context file so it can delete by run_id. "
            "Add the step: Given I load the write benchmark context from \"reports/write-context-<id>.json\" "
            "and make sure the file exists."
        )

    run_id = wb_meta.get("run_id")
    if not run_id:
        raise AssertionError(
            "write_benchmark_meta has no run_id. Refusing to delete by measurement only, "
            "because that would delete multiple runs at once (e.g. smoke + load). "
            "Fix the write benchmark so it stores run_id, or regenerate the write-context file."
        )

    meta_meas = wb_meta.get("measurement")
    if meta_meas and meta_meas != measurement:
        raise AssertionError(
            f"Measurement mismatch: feature wants '{measurement}', but write-context says '{meta_meas}'. "
            "Refusing to delete."
        )

    points_before = _count_points_for_measurement(context, measurement, run_id=str(run_id))

    expected_points = None
    if wb_meta.get("measurement") == measurement:
        expected_points = wb_meta.get("total_points")

    delete_api = client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"

    # IMPORTANT: run_id MUST be a TAG on the written points for this predicate to match.
    predicate = f'_measurement="{measurement}" AND run_id="{run_id}"'

    ok = True
    status_code = 204
    t0 = time.perf_counter()
    logger.debug(f"t0 = {t0:.6f}s – starting delete for measurement={measurement} run_id={run_id}")
    try:
        delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket, org=org)
    except Exception as exc:
        logger.info(f" delete for measurement={measurement} run_id={run_id} failed: {exc}")
        ok = False
        status_code = 500

    t1 = time.perf_counter()
    logger.debug(f"t1 = {t1:.6f}s – delete completed for measurement={measurement} run_id={run_id}")
    latency_s = t1 - t0
    logger.debug("latency_s = " + str(latency_s))

    points_after = _count_points_for_measurement(context, measurement, run_id=str(run_id))

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
        "run_id": str(run_id),
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
        f" measurement={measurement} run_id={run_id} "
        f"before={points_before}, after={points_after}, "
        f"latency={latency_s:.6f}s, ok={ok}"
    )


@then('no points for measurement "{measurement}" shall remain in the SUT bucket')
def step_ensure_no_points_remain(context: Context, measurement: str) -> None:
    """
    Verifies that the dataset for the loaded run_id is deleted in the SUT bucket.
    """
    wb_meta = getattr(context, "write_benchmark_meta", None)
    if not isinstance(wb_meta, dict):
        raise AssertionError(
            "No write benchmark context loaded. "
            "You must load a write-context file before asserting deletion."
        )

    run_id = wb_meta.get("run_id")
    if not run_id:
        raise AssertionError(
            "write_benchmark_meta has no run_id. Cannot verify run-scoped deletion."
        )

    points_after = _count_points_for_measurement(context, measurement, run_id=str(run_id))
    if points_after != 0:
        raise AssertionError(
            f"Expected 0 points remaining for measurement={measurement} run_id={run_id}, "
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
        outfile=outfile,
        data=result,
        logger_=logger,
        log_prefix="Stored generic delete benchmark result to ",
    )

    _export_delete_result_to_main_influx(
        context=context,
        meta=meta,
        summary=summary,
        outfile=outfile,
    )
