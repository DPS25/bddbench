import json
import logging
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from behave import given, when, then
from behave.runner import Context
from influxdb_client import Point
from influxdb_client.rest import ApiException

from src.utils import (
    generate_base_point,
    get_main_influx_write_api,
    main_influx_is_configured,
    scenario_id_from_outfile,
    write_json_report,
    write_to_influx,
)

logger = logging.getLogger("bddbench.influx_multi_bucket_delete_steps")


# helpers

def _truthy(value: object) -> bool:
    """Parse Behave -D flags like true/false/1/0/yes/no."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on"}


def _load_json_file(path_str: str) -> dict:
    fp = Path(path_str)
    if not fp.exists():
        raise FileNotFoundError(
            f"Missing required file: {fp}. "
            f"Run multi-bucket write first to generate it."
        )
    with fp.open("r", encoding="utf-8") as f:
        return json.load(f)


def _extract_multi_write_meta(payload: dict, infile: str) -> Dict[str, Any]:
    meta = payload.get("multi_write_benchmark_meta") or payload.get("meta")
    if not isinstance(meta, dict):
        raise AssertionError(f"{infile} does not contain 'multi_write_benchmark_meta' (dict).")
    return meta


def _require_run_scoped_meta(meta: Dict[str, Any]) -> Tuple[str, str, str, int]:
    """
    Enforce deletion by (measurement + run_id) across buckets.
    Returns (run_id, measurement, bucket_prefix, bucket_count).
    """
    run_id = meta.get("run_id")
    if not run_id:
        raise AssertionError(
            "multi_write_benchmark_meta has no run_id. Refusing to delete without run scope."
        )

    measurement = meta.get("measurement")
    if not measurement:
        raise AssertionError("multi_write_benchmark_meta has no measurement.")

    bucket_prefix = meta.get("bucket_prefix")
    if not bucket_prefix:
        raise AssertionError("multi_write_benchmark_meta has no bucket_prefix.")

    bucket_count = meta.get("bucket_count")
    if not isinstance(bucket_count, int) or bucket_count <= 0:
        raise AssertionError(f"Invalid bucket_count in meta: {bucket_count!r}")

    return str(run_id), str(measurement), str(bucket_prefix), int(bucket_count)


def _bucket_names(bucket_prefix: str, bucket_count: int) -> List[str]:
    return [f"{bucket_prefix}_{i}" for i in range(bucket_count)]


# flux counting

def _count_points(
    context: Context,
    *,
    bucket: str,
    measurement: str,
    run_id: Optional[str] = None,
) -> int:
    """
    Counts points in the given bucket for measurement (and optionally run_id).
    Assumes benchmark points store the numeric value in field `_field == "value"`.
    """
    org = context.influxdb.sut.org
    query_api = context.influxdb.sut.query_api

    run_id_filter = f'  |> filter(fn: (r) => r["run_id"] == "{run_id}")\n' if run_id else ""

    flux = f"""
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
{run_id_filter}  |> filter(fn: (r) => r["_field"] == "value")
  |> count()
""".strip()

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



# datatypes

@dataclass
class BucketDeleteMetrics:
    bucket: str
    latency_s: float
    status_code: int
    ok: bool
    points_before: int
    points_after: int



# core delete operations

def _delete_bucket_run_scoped(
    context: Context,
    *,
    bucket: str,
    measurement: str,
    run_id: str,
) -> BucketDeleteMetrics:
    sut = context.influxdb.sut
    if not getattr(sut, "client", None):
        raise RuntimeError("SUT InfluxDB client is not configured on context.influxdb.sut")

    points_before = _count_points(context, bucket=bucket, measurement=measurement, run_id=run_id)

    delete_api = sut.client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"
    predicate = f'_measurement="{measurement}" AND run_id="{run_id}"'

    t0 = time.perf_counter()
    try:
        delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket, org=sut.org)
        ok = True
        status_code = 204
    except Exception as exc:
        logger.info(" delete failed bucket=%s measurement=%s run_id=%s: %s", bucket, measurement, run_id, exc)
        ok = False
        status_code = 500
    t1 = time.perf_counter()

    points_after = _count_points(context, bucket=bucket, measurement=measurement, run_id=run_id)

    return BucketDeleteMetrics(
        bucket=bucket,
        latency_s=(t1 - t0),
        status_code=status_code,
        ok=ok,
        points_before=points_before,
        points_after=points_after,
    )


def _wipe_bucket_all(context: Context, *, bucket: str) -> BucketDeleteMetrics:
    sut = context.influxdb.sut
    if not getattr(sut, "client", None):
        raise RuntimeError("SUT InfluxDB client is not configured on context.influxdb.sut")

    delete_api = sut.client.delete_api()
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"

    t0 = time.perf_counter()
    try:
        delete_api.delete(start=start, stop=stop, predicate="", bucket=bucket, org=sut.org)
        ok = True
        status_code = 204
    except Exception as exc:
        logger.info(" bucket wipe failed bucket=%s: %s", bucket, exc)
        ok = False
        status_code = 500
    t1 = time.perf_counter()

    # For wipe_all, we keep counts as -1 (consistent with your single-bucket wipe logic).
    return BucketDeleteMetrics(
        bucket=bucket,
        latency_s=(t1 - t0),
        status_code=status_code,
        ok=ok,
        points_before=-1,
        points_after=-1,
    )



# export to main influx

def _build_multi_delete_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    context: Context,
) -> Point:
    p = generate_base_point(context=context, measurement="bddbench_multi_delete_result")
    return (
        p.tag("measurement", str(meta.get("measurement", "")))
         .tag("run_id", str(meta.get("run_id", "")))
         .tag("bucket_prefix", str(meta.get("bucket_prefix", "")))
         .tag("scenario_id", scenario_id or "")
         .field("bucket_count", int(meta.get("bucket_count", 0) or 0))
         .field("points_before", int(summary.get("points_before", 0) or 0))
         .field("points_after", int(summary.get("points_after", 0) or 0))
         .field("deleted_points", int(summary.get("deleted_points", 0) or 0))
         .field("latency_s_total", float(summary.get("latency_s_total", 0.0) or 0.0))
         .field("ok", bool(summary.get("ok", False)))
         .field("errors_count", int(summary.get("errors_count", 0) or 0))
         .field("wipe_all", bool(summary.get("wipe_all", False)))
    )


def _export_multi_delete_result_to_main_influx(
    context: Context,
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    outfile: str,
) -> None:
    if not main_influx_is_configured(context):
        logger.info(" MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = bool(getattr(context.influxdb, "export_strict", False))

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info(" MAIN write_api missing – skipping export")
        return

    scenario_id = scenario_id_from_outfile(outfile, prefixes=("multi-delete-",))
    p = _build_multi_delete_export_point(meta=meta, summary=summary, scenario_id=scenario_id, context=context)

    try:
        write_to_influx(
            write_api=write_api,
            bucket=main.bucket,
            org=main.org,
            record=p,
            logger_=logger,
            strict=strict,
            success_msg="Exported multi-bucket delete result to MAIN Influx",
            failure_prefix="MAIN export failed",
        )
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


# behave steps

@given('I load the multi-bucket write benchmark context from "{infile}"')
def step_load_multi_write_context(context: Context, infile: str) -> None:
    payload = _load_json_file(infile)
    meta = _extract_multi_write_meta(payload, infile)
    context.multi_write_benchmark_meta = meta
    logger.info(
        "Loaded multi write context from %s (measurement=%s run_id=%s bucket_prefix=%s bucket_count=%s)",
        infile,
        meta.get("measurement"),
        meta.get("run_id"),
        meta.get("bucket_prefix"),
        meta.get("bucket_count"),
    )


def _run_multi_bucket_delete_from_loaded_meta(context: Context, *, wipe_all: bool) -> None:
    meta: Dict[str, Any] = getattr(context, "multi_write_benchmark_meta", None)
    if not isinstance(meta, dict):
        raise AssertionError("No multi write meta loaded on context.multi_write_benchmark_meta")

    run_id, measurement, bucket_prefix, bucket_count = _require_run_scoped_meta(meta)
    buckets = _bucket_names(bucket_prefix, bucket_count)

    per_bucket: List[BucketDeleteMetrics] = []

    for b in buckets:
        if wipe_all:
            per_bucket.append(_wipe_bucket_all(context, bucket=b))
        else:
            per_bucket.append(_delete_bucket_run_scoped(context, bucket=b, measurement=measurement, run_id=run_id))

    errors = [m for m in per_bucket if not m.ok]
    ok_all = len(errors) == 0

    if wipe_all:
        points_before = -1
        points_after = -1
        deleted_points = None
    else:
        points_before = sum(m.points_before for m in per_bucket)
        points_after = sum(m.points_after for m in per_bucket)
        deleted_points = points_before - points_after

    latency_total = sum(m.latency_s for m in per_bucket)

    # For reporting/export steps
    context.multi_delete_metrics = per_bucket
    context.multi_delete_meta = {
        "measurement": measurement,
        "run_id": run_id,
        "bucket_prefix": bucket_prefix,
        "bucket_count": bucket_count,
        "buckets": buckets,
        "org": context.influxdb.sut.org,
        "sut_url": context.influxdb.sut.url,
    }
    context.multi_delete_summary = {
        "wipe_all": wipe_all,
        "points_before": points_before,
        "points_after": points_after,
        "deleted_points": deleted_points,
        "latency_s_total": latency_total,
        "ok": ok_all,
        "errors_count": len(errors),
    }


@when("I run the multi-bucket delete benchmark")
def step_run_multi_bucket_delete_benchmark(context: Context) -> None:
    """
    Multi-bucket delete is file-driven like single-bucket delete.

    Default:
      - loads reports/multi-write-context-smoke.json, deletes that run in all buckets, stores report
      - loads reports/multi-write-context-load.json, deletes that run in all buckets, stores report

    With -D delete_all=true:
      - loads smoke context to get bucket_prefix/bucket_count
      - wipes all buckets in that set (predicate=""), stores one report
    """
    delete_all = _truthy(context.config.userdata.get("delete_all"))

    if delete_all:
        step_load_multi_write_context(context, "reports/multi-write-context-smoke.json")
        _run_multi_bucket_delete_from_loaded_meta(context, wipe_all=True)
        step_store_multi_bucket_delete_result(context, "reports/multi-delete-all.json")
        return

    # smoke
    step_load_multi_write_context(context, "reports/multi-write-context-smoke.json")
    _run_multi_bucket_delete_from_loaded_meta(context, wipe_all=False)
    step_ensure_no_points_remain_multi(context)
    step_store_multi_bucket_delete_result(context, "reports/multi-delete-smoke.json")

    # load
    step_load_multi_write_context(context, "reports/multi-write-context-load.json")
    _run_multi_bucket_delete_from_loaded_meta(context, wipe_all=False)
    step_ensure_no_points_remain_multi(context)
    step_store_multi_bucket_delete_result(context, "reports/multi-delete-load.json")


@then("no points from that multi-bucket run shall remain in the SUT buckets")
def step_ensure_no_points_remain_multi(context: Context) -> None:
    meta: Dict[str, Any] = getattr(context, "multi_write_benchmark_meta", None)
    if not isinstance(meta, dict):
        raise AssertionError("No multi write meta loaded; cannot verify deletion.")

    run_id, measurement, bucket_prefix, bucket_count = _require_run_scoped_meta(meta)
    buckets = _bucket_names(bucket_prefix, bucket_count)

    for b in buckets:
        remaining = _count_points(context, bucket=b, measurement=measurement, run_id=run_id)
        if remaining != 0:
            raise AssertionError(
                f"Expected 0 points remaining for bucket={b} measurement={measurement} run_id={run_id}, "
                f"but found {remaining}"
            )


@then('I store the multi-bucket delete benchmark result as "{outfile}"')
def step_store_multi_bucket_delete_result(context: Context, outfile: str) -> None:
    per_bucket: List[BucketDeleteMetrics] = getattr(context, "multi_delete_metrics", None)
    meta: Dict[str, Any] = getattr(context, "multi_delete_meta", {})
    summary: Dict[str, Any] = getattr(context, "multi_delete_summary", {})

    if per_bucket is None:
        raise AssertionError("No multi delete metrics recorded on context.multi_delete_metrics")

    result: Dict[str, Any] = {
        "meta": meta,
        "summary": summary,
        "per_bucket": [asdict(m) for m in per_bucket],
        "created_at_epoch_s": time.time(),
    }

    write_json_report(
        outfile=outfile,
        data=result,
        logger_=logger,
        log_prefix="Stored multi-bucket delete benchmark result to ",
    )

    export_meta = {
        "measurement": meta.get("measurement", ""),
        "run_id": meta.get("run_id", ""),
        "bucket_prefix": meta.get("bucket_prefix", ""),
        "bucket_count": int(meta.get("bucket_count", 0) or 0),
    }
    export_summary = {
        "wipe_all": bool(summary.get("wipe_all", False)),
        "points_before": summary.get("points_before", 0) or 0,
        "points_after": summary.get("points_after", 0) or 0,
        "deleted_points": summary.get("deleted_points", 0) or 0,
        "latency_s_total": summary.get("latency_s_total", 0.0) or 0.0,
        "ok": bool(summary.get("ok", False)),
        "errors_count": int(summary.get("errors_count", 0) or 0),
    }

    _export_multi_delete_result_to_main_influx(
        context=context,
        meta=export_meta,
        summary=export_summary,
        outfile=outfile,
    )
