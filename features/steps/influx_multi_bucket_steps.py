import logging
import os
import time
import statistics
from dataclasses import dataclass, asdict
from typing import List, Dict, Any

from behave import when, then
from behave.runner import Context
from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils import (
    influx_precision_from_str,
    base_timestamp_for_precision,
    build_benchmark_point,
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
    build_multi_write_export_point,
)

logger = logging.getLogger("bddbench.steps.influx_multi_bucket_steps")


# ----------- Datatypes ------------

@dataclass
class BatchWriteMetrics:
    batch_index: int
    latency_s: float
    points: int
    status_code: int
    ok: bool


# ---------- Helpers ----------

def _export_multi_write_result_to_main_influx(
    result: Dict[str, Any],
    outfile: str,
    context: Context,
) -> None:
    """
    Sends a compact summary of the multi-bucket write benchmark result to the
    'main' InfluxDB.

    Uses context.influxdb.main.* from environment.py.
    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning.
    """
    if not main_influx_is_configured(context):
        logger.info("[multi-write-bench] MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = os.getenv("INFLUXDB_EXPORT_STRICT", "0").strip().lower() in ("1", "true", "yes")

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info("[multi-write-bench] MAIN write_api missing – skipping export")
        return

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    scenario_id = scenario_id_from_outfile(outfile, prefixes=("multi-write-",))

    p = build_multi_write_export_point(meta=meta, summary=summary, scenario_id=scenario_id)

    try:
        write_api.write(bucket=main.bucket, org=main.org, record=p)
        logger.info("[multi-write-bench] Exported multi-write result to main Influx")
    except ApiException as exc:
        msg = f"[multi-write-bench] MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger.warning(msg)
    except Exception as exc:
        msg = f"[multi-write-bench] MAIN export failed: {exc}"
        if strict:
            raise
        logger.warning(msg)


def _create_bucket_compat(buckets_api: Any, bucket_name: str, org: str) -> None:
    """
    InfluxDB client versions differ: some expect org=..., others org_id=...
    We try both to avoid hard failures.
    """
    try:
        buckets_api.create_bucket(bucket_name=bucket_name, org=org)
    except TypeError:
        raise

def _resolve_org_id(client: InfluxDBClient, org: str) -> str:
    orgs_api = client.organizations_api()
    orgs = orgs_api.find_organizations(org=org)
    if not orgs:
        raise RuntimeError(f"Could not resolve org_id for org name {org!r}")
    return orgs[0].id

def _run_duration_writer_worker(
    writer_id: int,
    client: InfluxDBClient,
    bucket: str,
    org: str,
    measurement: str,
    batch_size: int,
    precision_enum: WritePrecision,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    base_ts: int,
    stop_at: float,
) -> List[BatchWriteMetrics]:
    """
    A writer repeatedly writes batches of points until stop_at is reached and
    returns a list of BatchWriteMetrics for all written batches.
    """
    metrics: List[BatchWriteMetrics] = []
    batch_index = 0

    # Thread-local WriteApi (avoid sharing a single instance across threads)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    while time.perf_counter() < stop_at:
        points = []
        for i in range(batch_size):
            # Keep the original scheme; if you want globally unique idx across writers,
            # you can incorporate writer_id into global_idx.
            global_idx = batch_index * batch_size + i
            p = build_benchmark_point(
                measurement=measurement,
                base_ts=base_ts,
                idx=global_idx,
                point_complexity=point_complexity,
                tag_cardinality=tag_cardinality,
                time_ordering=time_ordering,
                precision=precision_enum,
            )
            p = p.tag("writer_id", str(writer_id))
            points.append(p)

        t0 = time.perf_counter()
        try:
            write_api.write(
                bucket=bucket,
                org=org,
                record=points,
            )
            t1 = time.perf_counter()
            m = BatchWriteMetrics(
                batch_index=batch_index,
                latency_s=(t1 - t0),
                points=len(points),
                status_code=204,
                ok=True,
            )
        except Exception as exc:
            t1 = time.perf_counter()
            m = BatchWriteMetrics(
                batch_index=batch_index,
                latency_s=(t1 - t0),
                points=len(points),
                status_code=500,
                ok=False,
            )
            logger.info(
                "[multi-write-bench] writer=%s batch=%s failed: %s",
                writer_id,
                batch_index,
                exc,
            )

        metrics.append(m)
        batch_index += 1

    return metrics


# ---------- Scenario-Steps ----------

@when(
    'I run a multi-bucket write benchmark on base measurement "{measurement}" '
    'with bucket prefix "{bucket_prefix}" creating {bucket_count:d} buckets, '
    'batch size {batch_size:d}, {parallel_writers:d} parallel writers per bucket, '
    'compression "{compression}", timestamp precision "{precision}", '
    'point complexity "{point_complexity}", tag cardinality {tag_cardinality:d} '
    'and time ordering "{time_ordering}" for {duration_s:d} seconds'
)
def step_run_multi_bucket_write_benchmark(
    context: Context,
    measurement: str,
    bucket_prefix: str,
    bucket_count: int,
    batch_size: int,
    parallel_writers: int,
    compression: str,
    precision: str,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    duration_s: int,
):
    url = context.influxdb.sut.url
    token = context.influxdb.sut.token
    org = context.influxdb.sut.org

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN and INFLUX_ORG must be set for the SUT")

    precision_enum = influx_precision_from_str(precision)
    base_ts = base_timestamp_for_precision(precision_enum)

    enable_gzip = compression == "gzip"
    bench_client = InfluxDBClient(url=url, token=token, org=org, enable_gzip=enable_gzip)

    try:
        buckets_api = bench_client.buckets_api()

        created_buckets: List[str] = []
        for i in range(bucket_count):
            org_id = None
            try:
                org_id = _resolve_org_id(bench_client, org)
            except Exception as exc:
                logger.info("[multi-write-bench] Could not resolve org_id (will try org=... path): %s", exc)

        for i in range(bucket_count):
            bucket_name = f"{bucket_prefix}_{i}"
            try:
                _create_bucket_compat(buckets_api, bucket_name=bucket_name, org=org)
                try:
                    _create_bucket_compat(buckets_api, bucket_name=bucket_name, org=org)
                except TypeError:
                    if not org_id:
                        raise
                    buckets_api.create_bucket(bucket_name=bucket_name, org_id=org_id)
                logger.info("[multi-write-bench] Created bucket: %s", bucket_name)
            except Exception as exc:
                # If it already exists, the benchmark can still proceed
                logger.info("[multi-write-bench] Bucket %s may already exist: %s", bucket_name, exc)
            created_buckets.append(bucket_name)

        logger.debug(
            "starting multi-bucket write benchmark with base_ts=%s (%s)",
            base_ts,
            precision_enum,
        )

        stop_at = time.perf_counter() + duration_s
        all_metrics: List[BatchWriteMetrics] = []

        started_at = time.perf_counter()
        max_workers = max(1, bucket_count * parallel_writers)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for bucket_idx, bucket_name in enumerate(created_buckets):
                for w in range(parallel_writers):
                    writer_id = bucket_idx * parallel_writers + w
                    futures.append(
                        executor.submit(
                            _run_duration_writer_worker,
                            writer_id=writer_id,
                            client=bench_client,
                            bucket=bucket_name,
                            org=org,
                            measurement=measurement,
                            batch_size=batch_size,
                            precision_enum=precision_enum,
                            point_complexity=point_complexity,
                            tag_cardinality=tag_cardinality,
                            time_ordering=time_ordering,
                            base_ts=base_ts,
                            stop_at=stop_at,
                        )
                    )

            for fut in as_completed(futures):
                all_metrics.extend(fut.result())

        total_duration_s = time.perf_counter() - started_at
        total_points = sum(m.points for m in all_metrics)

        context.multi_write_batches = all_metrics
        context.multi_write_benchmark_meta = {
            "measurement": measurement,
            "bucket_prefix": bucket_prefix,
            "bucket_count": bucket_count,
            "batch_size": batch_size,
            "parallel_writers_per_bucket": parallel_writers,
            "compression": compression,
            "precision": precision,
            "point_complexity": point_complexity,
            "tag_cardinality": tag_cardinality,
            "time_ordering": time_ordering,
            "bucket": f"{bucket_prefix}_*",
            "org": org,
            "sut_url": url,
            "total_points": total_points,
            "total_duration_s": total_duration_s,
        }

        latencies = [m.latency_s for m in all_metrics]
        errors = [m for m in all_metrics if not m.ok]

        throughput_points_per_s = total_points / total_duration_s if total_duration_s > 0 else None
        error_rate = (len(errors) / len(all_metrics)) if all_metrics else 0.0

        context.multi_write_summary = {
            "latencies_s": latencies,
            "latency_stats": {
                "min": min(latencies) if latencies else None,
                "max": max(latencies) if latencies else None,
                "avg": statistics.mean(latencies) if latencies else None,
                "median": statistics.median(latencies) if latencies else None,
            },
            "throughput": {
                "points_per_s": throughput_points_per_s,
            },
            "error_rate": error_rate,
            "errors_count": len(errors),
        }

    finally:
        bench_client.close()


@then('I store the multi-bucket write benchmark result as "{outfile}"')
def step_store_multi_bucket_write_result(context: Context, outfile: str):
    batches: List[BatchWriteMetrics] = getattr(context, "multi_write_batches", [])
    meta = getattr(context, "multi_write_benchmark_meta", {})
    summary = getattr(context, "multi_write_summary", {})

    result: Dict[str, Any] = {
        "meta": meta,
        "summary": summary,
        "batches": [asdict(b) for b in batches],
        "created_at_epoch_s": time.time(),
    }

    write_json_report(
        outfile,
        result,
        logger_=logger,
        log_prefix="Stored multi-bucket write benchmark result to ",
    )

    _export_multi_write_result_to_main_influx(result, outfile, context)
