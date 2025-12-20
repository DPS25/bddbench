import logging
import os
import time
import statistics
from influxdb_client.rest import ApiException
from dataclasses import dataclass, asdict
from typing import List, Dict, Any

from behave import when, then
from behave.runner import Context
from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from src.utils import (
    influx_precision_from_str,
    base_timestamp_for_precision,
    build_benchmark_point,
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
    build_write_export_point,
)
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(f"bddbench.steps.influx_write_steps")

# ----------- Datatype ------------


@dataclass
class BatchWriteMetrics:
    batch_index: int
    latency_s: float
    points: int
    status_code: int
    ok: bool


# ---------- Helpers ----------
def _maybe_cleanup_before_run(context, measurement: str):
    # cleanup logic not implemented
    logging.debug(
        f"[write-bench] NOTE: cleanup for measurement={measurement} is not implemented here."
    )

def _export_write_result_to_main_influx(
    result: Dict[str, Any],
    outfile: str,
    context: Context,
) -> None:
    """
    Sends a compact summary of the write benchmark result to the 'main' InfluxDB.

    Uses context.influxdb.main.* from environment.py.
    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning
    """
    if not main_influx_is_configured(context):
        logger.info("[write-bench] MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = os.getenv("INFLUXDB_EXPORT_STRICT", "0").strip().lower() in ("1", "true", "yes")

    client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info("[write-bench] MAIN write_api missing – skipping export")
        return

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    scenario_id = scenario_id_from_outfile(outfile, prefixes=("write-",))

    # >>> hier kommt jetzt der Utils-Helper zum Einsatz <<<
    p = build_write_export_point(meta=meta, summary=summary, scenario_id=scenario_id)

    try:
        write_api.write(bucket=main.bucket, org=main.org, record=p)
        logger.info("Exported write result to MAIN Influx")
    except ApiException as exc:
        msg = f"[write-bench] MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger.warning(msg)
    except Exception as exc:
        msg = f"[write-bench] MAIN export failed: {exc}"
        if strict:
            raise
        logger.warning(msg)
        
def _run_writer_worker(
    writer_id: int,
    client: InfluxDBClient,
    bucket: str,
    org: str,
    measurement: str,
    batch_size: int,
    batches: int,
    precision_enum: WritePrecision,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    base_ts: int,
) -> List[BatchWriteMetrics]:
    """
    A writer writes `batches` batches per `batch_size` points
    and deliveres a list of BatchWriteMetrics.
    """
    write_api = client.write_api(write_options=SYNCHRONOUS)
    metrics: List[BatchWriteMetrics] = []

    for b in range(batches):
        batch_index = writer_id * batches + b

        points = []
        for i in range(batch_size):
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
            points.append(p)

        t0 = time.perf_counter()
        try:
            write_api.write(
                bucket=bucket,
                org=org,
                record=points,
            )
            t1 = time.perf_counter()
            latency_s = t1 - t0
            m = BatchWriteMetrics(
                batch_index=batch_index,
                latency_s=latency_s,
                points=len(points),
                status_code=204,
                ok=True,
            )
        except Exception as exc:
            t1 = time.perf_counter()
            latency_s = t1 - t0
            m = BatchWriteMetrics(
                batch_index=batch_index,
                latency_s=latency_s,
                points=len(points),
                status_code=500,
                ok=False,
            )
            logging.info(f"[write-bench] writer={writer_id} batch={batch_index} failed: {exc}")

        metrics.append(m)

    return metrics


# ---------- Scenario-Step ----------

@when(
    'I run a generic write benchmark on measurement "{measurement}" '
    "with batch size {batch_size:d}, {parallel_writers:d} parallel writers, "
    'compression "{compression}", timestamp precision "{precision}", '
    'point complexity "{point_complexity}", tag cardinality {tag_cardinality:d} '
    'and time ordering "{time_ordering}" for {batches:d} batches'
)
def step_run_write_benchmark(
    context,
    measurement,
    batch_size,
    parallel_writers,
    compression,
    precision,
    point_complexity,
    tag_cardinality,
    time_ordering,
    batches,
):
    """
    Does a Write-Benchmark woth the parameters from Scenario Outline.

    Inputs by specifikation:
      - batch_size
      - parallel_writers
      - compression (none/gzip)
      - precision (ns/ms/s)
      - point_complexity (low/high)
      - tag_cardinality
      - time_ordering (in_order/out_of_order)
      - batches (Anzahl Batches pro Writer)

    Outputs:
      - list Batch-Latenzen
      - Throughput (points/s)
      - Error-Rate
    """
    logger.debug("starting generic write benchmark...")
    url = context.influxdb.sut.url
    token = context.influxdb.sut.token
    org = context.influxdb.sut.org
    bucket = context.influxdb.sut.bucket

    if not url or not token or not org or not bucket:
        raise RuntimeError(
            "INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET must be set"
        )

    logger.debug("got SUT InfluxDB connection parameters")
    precision_enum = influx_precision_from_str(precision)
    base_ts = base_timestamp_for_precision(precision_enum)

    _maybe_cleanup_before_run(context, measurement)

    enable_gzip = compression == "gzip"
    bench_client = InfluxDBClient(url=url, token=token, org=org, enable_gzip=enable_gzip)
    total_batches = parallel_writers * batches
    total_points = total_batches * batch_size

    logger.debug(f"using base timestamp: {base_ts} ({precision_enum})")
    all_metrics: List[BatchWriteMetrics] = []

    started_at = time.perf_counter()
    try:    
        with ThreadPoolExecutor(max_workers=parallel_writers) as executor:
            futures = [
                executor.submit(
                    _run_writer_worker,
                    writer_id=w,
                    client=bench_client,
                    bucket=bucket,
                    org=org,
                    measurement=measurement,
                    batch_size=batch_size,
                    batches=batches,
                    precision_enum=precision_enum,
                    point_complexity=point_complexity,
                    tag_cardinality=tag_cardinality,
                    time_ordering=time_ordering,
                    base_ts=base_ts,
                )
                for w in range(parallel_writers)
            ]

            for fut in as_completed(futures):
                all_metrics.extend(fut.result())

        total_duration_s = time.perf_counter() - started_at
    finally:
        bench_client.close()

    logger.debug(f"completed write benchmark in {total_duration_s:.3f} seconds")
    context.write_batches = all_metrics
    context.write_benchmark_meta = {
        "measurement": measurement,
        "batch_size": batch_size,
        "parallel_writers": parallel_writers,
        "compression": compression,
        "precision": precision,
        "point_complexity": point_complexity,
        "tag_cardinality": tag_cardinality,
        "time_ordering": time_ordering,
        "bucket": bucket,
        "org": org,
        "sut_url": url,
        "total_batches": total_batches,
        "total_points": total_points,
        "total_duration_s": total_duration_s,
    }

    latencies = [m.latency_s for m in all_metrics]
    errors = [m for m in all_metrics if not m.ok]

    throughput_points_per_s = (
        total_points / total_duration_s if total_duration_s > 0 else None
    )
    error_rate = len(errors) / len(all_metrics) if all_metrics else 0.0

    context.write_summary = {
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
    logger.debug("generic write benchmark done")


# ---------- write result ----------

@then('I store the generic write benchmark result as "{outfile}"')
def step_store_write_result(context, outfile):
    logger.debug("storing benchmark result")
    batches: List[BatchWriteMetrics] = getattr(context, "write_batches", [])
    meta = getattr(context, "write_benchmark_meta", {})
    summary = getattr(context, "write_summary", {})

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
        log_prefix="Stored generic write benchmark result to ",
    )

    _export_write_result_to_main_influx(result, outfile, context)
