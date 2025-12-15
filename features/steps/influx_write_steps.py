import logging
import os
import time
import json
import math
import random
import statistics
from influxdb_client.rest import ApiException
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

from behave import given, when, then
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
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

def _precision_from_str(p: str) -> WritePrecision:
    p = p.lower()
    if p == "ns":
        return WritePrecision.NS
    if p == "ms":
        return WritePrecision.MS
    if p == "s":
        return WritePrecision.S
    raise ValueError(f"Unsupported precision: {p}")


def _build_point(
    measurement: str,
    base_ts: int,
    idx: int,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    precision: WritePrecision,
) -> Point:
    """
    Creates a point containing:
      - configured Tag-Cardinality
      - configured amount of fields (point_complexity)
      - optional out-of-order timestamps
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


def _maybe_cleanup_before_run(context, measurement: str):
    # cleanup logic not implemented
    logging.debug(
        f"[write-bench] NOTE: cleanup for measurement={measurement} is not implemented here."
    )


def _export_write_result_to_main_influx(result: Dict[str, Any], outfile: str, context: Context) -> None:
    """
    Sends a compact summary of the write benchmark result to the 'main' InfluxDB.

    Uses context.influxdb.main.* from environment.py.
    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning
    """
    main_url = context.influxdb.main.url
    main_token = context.influxdb.main.token
    main_org = context.influxdb.main.org
    main_bucket = context.influxdb.main.bucket
    strict = (os.getenv("INFLUXDB_EXPORT_STRICT", "0").strip().lower() in ("1", "true", "yes"))

    if not main_url or not main_token or not main_org or not main_bucket:
        logging.info("INFLUXDB_MAIN_* not fully set – skipping export to MAIN Influx")
        return

    scenario_id = None
    base_name = os.path.basename(outfile)
    if base_name.startswith("write-") and base_name.endswith(".json"):
        scenario_id = base_name[len("write-") : -len(".json")]

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    latency_stats = summary.get("latency_stats", {})
    throughput = summary.get("throughput", {})
    logger.debug("exporting write benchmark result to main InfluxDB")
    write_api = context.influxdb.main.write_api

    p = (
        Point("bddbench_write_result")
        # tags
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("precision", str(meta.get("precision", "")))
        .tag("point_complexity", str(meta.get("point_complexity", "")))
        .tag("time_ordering", str(meta.get("time_ordering", "")))
        .tag("sut_bucket", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        # fields
        .field("total_points", int(meta.get("total_points", 0)))
        .field("total_batches", int(meta.get("total_batches", 0)))
        .field("total_duration_s", float(meta.get("total_duration_s", 0.0)))
        .field("throughput_points_per_s", float(throughput.get("points_per_s") or 0.0))
        .field("error_rate", float(summary.get("error_rate", 0.0)))
        .field("errors_count", int(summary.get("errors_count", 0)))
        .field("latency_min_s", float(latency_stats.get("min") or 0.0))
        .field("latency_max_s", float(latency_stats.get("max") or 0.0))
        .field("latency_avg_s", float(latency_stats.get("avg") or 0.0))
        .field("latency_median_s", float(latency_stats.get("median") or 0.0))
    )

    try:
        write_api.write(bucket=main_bucket, org=main_org, record=p)
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
    compression: str,
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
            p = _build_point(
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
    precision_enum = _precision_from_str(precision)

    _maybe_cleanup_before_run(context, measurement)

    client = context.influxdb.sut.client

    total_batches = parallel_writers * batches
    total_points = total_batches * batch_size

    if precision_enum == WritePrecision.NS:
        base_ts = time.time_ns()
    elif precision_enum == WritePrecision.MS:
        base_ts = int(time.time() * 1000)
    else:
        base_ts = int(time.time())
    logger.debug(f"using base timestamp: {base_ts} ({precision_enum})")
    all_metrics: List[BatchWriteMetrics] = []

    started_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=parallel_writers) as executor:
        futures = [
            executor.submit(
                _run_writer_worker,
                writer_id=w,
                client=client,
                bucket=bucket,
                org=org,
                measurement=measurement,
                batch_size=batch_size,
                batches=batches,
                compression=compression,
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

    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"storing benchmark result: {out_path}")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    logger.debug("stored benchmark result")
    logger.info("=== Generic Write Benchmark Result ===")
    logger.debug(json.dumps(result, indent=2))

    _export_write_result_to_main_influx(result, outfile, context)
