
import os
import time
import json
import math
import random
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

from behave import given, when, then
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------- Background-Steps (from environment) ----------

@given("a generic InfluxDB v2 endpoint is configured from environment")
def step_generic_endpoint_from_env(context):
    """
    Reads INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG from the environment
    and saves them in the context.
    """
    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG must be set in environment")

    context.influx_url = url
    context.influx_token = token
    context.influx_org = org


@given("a generic target bucket from environment is available")
def step_generic_bucket_from_env(context):
    """
    Reads INFLUX_BUCKET from the environment and saves it in the context.
    """
    bucket = os.getenv("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set in environment")
    context.influx_bucket = bucket


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

    p = (
        p
        .field("value", float(idx))
        .field("seq", idx)
    )

    if point_complexity == "high":
        p = (
            p
            .field("aux1", float(idx % 100))
            .field("aux2", math.sin(idx))
            .field("aux3", math.cos(idx))
        )

    p = p.time(ts, precision)
    return p


def _maybe_cleanup_before_run(context, measurement: str):
   # cleanup logic not implemented
    print(f"[write-bench] NOTE: cleanup for measurement={measurement} is not implemented here.")


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
            print(f"[write-bench] writer={writer_id} batch={batch_index} failed: {exc}")

        metrics.append(m)

    return metrics


# ---------- Scenario-Step ----------

@when(
    'I run a generic write benchmark on measurement "{measurement}" '
    'with batch size {batch_size:d}, {parallel_writers:d} parallel writers, '
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
    url = getattr(context, "influx_url", None) or os.getenv("INFLUX_URL")
    token = getattr(context, "influx_token", None) or os.getenv("INFLUX_TOKEN")
    org = getattr(context, "influx_org", None) or os.getenv("INFLUX_ORG")
    bucket = getattr(context, "influx_bucket", None) or os.getenv("INFLUX_BUCKET")

    if not url or not token or not org or not bucket:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET must be set")

    precision_enum = _precision_from_str(precision)

    _maybe_cleanup_before_run(context, measurement)

    client = InfluxDBClient(
        url=url,
        token=token,
        org=org,
        enable_gzip=(compression == "gzip"),
    )

    total_batches = parallel_writers * batches
    total_points = total_batches * batch_size

    if precision_enum == WritePrecision.NS:
        base_ts = time.time_ns()
    elif precision_enum == WritePrecision.MS:
        base_ts = int(time.time() * 1000)
    else:
        base_ts = int(time.time())

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


# ---------- write result ----------

@then('I store the generic write benchmark result as "{outfile}"')
def step_store_write_result(context, outfile):
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
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print("=== Generic Write Benchmark Result ===")
    print(json.dumps(result, indent=2))
