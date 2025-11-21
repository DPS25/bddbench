import logging
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
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(f"bddbench.influx_write_steps")

# ---------- Background-Steps (from environment) ----------


# @given("a SUT InfluxDB v2 endpoint is configured and reachable")
# def step_bucket_from_env(context: Context):
#     if not context.influxdb.sut.client.ping():
#         logging.getLogger("bdd_journal").error("SUT InfluxDB endpoint is not reachable")
#         raise RuntimeError("SUT InfluxDB endpoint is not reachable")
#
#
# @given("the target bucket '{bucket}' from environment is available")
# def step_target_bucket_available(context: Context, bucket: str) -> None:
#     assert context.influxdb.sut.bucket is not None, (
#         "SUT InfluxDB bucket is not configured"
#     )
#     bucket_api = context.influxdb.sut.client.buckets_api()
#     bucket_response = bucket_api.find_bucket_by_name(bucket)
#     assert bucket_response is not None, (
#         f"SUT InfluxDB bucket '{bucket}' is not available"
#     )


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
    print(
        f"[write-bench] NOTE: cleanup for measurement={measurement} is not implemented here."
    )


def _export_write_result_to_main_influx(result: Dict[str, Any], outfile: str) -> None:
    """
    Sends a compact summary of the write benchmark result to the 'main' InfluxDB.

    Expects MAIN_INFLUX_URL, MAIN_INFLUX_TOKEN, MAIN_INFLUX_ORG, MAIN_INFLUX_BUCKET
    to be set in the environment. If not set, the export is skipped.
    """
    main_url = os.getenv("MAIN_INFLUX_URL")
    main_token = os.getenv("MAIN_INFLUX_TOKEN")
    main_org = os.getenv("MAIN_INFLUX_ORG")
    main_bucket = os.getenv("MAIN_INFLUX_BUCKET")

    if not main_url or not main_token or not main_org or not main_bucket:
        print(
            "[write-bench] MAIN_INFLUX_* not fully set – skipping export to main Influx"
        )
        return

    scenario_id = None
    base_name = os.path.basename(outfile)
    if base_name.startswith("write-") and base_name.endswith(".json"):
        scenario_id = base_name[len("write-") : -len(".json")]

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    latency_stats = summary.get("latency_stats", {})
    throughput = summary.get("throughput", {})

    client = InfluxDBClient(url=main_url, token=main_token, org=main_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

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

    write_api.write(bucket=main_bucket, org=main_org, record=p)
    client.close()

    print("[write-bench] Exported write result to main Influx")


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
    url = context.influxdb.sut.url
    token = context.influxdb.sut.token
    org = context.influxdb.sut.org
    bucket = context.influxdb.sut.bucket

    if not url or not token or not org or not bucket:
        raise RuntimeError(
            "INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET must be set"
        )

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

    _export_write_result_to_main_influx(result, outfile)
