import os
import time
import math
import random
import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

from behave import when, then
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from concurrent.futures import ThreadPoolExecutor, as_completed


# ----------- Datatypes ------------

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


def _export_multi_write_result_to_main_influx(result: Dict[str, Any], outfile: str) -> None:
    main_url = os.getenv("MAIN_INFLUX_URL")
    main_token = os.getenv("MAIN_INFLUX_TOKEN")
    main_org = os.getenv("MAIN_INFLUX_ORG")
    main_bucket = os.getenv("MAIN_INFLUX_BUCKET")

    if not main_url or not main_token or not main_org or not main_bucket:
        print("[multi-write-bench] MAIN_INFLUX_* not fully set – skipping export to main Influx")
        return

    scenario_id = None
    base_name = os.path.basename(outfile)
    if base_name.startswith("multi-write-") and base_name.endswith(".json"):
        scenario_id = base_name[len("multi-write-"):-len(".json")]

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    latency_stats = summary.get("latency_stats", {})
    throughput = summary.get("throughput", {})

    client = InfluxDBClient(url=main_url, token=main_token, org=main_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    p = (
        Point("bddbench_multi_write_result")
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("bucket_prefix", str(meta.get("bucket_prefix", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("precision", str(meta.get("precision", "")))
        .tag("point_complexity", str(meta.get("point_complexity", "")))
        .tag("time_ordering", str(meta.get("time_ordering", "")))
        .tag("sut_bucket_pattern", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        .field("bucket_count", int(meta.get("bucket_count", 0)))
        .field("total_points", int(meta.get("total_points", 0)))
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

    print("[multi-write-bench] Exported multi-write result to main Influx")


def _run_duration_writer_worker(
    writer_id: int,
    client: InfluxDBClient,
    bucket: str,
    org: str,
    measurement: str,
    batch_size: int,
    compression: str,  
    precision_enum: WritePrecision,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    base_ts: int,
    stop_at: float,
) -> List[BatchWriteMetrics]:

    write_api = client.write_api(write_options=SYNCHRONOUS)
    metrics: List[BatchWriteMetrics] = []

    batch_index = 0

    while time.perf_counter() < stop_at:
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
            print(f"[multi-write-bench] writer={writer_id} batch={batch_index} failed: {exc}")

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
    context,
    measurement,
    bucket_prefix,
    bucket_count,
    batch_size,
    parallel_writers,
    compression,
    precision,
    point_complexity,
    tag_cardinality,
    time_ordering,
    duration_s,
):
    url = getattr(context, "influx_url", None) or os.getenv("INFLUX_URL")
    token = getattr(context, "influx_token", None) or os.getenv("INFLUX_TOKEN")
    org = getattr(context, "influx_org", None) or os.getenv("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG must be set")

    precision_enum = _precision_from_str(precision)

    client = InfluxDBClient(
        url=url,
        token=token,
        org=org,
        enable_gzip=(compression == "gzip"),
    )
    buckets_api = client.buckets_api()

    created_buckets: List[str] = []
    for i in range(bucket_count):
        bucket_name = f"{bucket_prefix}_{i}"
        try:
            buckets_api.create_bucket(bucket_name=bucket_name, org=org)
            print(f"[multi-write-bench] Created bucket: {bucket_name}")
        except Exception as exc:
            print(f"[multi-write-bench] Bucket {bucket_name} may already exist: {exc}")
        created_buckets.append(bucket_name)

    if precision_enum == WritePrecision.NS:
        base_ts = time.time_ns()
    elif precision_enum == WritePrecision.MS:
        base_ts = int(time.time() * 1000)
    else:
        base_ts = int(time.time())

    stop_at = time.perf_counter() + duration_s

    all_metrics: List[BatchWriteMetrics] = []

    started_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=bucket_count * parallel_writers) as executor:
        futures = []
        for bucket_idx, bucket_name in enumerate(created_buckets):
            for w in range(parallel_writers):
                writer_id = bucket_idx * parallel_writers + w
                futures.append(
                    executor.submit(
                        _run_duration_writer_worker,
                        writer_id=writer_id,
                        client=client,
                        bucket=bucket_name,
                        org=org,
                        measurement=measurement,
                        batch_size=batch_size,
                        compression=compression,
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

    throughput_points_per_s = (
        total_points / total_duration_s if total_duration_s > 0 else None
    )
    error_rate = len(errors) / len(all_metrics) if all_metrics else 0.0

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


@then('I store the multi-bucket write benchmark result as "{outfile}"')
def step_store_multi_bucket_write_result(context, outfile):
    batches: List[BatchWriteMetrics] = getattr(context, "multi_write_batches", [])
    meta = getattr(context, "multi_write_benchmark_meta", {})
    summary = getattr(context, "multi_write_summary", {})

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

    print("=== Multi-Bucket Write Benchmark Result ===")
    print(json.dumps(result, indent=2))

    _export_multi_write_result_to_main_influx(result, outfile)
