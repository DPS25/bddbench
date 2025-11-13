# file: gherkin-poc/influx_generic_steps.py

import os
import time
import uuid
import json
import math
import statistics
from pathlib import Path
from typing import List

from behave import given, when, then
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# ---------- helpers ----------

def _ensure_client(context):
    if hasattr(context, "influx_client"):
        return

    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG müssen im Environment gesetzt sein")

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    context.influx_client = client
    context.write_api = write_api
    context.influx_org = org
    env_bucket = os.getenv("INFLUX_BUCKET")
    if env_bucket:
        context.influx_bucket = env_bucket

    context.run_id = str(uuid.uuid4())
    context.write_latencies_ms: List[float] = []


def _percentile_ms(vals: List[float], p: int) -> float:
    if not vals:
        return 0.0
    sorted_vals = sorted(vals)
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[int(f)] * (c - k)
    d1 = sorted_vals[int(c)] * (k - f)
    return d0 + d1


# ---------- background steps ----------

@given("an InfluxDB v2 endpoint is configured from environment")
def step_influx_from_env(context):
    _ensure_client(context)


@given("a target bucket from environment is available")
def step_bucket_from_env(context):
    if not getattr(context, "influx_bucket", None):
        env_bucket = os.getenv("INFLUX_BUCKET")
        if not env_bucket:
            raise RuntimeError("INFLUX_BUCKET muss im Environment gesetzt sein oder im Feature angegeben werden")
        context.influx_bucket = env_bucket


# ---------- scenario steps ----------

@when('I run a generic write benchmark with {points_per_second:d} points per second for {duration_seconds:d} seconds using measurement "{measurement}"')
def step_run_generic_benchmark(context, points_per_second, duration_seconds, measurement):
    """
    Sehr ähnlich zu eurem write-latency-POC, aber rein über Parameter gesteuert.
    Wir messen jede Schreiboperation und sammeln die Latenzen in context.write_latencies_ms.
    """
    _ensure_client(context)

    bucket = context.influx_bucket
    org = context.influx_org

    rate = points_per_second
    duration = duration_seconds

    context.write_latencies_ms = [] 
    total_points = rate * duration

    started_at = time.perf_counter()

    for s in range(duration):
        sec_window_start = time.perf_counter()
        for i in range(rate):
            p = (
                Point(measurement)
                .tag("run_id", context.run_id)
                .field("value", (s * rate) + i)
                .time(time.time_ns(), WritePrecision.NS)
            )

            t0 = time.perf_counter()
            context.write_api.write(bucket=bucket, org=org, record=p)
            t1 = time.perf_counter()

            lat_ms = (t1 - t0) * 1000.0
            context.write_latencies_ms.append(lat_ms)

        elapsed = time.perf_counter() - sec_window_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

    actual_duration = time.perf_counter() - started_at

    context.benchmark_meta = {
        "bucket": bucket,
        "org": org,
        "measurement": measurement,
        "rate": rate,
        "duration_s": duration,
        "actual_duration_s": actual_duration,
        "total_points": total_points,
        "run_id": context.run_id,
    }


@then('I store the generic benchmark result as "{outfile}"')
def step_store_result(context, outfile):
    lats = getattr(context, "write_latencies_ms", [])
    meta = getattr(context, "benchmark_meta", {})

    result = {
        "meta": meta,
        "latency_ms": {
            "count": len(lats),
            "min": min(lats) if lats else None,
            "p50": _percentile_ms(lats, 50) if lats else None,
            "p90": _percentile_ms(lats, 90) if lats else None,
            "p95": _percentile_ms(lats, 95) if lats else None,
            "p99": _percentile_ms(lats, 99) if lats else None,
            "max": max(lats) if lats else None,
            "avg": statistics.mean(lats) if lats else None,
        },
        "raw_latencies_ms": lats,
        "created_at_epoch_s": time.time(),
    }

    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print("=== Generic Benchmark Result ===")
    print(json.dumps(result, indent=2))
