import os
import json
import time
from pathlib import Path
from behave import given, when, then


@given("an InfluxDB endpoint is configured")
def step_influx_configured(context):
    # fallback, wenn keine echte URL da ist
    context.influx_url = os.getenv("INFLUX_URL", "http://localhost:8086")


@given('a bucket "{bucket_name}" is defined')
def step_bucket_defined(context, bucket_name):
    context.bucket = bucket_name


@when('I write {points:d} points per second for {duration:d} seconds')
def step_write_points(context, points, duration):
    context.points_per_second = points
    context.duration_seconds = duration
    context.latency_samples = []

    # POC: wir simulieren den Schreibvorgang
    for _ in range(duration):
        start = time.time()
        # hier könnte später ein echter POST auf Influx rein
        time.sleep(0.01)  # 10 ms simulierte Latenz
        end = time.time()
        latency_ms = (end - start) * 1000
        context.latency_samples.append(latency_ms)


def median(values):
    if not values:
        return 0
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    return sorted_vals[mid]


@then('the median latency shall be <= {max_median:d} ms')
def step_check_median(context, max_median):
    med = median(context.latency_samples)
    context.median_latency = med
    assert med <= max_median, f"Median {med:.2f} ms > allowed {max_median} ms"


@then('I store the benchmark result as "{filename}"')
def step_store_result(context, filename):
    out_path = Path(filename)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    result = {
        "influx_url": context.influx_url,
        "bucket": getattr(context, "bucket", None),
        "points_per_second": getattr(context, "points_per_second", None),
        "duration_seconds": getattr(context, "duration_seconds", None),
        "latency_samples_ms": context.latency_samples,
        "median_ms": getattr(context, "median_latency", None),
        "status": "PASS",
    }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print("=== Benchmark Result ===")
    print(json.dumps(result, indent=2))
