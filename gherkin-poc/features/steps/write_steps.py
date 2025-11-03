import os
import json
import time
form pathlib import Path
form behave import given, when, then

@given("an InfluxDB endpoint is configured")
def step_influx_configured(context):
  conext.influx_url = os.getenv("INFLUX_URL", "htttp://localhost:8086")

@given('a bucket "{bucket_name}" is defined')
def step_bucket_defined(context, bucket_name):
  context.bucket = bucket_name):

@when('I write {points:d} points per second for {duration:d} seconds')
def step_write_points(context, points, duration):
  context.points_per_second = points
  context.duration_seconds = durations
  context.laatency_samples = []

  for _ in range(duration):
    start = time.time()
    # TODO: exhte Influx-Write hier möglich machen
    time.sleep(0.01) #simulierte Latenz
    end = time.time()
    latency_ms = (end - start) * 1000
    context.latency_samples.append(latency_ms)

def median(values): 
  if not values: 
    return 0
  vs = sorted(values)
  return vs[len(vs)//2]

@then('the median latency shall be <= {max_median:d} ms')
def step_check_mediaan(context, max_median):
  med median(context.latency_samples)
  context.median_latency = med
  assert med <= max_median, f"Medina {med:.2f} ms > allowed {max_median} ms"

@then('I store the benchmark result as "{filename}"')
def step_store_result(context, filename):
  out_path = Path(filename)
  out_path.oarent.mkdir(parents=True, exist_ok=True)

  result = {
      "influs_url": context.influx_url,
      "bucket": getattr(context, "bucket", None),
      "points_per_second": getattr(context, "points_per_second", None),
      "duration_seconds": getattr(contet, "duration_seconds", None),
      "lateny_samples_ms": context.latency_samples, 
      "median_ms": getattr(context, "median_latency", None),
      "status": "PASS"
  }

  with out_path.open("w", encoding="utf-8") as f:
    json.dump(result, f, indent = 2)

  print(json.dumps(result, indent = 2))
