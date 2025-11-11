import os
import time
import uuid
import statistics

from behave import given, when, then
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

@given("an InfluxDB v2 endpoint is configured from environment")
def step_config_from_env(context):
    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError(
            "INFLUX_URL, INFLUX_TOKEN oder INFLUX_ORG nicht gesetzt – bitte beim behave-Aufruf mitgeben!"
        )

    client = InfluxDBClient(url=url, token=token, org=org)
    context.influx_client = client
    context.influx_org = org
    context.influx_url = url
    context.write_api = client.write_api(write_options=SYNCHRONOUS)
    context.query_api = client.query_api()

@given("a target bucket from environment is available")
def step_bucket_from_env(context):
    bucket = os.getenv("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET nicht gesetzt!")
    context.influx_bucket = bucket


@when('I write {count:d} points with measurement "{measurement}"')
def step_write_points(context, count, measurement):
    run_id = str(uuid.uuid4())
    context.run_id = run_id
    latencies = []

    base_time = time.time_ns()

    for i in range(count):
        point = (
            Point(measurement)
            .tag("run_id", run_id)
            .field("value", i)
            .time(base_time + i, WritePrecision.NS)
        )

        start = time.perf_counter()
        context.write_api.write(
            bucket=context.influx_bucket,
            org=context.influx_org,
            record=point,
        )
        end = time.perf_counter()
        latencies.append((end - start) * 1000.0)

    context.write_latencies_ms = latencies


@then("I can read back {expected_count:d} points with the same run id")
def step_read_back(context, expected_count):
    flux = f'''
from(bucket: "{context.influx_bucket}")
  |> range(start: -10m)
  |> filter(fn: (r) => r["_measurement"] == "bddbench_write")
  |> filter(fn: (r) => r["run_id"] == "{context.run_id}")
  |> keep(columns: ["_time", "_value", "run_id"])
    '''

    tables = context.query_api.query(org=context.influx_org, query=flux)
    rows = sum(len(t.records) for t in tables)
    if rows != expected_count:
        raise AssertionError(
            f"expected {expected_count} points for run_id={context.run_id}, got {rows}"
        )


@then("the average write latency shall be <= {max_avg_ms:d} ms")
def step_check_avg_latency(context, max_avg_ms):
    lats = getattr(context, "write_latencies_ms", [])
    if not lats:
        raise AssertionError("no write latencies recorded")

    avg = statistics.mean(lats)
    if avg > max_avg_ms:
        raise AssertionError(
            f"average latency {avg:.2f} ms exceeded limit {max_avg_ms} ms"
        )
