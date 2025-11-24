import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from behave import when, then
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# =====================================================================
#                       INTERNAL HELPERS
# =====================================================================

def _ensure_sut_client(context) -> None:
    """
    Ensure we have a client to the SUT InfluxDB on the behave context.

    Uses env vars:
        INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG
    """
    if getattr(context, "sut_client", None):
        return

    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL / INFLUX_TOKEN / INFLUX_ORG must be set for SUT.")

    client = InfluxDBClient(url=url, token=token, org=org)
    context.sut_client = client
    context.sut_org = org
    context.sut_write_api = client.write_api(write_options=SYNCHRONOUS)
    context.sut_query_api = client.query_api()
    context.sut_delete_api = client.delete_api()


def _ensure_sut_bucket(context) -> None:
    """
    Ensure the SUT bucket (where we write/delete test data) is present on context.

    Uses env var:
        INFLUX_BUCKET
    """
    bucket = os.getenv("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set for SUT.")
    context.sut_bucket = bucket


def _ensure_results_client(context) -> None:
    """
    Ensure we have a client to the MAIN InfluxDB where we store benchmark results.

    Uses env vars:
        MAIN_INFLUX_URL, MAIN_INFLUX_TOKEN, MAIN_INFLUX_ORG, MAIN_INFLUX_BUCKET
    """
    if getattr(context, "results_client", None):
        return

    url = os.getenv("MAIN_INFLUX_URL")
    token = os.getenv("MAIN_INFLUX_TOKEN")
    org = os.getenv("MAIN_INFLUX_ORG")
    bucket = os.getenv("MAIN_INFLUX_BUCKET")

    if not url or not token or not org or not bucket:
        raise RuntimeError(
            "MAIN_INFLUX_URL / MAIN_INFLUX_TOKEN / MAIN_INFLUX_ORG / MAIN_INFLUX_BUCKET "
            "must be set to export delete benchmark results."
        )

    client = InfluxDBClient(url=url, token=token, org=org)
    context.results_client = client
    context.results_org = org
    context.results_bucket = bucket
    context.results_write_api = client.write_api(write_options=SYNCHRONOUS)


def _ns_to_rfc3339(ns: int) -> str:
    """Convert nanoseconds since epoch to RFC3339 timestamp string (UTC)."""
    dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    return dt.isoformat()


def _wide_range_rfc3339() -> tuple[str, str]:
    """
    Very wide time-range for delete / verification when we want to delete
    an entire measurement from the SUT test bucket.
    """
    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"
    return start, stop


# =====================================================================
#  ORIGINAL DELETE-TEST LOGIC   (using run_id)
# =====================================================================

@when('I write {count:d} delete-test points with measurement "{measurement}"')
def step_write_delete_test_points(context, count: int, measurement: str) -> None:
    """
    Writes 'count' points for a delete benchmark run into the SUT InfluxDB.

    All points:
        measurement = measurement (e.g. "bddbench_delete")
        tag "run_id" = context.delete_run_id (unique for this run)
        field "value" = running index

    We also record the min/max timestamp in ns so we can use them for delete
    and for verification queries.
    """
    _ensure_sut_client(context)
    _ensure_sut_bucket(context)

    run_id = str(uuid.uuid4())
    context.delete_run_id = run_id
    context.delete_expected_count = count

    bucket = context.sut_bucket

    min_ts_ns: Optional[int] = None
    max_ts_ns: Optional[int] = None

    for i in range(count):
        ts_ns = time.time_ns()

        if min_ts_ns is None or ts_ns < min_ts_ns:
            min_ts_ns = ts_ns
        if max_ts_ns is None or ts_ns > max_ts_ns:
            max_ts_ns = ts_ns

        p = (
            Point(measurement)
            .tag("run_id", run_id)
            .tag("kind", "delete_bench")
            .field("value", i)
            .time(ts_ns, WritePrecision.NS)
        )

        context.sut_write_api.write(
            bucket=bucket,
            org=context.sut_org,
            record=p,
        )

    # store time range for delete + verification (add a little slack on stop)
    context.delete_time_start_ns = min_ts_ns
    context.delete_time_end_ns = max_ts_ns
    context.delete_measurement = measurement


@when("I delete all points for this delete-test run")
def step_delete_points_for_run(context) -> None:
    """
    Calls the SUT's delete API once with a predicate targeting this run_id and
    the previously recorded time range. Measures delete duration and also
    writes a benchmark result to the MAIN InfluxDB (if configured).
    """
    _ensure_sut_client(context)
    _ensure_sut_bucket(context)

    run_id = getattr(context, "delete_run_id", None)
    if not run_id:
        raise RuntimeError("delete_run_id not set – did the write phase run before?")

    start_ns = getattr(context, "delete_time_start_ns", None)
    end_ns = getattr(context, "delete_time_end_ns", None)
    if start_ns is None or end_ns is None:
        raise RuntimeError("Time range for delete not set.")

    # Convert to RFC3339 strings; extend stop slightly to be safe
    start_rfc3339 = _ns_to_rfc3339(start_ns)
    stop_rfc3339 = _ns_to_rfc3339(end_ns + 1_000_000)  # +1 ms

    bucket = context.sut_bucket
    predicate = f'run_id="{run_id}"'

    t0 = time.perf_counter()
    context.sut_delete_api.delete(
        start=start_rfc3339,
        stop=stop_rfc3339,
        predicate=predicate,
        bucket=bucket,
        org=context.sut_org,
    )
    t1 = time.perf_counter()

    delete_duration_ms = (t1 - t0) * 1000.0
    context.delete_duration_ms = delete_duration_ms

    # Try to log the delete result to MAIN InfluxDB
    try:
        _ensure_results_client(context)
    except RuntimeError:
        # MAIN_* is optional; if not configured, silently skip result logging.
        return

    p = (
        Point("delete_benchmark")
        .tag("run_id", run_id)
        .tag("sut_bucket", bucket)
        .field("delete_duration_ms", float(delete_duration_ms))
        .field("count", int(getattr(context, "delete_expected_count", 0)))
    )

    context.results_write_api.write(
        bucket=context.results_bucket,
        org=context.results_org,
        record=p,
    )


@then("no points for this delete-test run shall remain")
def step_verify_no_points_remain(context) -> None:
    _ensure_sut_client(context)
    _ensure_sut_bucket(context)

    run_id = getattr(context, "delete_run_id", None)
    if not run_id:
        raise RuntimeError("delete_run_id not set – did the write phase run before?")

    start_ns = getattr(context, "delete_time_start_ns", None)
    end_ns = getattr(context, "delete_time_end_ns", None)
    if start_ns is None or end_ns is None:
        raise RuntimeError("Time range for verification not set.")

    start_rfc3339 = _ns_to_rfc3339(start_ns)
    stop_rfc3339 = _ns_to_rfc3339(end_ns + 1_000_000)

    bucket = context.sut_bucket
    measurement = getattr(context, "delete_measurement", "bddbench_delete")

    flux = f'''
from(bucket: "{bucket}")
  |> range(start: time(v: "{start_rfc3339}"), stop: time(v: "{stop_rfc3339}"))
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["run_id"] == "{run_id}")
  |> keep(columns: ["_time", "_value", "run_id"])
    '''

    tables = context.sut_query_api.query(org=context.sut_org, query=flux)
    rows = sum(len(t.records) for t in tables)

    if rows != 0:
        raise AssertionError(
            f"{rows} points for run_id={run_id} still present – delete incomplete."
        )


# =====================================================================
#  NEW: delete the points written by the generic write benchmark
# =====================================================================

@when("I delete all points for this generic write benchmark")
def step_delete_points_for_generic_write_benchmark(context) -> None:
    """
    Uses context.write_benchmark_meta from influx_write_steps.py to find
    which bucket + measurement were used, then deletes *all points for that
    measurement* in that bucket and measures the delete duration.
    """
    _ensure_sut_client(context)
    _ensure_sut_bucket(context)

    meta = getattr(context, "write_benchmark_meta", None)
    if not meta:
        raise RuntimeError(
            "write_benchmark_meta not found on context – "
            "was the generic write benchmark step executed before?"
        )

    measurement = meta.get("measurement")
    if not measurement:
        raise RuntimeError("write_benchmark_meta['measurement'] not set.")

    bucket = meta.get("bucket") or context.sut_bucket
    org = context.sut_org

    start_rfc3339, stop_rfc3339 = _wide_range_rfc3339()
    predicate = f'_measurement="{measurement}"'

    t0 = time.perf_counter()
    context.sut_delete_api.delete(
        start=start_rfc3339,
        stop=stop_rfc3339,
        predicate=predicate,
        bucket=bucket,
        org=org,
    )
    t1 = time.perf_counter()

    delete_duration_ms = (t1 - t0) * 1000.0
    context.delete_duration_ms = delete_duration_ms
    context.delete_measurement = measurement
    context.delete_expected_count = int(meta.get("total_points", 0))

    # Log result to MAIN Influx if configured
    try:
        _ensure_results_client(context)
    except RuntimeError:
        # MAIN_* is optional; if not configured, just skip export.
        return

    # optional: pick up "size" from Scenario Outline examples, if present
    size_label = None
    scenario = getattr(context, "scenario", None)
    if getattr(scenario, "outline", None) and getattr(scenario, "_row", None):
        try:
            header = scenario.outline.examples[0].table.headings
            row_dict = dict(zip(header, scenario._row.cells))
            size_label = row_dict.get("size")
        except Exception:
            pass

    p = (
        Point("bddbench_delete_result")
        .tag("sut_bucket", bucket)
        .tag("sut_org", org)
        .tag("sut_influx_url", os.getenv("INFLUX_URL", ""))
        .tag("measurement", measurement)
        .tag("size", size_label or "unknown")
        .field("delete_duration_ms", float(delete_duration_ms))
        .field("expected_points", int(context.delete_expected_count))
    )

    context.results_write_api.write(
        bucket=context.results_bucket,
        org=context.results_org,
        record=p,
    )


@then("no points for this generic write benchmark shall remain")
def step_verify_no_points_remain_for_generic_write_benchmark(context) -> None:
    """
    Checks that no points with the measurement used by the generic write
    benchmark remain in the SUT bucket.
    """
    _ensure_sut_client(context)
    _ensure_sut_bucket(context)

    meta = getattr(context, "write_benchmark_meta", None)
    if not meta:
        raise RuntimeError(
            "write_benchmark_meta not found – cannot verify remaining points "
            "for generic write benchmark."
        )

    measurement = meta.get("measurement")
    if not measurement:
        raise RuntimeError("write_benchmark_meta['measurement'] not set.")

    bucket = meta.get("bucket") or context.sut_bucket
    org = context.sut_org

    start_rfc3339, stop_rfc3339 = _wide_range_rfc3339()

    flux = f'''
from(bucket: "{bucket}")
  |> range(start: time(v: "{start_rfc3339}"), stop: time(v: "{stop_rfc3339}"))
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> limit(n: 1)
    '''

    tables = context.sut_query_api.query(org=org, query=flux)
    rows = sum(len(t.records) for t in tables)

    if rows != 0:
        raise AssertionError(
            f"{rows} points for measurement={measurement} still present – "
            "generic write delete incomplete."
        )


# =====================================================================
#            SHARED ASSERTION FOR DELETE LATENCY
# =====================================================================

@then("the delete duration shall be <= {max_ms:d} ms")
def step_check_delete_duration(context, max_ms: int) -> None:
    delete_duration_ms = getattr(context, "delete_duration_ms", None)
    if delete_duration_ms is None:
        raise AssertionError("No delete duration recorded (delete_duration_ms is missing).")

    if delete_duration_ms > max_ms:
        raise AssertionError(
            f"Delete took {delete_duration_ms:.2f} ms, threshold {max_ms} ms exceeded."
        )
