import os
import time
import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

from influxdb_client import InfluxDBClient #py lib
from behave import given, when, then
from concurrent.futures import ThreadPoolExecutor, as_completed

@given("a generic InfluxDB v2 endpoint is configured from environment")
def step_generic_endpoint_from_env(context):
    """
    Reads INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG from the environment and saves them in the context.
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
    Reads INFLUX_BUCKET and saves in context.
    """
    bucket = os.getenv("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set in environment")
    context.influx_bucket = bucket

# ---------- Datatypes -----------

@dataclass
class QueryRunMetrics:
    client_id: int
    status_code: int
    ok: bool
    time_to_first_result_s: float | None
    total_time_s: float | None
    bytes_returned: int
    rows_returned: int

# ---------- Helpers ----------

def _build_flux_query(bucket: str,
                      measurement: str,
                      time_range: str,
                      query_type: str,
                      result_size: str) -> str:
    """
    BUilds a simple Flux-Query depending on query_type and result_size

    assumptions:
      - measurement-field "_measurement" gets set via filter
      - result_size small/large controls limit()
    """
    limit_small = 500
    limit_large = 50_000

    limit_n = limit_small if result_size == "small" else limit_large

    base = f'''
from(bucket: "{bucket}")
  |> range(start: -{time_range})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
'''.strip()

    if query_type == "filter":
        flux = f"""{base}
  |> filter(fn: (r) => exists r["value"])
  |> limit(n: {limit_n})
"""
    elif query_type == "aggregate":
        flux = f"""{base}
  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
  |> limit(n: {limit_n})
"""
    elif query_type == "group_by":
        flux = f"""{base}
  |> group(columns: ["run_id"])
  |> limit(n: {limit_n})
"""
    elif query_type == "pivot":
        flux = f"""{base}
  |> limit(n: {limit_n})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
"""
    elif query_type == "join":
        flux = f"""
left = {base}
  |> filter(fn: (r) => r["_field"] == "value")
  |> limit(n: {limit_n})

right = {base}
  |> filter(fn: (r) => r["_field"] == "value")
  |> limit(n: {limit_n})

join(
  tables: {{left: left, right: right}},
  on: ["_time", "run_id"]
)
"""
    else:
        flux = f"""{base}
  |> limit(n: {limit_n})
"""

    return flux

def _run_single_query(
    # Execute a Flux query and collect timing/size metrics
    client_id: int,
    base_url: str,
    token: str,
    org: str,
    flux: str,
    output_format: str,
    compression: str,
) -> QueryRunMetrics:
    
    enable_gzip = compression == "gzip"

    t_start = time.perf_counter()

    try:
        with InfluxDBClient(
            url=base_url,
            token=token,
            org=org,
            enable_gzip=enable_gzip,
            timeout=300_000,  # ms
        ) as client:
            query_api = client.query_api()

            bytes_returned = 0
            rows_returned = 0
            time_to_first: float | None = None

            if output_format == "csv":
                csv_iter = query_api.query_csv(query=flux, org=org)

                for row in csv_iter:
                    now = time.perf_counter()
                    if time_to_first is None:
                        time_to_first = now - t_start

                    if isinstance(row, str):
                        line = row
                    else:
                        line = ",".join("" if cell is None else str(cell) for cell in row)

                    rows_returned += 1
                    bytes_returned += len((line + "\n").encode("utf-8"))

            else:
              
                tables = query_api.query(query=flux, org=org)
                for table in tables:
                    for record in table.records:
                        now = time.perf_counter()
                        if time_to_first is None:
                            time_to_first = now - t_start

                        rows_returned += 1
                        bytes_returned += len(
                            json.dumps(
                                record.values,
                                separators=(",", ":"),
                                ensure_ascii=False,
                            ).encode("utf-8")
                        ) + 1

    except Exception:
        return QueryRunMetrics(
            client_id=client_id,
            status_code=0,
            ok=False,
            time_to_first_result_s=None,
            total_time_s=None,
            bytes_returned=0,
            rows_returned=0,
        )

    t_end = time.perf_counter()

    return QueryRunMetrics(
        client_id=client_id,
        # The python client raises on non-2xx; reaching here implies success.
        status_code=200,
        ok=True,
        time_to_first_result_s=time_to_first,
        total_time_s=t_end - t_start,
        bytes_returned=bytes_returned,
        rows_returned=rows_returned,
    )


def _summarize_query_runs(runs: List[QueryRunMetrics]) -> Dict[str, Any]:
    if not runs:
        return {}

    def safe_vals(fn):
        vals = [getattr(r, fn) for r in runs if getattr(r, fn) is not None]
        return vals

    ttf = safe_vals("time_to_first_result_s")
    ttotal = safe_vals("total_time_s")
    bytes_vals = safe_vals("bytes_returned")
    rows_vals = safe_vals("rows_returned")

    error_rate = (
        len([r for r in runs if not r.ok]) / len(runs)
        if runs else 0.0
    )

    def agg(vals):
        if not vals:
            return {"min": None, "max": None, "avg": None, "median": None}
        return {
            "min": min(vals),
            "max": max(vals),
            "avg": statistics.mean(vals),
            "median": statistics.median(vals),
        }

    duration_avg = statistics.mean(ttotal) if ttotal else None

    throughput_bytes_per_s = None
    throughput_rows_per_s = None
    if duration_avg and bytes_vals:
        throughput_bytes_per_s = statistics.mean(bytes_vals) / duration_avg
    if duration_avg and rows_vals:
        throughput_rows_per_s = statistics.mean(rows_vals) / duration_avg

    return {
        "time_to_first_result_s": agg(ttf),
        "total_time_s": agg(ttotal),
        "bytes_returned": agg(bytes_vals),
        "rows_returned": agg(rows_vals),
        "throughput": {
            "bytes_per_s": throughput_bytes_per_s,
            "rows_per_s": throughput_rows_per_s,
        },
        "error_rate": error_rate,
    }


# ----------- Scenario Steps -------------

@when(
    'I run a generic query benchmark on measurement "{measurement}" '
    'with time range "{time_range}" using query type "{query_type}" '
    'and result size "{result_size}" with {concurrent_clients:d} concurrent clients, '
    'output format "{output_format}" and compression "{compression}"'
)
def step_run_query_benchmark(context,
                             measurement,
                             time_range,
                             query_type,
                             result_size,
                             concurrent_clients,
                             output_format,
                             compression):
    """
    runs the same Flux-Query n times parallel from (concurrent_clients) and measures:
      - time_to_first_result
      - total_time
      - bytes/rows
      - error_rate
    """
    base_url = getattr(context, "influx_url", None) or os.getenv("INFLUX_URL")
    token = getattr(context, "influx_token", None) or os.getenv("INFLUX_TOKEN")
    org = getattr(context, "influx_org", None) or os.getenv("INFLUX_ORG")
    bucket = getattr(context, "influx_bucket", None) or os.getenv("INFLUX_BUCKET")

    if not base_url or not token or not org or not bucket:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET must be set")

    flux = _build_flux_query(bucket, measurement, time_range, query_type, result_size)

    runs: List[QueryRunMetrics] = []
    with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
        futures = [
            executor.submit(
                _run_single_query,
                client_id=i,
                base_url=base_url,
                token=token,
                org=org,
                flux=flux,
                output_format=output_format,
                compression=compression,
            )
            for i in range(concurrent_clients)
        ]

        for fut in as_completed(futures):
            runs.append(fut.result())

    context.query_runs = runs
    context.query_benchmark_meta = {
        "measurement": measurement,
        "time_range": time_range,
        "query_type": query_type,
        "result_size": result_size,
        "concurrent_clients": concurrent_clients,
        "output_format": output_format,
        "compression": compression,
        "bucket": bucket,
        "org": org,
    }
    context.query_summary = _summarize_query_runs(runs)


@then('I store the generic query benchmark result as "{outfile}"')
def step_store_query_result(context, outfile):
    runs: List[QueryRunMetrics] = getattr(context, "query_runs", [])
    meta = getattr(context, "query_benchmark_meta", {})
    summary = getattr(context, "query_summary", {})

    result = {
        "meta": meta,
        "summary": summary,
        "runs": [asdict(r) for r in runs],
        "created_at_epoch_s": time.time(),
    }

    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print("=== Generic Query Benchmark Result ===")
    print(json.dumps(result, indent=2))
