import os
import time
import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any
import logging

from behave.runner import Context
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from behave import given, when, then
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(f"bddbench.influx_query_steps")

@given("a SUT InfluxDB v2 endpoint is configured and reachable")
def step_bucket_from_env(context: Context):
    if not context.influxdb.sut.client.ping():
        logging.getLogger("bdd_journal").error("SUT InfluxDB endpoint is not reachable")
        raise RuntimeError("SUT InfluxDB endpoint is not reachable")


@given('the target bucket from the SUT config is available')
def step_target_bucket_available(context: Context) -> None:
    assert context.influxdb.sut.bucket is not None, (
        "SUT InfluxDB bucket is not configured"
    )
    bucket_api = context.influxdb.sut.client.buckets_api()
    bucket_response = bucket_api.find_bucket_by_name(context.influxdb.sut.bucket)
    assert bucket_response is not None, (
        f"SUT InfluxDB bucket '{context.influxdb.sut.bucket}' is not available"
    )


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


def _build_flux_query(
    bucket: str, measurement: str, time_range: str, query_type: str, result_size: str
) -> str:
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
                        line = ",".join(
                            "" if cell is None else str(cell) for cell in row
                        )

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
                        bytes_returned += (
                            len(
                                json.dumps(
                                    record.values,
                                    separators=(",", ":"),
                                    ensure_ascii=False,
                                ).encode("utf-8")
                            )
                            + 1
                        )

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

    error_rate = len([r for r in runs if not r.ok]) / len(runs) if runs else 0.0

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


def _export_query_result_to_main_influx(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    runs: List[QueryRunMetrics],
    outfile: str,
    context: Context,
) -> None:
    """
    Exports a compact summary of the query benchmark to the 'main' InfluxDB.

    Requires MAIN_INFLUX_URL, MAIN_INFLUX_TOKEN, MAIN_INFLUX_ORG, MAIN_INFLUX_BUCKET
    in the environmnet. If not fully set, the export is skipped
    """
    main_url = context.influxdb.main.url
    main_token = context.influxdb.main.token
    main_org = context.influxdb.main.org
    main_bucket = context.influxdb.main.bucket

    if not main_url or not main_token or not main_org or not main_bucket:
        print(
            "[query-bench] MAIN_INFLUX_* not fully set – skipping export to main Influx"
        )
        return

    scenario_id = None
    base_name = os.path.basename(outfile)
    if base_name.startswith("query-") and base_name.endswith(".json"):
        scenario_id = base_name[len("query-") : -len(".json")]

    total_runs = len(runs)
    errors_count = len([r for r in runs if not r.ok])
    error_rate = float(summary.get("error_rate", 0.0))

    ttf_stats = summary.get("time_to_first_result_s", {}) or {}
    total_time_stats = summary.get("total_time_s", {}) or {}
    bytes_stats = summary.get("bytes_returned", {}) or {}
    rows_stats = summary.get("rows_returned", {}) or {}
    throughput = summary.get("throughput", {}) or {}

    def _f(d: Dict[str, Any], key: str) -> float:
        v = d.get(key)
        return float(v) if v is not None else 0.0

    throughput_bytes_per_s = float(throughput.get("bytes_per_s") or 0.0)
    throughput_rows_per_s = float(throughput.get("rows_per_s") or 0.0)

    client = InfluxDBClient(url=main_url, token=main_token, org=main_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    p = (
        Point("bddbench_query_result")
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("time_range", str(meta.get("time_range", "")))
        .tag("query_type", str(meta.get("query_type", "")))
        .tag("result_size", str(meta.get("result_size", "")))
        .tag("output_format", str(meta.get("output_format", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("sut_bucket", str(meta.get("bucket", "")))
        .tag("sut_org", str(meta.get("org", "")))
        .tag("sut_influx_url", str(meta.get("sut_url", "")))
        .tag("scenario_id", scenario_id or "")
        .field("total_runs", int(total_runs))
        .field("errors_count", int(errors_count))
        .field("error_rate", error_rate)
        .field("ttf_min_s", _f(ttf_stats, "min"))
        .field("ttf_max_s", _f(ttf_stats, "max"))
        .field("ttf_avg_s", _f(ttf_stats, "avg"))
        .field("ttf_median_s", _f(ttf_stats, "median"))
        .field("total_time_min_s", _f(total_time_stats, "min"))
        .field("total_time_max_s", _f(total_time_stats, "max"))
        .field("total_time_avg_s", _f(total_time_stats, "avg"))
        .field("total_time_median_s", _f(total_time_stats, "median"))
        .field("bytes_min", _f(bytes_stats, "min"))
        .field("bytes_max", _f(bytes_stats, "max"))
        .field("bytes_avg", _f(bytes_stats, "avg"))
        .field("bytes_median", _f(bytes_stats, "median"))
        .field("rows_min", _f(rows_stats, "min"))
        .field("rows_max", _f(rows_stats, "max"))
        .field("rows_avg", _f(rows_stats, "avg"))
        .field("rows_median", _f(rows_stats, "median"))
        .field("throughput_bytes_per_s", throughput_bytes_per_s)
        .field("throughput_rows_per_s", throughput_rows_per_s)
    )

    write_api.write(bucket=main_bucket, org=main_org, record=p)
    client.close()

    logger.info("Exported query result to main Influx")


# ----------- Scenario Steps -------------


@when(
    'I run a generic query benchmark on measurement "{measurement}" '
    'with time range "{time_range}" using query type "{query_type}" '
    'and result size "{result_size}" with {concurrent_clients:d} concurrent clients, '
    'output format "{output_format}" and compression "{compression}"'
)
def step_run_query_benchmark(
    context,
    measurement,
    time_range,
    query_type,
    result_size,
    concurrent_clients,
    output_format,
    compression,
):
    """
    runs the same Flux-Query n times parallel from (concurrent_clients) and measures:
      - time_to_first_result
      - total_time
      - bytes/rows
      - error_rate
    """

    flux = _build_flux_query(
        context.influxdb.sut.bucket,
        measurement,
        time_range,
        query_type,
        result_size,
    )

    runs: List[QueryRunMetrics] = []
    with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
        futures = [
            executor.submit(
                _run_single_query,
                client_id=i,
                base_url=context.influxdb.sut.url,
                token=context.influxdb.sut.token,
                org=context.influxdb.sut.org,
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
        "bucket": context.influxdb.sut.bucket,
        "org": context.influxdb.sut.org,
        "sut_url": context.influxdb.sut.url,
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

    logger.info(
        f"Stored generic query benchmark result to {outfile}"
    )

    _export_query_result_to_main_influx(meta, summary, runs, outfile, context)
