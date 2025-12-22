import os
import time
import json
import statistics
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
import logging

from behave.runner import Context
from behave import when, then
from concurrent.futures import ThreadPoolExecutor, as_completed

from influxdb_client import InfluxDBClient, Point

from src.utils import (
    write_json_report,
    scenario_id_from_outfile,
    generate_base_point,
    export_point_to_main_influx,
)

logger = logging.getLogger("bddbench.influx_query_steps")


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
    Builds a simple Flux-Query depending on query_type and result_size.

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
  |> group(columns: ["device_id"])
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
  on: ["_time", "device_id"]
)
"""
    else:
        flux = f"""{base}
  |> limit(n: {limit_n})
"""

    return flux


def _run_single_query(
    client_id: int,
    base_url: str,
    token: str,
    org: str,
    flux: str,
    output_format: str,
    compression: str,
) -> QueryRunMetrics:
    """
    Execute a Flux query and collect timing/size metrics
    for a single logical client.
    """
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

    def safe_vals(fn: str):
        vals = [getattr(r, fn) for r in runs if getattr(r, fn) is not None]
        return vals

    ttf = safe_vals("time_to_first_result_s")
    ttotal = safe_vals("total_time_s")
    bytes_vals = safe_vals("bytes_returned")
    rows_vals = safe_vals("rows_returned")

    error_rate = len([r for r in runs if not r.ok]) / len(runs) if runs else 0.0

    def agg(vals: List[float]):
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




def _build_query_export_point(
    *,
    context: Context,
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    runs: List["QueryRunMetrics"],
) -> Point:
    """Build the Point for exporting a generic query benchmark summary to MAIN Influx."""

    ttf_stats = summary.get("time_to_first_result_s", {}) or {}
    total_time_stats = summary.get("total_time_s", {}) or {}
    bytes_stats = summary.get("bytes_returned", {}) or {}
    rows_stats = summary.get("rows_returned", {}) or {}
    throughput = summary.get("throughput", {}) or {}
    error_rate = float(summary.get("error_rate", 0.0) or 0.0)

    total_runs = len(runs)
    errors_count = len([r for r in runs if not getattr(r, "ok", False)])

    def _f(d: Dict[str, Any], key: str) -> float:
        v = d.get(key)
        return float(v) if v is not None else 0.0

    p = generate_base_point(
        context=context,
        measurement="bddbench_query_result",
        scenario_id=scenario_id,
    )

    # step-specific tags
    p.tag("source_measurement", str(meta.get("measurement", "")))
    p.tag("time_range", str(meta.get("time_range", "")))
    p.tag("query_type", str(meta.get("query_type", "")))
    p.tag("result_size", str(meta.get("result_size", "")))
    p.tag("output_format", str(meta.get("output_format", "")))
    p.tag("compression", str(meta.get("compression", "")))

    # fields
    p.field("total_runs", int(total_runs))
    p.field("errors_count", int(errors_count))
    p.field("error_rate", error_rate)

    p.field("ttf_min_s", _f(ttf_stats, "min"))
    p.field("ttf_max_s", _f(ttf_stats, "max"))
    p.field("ttf_avg_s", _f(ttf_stats, "avg"))
    p.field("ttf_median_s", _f(ttf_stats, "median"))

    p.field("total_time_min_s", _f(total_time_stats, "min"))
    p.field("total_time_max_s", _f(total_time_stats, "max"))
    p.field("total_time_avg_s", _f(total_time_stats, "avg"))
    p.field("total_time_median_s", _f(total_time_stats, "median"))

    p.field("bytes_min", _f(bytes_stats, "min"))
    p.field("bytes_max", _f(bytes_stats, "max"))
    p.field("bytes_avg", _f(bytes_stats, "avg"))
    p.field("bytes_median", _f(bytes_stats, "median"))

    p.field("rows_min", _f(rows_stats, "min"))
    p.field("rows_max", _f(rows_stats, "max"))
    p.field("rows_avg", _f(rows_stats, "avg"))
    p.field("rows_median", _f(rows_stats, "median"))

    p.field("throughput_bytes_per_s", float(throughput.get("bytes_per_s") or 0.0))
    p.field("throughput_rows_per_s", float(throughput.get("rows_per_s") or 0.0))

    return p


def _export_query_result_to_main_influx(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    runs: List[QueryRunMetrics],
    outfile: str,
    context: Context,
) -> None:
    """Sends a compact summary of the query benchmark result to MAIN InfluxDB (if configured)."""

    scenario_id = scenario_id_from_outfile(outfile, prefixes=("query-",))
    p = _build_query_export_point(
        context=context,
        meta=meta,
        summary=summary,
        scenario_id=scenario_id,
        runs=runs,
    )
    export_point_to_main_influx(context=context, point=p, bench_label="query", logger_=logger)


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

    write_json_report(
        outfile,
        result,
        logger_=logger,
        log_prefix="Stored generic query benchmark result to ",
    )

    _export_query_result_to_main_influx(meta, summary, runs, outfile, context)
