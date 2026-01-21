import logging
import os
import time
import statistics
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional

from behave import when, then
from behave.runner import Context
from influxdb_client import InfluxDBClient, WritePrecision, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils import (
    influx_precision_from_str,
    base_timestamp_for_precision,
    build_benchmark_point,
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
    write_to_influx, generate_base_point,
)

logger = logging.getLogger("bddbench.influx_multi_bucket_steps")


# ----------- Datatypes ------------

@dataclass
class BatchWriteMetrics:
    batch_index: int
    latency_s: float
    points: int
    status_code: int
    ok: bool


@dataclass
class QueryRunMetrics:
    """Metrics for a single query execution."""
    client_id: int
    bucket: str
    status_code: int
    ok: bool
    time_to_first_result_s: float | None
    total_time_s: float | None
    bytes_returned: int
    rows_returned: int


def _build_flux_query(
    bucket: str,
    measurement: str,
    time_range: str,
    query_type: str,
    result_size: str,
    run_id: Optional[str] = None,
) -> str:
    """Build a simple Flux query (mirrors the single-bucket query benchmark)."""

    limit_small = 500
    limit_large = 50_000
    limit_n = limit_small if result_size == "small" else limit_large

    base = f'''
from(bucket: "{bucket}")
  |> range(start: -{time_range})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
'''.strip()

    if run_id:
        base = f"""{base}
  |> filter(fn: (r) => r[\"run_id\"] == \"{run_id}\")
""".strip()

    if query_type == "filter":
        return f"""{base}
  |> filter(fn: (r) => exists r[\"value\"])
  |> limit(n: {limit_n})
"""

    if query_type == "aggregate":
        return f"""{base}
  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
  |> limit(n: {limit_n})
"""

    if query_type == "group_by":
        return f"""{base}
  |> group(columns: [\"device_id\"])
  |> limit(n: {limit_n})
"""

    if query_type == "pivot":
        return f"""{base}
  |> limit(n: {limit_n})
  |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")
"""

    if query_type == "join":
        return f"""
left = {base}
  |> filter(fn: (r) => r[\"_field\"] == \"value\")
  |> limit(n: {limit_n})

right = {base}
  |> filter(fn: (r) => r[\"_field\"] == \"value\")
  |> limit(n: {limit_n})

join(
  tables: {{left: left, right: right}},
  on: [\"_time\", \"device_id\"]
)
"""

    # default
    return f"""{base}
  |> limit(n: {limit_n})
"""


def _run_single_query(
    *,
    client_id: int,
    bucket: str,
    base_url: str,
    token: str,
    org: str,
    flux: str,
    output_format: str,
    compression: str,
) -> QueryRunMetrics:
    """Execute a Flux query and collect metrics (one logical client)."""

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
            bucket=bucket,
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
        bucket=bucket,
        status_code=200,
        ok=True,
        time_to_first_result_s=time_to_first,
        total_time_s=t_end - t_start,
        bytes_returned=bytes_returned,
        rows_returned=rows_returned,
    )


def _summarize_query_runs(runs: List[QueryRunMetrics]) -> Dict[str, Any]:
    """Aggregate a single summary across all buckets (joint report)."""
    if not runs:
        return {
            "total_runs": 0,
            "errors_count": 0,
            "error_rate": 0.0,
            "latency_stats": {},
            "bytes_stats": {},
            "rows_stats": {},
            "throughput": {},
            "queries_count": 0,
            "total_duration_s": 0.0,
        }

    def safe_vals(attr: str):
        return [getattr(r, attr) for r in runs if getattr(r, attr) is not None]

    ttf = safe_vals("time_to_first_result_s")
    ttotal = safe_vals("total_time_s")
    bytes_vals = safe_vals("bytes_returned")
    rows_vals = safe_vals("rows_returned")

    def agg(vals: List[float]):
        if not vals:
            return {"min": None, "max": None, "avg": None, "median": None}
        return {
            "min": min(vals),
            "max": max(vals),
            "avg": statistics.mean(vals),
            "median": statistics.median(vals),
        }

    total_runs = len(runs)
    errors_count = len([r for r in runs if not r.ok])
    total_duration_s = max(ttotal) if ttotal else 0.0
    queries_count = total_runs

    throughput_queries_per_s = (queries_count / total_duration_s) if total_duration_s > 0 else None
    throughput_bytes_per_s = (sum(bytes_vals) / total_duration_s) if total_duration_s > 0 and bytes_vals else None
    throughput_rows_per_s = (sum(rows_vals) / total_duration_s) if total_duration_s > 0 and rows_vals else None

    ttf_stats = agg(ttf)
    total_stats = agg(ttotal)

    return {
        "queries_count": queries_count,
        "total_duration_s": total_duration_s,
        "total_runs": total_runs,
        "errors_count": errors_count,
        "error_rate": (errors_count / total_runs) if total_runs else 0.0,
        "latency_stats": {
            "ttf_min": ttf_stats["min"],
            "ttf_max": ttf_stats["max"],
            "ttf_avg": ttf_stats["avg"],
            "ttf_median": ttf_stats["median"],
            "total_min": total_stats["min"],
            "total_max": total_stats["max"],
            "total_avg": total_stats["avg"],
            "total_median": total_stats["median"],
        },
        "bytes_stats": agg(bytes_vals),
        "rows_stats": agg(rows_vals),
        "throughput": {
            "queries_per_s": throughput_queries_per_s,
            "bytes_per_s": throughput_bytes_per_s,
            "rows_per_s": throughput_rows_per_s,
        },
    }


# ---------- Helpers ----------
def build_multi_write_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    context: Context
) -> Point:
    latency_stats = summary.get("latency_stats", {}) or {}
    throughput = summary.get("throughput", {}) or {}
    p = generate_base_point(context=context, measurement="bddbench_multi_write_result")
    return (
        p
        .tag("source_measurement", str(meta.get("measurement", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("precision", str(meta.get("precision", "")))
        .tag("point_complexity", str(meta.get("point_complexity", "")))
        .tag("time_ordering", str(meta.get("time_ordering", "")))
        .tag("sut_bucket_prefix", str(meta.get("bucket_prefix", "")))
        .tag("scenario_id", scenario_id or "")
        .field("bucket_count", int(meta.get("bucket_count", 0)))
        .field("total_points", int(meta.get("total_points", 0)))
        .field("total_batches", int(meta.get("total_batches", 0)))
        .field("total_duration_s", float(meta.get("total_duration_s", 0.0)))
        .field(
            "throughput_points_per_s",
            float(throughput.get("points_per_s") or 0.0),
        )
        .field("error_rate", float(summary.get("error_rate", 0.0)))
        .field("errors_count", int(summary.get("errors_count", 0)))
        .field("latency_min_s", float(latency_stats.get("min") or 0.0))
        .field("latency_max_s", float(latency_stats.get("max") or 0.0))
        .field("latency_avg_s", float(latency_stats.get("avg") or 0.0))
        .field("latency_median_s", float(latency_stats.get("median") or 0.0))
    )

def _export_multi_write_result_to_main_influx(
    result: Dict[str, Any],
    outfile: str,
    context: Context,
) -> None:
    """
    Sends a compact summary of the multi-bucket write benchmark result to the
    'main' InfluxDB.

    Uses context.influxdb.main.* from environment.py.
    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning.
    """
    if not main_influx_is_configured(context):
        logger.info(" MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = bool(getattr(context.influxdb, "export_strict", False))

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info(" MAIN write_api missing – skipping export")
        return

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    scenario_id = scenario_id_from_outfile(outfile, prefixes=("multi-write-",))

    p = build_multi_write_export_point(meta=meta, summary=summary, scenario_id=scenario_id, context=context)

    try:
        write_to_influx(
            write_api=write_api,
            bucket=main.bucket,
            org=main.org,
            record=p,
            logger_=logger,
            strict=strict,
            success_msg="Exported write result to MAIN Influx",
            failure_prefix="MAIN export failed",
        )
    except ApiException as exc:
        msg = f" MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger.warning(msg)
    except Exception as exc:
        msg = f" MAIN export failed: {exc}"
        if strict:
            raise
        logger.warning(msg)


def build_multi_query_export_point(
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
    context: Context,
) -> Point:
    latency_stats = summary.get("latency_stats", {}) or {}
    throughput = summary.get("throughput", {}) or {}
    p = generate_base_point(context=context, measurement="bddbench_multi_query_result")
    return (
        p
        .tag("measurement", str(meta.get("measurement", "")))
        .tag("time_range", str(meta.get("time_range", "")))
        .tag("query_type", str(meta.get("query_type", "")))
        .tag("result_size", str(meta.get("result_size", "")))
        .tag("output_format", str(meta.get("output_format", "")))
        .tag("compression", str(meta.get("compression", "")))
        .tag("sut_bucket_prefix", str(meta.get("bucket_prefix", "")))
        .tag("scenario_id", scenario_id or "")
        .field("bucket_count", int(meta.get("bucket_count", 0)))
        .field("concurrent_clients_per_bucket", int(meta.get("concurrent_clients_per_bucket", 0)))
        .field("total_concurrent_clients", int(meta.get("total_concurrent_clients", 0)))
        .field("queries_count", int(summary.get("queries_count", 0)))
        .field("total_duration_s", float(summary.get("total_duration_s", 0.0)))
        .field("total_runs", int(summary.get("total_runs", 0)))
        .field("errors_count", int(summary.get("errors_count", 0)))
        .field("error_rate", float(summary.get("error_rate", 0.0)))
        .field("throughput_queries_per_s", float(throughput.get("queries_per_s") or 0.0))
        .field("throughput_rows_per_s", float(throughput.get("rows_per_s") or 0.0))
        .field("throughput_bytes_per_s", float(throughput.get("bytes_per_s") or 0.0))
        .field("ttf_min_s", float(latency_stats.get("ttf_min") or 0.0))
        .field("ttf_max_s", float(latency_stats.get("ttf_max") or 0.0))
        .field("ttf_avg_s", float(latency_stats.get("ttf_avg") or 0.0))
        .field("ttf_median_s", float(latency_stats.get("ttf_median") or 0.0))
        .field("total_min_s", float(latency_stats.get("total_min") or 0.0))
        .field("total_max_s", float(latency_stats.get("total_max") or 0.0))
        .field("total_avg_s", float(latency_stats.get("total_avg") or 0.0))
        .field("total_median_s", float(latency_stats.get("total_median") or 0.0))
    )


def _export_multi_query_result_to_main_influx(
    *,
    result: Dict[str, Any],
    outfile: str,
    context: Context,
) -> None:
    if not main_influx_is_configured(context):
        logger.info(" MAIN influx not configured – skipping export")
        return

    main = context.influxdb.main
    strict = bool(getattr(context.influxdb, "export_strict", False))

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        logger.info(" MAIN write_api missing – skipping export")
        return

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    scenario_id = scenario_id_from_outfile(outfile, prefixes=("multi-query-",))

    p = build_multi_query_export_point(meta=meta, summary=summary, scenario_id=scenario_id, context=context)

    try:
        write_to_influx(
            write_api=write_api,
            bucket=main.bucket,
            org=main.org,
            record=p,
            logger_=logger,
            strict=strict,
            success_msg="Exported multi-query result to MAIN Influx",
            failure_prefix="MAIN export failed",
        )
    except ApiException as exc:
        msg = f" MAIN export failed: HTTP {exc.status} {exc.reason} - {exc.body}"
        if strict:
            raise
        logger.warning(msg)
    except Exception as exc:
        msg = f" MAIN export failed: {exc}"
        if strict:
            raise
        logger.warning(msg)


def _create_bucket_compat(
    buckets_api: Any,
    *,
    bucket_name: str,
    org: str,
    org_id: str | None,
) -> None:
    """
    InfluxDB client versions differ: some expect org=..., others org_id=...
    We try both to avoid hard failures.
    """
    try:
        buckets_api.create_bucket(bucket_name=bucket_name, org=org)
    except TypeError:
        if not org_id:
            raise
        buckets_api.create_bucket(bucket_name=bucket_name, org_id=org_id)


def _resolve_org_id(client: InfluxDBClient, org: str) -> str:
    orgs_api = client.organizations_api()
    orgs = orgs_api.find_organizations(org=org)
    if not orgs:
        raise RuntimeError(f"Could not resolve org_id for org name {org!r}")
    return orgs[0].id

def _run_duration_writer_worker(
    writer_id: int,
    client: InfluxDBClient,
    bucket: str,
    org: str,
    measurement: str,
    batch_size: int,
    precision_enum: WritePrecision,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    base_ts: int,
    stop_at: float,
    run_id: str | None,
) -> List[BatchWriteMetrics]:
    """
    A writer repeatedly writes batches of points until stop_at is reached and
    returns a list of BatchWriteMetrics for all written batches.
    """
    metrics: List[BatchWriteMetrics] = []
    batch_index = 0

    # Thread-local WriteApi (avoid sharing a single instance across threads)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    while time.perf_counter() < stop_at:
        points = []
        for i in range(batch_size):
            # Keep the original scheme; if you want globally unique idx across writers,
            # you can incorporate writer_id into global_idx.
            global_idx = batch_index * batch_size + i
            p = build_benchmark_point(
                measurement=measurement,
                base_ts=base_ts,
                idx=global_idx,
                point_complexity=point_complexity,
                tag_cardinality=tag_cardinality,
                time_ordering=time_ordering,
                precision=precision_enum,
                run_id=run_id,
            )
            p = p.tag("writer_id", str(writer_id))
            points.append(p)

        t0 = time.perf_counter()
        try:
            write_api.write(
                bucket=bucket,
                org=org,
                record=points,
            )
            t1 = time.perf_counter()
            m = BatchWriteMetrics(
                batch_index=batch_index,
                latency_s=(t1 - t0),
                points=len(points),
                status_code=204,
                ok=True,
            )
        except Exception as exc:
            t1 = time.perf_counter()
            m = BatchWriteMetrics(
                batch_index=batch_index,
                latency_s=(t1 - t0),
                points=len(points),
                status_code=500,
                ok=False,
            )
            logger.info(
                " writer=%s batch=%s failed: %s",
                writer_id,
                batch_index,
                exc,
            )

        metrics.append(m)
        batch_index += 1

    return metrics


# ---------- Scenario-Steps ----------			   
@when(
    'I run a generic multi-bucket query benchmark on measurement "{measurement}" '
    'with time range "{time_range}" using query type "{query_type}" and result size "{result_size}" '
    'against bucket prefix "{bucket_prefix}" querying {bucket_count:d} buckets with '
    '{concurrent_clients:d} concurrent clients per bucket, output format "{output_format}" '
    'and compression "{compression}"'
)
def step_run_multi_bucket_query_benchmark(
    context: Context,
    measurement: str,
    time_range: str,
    query_type: str,
    result_size: str,
    bucket_prefix: str,
    bucket_count: int,
    concurrent_clients: int,
    output_format: str,
    compression: str,
):
    """
    Multi-bucket query benchmark.

    Parallelism matches the multi-bucket write benchmark:
      - max_workers = bucket_count * concurrent_clients
      - tasks are scheduled per bucket * per client
    """

    url = context.influxdb.sut.url
    token = context.influxdb.sut.token
    org = context.influxdb.sut.org
    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL, INFLUX_TOKEN and INFLUX_ORG must be set for the SUT")

    enable_gzip = compression == "gzip"

    # Ensure buckets exist so the scenario is runnable standalone.
    bench_client = InfluxDBClient(url=url, token=token, org=org, enable_gzip=enable_gzip)
    created_buckets: List[str] = [f"{bucket_prefix}_{i}" for i in range(bucket_count)]
    try:
        buckets_api = bench_client.buckets_api()
        org_id: str | None = None
        try:
            org_id = _resolve_org_id(bench_client, org)
        except Exception as exc:
            logger.info("Could not resolve org_id (will try org=... path): %s", exc)

        for bucket_name in created_buckets:
            try:
                _create_bucket_compat(buckets_api, bucket_name=bucket_name, org=org, org_id=org_id)
                logger.info("Created bucket: %s", bucket_name)
            except Exception as exc:
                logger.info("Bucket %s may already exist: %s", bucket_name, exc)

    finally:
        bench_client.close()

    runs: List[QueryRunMetrics] = []

    max_workers = max(1, bucket_count * concurrent_clients)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for bucket_idx, bucket_name in enumerate(created_buckets):
            flux = _build_flux_query(
                bucket=bucket_name,
                measurement=measurement,
                time_range=time_range,
                query_type=query_type,
                result_size=result_size,
                run_id=getattr(context, "run_id", None),
            )
            for c in range(concurrent_clients):
                client_id = bucket_idx * concurrent_clients + c
                futures.append(
                    executor.submit(
                        _run_single_query,
                        client_id=client_id,
                        bucket=bucket_name,
                        base_url=url,
                        token=token,
                        org=org,
                        flux=flux,
                        output_format=output_format,
                        compression=compression,
                    )
                )

        for fut in as_completed(futures):
            runs.append(fut.result())

    context.multi_query_runs = runs
    context.multi_query_benchmark_meta = {
        "run_id": getattr(context, "run_id", None),
        "measurement": measurement,
        "time_range": time_range,
        "query_type": query_type,
        "result_size": result_size,
        "bucket_prefix": bucket_prefix,
        "bucket_count": bucket_count,
        "concurrent_clients_per_bucket": concurrent_clients,
        "total_concurrent_clients": bucket_count * concurrent_clients,
        "output_format": output_format,
        "compression": compression,
        "bucket": f"{bucket_prefix}_*",
        "org": org,
        "sut_url": url,
    }
    context.multi_query_summary = _summarize_query_runs(runs)


@then('I store the generic multi-bucket query benchmark result as "{outfile}"')
def step_store_multi_bucket_query_result(context: Context, outfile: str):
    runs: List[QueryRunMetrics] = getattr(context, "multi_query_runs", [])
    meta = getattr(context, "multi_query_benchmark_meta", {})
    summary = getattr(context, "multi_query_summary", {})

    result: Dict[str, Any] = {
        "meta": meta,
        "summary": summary,
        "runs": [asdict(r) for r in runs],
        "created_at_epoch_s": time.time(),
    }

    write_json_report(
        outfile,
        result,
        logger_=logger,
        log_prefix="Stored generic multi-bucket query benchmark result to ",
    )

    _export_multi_query_result_to_main_influx(result=result, outfile=outfile, context=context)
