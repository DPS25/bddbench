import json
import logging
import statistics
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from behave import then, when
from behave.runner import Context
from concurrent.futures import ThreadPoolExecutor, as_completed

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from src.utils import (
    base_timestamp_for_precision,
    build_benchmark_point,
    generate_base_point,
    get_main_influx_write_api,
    influx_precision_from_str,
    main_influx_is_configured,
    scenario_id_from_outfile,
    write_json_report,
    write_to_influx,
)

logger = logging.getLogger("bddbench.influx_e2e_steps")


# ==========================================================
# Data types
# ==========================================================

@dataclass
class BatchWriteMetrics:
    bucket: str
    writer_id: int
    batch_index: int
    latency_s: float
    points: int
    status_code: int
    ok: bool


@dataclass
class QueryRunMetrics:
    bucket: str
    client_id: int
    iteration: int
    status_code: int
    ok: bool
    time_to_first_result_s: float | None
    total_time_s: float | None
    bytes_returned: int
    rows_returned: int


@dataclass
class DeleteRunMetrics:
    bucket: str
    latency_s: float
    status_code: int
    ok: bool
    points_before: int
    points_after: int


# ==========================================================
# Bucket helpers 
# ==========================================================

def _resolve_org_id(client: InfluxDBClient, org: str) -> Optional[str]:
    try:
        orgs = client.organizations_api().find_organizations(org=org)
        if not orgs:
            return None
        return orgs[0].id
    except Exception:
        return None


def _create_bucket_compat(
    buckets_api: Any,
    *,
    bucket_name: str,
    org: str,
    org_id: Optional[str],
) -> None:
    try:
        buckets_api.create_bucket(bucket_name=bucket_name, org=org)
    except TypeError:
        if not org_id:
            raise
        buckets_api.create_bucket(bucket_name=bucket_name, org_id=org_id)


def _ensure_bucket_exists(buckets_api: Any, bucket_name: str) -> None:
    try:
        b = buckets_api.find_bucket_by_name(bucket_name=bucket_name)
    except TypeError:
        b = buckets_api.find_bucket_by_name(bucket_name)
    if b is None:
        raise AssertionError(
            f"Bucket '{bucket_name}' does not exist and could not be created. "
            "Check token permissions (buckets read/write) and org/bucket_prefix."
        )


# ==========================================================
# Write stage
# ==========================================================

def _run_writer_worker(
    *,
    bucket: str,
    writer_id: int,
    client: InfluxDBClient,
    org: str,
    measurement: str,
    batch_size: int,
    batches: int,
    precision_enum: WritePrecision,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    base_ts: int,
    run_id: Optional[str],
) -> List[BatchWriteMetrics]:
    write_api = client.write_api(write_options=SYNCHRONOUS)
    metrics: List[BatchWriteMetrics] = []

    for b in range(batches):
        batch_index = writer_id * batches + b

        points: List[Any] = []
        for i in range(batch_size):
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
            write_api.write(bucket=bucket, org=org, record=points)
            t1 = time.perf_counter()
            metrics.append(
                BatchWriteMetrics(
                    bucket=bucket,
                    writer_id=writer_id,
                    batch_index=batch_index,
                    latency_s=(t1 - t0),
                    points=len(points),
                    status_code=204,
                    ok=True,
                )
            )
        except Exception as exc:
            t1 = time.perf_counter()
            logger.info("bucket=%s writer=%s batch=%s failed: %s", bucket, writer_id, batch_index, exc)
            metrics.append(
                BatchWriteMetrics(
                    bucket=bucket,
                    writer_id=writer_id,
                    batch_index=batch_index,
                    latency_s=(t1 - t0),
                    points=len(points),
                    status_code=500,
                    ok=False,
                )
            )

    return metrics


def _summarize_write_batches(all_batches: List[BatchWriteMetrics], total_points: int, total_duration_s: float) -> Dict[str, Any]:
    latencies = [b.latency_s for b in all_batches]
    errors = [b for b in all_batches if not b.ok]
    throughput_points_per_s = (total_points / total_duration_s) if total_duration_s > 0 else None
    return {
        "latency_stats": {
            "min": min(latencies) if latencies else None,
            "max": max(latencies) if latencies else None,
            "avg": statistics.mean(latencies) if latencies else None,
            "median": statistics.median(latencies) if latencies else None,
        },
        "throughput": {"points_per_s": throughput_points_per_s},
        "error_rate": (len(errors) / len(all_batches)) if all_batches else 0.0,
        "errors_count": len(errors),
        "batches_count": len(all_batches),
    }


# ==========================================================
# Query stage
# ==========================================================

def _build_flux_query(
    *,
    bucket: str,
    measurement: str,
    time_range: str,
    query_type: str,
    result_size: str,
    run_id: Optional[str] = None,
) -> str:
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
  |> filter(fn: (r) => r["run_id"] == "{run_id}")
""".strip()

    if query_type == "filter":
        return f"""{base}
  |> filter(fn: (r) => exists r["value"])
  |> limit(n: {limit_n})
"""
    if query_type == "aggregate":
        return f"""{base}
  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
  |> limit(n: {limit_n})
"""
    if query_type == "group_by":
        return f"""{base}
  |> group(columns: ["device_id"])
  |> limit(n: {limit_n})
"""
    if query_type == "pivot":
        return f"""{base}
  |> limit(n: {limit_n})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
"""
    return f"""{base}
  |> limit(n: {limit_n})
"""


def _run_single_query(
    *,
    bucket: str,
    client_id: int,
    iteration: int,
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
            timeout=300_000,
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

    except Exception as exc:
        logger.info("query bucket=%s client=%s iter=%s failed: %s", bucket, client_id, iteration, exc)
        return QueryRunMetrics(
            bucket=bucket,
            client_id=client_id,
            iteration=iteration,
            status_code=0,
            ok=False,
            time_to_first_result_s=None,
            total_time_s=None,
            bytes_returned=0,
            rows_returned=0,
        )

    t_end = time.perf_counter()
    return QueryRunMetrics(
        bucket=bucket,
        client_id=client_id,
        iteration=iteration,
        status_code=200,
        ok=True,
        time_to_first_result_s=time_to_first,
        total_time_s=t_end - t_start,
        bytes_returned=bytes_returned,
        rows_returned=rows_returned,
    )


def _summarize_query_runs(runs: List[QueryRunMetrics]) -> Dict[str, Any]:
    if not runs:
        return {
            "total_runs": 0,
            "errors_count": 0,
            "error_rate": 0.0,
            "latency_stats": {},
            "throughput": {},
        }

    def vals(name: str) -> List[float]:
        out: List[float] = []
        for r in runs:
            v = getattr(r, name)
            if v is not None:
                out.append(float(v))
        return out

    ttf = vals("time_to_first_result_s")
    ttotal = vals("total_time_s")

    errors_count = len([r for r in runs if not r.ok])

    def agg(v: List[float]) -> Dict[str, Any]:
        if not v:
            return {"min": None, "max": None, "avg": None, "median": None}
        return {"min": min(v), "max": max(v), "avg": statistics.mean(v), "median": statistics.median(v)}

    total_duration_s = max(ttotal) if ttotal else 0.0
    throughput_runs_per_s = (len(runs) / total_duration_s) if total_duration_s > 0 else None

    return {
        "total_runs": len(runs),
        "errors_count": errors_count,
        "error_rate": (errors_count / len(runs)) if runs else 0.0,
        "latency_stats": {
            "ttf": agg(ttf),
            "total": agg(ttotal),
        },
        "throughput": {"runs_per_s": throughput_runs_per_s},
    }


# ==========================================================
# Delete stage (count + delete)
# ==========================================================

def _count_points(query_api: Any, org: str, bucket: str, measurement: str, run_id: Optional[str]) -> int:
    run_filter = "" if not run_id else f'  |> filter(fn: (r) => r["run_id"] == "{run_id}")\n'
    flux = f"""
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
{run_filter}  |> filter(fn: (r) => r["_field"] == "value")
  |> count()
""".strip()

    tables = query_api.query(org=org, query=flux)
    total = 0
    for table in tables:
        for record in table.records:
            try:
                total += int(record.get_value())
            except (TypeError, ValueError):
                pass
    return total


def _delete_bucket_data(
    *,
    bucket: str,
    org: str,
    delete_api: Any,
    query_api: Any,
    measurement: str,
    run_id: Optional[str],
) -> DeleteRunMetrics:
    points_before = _count_points(query_api, org, bucket, measurement, run_id)

    start = "1970-01-01T00:00:00Z"
    stop = "2100-01-01T00:00:00Z"
    predicate = f'_measurement="{measurement}"'
    if run_id:
        predicate += f' AND run_id="{run_id}"'

    t0 = time.perf_counter()
    ok = True
    status = 204
    try:
        delete_api.delete(start=start, stop=stop, predicate=predicate, bucket=bucket, org=org)
    except Exception as exc:
        logger.info("delete bucket=%s failed: %s", bucket, exc)
        ok = False
        status = 500
    t1 = time.perf_counter()

    points_after = _count_points(query_api, org, bucket, measurement, run_id)
    return DeleteRunMetrics(
        bucket=bucket,
        latency_s=(t1 - t0),
        status_code=status,
        ok=ok,
        points_before=points_before,
        points_after=points_after,
    )


# ==========================================================
# MAIN export 
# ==========================================================

def build_e2e_export_point(
    *,
    context: Context,
    meta: Dict[str, Any],
    summary: Dict[str, Any],
    scenario_id: str,
) -> Point:
    w = summary.get("write", {}) or {}
    q = summary.get("query", {}) or {}
    d = summary.get("delete", {}) or {}

    w_lat = (w.get("latency_stats", {}) or {})
    q_lat_total = ((q.get("latency_stats", {}) or {}).get("total", {}) or {})

    return (
        generate_base_point(context=context, measurement="bddbench_e2e_result")
        .tag("mode", str(meta.get("mode", "")))
        .tag("group", str(meta.get("group", "")))
        .tag("bucket_prefix", str(meta.get("bucket_prefix", "")))
        .tag("base_measurement", str(meta.get("base_measurement", "")))
        .tag("source_measurement", str(meta.get("measurement", "")))  
        .tag("precision", str(meta.get("precision", "")))
        .tag("write_compression", str(meta.get("write_compression", "")))
        .tag("query_type", str(meta.get("query_type", "")))
        .tag("result_size", str(meta.get("result_size", "")))
        .tag("output_format", str(meta.get("output_format", "")))
        .tag("query_compression", str(meta.get("query_compression", "")))
        .tag("scenario_id", scenario_id or "")
        .field("bucket_count", int(meta.get("bucket_count", 0)))
        .field("expected_points_per_bucket", int(meta.get("expected_points_per_bucket", 0)))
        .field("expected_total_points", int(meta.get("expected_total_points", 0)))
        .field("write_total_points", int(meta.get("write_total_points", 0)))
        .field("write_total_duration_s", float(meta.get("write_total_duration_s", 0.0)))
        .field("write_throughput_points_per_s", float((w.get("throughput", {}) or {}).get("points_per_s") or 0.0))
        .field("write_error_rate", float(w.get("error_rate", 0.0)))
        .field("write_errors_count", int(w.get("errors_count", 0)))
        .field("write_latency_avg_s", float(w_lat.get("avg") or 0.0))
        .field("query_total_runs", int(q.get("total_runs", 0)))
        .field("query_error_rate", float(q.get("error_rate", 0.0)))
        .field("query_latency_total_avg_s", float(q_lat_total.get("avg") or 0.0))
        .field("delete_latency_s", float(d.get("delete_latency_s", 0.0)))
        .field("delete_deleted_points", int(d.get("deleted_points", 0)))
        .field("ok", bool(summary.get("ok", False)))
    )

def _export_e2e_result_to_main_influx(result: Dict[str, Any], outfile: str, context: Context) -> None:
    if not main_influx_is_configured(context):
        return

    main = context.influxdb.main
    strict = bool(getattr(context.influxdb, "export_strict", False))

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        return

    meta = result.get("meta", {})
    summary = result.get("summary", {})
    scenario_id = scenario_id_from_outfile(outfile, prefixes=("e2e-",))

    p = build_e2e_export_point(
        context=context,
        meta=meta,
        summary=summary,
        scenario_id=scenario_id,
    )

    write_to_influx(
        write_api=write_api,
        bucket=main.bucket,
        org=main.org,
        record=p,
        logger_=logger,
        strict=strict,
        success_msg="Exported e2e result to MAIN Influx",
        failure_prefix="MAIN export failed",
    )


# ==========================================================
# Behave steps
# ==========================================================

@when(
    'I run an end-to-end benchmark with bucket prefix "{bucket_prefix}" on base measurement "{measurement}" with {bucket_count:d} buckets, '
    'write batch size {batch_size:d}, {parallel_writers:d} parallel writers, write compression "{write_compression}", timestamp precision "{precision}", '
    'point complexity "{point_complexity}", tag cardinality {tag_cardinality:d} and time ordering "{time_ordering}" for {batches:d} batches, '
    'expecting {expected_points_per_bucket:d} points per bucket and {expected_total_points:d} total points, '
    'then query with time range "{time_range}", query type "{query_type}", result size "{result_size}", '
    '{concurrent_clients:d} concurrent clients, {query_repeats:d} repeats per client, output format "{output_format}" and query compression "{query_compression}", '
    'and finally delete all written points'
)
def step_run_e2e(
    context: Context,
    bucket_prefix: str,
    measurement: str,
    bucket_count: int,
    batch_size: int,
    parallel_writers: int,
    write_compression: str,
    precision: str,
    point_complexity: str,
    tag_cardinality: int,
    time_ordering: str,
    batches: int,
    expected_points_per_bucket: int,
    expected_total_points: int,
    time_range: str,
    query_type: str,
    result_size: str,
    concurrent_clients: int,
    query_repeats: int,
    output_format: str,
    query_compression: str,
):
    sut = context.influxdb.sut
    if not getattr(sut, "client", None):
        raise RuntimeError("SUT InfluxDB client is not configured")

    run_id = getattr(context, "run_id", None)
    run_suffix = str(run_id) if run_id else "run"
    scenario_tags = set(getattr(getattr(context, "scenario", None), "tags", []) or [])
    mode = "experimental" if "experimental" in scenario_tags else "normal"
    group = "multibucket" if "multibucket" in scenario_tags else "singlebucket"

    bucket_names = [f"{bucket_prefix}_{run_suffix}_{i}" for i in range(bucket_count)]
    measurement_name = f"{measurement}_{run_suffix}"

    buckets_api = sut.client.buckets_api()
    org_id = _resolve_org_id(sut.client, sut.org)

    for bn in bucket_names:
        try:
            _create_bucket_compat(
                buckets_api,
                bucket_name=bn,
                org=sut.org,
                org_id=org_id,
            )
        except Exception:
            pass
        _ensure_bucket_exists(buckets_api, bn)

    precision_enum = influx_precision_from_str(precision)
    base_ts = base_timestamp_for_precision(precision_enum)
    enable_gzip_write = (write_compression == "gzip")

    points_per_bucket_calc = parallel_writers * batches * batch_size
    total_points_calc = bucket_count * points_per_bucket_calc

    if expected_points_per_bucket != points_per_bucket_calc:
        raise AssertionError(
            f"expected_points_per_bucket={expected_points_per_bucket} but calculated={points_per_bucket_calc} "
            f"(parallel_writers={parallel_writers}, batches={batches}, batch_size={batch_size})"
        )
    if expected_total_points != total_points_calc:
        raise AssertionError(
            f"expected_total_points={expected_total_points} but calculated={total_points_calc} "
            f"(bucket_count={bucket_count} * points_per_bucket={points_per_bucket_calc})"
        )

    bench_client = InfluxDBClient(url=sut.url, token=sut.token, org=sut.org, enable_gzip=enable_gzip_write)

    write_started = time.perf_counter()
    write_batches: List[BatchWriteMetrics] = []
    try:
        max_workers = max(1, bucket_count * parallel_writers)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = []
            for bn in bucket_names:
                for w in range(parallel_writers):
                    futs.append(
                        ex.submit(
                            _run_writer_worker,
                            bucket=bn,
                            writer_id=w,
                            client=bench_client,
                            org=sut.org,
                            measurement=measurement_name,
                            batch_size=batch_size,
                            batches=batches,
                            precision_enum=precision_enum,
                            point_complexity=point_complexity,
                            tag_cardinality=tag_cardinality,
                            time_ordering=time_ordering,
                            base_ts=base_ts,
                            run_id=run_id,
                        )
                    )
            for f in as_completed(futs):
                write_batches.extend(f.result())
    finally:
        bench_client.close()

    write_total_duration_s = time.perf_counter() - write_started
    write_summary = _summarize_write_batches(write_batches, total_points_calc, write_total_duration_s)

    time.sleep(0.25)

    query_runs: List[QueryRunMetrics] = []
    query_started = time.perf_counter()

    def submit_queries(ex: ThreadPoolExecutor, bn: str):
        flux = _build_flux_query(
            bucket=bn,
            measurement=measurement_name,
            time_range=time_range,
            query_type=query_type,
            result_size=result_size,
            run_id=run_id,
        )
        for c in range(concurrent_clients):
            for it in range(query_repeats):
                yield ex.submit(
                    _run_single_query,
                    bucket=bn,
                    client_id=c,
                    iteration=it,
                    base_url=sut.url,
                    token=sut.token,
                    org=sut.org,
                    flux=flux,
                    output_format=output_format,
                    compression=query_compression,
                )

    max_q_workers = max(1, min(256, bucket_count * concurrent_clients))
    with ThreadPoolExecutor(max_workers=max_q_workers) as ex:
        futs = []
        for bn in bucket_names:
            futs.extend(list(submit_queries(ex, bn)))
        for f in as_completed(futs):
            query_runs.append(f.result())

    _ = time.perf_counter() - query_started
    query_summary = _summarize_query_runs(query_runs)

    query_api = sut.client.query_api()
    delete_api = sut.client.delete_api()

    delete_started = time.perf_counter()
    delete_runs: List[DeleteRunMetrics] = []
    with ThreadPoolExecutor(max_workers=max(1, min(32, bucket_count))) as ex:
        futs = [
            ex.submit(
                _delete_bucket_data,
                bucket=bn,
                org=sut.org,
                delete_api=delete_api,
                query_api=query_api,
                measurement=measurement_name,
                run_id=run_id,
            )
            for bn in bucket_names
        ]
        for f in as_completed(futs):
            delete_runs.append(f.result())
    delete_wall_s = time.perf_counter() - delete_started

    total_before = sum(d.points_before for d in delete_runs)
    total_after = sum(d.points_after for d in delete_runs)
    del_errors = len([d for d in delete_runs if not d.ok])

    delete_summary = {
        "delete_latency_s": delete_wall_s,
        "points_before": total_before,
        "points_after": total_after,
        "deleted_points": total_before - total_after,
        "ok": (del_errors == 0) and (total_after == 0),
        "status_code": 204 if del_errors == 0 else 500,
        "errors_count": del_errors,
    }

    ok = (
        write_summary.get("errors_count", 0) == 0
        and query_summary.get("errors_count", 0) == 0
        and delete_summary["ok"]
    )

    context.e2e_meta = {
        "run_id": run_id,
        "mode": mode,             
        "group": group,
        "bucket_prefix": bucket_prefix,
        "bucket_count": bucket_count,
        "buckets": bucket_names,
        "base_measurement": measurement,
        "measurement": measurement_name,
        "org": sut.org,
        "sut_url": sut.url,
        "precision": precision,
        "write_compression": write_compression,
        "point_complexity": point_complexity,
        "tag_cardinality": tag_cardinality,
        "time_ordering": time_ordering,
        "batch_size": batch_size,
        "parallel_writers": parallel_writers,
        "batches": batches,
        "expected_points_per_bucket": expected_points_per_bucket,
        "expected_total_points": expected_total_points,
        "write_total_points": total_points_calc,
        "write_total_duration_s": write_total_duration_s,
        "time_range": time_range,
        "query_type": query_type,
        "result_size": result_size,
        "concurrent_clients": concurrent_clients,
        "query_repeats": query_repeats,
        "output_format": output_format,
        "query_compression": query_compression,
    }

    context.e2e_summary = {"ok": ok, "write": write_summary, "query": query_summary, "delete": delete_summary}
    context.e2e_details = {
        "mode": mode,
        "group": group,
        "write_batches": [asdict(b) for b in write_batches],
        "query_runs": [asdict(r) for r in query_runs],
        "delete_runs": [asdict(d) for d in delete_runs],
    }


@then('I store the end-to-end benchmark result as "{outfile}"')
def step_store_e2e(context: Context, outfile: str) -> None:
    meta: Dict[str, Any] = getattr(context, "e2e_meta", {})
    summary: Dict[str, Any] = getattr(context, "e2e_summary", {})
    details: Dict[str, Any] = getattr(context, "e2e_details", {})

    result = {"meta": meta, "summary": summary, "details": details, "created_at_epoch_s": time.time()}

    write_json_report(outfile, result, logger_=logger, log_prefix="Stored end-to-end benchmark result to ")
    _export_e2e_result_to_main_influx(result, outfile, context)
