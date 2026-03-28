import logging
import os
import random
import statistics
import string
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from behave import then, when
from influxdb_client import InfluxDBClient, Point, WritePrecision

try:
    from influxdb_client.domain import User
except Exception:  # pragma: no cover
    from influxdb_client.domain.user import User  # type: ignore

from src.utils import (
    generate_base_point,  # ✅ 关键：用它生成 Point
    get_main_influx_write_api,
    main_influx_is_configured,
    scenario_id_from_outfile,
    write_json_report,
)

logger = logging.getLogger("bddbench.influx_user_steps")


# ---------------- Datatypes ----------------

@dataclass
class UserOpMetric:
    op: str  # "me" | "create" | "update_password" | "update" | "find" | "delete"
    latency_s: float
    ok: bool
    status_code: int
    # capture a real sampling timestamp for raw points (ns precision)
    ts_ns: int = field(default_factory=time.time_ns)


@dataclass(frozen=True)
class _UserFixture:
    user_id: str
    username: str
    password: str


# ---------------- Helpers ----------------

def _status_from_exc(e: Exception) -> int:
    status = getattr(e, "status", None)
    if isinstance(status, int):
        return status
    resp = getattr(e, "response", None)
    status2 = getattr(resp, "status", None)
    if isinstance(status2, int):
        return status2
    return 500


def _safe_barrier_wait(barrier: threading.Barrier) -> bool:
    try:
        barrier.wait()
        return True
    except threading.BrokenBarrierError:
        return False


def _generate_username(complexity: str) -> str:
    base = f"bench_user_{uuid.uuid4().hex[:8]}"
    if complexity == "high":
        suffix = "".join(
            random.choices(
                string.ascii_letters + string.digits + "!@#$%",
                k=20,
            )
        )
        return f"{base}_{suffix}"
    return base


def _generate_password(complexity: str) -> str:
    """
    In InfluxDB v2, auth is typically token-based, but we still model password
    complexity as a benchmark dimension and benchmark update_password endpoint.
    """
    base = uuid.uuid4().hex
    if complexity == "high":
        return base + "".join(
            random.choices(string.ascii_letters + string.digits + "!@#$%^&*()", k=24)
        )
    return base[:12]


# ---------------- Workers ----------------

def _run_me_worker(
    client_idx: int,
    url: str,
    token: str,
    org: str,
    duration: int,
) -> List[UserOpMetric]:
    """
    Worker that repeatedly calls /api/v2/me via UsersApi.me(),
    measuring latency and success/failure.
    """
    _ = client_idx  # reserved for future tagging/debug
    metrics: List[UserOpMetric] = []
    end_time = time.perf_counter() + duration

    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()

        while time.perf_counter() < end_time:
            t0 = time.perf_counter()
            try:
                _ = users_api.me()
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="me",
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=200,
                        ts_ns=time.time_ns(),
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="me",
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=_status_from_exc(e),
                        ts_ns=time.time_ns(),
                    )
                )
                # small backoff on error
                time.sleep(0.01)

    return metrics


def _run_lifecycle_worker(
    worker_idx: int,
    url: str,
    token: str,
    org: str,
    username_complexity: str,
    password_complexity: str,
    iterations: int,
    barrier: threading.Barrier,
) -> List[UserOpMetric]:
    """
    Bug Report compliant lifecycle worker:

    - Create / UpdatePassword / Update / Find / Delete are benchmarked SEPARATELY
      (each request has its own t0/t1).
    - Each phase is executed concurrently across threads using a shared barrier:
        multiple create at the same time
        multiple update_password at the same time
        multiple update at the same time
        multiple find at the same time
        multiple delete at the same time
    - password is generated AND used in update_password().
    """
    _ = worker_idx  # reserved for future tagging/debug
    metrics: List[UserOpMetric] = []
    fixtures: List[Optional[_UserFixture]] = []

    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()

        # ---------------- Phase 1: CREATE ----------------
        for _i in range(iterations):
            username = _generate_username(username_complexity)
            password = _generate_password(password_complexity)

            if not _safe_barrier_wait(barrier):
                break

            t0 = time.perf_counter()
            try:
                user = users_api.create_user(name=username)
                t1 = time.perf_counter()
                fixtures.append(_UserFixture(user_id=user.id, username=username, password=password))
                metrics.append(
                    UserOpMetric(
                        op="create",
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=201,
                        ts_ns=time.time_ns(),
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                fixtures.append(None)
                metrics.append(
                    UserOpMetric(
                        op="create",
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=_status_from_exc(e),
                        ts_ns=time.time_ns(),
                    )
                )

        if barrier.broken:
            return metrics

        while len(fixtures) < iterations:
            fixtures.append(None)

        # ---------------- Phase 2: UPDATE_PASSWORD ----------------
        for fx in fixtures:
            if not _safe_barrier_wait(barrier):
                break
            if fx is None:
                continue

            t0 = time.perf_counter()
            try:
                users_api.update_password(fx.user_id, fx.password)
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="update_password",
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=204,
                        ts_ns=time.time_ns(),
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="update_password",
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=_status_from_exc(e),
                        ts_ns=time.time_ns(),
                    )
                )

        if barrier.broken:
            return metrics

        # ---------------- Phase 3: UPDATE ----------------
        for fx in fixtures:
            if not _safe_barrier_wait(barrier):
                break
            if fx is None:
                continue

            updated_name = f"{fx.username}_upd"
            t0 = time.perf_counter()
            try:
                users_api.update_user(User(id=fx.user_id, name=updated_name))
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="update",
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=200,
                        ts_ns=time.time_ns(),
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="update",
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=_status_from_exc(e),
                        ts_ns=time.time_ns(),
                    )
                )

        if barrier.broken:
            return metrics

        # ---------------- Phase 4: FIND ----------------
        for fx in fixtures:
            if not _safe_barrier_wait(barrier):
                break
            if fx is None:
                continue

            t0 = time.perf_counter()
            try:
                found = users_api.find_users(id=fx.user_id)
                if not found or not getattr(found, "users", None):
                    raise RuntimeError(f"User {fx.user_id} not found")
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="find",
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=200,
                        ts_ns=time.time_ns(),
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="find",
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=_status_from_exc(e),
                        ts_ns=time.time_ns(),
                    )
                )

        if barrier.broken:
            return metrics

        # ---------------- Phase 5: DELETE ----------------
        for fx in fixtures:
            if not _safe_barrier_wait(barrier):
                break
            if fx is None:
                continue

            t0 = time.perf_counter()
            try:
                users_api.delete_user(fx.user_id)
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="delete",
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=204,
                        ts_ns=time.time_ns(),
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        op="delete",
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=_status_from_exc(e),
                        ts_ns=time.time_ns(),
                    )
                )

    return metrics


# ---------------- Summarize / Export ----------------

def _summarize_and_store(
    context,
    metrics: List[UserOpMetric],
    meta: Dict[str, Any],
    outfile: str,
) -> None:
    """
    Aggregates metrics, stores them in the Behave context,
    and writes a JSON summary report to disk.
    Also adds per-op stats (CRUD results are not "useless").
    """
    latencies = [m.latency_s for m in metrics]
    errors = [m for m in metrics if not m.ok]

    total_ops = len(metrics)
    duration = float(meta.get("total_duration_s", 1.0) or 1.0)
    if duration <= 0:
        duration = 1.0

    stats = {
        "min": min(latencies) if latencies else 0.0,
        "max": max(latencies) if latencies else 0.0,
        "avg": statistics.mean(latencies) if latencies else 0.0,
        "median": statistics.median(latencies) if latencies else 0.0,
    }

    per_op: Dict[str, Dict[str, float]] = {}
    by_op: Dict[str, List[float]] = {}
    for m in metrics:
        by_op.setdefault(m.op, []).append(m.latency_s)
    for op, vals in by_op.items():
        per_op[op] = {
            "min": min(vals) if vals else 0.0,
            "max": max(vals) if vals else 0.0,
            "avg": statistics.mean(vals) if vals else 0.0,
            "median": statistics.median(vals) if vals else 0.0,
            "count": float(len(vals)),
        }

    context.user_bench_results = {
        "meta": meta,
        "stats": stats,
        "per_op": per_op,
        "throughput": total_ops / duration,
        "total_ops": total_ops,
        "error_count": len(errors),
    }

    final_report = {
        "meta": meta,
        "summary": {
            "stats_s": stats,
            "per_op_stats_s": per_op,
            "throughput_ops_s": total_ops / duration,
            "total_ops": total_ops,
            "errors": len(errors),
        },
        "created_at": time.time(),
    }

    write_json_report(
        outfile,
        final_report,
        logger_=logger,
        log_prefix="User benchmark saved to ",
    )


def _write_points_chunked(write_api, bucket: str, org: str, points: List[Point], chunk_size: int = 5000) -> None:
    for i in range(0, len(points), chunk_size):
        write_api.write(bucket=bucket, org=org, record=points[i : i + chunk_size])


def _export_to_main_influx(context, outfile: str) -> None:
    """
    Writes BOTH:
      (1) summary point via generate_base_point() + extend (to match team convention)
      (2) raw per-call points to MAIN influx

    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning.
    """
    if not hasattr(context, "user_bench_results"):
        return

    if not main_influx_is_configured(context):
        logger.info(" MAIN influx not configured – skipping export")
        return

    strict = bool(getattr(context.influxdb, "export_strict", False))
    main = context.influxdb.main

    _client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        msg = " MAIN write_api missing – skipping export"
        if strict:
            raise RuntimeError(msg)
        logger.warning(msg)
        return

    data = context.user_bench_results
    meta = data["meta"]
    stats = data["stats"]

    scenario_id = scenario_id_from_outfile(
        outfile,
        prefixes=("user-me-", "user-lifecycle-"),
    )

    # Ensure run_uuid is consistent between JSON and Influx points
    meta_last = getattr(context, "last_user_meta", {}) or meta
    run_uuid = str(meta_last.get("run_uuid") or uuid.uuid4())
    meta["run_uuid"] = run_uuid
    meta_last["run_uuid"] = run_uuid

    now = datetime.now(timezone.utc)

    # ---------------- (2) RAW per-call export ----------------
    metrics: List[UserOpMetric] = getattr(context, "last_user_metrics", [])
    meta_last2: Dict[str, Any] = getattr(context, "last_user_meta", {}) or meta

    raw_points: List[Point] = []
    for m in metrics:
        # ✅ team requirement: generate_base_point() then extend
        p = generate_base_point(context=context, measurement="user_api_op_latency")

        # keep existing dimensions (explicit to avoid relying on base defaults)
        p.tag("scenario_id", str(scenario_id))
        p.tag("feature_name", os.path.basename(outfile))
        p.tag("sut", str(meta_last2.get("sut_url", "unknown")))
        p.tag("test_type", "user_benchmark")
        p.tag("operation", str(meta_last2.get("operation", "unknown")))  # me / lifecycle_crud
        p.tag("op", str(m.op))  # create/update_password/update/find/delete/me
        p.tag("username_complexity", str(meta_last2.get("username_complexity", "none")))
        p.tag("password_complexity", str(meta_last2.get("password_complexity", "none")))
        p.tag("concurrency", str(meta_last2.get("concurrency", 1)))
        p.tag("run_uuid", run_uuid)

        p.field("latency_ms", float(m.latency_s) * 1000.0)
        p.field("ok", bool(m.ok))
        p.field("status_code", int(m.status_code))

        # raw point uses real sampling time (ns)
        p.time(m.ts_ns, WritePrecision.NS)

        raw_points.append(p)

    try:
        if raw_points:
            _write_points_chunked(write_api, bucket=main.bucket, org=main.org, points=raw_points, chunk_size=5000)
            logger.info("Exported RAW user op latencies to Main InfluxDB")
    except Exception as e:
        msg = f"Failed to export RAW points to Main InfluxDB: {e}"
        if strict:
            raise
        logger.error(msg)

    # ---------------- (1) Summary export ----------------
    # IMPORTANT: historical measurement for user summary was "UserPerformancePoint"
    # because BasePoint uses Point(self.__class__.__name__) in to_point()
    SUMMARY_MEASUREMENT = "UserPerformancePoint"

    # ✅ team requirement: generate_base_point() then extend
    p = generate_base_point(context=context, measurement=SUMMARY_MEASUREMENT)

    # Tags: align with UserPerformancePoint.to_point()
    p.tag("scenario_id", str(scenario_id))
    p.tag("operation", str(meta.get("operation", "unknown")))
    p.tag("username_complexity", str(meta.get("username_complexity", meta.get("complexity", "none"))))
    p.tag("password_complexity", str(meta.get("password_complexity", "none")))
    p.tag("sut_influx_url", str(meta.get("sut_url", "")))
    p.tag("sut_org", str(meta.get("sut_org", "")))
    p.tag("sut_bucket", str(meta.get("sut_bucket", "")))
    p.tag("concurrency", str(int(meta.get("concurrency", 1))))
    p.tag("run_uuid", run_uuid)

    # Fields: align with UserPerformancePoint.to_point()
    p.field("ops_per_sec", float(data["throughput"]))
    p.field("latency_avg_ms", float(stats["avg"]) * 1000.0)
    p.field("latency_min_ms", float(stats["min"]) * 1000.0)
    p.field("latency_max_ms", float(stats["max"]) * 1000.0)
    p.field("error_count", int(data["error_count"]))

    # extra_metrics we used previously
    p.field("total_ops", float(data["total_ops"]))
    p.field("total_duration_s", float(meta.get("total_duration_s", 0.0) or 0.0))

    p.time(now, WritePrecision.NS)

    try:
        write_api.write(
            bucket=main.bucket,
            org=main.org,
            record=p,
        )
        logger.info("Exported user SUMMARY metrics to Main InfluxDB")
    except Exception as e:
        msg = f"Failed to export SUMMARY to Main InfluxDB: {e}"
        if strict:
            raise
        logger.error(msg)


# ---------------- Behave Steps ----------------

@when('I run a "/me" benchmark with {concurrent_clients:d} concurrent clients for {duration_s:d} seconds')
def step_run_me_benchmark(context, concurrent_clients: int, duration_s: int) -> None:
    url = context.influxdb.sut.url
    token = context.influxdb.sut.token
    org = context.influxdb.sut.org

    all_metrics: List[UserOpMetric] = []
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
        futures = [
            executor.submit(_run_me_worker, i, url, token, org, duration_s)
            for i in range(concurrent_clients)
        ]
        for fut in as_completed(futures):
            all_metrics.extend(fut.result())

    total_time = time.perf_counter() - start_time

    meta = {
        "operation": "me",
        "concurrency": concurrent_clients,
        "target_duration": duration_s,
        "total_duration_s": total_time,
        "sut_url": url,
        "sut_org": getattr(context.influxdb.sut, "org", ""),
        "sut_bucket": getattr(context.influxdb.sut, "bucket", ""),
        "username_complexity": "none",
        "password_complexity": "none",
    }

    context.last_user_metrics = all_metrics
    context.last_user_meta = meta


@when(
    'I run a user lifecycle benchmark with username complexity "{username_complexity}", '
    'password complexity "{password_complexity}", {concurrent_clients:d} parallel threads '
    'for {iterations:d} iterations'
)
def step_run_lifecycle_benchmark(
    context,
    username_complexity: str,
    password_complexity: str,
    concurrent_clients: int,
    iterations: int,
) -> None:
    url = context.influxdb.sut.url
    token = context.influxdb.sut.token
    org = context.influxdb.sut.org

    all_metrics: List[UserOpMetric] = []
    start_time = time.perf_counter()

    barrier = threading.Barrier(concurrent_clients)

    with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
        futures = [
            executor.submit(
                _run_lifecycle_worker,
                i,  # worker_idx
                url,
                token,
                org,
                username_complexity,
                password_complexity,
                iterations,
                barrier,
            )
            for i in range(concurrent_clients)
        ]
        for fut in as_completed(futures):
            all_metrics.extend(fut.result())

    total_time = time.perf_counter() - start_time

    meta = {
        "operation": "lifecycle_crud",
        "concurrency": concurrent_clients,
        "iterations_per_thread": iterations,
        "total_duration_s": total_time,
        "sut_url": url,
        "sut_org": getattr(context.influxdb.sut, "org", ""),
        "sut_bucket": getattr(context.influxdb.sut, "bucket", ""),
        "username_complexity": username_complexity,
        "password_complexity": password_complexity,
    }

    context.last_user_metrics = all_metrics
    context.last_user_meta = meta


@then('I store the user benchmark result as "{outfile}"')
def step_store_user_result(context, outfile: str) -> None:
    metrics: List[UserOpMetric] = getattr(context, "last_user_metrics", [])
    meta: Dict[str, Any] = getattr(context, "last_user_meta", {})

    # make run_uuid visible in JSON and reuse in main-influx export
    meta["run_uuid"] = meta.get("run_uuid") or str(uuid.uuid4())
    context.last_user_meta = meta

    _summarize_and_store(context, metrics, meta, outfile)
    _export_to_main_influx(context, outfile)

