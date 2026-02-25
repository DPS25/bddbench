import logging
import os
import random
import statistics
import string
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from behave import then, when
from influxdb_client import InfluxDBClient, Point, WritePrecision

try:
    from influxdb_client.domain import User
except Exception:  # pragma: no cover
    from influxdb_client.domain.user import User  # type: ignore

from src.utils import (
    generate_base_point,
    get_main_influx_write_api,
    main_influx_is_configured,
    scenario_id_from_outfile,
    write_json_report,
    write_to_influx
)

logger = logging.getLogger("bddbench.influx_user_steps")


# ---------------- Datatypes ----------------

@dataclass
class UserOpMetric:
    op: str  # "me", "create", "update_password", "update", "find", "delete"
    latency_s: float
    ok: bool
    status_code: int
    ts_ns: int = field(default_factory=time.time_ns)



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
        suffix = "".join(random.choices(string.ascii_letters + string.digits + "!@#$%", k=20))
        return f"{base}_{suffix}"
    return base


def _generate_password(complexity: str) -> str:
    base = uuid.uuid4().hex
    if complexity == "high":
        return base + "".join(random.choices(string.ascii_letters + string.digits + "!@#$%^&*()", k=24))
    return base[:12]


@dataclass(frozen=True)
class _UserFixture:
    user_id: str
    username: str
    password: str


# ---------------- Workers ----------------

def _run_me_worker(url: str, token: str, org: str, duration: int) -> List[UserOpMetric]:
    metrics: List[UserOpMetric] = []
    end_time = time.perf_counter() + duration
    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()
        while time.perf_counter() < end_time:
            t0 = time.perf_counter()
            try:
                users_api.me()
                metrics.append(UserOpMetric("me", time.perf_counter() - t0, True, 200))
            except Exception as e:
                metrics.append(UserOpMetric("me", time.perf_counter() - t0, False, _status_from_exc(e)))
                time.sleep(0.01)
    return metrics


def _run_lifecycle_worker(
        url: str, token: str, org: str,
        u_comp: str, p_comp: str,
        iterations: int, barrier: threading.Barrier
) -> List[UserOpMetric]:
    metrics: List[UserOpMetric] = []
    fixtures: List[Optional[_UserFixture]] = []

    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()

        # Phase 1: CREATE
        for _ in range(iterations):
            uname, pwd = _generate_username(u_comp), _generate_password(p_comp)
            if not _safe_barrier_wait(barrier): break
            t0 = time.perf_counter()
            try:
                user = users_api.create_user(name=uname)
                fixtures.append(_UserFixture(user.id, uname, pwd))
                metrics.append(UserOpMetric("create", time.perf_counter() - t0, True, 201))
            except Exception as e:
                fixtures.append(None)
                metrics.append(UserOpMetric("create", time.perf_counter() - t0, False, _status_from_exc(e)))

        # Phases 2-5: UPDATE_PWD, UPDATE, FIND, DELETE (Pattern repeated for each)
        for phase, op_name, status_ok in [
            ("pwd", "update_password", 204),
            ("upd", "update", 200),
            ("find", "find", 200),
            ("del", "delete", 204)
        ]:
            if barrier.broken: break
            for fx in fixtures:
                if not _safe_barrier_wait(barrier) or fx is None: continue
                t0 = time.perf_counter()
                try:
                    if phase == "pwd":
                        users_api.update_password(fx.user_id, fx.password)
                    elif phase == "upd":
                        users_api.update_user(User(id=fx.user_id, name=f"{fx.username}_upd"))
                    elif phase == "find":
                        users_api.find_users(id=fx.user_id)
                    elif phase == "del":
                        users_api.delete_user(fx.user_id)
                    metrics.append(UserOpMetric(op_name, time.perf_counter() - t0, True, status_ok))
                except Exception as e:
                    metrics.append(UserOpMetric(op_name, time.perf_counter() - t0, False, _status_from_exc(e)))

    return metrics


# ---------------- Summarize / Export ----------------

def _build_user_export_points(context, outfile: str) -> List[Point]:
    data = context.user_bench_results
    meta = data["meta"]
    metrics: List[UserOpMetric] = getattr(context, "last_user_metrics", [])

    scenario_id = scenario_id_from_outfile(outfile, prefixes=("user-me-", "user-lifecycle-"))
    run_id = meta.get("run_id", "unknown")

    points = []
    # 1. RAW Operation Points
    for m in metrics:
        p = generate_base_point(context=context, measurement="bddbench_user_operation_result")
        p.tag("scenario_id", scenario_id).tag("op", m.op).tag("run_id", run_id)
        p.field("latency_ms", m.latency_s * 1000.0).field("ok", m.ok).field("status_code", m.status_code)
        p.time(m.ts_ns, WritePrecision.NS)
        points.append(p)

    # 2. Summary Point (naming convention: bddbench_user_benchmark_summary)
    p_sum = generate_base_point(context=context, measurement="bddbench_user_benchmark_summary")
    p_sum.tag("scenario_id", scenario_id).tag("run_id", run_id)
    p_sum.tag("operation", meta.get("operation", "unknown"))
    p_sum.field("throughput_ops_s", float(data["throughput"]))
    p_sum.field("latency_avg_ms", float(data["stats"]["avg"]) * 1000.0)
    p_sum.field("error_count", int(data["error_count"]))
    p_sum.field("total_ops", int(data["total_ops"]))

    points.append(p_sum)
    return points


def _export_to_main_influx(context, outfile: str) -> None:
    if not main_influx_is_configured(context): return

    main = context.influxdb.main
    strict = bool(getattr(context.influxdb, "export_strict", False))
    _, write_api = get_main_influx_write_api(context, create_client_if_missing=False)

    if write_api is None: return

    records = _build_user_export_points(context, outfile)
    write_to_influx(
        write_api=write_api,
        bucket=main.bucket,
        org=main.org,
        record=records,
        logger_=logger,
        strict=strict,
        success_msg=f"Exported user benchmark results",
        failure_prefix="User benchmark MAIN export failed"
    )


# ---------------- Behave Steps ----------------

@when('I run a "/me" benchmark with {concurrent:d} concurrent clients for {duration:d} seconds')
def step_run_me_benchmark(context, concurrent: int, duration: int) -> None:
    sut = context.influxdb.sut
    start_t = time.perf_counter()
    all_metrics = []

    with ThreadPoolExecutor(max_workers=concurrent) as executor:
        futures = [executor.submit(_run_me_worker, sut.url, sut.token, sut.org, duration) for _ in range(concurrent)]
        for fut in as_completed(futures): all_metrics.extend(fut.result())

    context.last_user_metrics = all_metrics
    context.last_user_meta = {
        "operation": "me", "concurrency": concurrent, "run_id": str(uuid.uuid4()),
        "total_duration_s": time.perf_counter() - start_t, "sut_url": sut.url
    }


@when(
    'I run a user lifecycle benchmark with username complexity "{u_comp}", password complexity "{p_comp}", {concurrent:d} parallel threads for {iterations:d} iterations')
def step_run_lifecycle_benchmark(context, u_comp, p_comp, concurrent, iterations):
    sut = context.influxdb.sut
    barrier = threading.Barrier(concurrent)
    start_t = time.perf_counter()
    all_metrics = []

    with ThreadPoolExecutor(max_workers=concurrent) as executor:
        futures = [
            executor.submit(_run_lifecycle_worker, sut.url, sut.token, sut.org, u_comp, p_comp, iterations, barrier) for
            _ in range(concurrent)]
        for fut in as_completed(futures): all_metrics.extend(fut.result())

    context.last_user_metrics = all_metrics
    context.last_user_meta = {
        "operation": "lifecycle_crud", "concurrency": concurrent, "run_id": str(uuid.uuid4()),
        "total_duration_s": time.perf_counter() - start_t, "username_complexity": u_comp, "password_complexity": p_comp
    }


@then('I store the user benchmark result as "{outfile}"')
def step_store_user_result(context, outfile: str) -> None:
    metrics = getattr(context, "last_user_metrics", [])
    meta = getattr(context, "last_user_meta", {})
    lats = [m.latency_s for m in metrics] if metrics else [0.0]

    stats = {"avg": statistics.mean(lats), "min": min(lats), "max": max(lats), "median": statistics.median(lats)}
    context.user_bench_results = {
        "meta": meta, "stats": stats, "total_ops": len(metrics),
        "throughput": len(metrics) / meta.get("total_duration_s", 1.0),
        "error_count": sum(1 for m in metrics if not m.ok)
    }

    write_json_report(outfile, context.user_bench_results, logger_=logger)
    _export_to_main_influx(context, outfile)