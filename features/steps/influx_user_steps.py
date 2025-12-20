import logging
import time
import uuid
import random
import string
import statistics
import os
from dataclasses import dataclass
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from behave import when, then
from influxdb_client import InfluxDBClient

from src.measurements.UserPerformancePoint import UserPerformancePoint
from src.utils import (
    write_json_report,
    scenario_id_from_outfile,
    main_influx_is_configured,
    get_main_influx_write_api,
)

logger = logging.getLogger("bddbench.influx_user_steps")


# ---------------- Datatypes ----------------

@dataclass
class UserOpMetric:
    latency_s: float
    ok: bool
    status_code: int


# ---------------- Helpers ----------------


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
    complexity as a benchmark dimension to satisfy the requirement.
    """
    base = uuid.uuid4().hex
    if complexity == "high":
        return base + "".join(
            random.choices(string.ascii_letters + string.digits + "!@#$%^&*()", k=24)
        )
    return base[:12]


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
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=200,
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                status = getattr(e, "status", 500)
                metrics.append(
                    UserOpMetric(
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=status,
                    )
                )
                time.sleep(0.01)

    return metrics


def _run_lifecycle_worker(
    url: str,
    token: str,
    org: str,
    username_complexity: str,
    password_complexity: str,
    iterations: int,
) -> List[UserOpMetric]:
    """
    Worker that creates, updates, retrieves, and deletes a user.
    """
    metrics: List[UserOpMetric] = []

    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()

        for _ in range(iterations):
            username = _generate_username(username_complexity)
            _password = _generate_password(password_complexity)

            t0 = time.perf_counter()
            try:
                user = users_api.create_user(name=username)

                updated_name = f"{username}_upd"
                user = users_api.update_user(user.id, name=updated_name)

                found = users_api.find_users(id=user.id)
                if not found or not getattr(found, "users", None):
                    raise RuntimeError(f"User {user.id} not found after update")

                users_api.delete_user(user.id)

                t1 = time.perf_counter()
                metrics.append(
                    UserOpMetric(
                        latency_s=t1 - t0,
                        ok=True,
                        status_code=201,
                    )
                )
            except Exception as e:
                t1 = time.perf_counter()
                status = getattr(e, "status", 500)
                metrics.append(
                    UserOpMetric(
                        latency_s=t1 - t0,
                        ok=False,
                        status_code=status,
                    )
                )
                logger.debug(f"Lifecycle failed: {e}")

    return metrics


def _summarize_and_store(
    context,
    metrics: List[UserOpMetric],
    meta: Dict[str, Any],
    outfile: str,
) -> None:
    """
    Aggregates metrics, stores them in the Behave context,
    and writes a JSON summary report to disk.
    """
    latencies = [m.latency_s for m in metrics]
    errors = [m for m in metrics if not m.ok]

    total_ops = len(metrics)
    duration = meta.get("total_duration_s", 1.0)
    if duration <= 0:
        duration = 1.0

    stats = {
        "min": min(latencies) if latencies else 0.0,
        "max": max(latencies) if latencies else 0.0,
        "avg": statistics.mean(latencies) if latencies else 0.0,
        "median": statistics.median(latencies) if latencies else 0.0,
    }

    context.user_bench_results = {
        "meta": meta,
        "stats": stats,
        "throughput": total_ops / duration,
        "total_ops": total_ops,
        "error_count": len(errors),
    }

    final_report = {
        "meta": meta,
        "summary": {
            "stats_s": stats,
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


def _export_to_main_influx(context, outfile: str) -> None:
    """
    Reads context.user_bench_results and writes them to the
    Main InfluxDB using UserPerformancePoint.

    Uses context.influxdb.main.* from environment.py.
    Controlled by INFLUXDB_EXPORT_STRICT:
      - "1"/"true"/"yes": export failures raise (fail scenario)
      - otherwise: export failures only log a warning.
    """
    if not hasattr(context, "user_bench_results"):
        return

    if not main_influx_is_configured(context):
        logger.info("[user-bench] MAIN influx not configured – skipping export")
        return

    strict = os.getenv("INFLUXDB_EXPORT_STRICT", "0").strip().lower() in ("1", "true", "yes")
    main = context.influxdb.main

    # client/write_api wie bei den anderen Benchmarks holen
    client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
    if write_api is None:
        msg = "[user-bench] MAIN write_api missing – skipping export"
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

    point_data = UserPerformancePoint(
        run_uuid=str(uuid.uuid4()),
        feature_name=os.path.basename(outfile),
        sut=str(meta.get("sut_url", "unknown")),
        test_type="user_benchmark",
        time=datetime.now(timezone.utc),

        scenario_id=scenario_id,

        operation=str(meta.get("operation", "unknown")),
        username_complexity=str(
            meta.get("username_complexity", meta.get("complexity", "none"))
        ),
        password_complexity=str(meta.get("password_complexity", "none")),
        concurrency=int(meta.get("concurrency", 1)),

        sut_org=str(meta.get("sut_org", "")),
        sut_bucket=str(meta.get("sut_bucket", "")),
        sut_influx_url=str(meta.get("sut_url", "")),

        throughput=float(data["throughput"]),
        latency_avg_ms=float(stats["avg"]) * 1000.0,
        latency_min_ms=float(stats["min"]) * 1000.0,
        latency_max_ms=float(stats["max"]) * 1000.0,
        error_count=int(data["error_count"]),

        extra_metrics={
            "total_ops": float(data["total_ops"]),
            "total_duration_s": float(meta.get("total_duration_s", 0.0) or 0.0),
        },
    )

    try:
        write_api.write(
            bucket=main.bucket,
            org=main.org,
            record=point_data.to_point(),
        )
        logger.info("Exported user metrics to Main InfluxDB")
    except Exception as e:
        msg = f"Failed to export to Main InfluxDB: {e}"
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

    with ThreadPoolExecutor(max_workers=concurrent_clients) as executor:
        futures = [
            executor.submit(
                _run_lifecycle_worker,
                url,
                token,
                org,
                username_complexity,
                password_complexity,
                iterations,
            )
            for _ in range(concurrent_clients)
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

    _summarize_and_store(context, metrics, meta, outfile)
    _export_to_main_influx(context, outfile)
