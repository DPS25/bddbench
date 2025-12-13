import logging
import time
import json
import uuid
import random
import string
import statistics
import os
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from behave import when, then
from influxdb_client import InfluxDBClient

from src.measurements.UserPerformancePoint import UserPerformancePoint

logger = logging.getLogger("bddbench.influx_user_steps")


# Datatypes

@dataclass
class UserOpMetric:
    latency_s: float
    ok: bool
    status_code: int


# Helpers

def _generate_credentials(complexity: str) -> str:
    """
    Generate a username based on the desired complexity.
    Only username complexity is varied here; password
    management in InfluxDB v2 is typically token-based.
    """
    base = f"bench_user_{uuid.uuid4().hex[:8]}"
    if complexity == "high":
        # Add random special chars and length
        suffix = "".join(
            random.choices(
                string.ascii_letters + string.digits + "!@#$%",
                k=20,
            )
        )
        return f"{base}_{suffix}"
    return base


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

    # Each worker gets its own client
    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()

        while time.perf_counter() < end_time:
            t0 = time.perf_counter()
            try:
                _ = users_api.me()
                t1 = time.perf_counter()
                metrics.append(UserOpMetric(latency_s=t1 - t0, ok=True, status_code=200))
            except Exception as e:
                t1 = time.perf_counter()
                status = getattr(e, "status", 500)
                metrics.append(UserOpMetric(latency_s=t1 - t0, ok=False, status_code=status))
                # Avoid a tight error loop
                time.sleep(0.01)

    return metrics


def _run_lifecycle_worker(
    url: str,
    token: str,
    org: str,
    complexity: str,
    iterations: int,
) -> List[UserOpMetric]:
    """
    Worker that creates, updates, finds, and deletes a user.

    This implements a full CRUD-style lifecycle:
      - Create user
      - Update user (e.g. change name)
      - Retrieve user (find)
      - Delete user
    """
    metrics: List[UserOpMetric] = []

    with InfluxDBClient(url=url, token=token, org=org, timeout=30000) as client:
        users_api = client.users_api()

        for _ in range(iterations):
            username = _generate_credentials(complexity)
            t0 = time.perf_counter()
            try:
                # 1. Create
                user = users_api.create_user(name=username)

                # 2. Update (change the name)
                updated_name = f"{username}_upd"
                user = users_api.update_user(user.id, name=updated_name)

                # 3. Find (retrieve)
                # Python client has find_users(), not find_user_by_id().
                found = users_api.find_users(id=user.id)
                if not found or not getattr(found, "users", None):
                    raise RuntimeError(f"User {user.id} not found after update")

                # 4. Delete
                users_api.delete_user(user.id)

                t1 = time.perf_counter()
                metrics.append(UserOpMetric(latency_s=t1 - t0, ok=True, status_code=201))
            except Exception as e:
                t1 = time.perf_counter()
                status = getattr(e, "status", 500)
                metrics.append(UserOpMetric(latency_s=t1 - t0, ok=False, status_code=status))
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

    # Avoid division by zero
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

    # Write JSON report
    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)

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

    with out_path.open("w") as f:
        json.dump(final_report, f, indent=2)

    logger.info(f"User benchmark saved to {outfile}")


def _export_to_main_influx(context, outfile: str) -> None:
    """
    Reads context.user_bench_results and writes them to the
    Main InfluxDB using UserPerformancePoint (Pydantic model).
    """
    if not hasattr(context, "user_bench_results"):
        return

    data = context.user_bench_results
    meta = data["meta"]
    stats = data["stats"]

    point_data = UserPerformancePoint(
        run_uuid=str(uuid.uuid4()),
        feature_name=os.path.basename(outfile),
        sut=meta.get("sut_url", "unknown"),
        test_type="user_benchmark",
        time=datetime.now(timezone.utc),

        operation=meta.get("operation", "unknown"),
        complexity=meta.get("complexity", "low"),
        concurrency=meta.get("concurrency", 1),

        throughput=data["throughput"],
        latency_avg_ms=stats["avg"] * 1000.0,
        latency_min_ms=stats["min"] * 1000.0,
        latency_max_ms=stats["max"] * 1000.0,
        error_count=data["error_count"],

        extra_metrics={
            "total_ops": float(data["total_ops"]),
        },
    )

    main = context.influxdb.main
    if main.client:
        try:
            main.write_api.write(
                bucket=main.bucket,
                org=main.org,
                record=point_data.to_point(),
            )
            logger.info("Exported user metrics to Main InfluxDB")
        except Exception as e:
            logger.error(f"Failed to export to Main InfluxDB: {e}")


# Behave Steps

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
        "complexity": "none",
    }

    context.last_user_metrics = all_metrics
    context.last_user_meta = meta


@when(
    'I run a user lifecycle benchmark with {complexity} username complexity, '
    '{concurrent_clients:d} parallel threads for {iterations:d} iterations'
)
def step_run_lifecycle_benchmark(
    context,
    complexity: str,
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
                complexity,
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
        "complexity": complexity,
    }

    context.last_user_metrics = all_metrics
    context.last_user_meta = meta


@then('I store the user benchmark result as "{outfile}"')
def step_store_user_result(context, outfile: str) -> None:
    metrics: List[UserOpMetric] = getattr(context, "last_user_metrics", [])
    meta: Dict[str, Any] = getattr(context, "last_user_meta", {})

    _summarize_and_store(context, metrics, meta, outfile)
    _export_to_main_influx(context, outfile)

