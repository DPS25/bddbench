#!/usr/bin/env python

import os
import time
import socket
import argparse
import statistics
from typing import List, Tuple


import requests
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


MEASUREMENT = "influx_admin_kpi"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark InfluxDB /health endpoint.")
    parser.add_argument(
        "--scenario",
        default="health_under_load",
        help="Scenario tag to write into KPI (e.g. health_under_load).",
    )
    parser.add_argument(
        "--duration-sec",
        type=int,
        default=60,
        help="Benchmark duration in seconds.",
    )
    parser.add_argument(
        "--rate-per-sec",
        type=float,
        default=5.0,
        help="Approximate request rate per second.",
    )
    parser.add_argument(
        "--env-tag",
        default=os.environ.get("BDD_ENV", "dev"),
        help="Environment tag (default from $BDD_ENV or 'dev').",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="Full /health URL. If not set, derived from INFLUX_URL + '/health'.",
    )
    return parser.parse_args()


def resolve_health_url(args: argparse.Namespace) -> str:
    if args.endpoint:
        return args.endpoint

    base_url = os.environ.get("INFLUX_URL")
    if not base_url:
        raise RuntimeError("INFLUX_URL not set and --endpoint not given.")

    base_url = base_url.rstrip("/")
    return f"{base_url}/health"


def get_influx_client() -> InfluxDBClient:
    url = os.environ.get("INFLUX_URL")
    token = os.environ.get("INFLUX_TOKEN")
    org = os.environ.get("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL / INFLUX_TOKEN / INFLUX_ORG must be set.")

    return InfluxDBClient(
        url=url,
        token=token,
        org=org,
    )


def collect_latency_stats(latencies_ms: List[float]) -> Tuple[float, float, float]:
    if not latencies_ms:
        return 0.0, 0.0, 0.0

    latencies_ms = sorted(latencies_ms)

    def percentile(p: float) -> float:
        if len(latencies_ms) == 1:
            return latencies_ms[0]
        index = int(round((p / 100.0) * (len(latencies_ms) - 1)))
        index = max(0, min(index, len(latencies_ms) - 1))
        return latencies_ms[index]

    return percentile(50), percentile(95), percentile(99)


def run_benchmark(health_url: str, duration_sec: int, rate_per_sec: float) -> dict:
    latencies_ms: List[float] = []
    total_requests = 0
    error_requests = 0
    status_classes = {}

    interval_s = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    stop_time = time.time() + duration_sec

    print(f"[benchmark_health] Target: {health_url}")
    print(f"[benchmark_health] Duration: {duration_sec}s, rate: {rate_per_sec}/s")

    while time.time() < stop_time:
        start = time.time()
        try:
            resp = requests.get(health_url, timeout=5.0)
            latency = (time.time() - start) * 1000.0
            latencies_ms.append(latency)
            total_requests += 1

            status_class = f"{resp.status_code // 100}xx"
            status_classes[status_class] = status_classes.get(status_class, 0) + 1

            if not resp.ok:
                error_requests += 1

        except Exception as exc:
            # Treat exceptions as 5xx-class errors
            latency = (time.time() - start) * 1000.0
            latencies_ms.append(latency)
            total_requests += 1
            error_requests += 1
            status_classes["5xx"] = status_classes.get("5xx", 0) + 1
            print(f"[benchmark_health] Request failed: {exc}")

        # sleep to keep target rate
        if interval_s > 0:
            elapsed = time.time() - start
            remaining = interval_s - elapsed
            if remaining > 0:
                time.sleep(remaining)

    p50, p95, p99 = collect_latency_stats(latencies_ms)
    error_rate = (error_requests / total_requests) if total_requests > 0 else 0.0
    req_per_min = (total_requests / duration_sec) * 60.0 if duration_sec > 0 else 0.0

    # choose the most frequent status_class
    if status_classes:
        main_status_class = max(status_classes.items(), key=lambda kv: kv[1])[0]
    else:
        main_status_class = "unknown"

    stats = {
        "total_requests": total_requests,
        "error_requests": error_requests,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "latency_p99_ms": p99,
        "error_rate": error_rate,
        "req_per_min": req_per_min,
        "status_class": main_status_class,
    }

    print("[benchmark_health] Done.")
    print("[benchmark_health] Stats:", stats)

    return stats


def write_kpi_to_influx(stats: dict, scenario: str, env_tag: str) -> None:
    bucket = os.environ.get("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set to write KPIs.")

    host = socket.gethostname()
    url = os.environ.get("INFLUX_URL", "").rstrip("/") + "/health"

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    point = (
        Point(MEASUREMENT)
        .tag("endpoint", "/health")
        .tag("method", "GET")
        .tag("status_class", stats["status_class"])
        .tag("scenario", scenario)
        .tag("env", env_tag)
        .tag("host", host)
        .field("latency_p50_ms", float(stats["latency_p50_ms"]))
        .field("latency_p95_ms", float(stats["latency_p95_ms"]))
        .field("latency_p99_ms", float(stats["latency_p99_ms"]))
        .field("req_per_min", float(stats["req_per_min"]))
        .field("error_rate", float(stats["error_rate"]))
        .field("total_requests", int(stats["total_requests"]))
        .field("error_requests", int(stats["error_requests"]))
    )

    print(f"[benchmark_health] Writing KPI to Influx bucket={bucket}")
    write_api.write(bucket=bucket, record=point)
    client.close()


def main() -> None:
    args = parse_args()
    health_url = resolve_health_url(args)
    stats = run_benchmark(
        health_url=health_url,
        duration_sec=args.duration_sec,
        rate_per_sec=args.rate_per_sec,
    )
    write_kpi_to_influx(stats, scenario=args.scenario, env_tag=args.env_tag)


if __name__ == "__main__":
    main()

