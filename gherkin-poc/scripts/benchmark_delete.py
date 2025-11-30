#!/usr/bin/env python

import os
import time
import socket
import argparse
from datetime import datetime, timedelta, timezone
from typing import Tuple, Optional

import requests
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

MEASUREMENT = "influx_admin_kpi"


# CLI  

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark InfluxDB /api/v2/delete endpoint."
    )
    parser.add_argument(
        "--scenario",
        default="delete_range_benchmark",
        help="Scenario tag (written into KPI).",
    )
    parser.add_argument(
        "--env-tag",
        default=os.environ.get("BDD_ENV", "dev"),
        help="Environment tag (default from $BDD_ENV or 'dev').",
    )
    parser.add_argument(
        "--range",
        default="5m",
        help="Delete range size (e.g. '5m', '1h', '24h'). "
             "Range is [now-range, now].",
    )
    parser.add_argument(
        "--predicate",
        default="true",
        help="Delete predicate string, see Influx /api/v2/delete docs.",
    )
    parser.add_argument(
        "--measurement",
        default=None,
        help="Optional measurement filter, if set will be added to predicate.",
    )
    parser.add_argument(
        "--probe-health",
        action="store_true",
        help="If set, run a small /health probe before and after the delete.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned delete range/predicate but do not execute delete.",
    )
    return parser.parse_args()



def get_influx_client() -> InfluxDBClient:
    url = os.environ.get("INFLUX_URL")
    token = os.environ.get("INFLUX_TOKEN")
    org = os.environ.get("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL / INFLUX_TOKEN / INFLUX_ORG must be set.")

    return InfluxDBClient(url=url, token=token, org=org)


def resolve_base_url() -> str:
    base = os.environ.get("INFLUX_URL")
    if not base:
        raise RuntimeError("INFLUX_URL must be set.")
    return base.rstrip("/")


def parse_range_to_start_stop(range_spec: str) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)

    if range_spec.endswith("m"):
        minutes = int(range_spec[:-1])
        delta = timedelta(minutes=minutes)
    elif range_spec.endswith("h"):
        hours = int(range_spec[:-1])
        delta = timedelta(hours=hours)
    elif range_spec.endswith("d"):
        days = int(range_spec[:-1])
        delta = timedelta(days=days)
    else:
        raise ValueError(f"Unsupported range spec: {range_spec!r}")

    start = now - delta
    stop = now
    return start, stop


def build_full_predicate(user_predicate: str, measurement: Optional[str]) -> str:
    pred = (user_predicate or "").strip() or "true"

    if not measurement:
        return pred

    if pred.lower() == "true":
        return f'_measurement="{measurement}"'

    return f'_measurement="{measurement}" AND ({pred})'



def quick_health_probe(num_requests: int = 5, timeout_s: float = 5.0) -> Tuple[float, int]:
    base_url = resolve_base_url()
    health_url = f"{base_url}/health"

    latencies = []
    errors = 0

    for _ in range(num_requests):
        start = time.time()
        try:
            resp = requests.get(health_url, timeout=timeout_s)
            latency_ms = (time.time() - start) * 1000.0
            latencies.append(latency_ms)
            if not resp.ok:
                errors += 1
        except Exception:
            latency_ms = (time.time() - start) * 1000.0
            latencies.append(latency_ms)
            errors += 1

    avg = sum(latencies) / len(latencies) if latencies else 0.0
    return avg, errors


# writer

def write_delete_kpi_to_influx(
    stats: dict,
    scenario: str,
    env_tag: str,
) -> None:
    bucket = os.environ.get("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set to write KPIs.")

    host = socket.gethostname()

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    point = (
        Point(MEASUREMENT)
        .tag("endpoint", "/api/v2/delete")
        .tag("method", "DELETE")
        .tag("scenario", scenario)
        .tag("env", env_tag)
        .tag("host", host)
        .tag("status_class", stats.get("status_class", "unknown"))
        .tag("delete_range", stats["range_spec"])
        .field("delete_duration_ms", float(stats["delete_duration_ms"]))
        .field("delete_range_sec", float(stats["delete_range_sec"]))
        .field("delete_error", int(stats["error"]))
    )

    # Optional extra fields if health probes were run
    if "health_before_avg_ms" in stats:
        point = point.field("health_before_avg_ms", float(stats["health_before_avg_ms"]))
    if "health_after_avg_ms" in stats:
        point = point.field("health_after_avg_ms", float(stats["health_after_avg_ms"]))

    print(f"[benchmark_delete] Writing KPI to Influx bucket={bucket}")
    write_api.write(bucket=bucket, record=point)
    client.close()


# master

def run_delete_benchmark(args: argparse.Namespace) -> dict:
    bucket = os.environ.get("INFLUX_BUCKET")
    org = os.environ.get("INFLUX_ORG")
    if not bucket or not org:
        raise RuntimeError("INFLUX_BUCKET / INFLUX_ORG must be set.")

    start_dt, stop_dt = parse_range_to_start_stop(args.range)
    delete_range_sec = (stop_dt - start_dt).total_seconds()

    start_iso = start_dt.isoformat()
    stop_iso = stop_dt.isoformat()
    predicate = build_full_predicate(args.predicate, args.measurement)

    print("[benchmark_delete] Bucket:   ", bucket)
    print("[benchmark_delete] Range:    ", f"{start_iso}  ->  {stop_iso}")
    print("[benchmark_delete] Range len:", f"{delete_range_sec:.1f} s")
    print("[benchmark_delete] Predicate:", predicate)

    stats: dict = {
        "range_spec": args.range,
        "delete_range_sec": delete_range_sec,
        "delete_duration_ms": 0.0,
        "error": 0,
        "status_class": "unknown",
    }

    if args.probe_health:
        avg_ms, errors = quick_health_probe()
        print(f"[benchmark_delete] Health BEFORE delete: avg={avg_ms:.2f} ms, errors={errors}")
        stats["health_before_avg_ms"] = avg_ms

    if args.dry_run:
        print("[benchmark_delete] DRY RUN: not executing delete.")
        return stats

    client = get_influx_client()
    delete_api = client.delete_api()

    t0 = time.time()
    try:
        delete_api.delete(
            start=start_iso,
            stop=stop_iso,
            predicate=predicate,
            bucket=bucket,
            org=org,
        )
        duration_ms = (time.time() - t0) * 1000.0
        stats["delete_duration_ms"] = duration_ms
        stats["status_class"] = "2xx"
        print(f"[benchmark_delete] Delete completed in {duration_ms:.2f} ms")
    except Exception as exc:
        duration_ms = (time.time() - t0) * 1000.0
        stats["delete_duration_ms"] = duration_ms
        stats["error"] = 1
        stats["status_class"] = "5xx"
        print(f"[benchmark_delete] Delete FAILED after {duration_ms:.2f} ms: {exc}")
    finally:
        client.close()

    if args.probe_health:
        avg_ms, errors = quick_health_probe()
        print(f"[benchmark_delete] Health AFTER delete: avg={avg_ms:.2f} ms, errors={errors}")
        stats["health_after_avg_ms"] = avg_ms

    return stats


def main() -> None:
    args = parse_args()
    stats = run_delete_benchmark(args)
    write_delete_kpi_to_influx(stats, scenario=args.scenario, env_tag=args.env_tag)


if __name__ == "__main__":
    main()

