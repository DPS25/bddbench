#!/usr/bin/env python

import os
import time
import socket
import uuid
import argparse
from typing import List, Tuple, Dict, Any

import requests
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

MEASUREMENT = "influx_admin_kpi"


# CLI

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark InfluxDB user management endpoints."
    )
    parser.add_argument(
        "--scenario",
        default="user_mgmt_bulk",
        help="Scenario tag (written into KPI).",
    )
    parser.add_argument(
        "--env-tag",
        default=os.environ.get("BDD_ENV", "dev"),
        help="Environment tag (default from $BDD_ENV or 'dev').",
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=50,
        help="Number of users to create/delete.",
    )
    parser.add_argument(
        "--user-prefix",
        default="bddbench-user",
        help="Prefix for generated user names.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done but do not create/delete users.",
    )
    return parser.parse_args()



def resolve_base_url() -> str:
    base = os.environ.get("INFLUX_URL")
    if not base:
        raise RuntimeError("INFLUX_URL must be set.")
    return base.rstrip("/")


def get_auth_headers() -> Dict[str, str]:
    token = os.environ.get("INFLUX_TOKEN")
    if not token:
        raise RuntimeError("INFLUX_TOKEN must be set.")
    return {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def collect_percentiles(latencies_ms: List[float]) -> Tuple[float, float, float]:
    if not latencies_ms:
        return 0.0, 0.0, 0.0
    latencies_ms = sorted(latencies_ms)

    def percentile(p: float) -> float:
        if len(latencies_ms) == 1:
            return latencies_ms[0]
        idx = int(round((p / 100.0) * (len(latencies_ms) - 1)))
        idx = max(0, min(idx, len(latencies_ms) - 1))
        return latencies_ms[idx]

    return percentile(50), percentile(95), percentile(99)


def get_influx_client() -> InfluxDBClient:
    url = os.environ.get("INFLUX_URL")
    token = os.environ.get("INFLUX_TOKEN")
    org = os.environ.get("INFLUX_ORG")

    if not url or not token or not org:
        raise RuntimeError("INFLUX_URL / INFLUX_TOKEN / INFLUX_ORG must be set.")

    return InfluxDBClient(url=url, token=token, org=org)


# writer

def write_user_kpi_to_influx(stats: dict, scenario: str, env_tag: str) -> None:
    bucket = os.environ.get("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set to write KPIs.")

    host = socket.gethostname()

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    point = (
        Point(MEASUREMENT)
        .tag("endpoint", "/api/v2/users")
        .tag("method", "MIXED")
        .tag("scenario", scenario)
        .tag("env", env_tag)
        .tag("host", host)
        .tag("status_class", "2xx" if stats["total_errors"] == 0 else "5xx")
        .field("num_users", int(stats["num_users"]))
        # create
        .field("create_p50_ms", float(stats["create_p50_ms"]))
        .field("create_p95_ms", float(stats["create_p95_ms"]))
        .field("create_p99_ms", float(stats["create_p99_ms"]))
        .field("create_errors", int(stats["create_errors"]))
        # delete
        .field("delete_p50_ms", float(stats["delete_p50_ms"]))
        .field("delete_p95_ms", float(stats["delete_p95_ms"]))
        .field("delete_p99_ms", float(stats["delete_p99_ms"]))
        .field("delete_errors", int(stats["delete_errors"]))
        # list
        .field("list_latency_ms", float(stats["list_latency_ms"]))
        .field("list_errors", int(stats["list_errors"]))
        # totals
        .field("total_errors", int(stats["total_errors"]))
    )

    print(f"[benchmark_user_mgmt] Writing KPI to Influx bucket={bucket}")
    write_api.write(bucket=bucket, record=point)
    client.close()


# master

def run_user_mgmt_benchmark(args: argparse.Namespace) -> dict:
    base_url = resolve_base_url()
    headers = get_auth_headers()

    users_endpoint = f"{base_url}/api/v2/users"

    created_ids: List[str] = []
    create_latencies: List[float] = []
    delete_latencies: List[float] = []

    create_errors = 0
    delete_errors = 0
    list_errors = 0
    list_latency_ms = 0.0

    # --- CREATE ---
    print(f"[benchmark_user_mgmt] Creating {args.num_users} users ...")

    if not args.dry_run:
        for i in range(args.num_users):
            name = f"{args.user_prefix}-{uuid.uuid4().hex[:8]}"
            payload = {
                # NOTE: Adjust fields according to InfluxDB API schema if needed.
                "name": name,
            }

            t0 = time.time()
            try:
                resp = requests.post(users_endpoint, json=payload, headers=headers, timeout=10.0)
                latency_ms = (time.time() - t0) * 1000.0
                create_latencies.append(latency_ms)

                if resp.status_code != 201:
                    create_errors += 1
                    print(f"[benchmark_user_mgmt] CREATE error ({resp.status_code}): {resp.text}")
                else:
                    data = resp.json()
                    user_id = data.get("id")
                    if user_id:
                        created_ids.append(user_id)
                    else:
                        create_errors += 1
                        print("[benchmark_user_mgmt] CREATE response missing 'id'")
            except Exception as exc:
                latency_ms = (time.time() - t0) * 1000.0
                create_latencies.append(latency_ms)
                create_errors += 1
                print(f"[benchmark_user_mgmt] CREATE exception: {exc}")
    else:
        print("[benchmark_user_mgmt] DRY RUN: skipping actual user creation.")

    # --- LIST ---
    print("[benchmark_user_mgmt] Listing users ...")

    if not args.dry_run:
        t0 = time.time()
        try:
            resp = requests.get(users_endpoint, headers=headers, timeout=10.0)
            list_latency_ms = (time.time() - t0) * 1000.0

            if resp.status_code != 200:
                list_errors += 1
                print(f"[benchmark_user_mgmt] LIST error ({resp.status_code}): {resp.text}")
        except Exception as exc:
            list_latency_ms = (time.time() - t0) * 1000.0
            list_errors += 1
            print(f"[benchmark_user_mgmt] LIST exception: {exc}")
    else:
        print("[benchmark_user_mgmt] DRY RUN: skipping actual list request.")

    # --- DELETE ---
    print(f"[benchmark_user_mgmt] Deleting {len(created_ids)} users ...")

    if not args.dry_run:
        for user_id in created_ids:
            url = f"{users_endpoint}/{user_id}"
            t0 = time.time()
            try:
                resp = requests.delete(url, headers=headers, timeout=10.0)
                latency_ms = (time.time() - t0) * 1000.0
                delete_latencies.append(latency_ms)

                # 204 is typical for DELETE success
                if resp.status_code not in (200, 204):
                    delete_errors += 1
                    print(f"[benchmark_user_mgmt] DELETE error ({resp.status_code}): {resp.text}")
            except Exception as exc:
                latency_ms = (time.time() - t0) * 1000.0
                delete_latencies.append(latency_ms)
                delete_errors += 1
                print(f"[benchmark_user_mgmt] DELETE exception: {exc}")
    else:
        print("[benchmark_user_mgmt] DRY RUN: skipping actual user deletion.")

    # --- aggregate stats ---
    create_p50, create_p95, create_p99 = collect_percentiles(create_latencies)
    delete_p50, delete_p95, delete_p99 = collect_percentiles(delete_latencies)

    total_errors = create_errors + delete_errors + list_errors

    stats = {
        "num_users": args.num_users,
        "create_p50_ms": create_p50,
        "create_p95_ms": create_p95,
        "create_p99_ms": create_p99,
        "create_errors": create_errors,
        "delete_p50_ms": delete_p50,
        "delete_p95_ms": delete_p95,
        "delete_p99_ms": delete_p99,
        "delete_errors": delete_errors,
        "list_latency_ms": list_latency_ms,
        "list_errors": list_errors,
        "total_errors": total_errors,
    }

    print("[benchmark_user_mgmt] Stats:", stats)
    return stats


def main() -> None:
    args = parse_args()
    stats = run_user_mgmt_benchmark(args)
    write_user_kpi_to_influx(stats, scenario=args.scenario, env_tag=args.env_tag)


if __name__ == "__main__":
    main()
