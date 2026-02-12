from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

from .common import (
    InfluxConfig,
    PostgrestConfig,
    git_sha_short,
    influx_write_point,
    percentile,
    postgrest_get,
    postgrest_table_url,
)


def _decode_err(b: bytes) -> str:
    if not b:
        return ""
    return b[:300].decode("utf-8", errors="replace")


def do_get(cfg: PostgrestConfig, url: str) -> Tuple[bool, float, int, int, str]:
    t0 = time.perf_counter()
    status, body = postgrest_get(cfg, url)
    lat_s = time.perf_counter() - t0
    ok = 200 <= status < 300
    body_len = len(body) if body else 0
    err = "" if ok else _decode_err(body) or f"HTTP {status}"
    return ok, lat_s, status, body_len, err


def main() -> int:
    ap = argparse.ArgumentParser(description="PostgREST query benchmark")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--run-id-file", default="reports/postgrest_last_run_id.txt")
    ap.add_argument("--scenario-id", default="postgrest_default")
    ap.add_argument("--table", default=None)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--repeats", type=int, default=200)
    ap.add_argument("--limit", type=int, default=1000)
    args = ap.parse_args()

    cfg = PostgrestConfig.from_env()
    if args.table:
        cfg.table = args.table

    run_id = args.run_id
    if not run_id:
        with open(args.run_id_file, "r", encoding="utf-8") as f:
            run_id = f.read().strip()

    influx = InfluxConfig.from_env()
    sha = git_sha_short()
    base = postgrest_table_url(cfg)

    repeats = max(1, int(args.repeats))
    limit = max(1, int(args.limit))
    conc = max(1, int(args.concurrency))

    queries = [
        ("recent_small", f"{base}?run_id=eq.{run_id}&select=ts,seq&order=ts.desc&limit={limit}"),
        ("recent_full",  f"{base}?run_id=eq.{run_id}&select=*&order=ts.desc&limit={limit}"),
        ("light",        f"{base}?run_id=eq.{run_id}&select=seq&limit={limit}"),
    ]

    for qname, qurl in queries:
        lat_ms: List[float] = []
        bytes_list: List[int] = []
        err_count = 0

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=conc) as ex:
            futs = [ex.submit(do_get, cfg, qurl) for _ in range(repeats)]
            for fut in as_completed(futs):
                ok, lat_s, status, body_len, err = fut.result()
                lat_ms.append(lat_s * 1000.0)
                bytes_list.append(body_len)
                if not ok:
                    err_count += 1
                    print(f"[query:{qname}] failed status={status} err={err}")

        wall_s = time.perf_counter() - t0
        qps = (repeats / wall_s) if wall_s > 0 else 0.0
        avg_bytes = (sum(bytes_list) / len(bytes_list)) if bytes_list else 0.0

        print(f"\n=== QUERY TYPE: {qname} ===")
        print(f"qps        : {qps:.2f}")
        print(f"lat_p50_ms : {percentile(lat_ms, 50):.3f}")
        print(f"lat_p95_ms : {percentile(lat_ms, 95):.3f}")
        print(f"lat_p99_ms : {percentile(lat_ms, 99):.3f}")
        print(f"avg_bytes  : {avg_bytes:.1f}")
        print(f"errors     : {err_count}")
        print("=== end ===\n")

        try:
            influx_write_point(
                influx=influx,
                measurement=influx.measurement,
                tags={
                    "sut": "postgrest",
                    "action": "query",
                    "query_type": qname,
                    "scenario_id": args.scenario_id,
                    "run_id": run_id,
                    "git_sha": sha,
                },
                fields={
                    "repeats": int(repeats),
                    "errors": int(err_count),
                    "duration_s": float(wall_s),
                    "qps": float(qps),
                    "lat_p50_ms": float(percentile(lat_ms, 50)),
                    "lat_p95_ms": float(percentile(lat_ms, 95)),
                    "lat_p99_ms": float(percentile(lat_ms, 99)),
                    "avg_bytes": float(avg_bytes),
                    "limit": int(limit),
                    "concurrency": int(conc),
                },
            )
            print(f"[query:{qname}] reported metrics -> main influxdb")
        except Exception as e:
            print(f"[WARN] [query:{qname}] main influxdb write failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

