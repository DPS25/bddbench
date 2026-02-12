from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from .common import (
    InfluxConfig,
    PostgrestConfig,
    git_sha_short,
    influx_write_point,
    new_run_id,
    now_rfc3339,
    percentile,
    postgrest_post_json,
    postgrest_table_url,
    random_payload,
)


def _decode_err(b: bytes) -> str:
    if not b:
        return ""
    return b[:300].decode("utf-8", errors="replace")


def post_batch(cfg: PostgrestConfig, url: str, rows: List[Dict[str, Any]]) -> Tuple[bool, float, int, str, int]:
    t0 = time.perf_counter()
    status, body = postgrest_post_json(cfg, url, rows)
    latency_s = time.perf_counter() - t0
    ok = 200 <= status < 300
    err = "" if ok else _decode_err(body) or f"HTTP {status}"
    return ok, latency_s, status, err, len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="PostgREST write benchmark")
    ap.add_argument("--records", type=int, default=2000)
    ap.add_argument("--payload-bytes", type=int, default=128)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--table", default=None)
    ap.add_argument("--scenario-id", default="postgrest_default")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--run-id-file", default="reports/postgrest_last_run_id.txt")
    args = ap.parse_args()

    cfg = PostgrestConfig.from_env()
    if args.table:
        cfg.table = args.table

    influx = InfluxConfig.from_env()
    run_id = args.run_id or new_run_id()
    sha = git_sha_short()

    url = postgrest_table_url(cfg)
    payload = random_payload(args.payload_bytes)

    n = int(args.records)
    bs = max(1, int(args.batch_size))
    conc = max(1, int(args.concurrency))

    # build batches
    batches: List[List[Dict[str, Any]]] = []
    for start in range(0, n, bs):
        end = min(n, start + bs)
        rows = []
        for i in range(start, end):
            rows.append(
                {
                    "run_id": run_id,
                    "ts": now_rfc3339(),
                    "seq": i,
                    "payload": payload,
                }
            )
        batches.append(rows)

    t_wall0 = time.perf_counter()
    lat_ms: List[float] = []
    ok_records = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=conc) as ex:
        futs = [ex.submit(post_batch, cfg, url, rows) for rows in batches]
        for fut in as_completed(futs):
            ok, lat_s, status, err, row_count = fut.result()
            lat_ms.append(lat_s * 1000.0)
            if ok:
                ok_records += row_count
            else:
                errors += 1
                print(f"[write] batch failed status={status} err={err}")

    wall_s = time.perf_counter() - t_wall0
    throughput_rps = (ok_records / wall_s) if wall_s > 0 else 0.0

    print("\n=== WRITE RESULT ===")
    print(f"run_id        : {run_id}")
    print(f"ok_records    : {ok_records}/{n}")
    print(f"duration_s    : {wall_s:.4f}")
    print(f"throughput_rps: {throughput_rps:.2f}")
    print(f"lat_p50_ms    : {percentile(lat_ms, 50):.3f}")
    print(f"lat_p95_ms    : {percentile(lat_ms, 95):.3f}")
    print(f"lat_p99_ms    : {percentile(lat_ms, 99):.3f}")
    print(f"errors        : {errors}")
    print("=== end ===\n")

    # persist run_id for query/delete
    os.makedirs(os.path.dirname(args.run_id_file), exist_ok=True)
    with open(args.run_id_file, "w", encoding="utf-8") as f:
        f.write(run_id + "\n")
    print(f"[write] saved run_id -> {args.run_id_file}")

    # report to main influx (best-effort)
    try:
        influx_write_point(
            influx=influx,
            measurement=influx.measurement,
            tags={
                "sut": "postgrest",
                "action": "write",
                "scenario_id": args.scenario_id,
                "run_id": run_id,
                "git_sha": sha,
            },
            fields={
                "records": int(n),
                "ok_records": int(ok_records),
                "errors": int(errors),
                "duration_s": float(wall_s),
                "throughput_rps": float(throughput_rps),
                "lat_p50_ms": float(percentile(lat_ms, 50)),
                "lat_p95_ms": float(percentile(lat_ms, 95)),
                "lat_p99_ms": float(percentile(lat_ms, 99)),
                "batch_size": int(bs),
                "concurrency": int(conc),
                "payload_bytes": int(args.payload_bytes),
            },
        )
        print("[write] reported metrics -> main influxdb")
    except Exception as e:
        print(f"[WARN] main influxdb write failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

