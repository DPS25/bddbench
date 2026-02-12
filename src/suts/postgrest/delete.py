from __future__ import annotations

import argparse
import time

from .common import (
    InfluxConfig,
    PostgrestConfig,
    git_sha_short,
    influx_write_point,
    postgrest_delete,
    postgrest_table_url,
)


def _decode_err(b: bytes) -> str:
    if not b:
        return ""
    return b[:300].decode("utf-8", errors="replace")


def main() -> int:
    ap = argparse.ArgumentParser(description="PostgREST delete/cleanup benchmark")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--run-id-file", default="reports/postgrest_last_run_id.txt")
    ap.add_argument("--scenario-id", default="postgrest_default")
    ap.add_argument("--table", default=None)
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
    url = f"{base}?run_id=eq.{run_id}"

    t0 = time.perf_counter()
    status, body = postgrest_delete(cfg, url)
    wall_s = time.perf_counter() - t0

    ok = 200 <= status < 300
    err = "" if ok else _decode_err(body) or f"HTTP {status}"

    print("\n=== DELETE RESULT ===")
    print(f"run_id     : {run_id}")
    print(f"ok         : {ok}")
    print(f"status     : {status}")
    print(f"duration_s : {wall_s:.4f}")
    if err:
        print(f"err        : {err}")
    print("=== end ===\n")

    try:
        influx_write_point(
            influx=influx,
            measurement=influx.measurement,
            tags={
                "sut": "postgrest",
                "action": "delete",
                "scenario_id": args.scenario_id,
                "run_id": run_id,
                "git_sha": sha,
            },
            fields={
                "ok": bool(ok),
                "status": int(status),
                "duration_s": float(wall_s),
            },
        )
        print("[delete] reported metrics -> main influxdb")
    except Exception as e:
        print(f"[WARN] [delete] main influxdb write failed: {e}")

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())

