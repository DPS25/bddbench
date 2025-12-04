from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


@dataclass(frozen=True)
class Conn:
    url: str
    org: str
    token: str


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env_any(*names: str) -> str | None:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def _conn_from_env(target: str) -> Conn:
    if target == "sut":
        url = _env_any("INFLUXDB_SUT_URL", "SUT_INFLUX_URL")
        org = _env_any("INFLUXDB_SUT_ORG", "SUT_INFLUX_ORG")
        token = _env_any("INFLUXDB_SUT_TOKEN", "SUT_INFLUX_TOKEN")
    else:
        url = _env_any("INFLUXDB_MAIN_URL", "MAIN_INFLUX_URL")
        org = _env_any("INFLUXDB_MAIN_ORG", "MAIN_INFLUX_ORG")
        token = _env_any("INFLUXDB_MAIN_TOKEN", "MAIN_INFLUX_TOKEN")

    if not url or not org or not token:
        raise RuntimeError(
            f"Missing env for target={target}. Need URL/ORG/TOKEN. "
            f"Got url={bool(url)} org={bool(org)} token={bool(token)}"
        )
    return Conn(url=url, org=org, token=token)


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _unique(seq: List[str]) -> List[str]:
    out, seen = [], set()
    for s in seq:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def run_cleanup_cli(args) -> int:
    target: str = args.target
    if target == "main" and not getattr(args, "allow_main", False):
        raise RuntimeError("Refusing to run on MAIN without --allow-main.")
    conn = _conn_from_env(target)
    stop = args.stop or _now_rfc3339()
    delete_buckets = bool(args.delete_buckets)
    delete_data = bool(args.delete_data)
    if not delete_buckets and not delete_data:
        # sensible default:
        delete_buckets = True

    # late import
    from influxdb_client import InfluxDBClient

    state = _load_state(Path(args.state_file)) if args.from_state else {}
    created_buckets = state.get("created_buckets", {}).get(target, []) if args.from_state else []
    written_data = state.get("written_data", []) if args.from_state else []

    with InfluxDBClient(url=conn.url, token=conn.token, org=conn.org) as client:
        buckets_api = client.buckets_api()
        existing = buckets_api.find_buckets(org=conn.org).buckets or []
        existing_names = [b.name for b in existing if getattr(b, "name", None)]

        # --- select bucket candidates ---
        buckets: List[str] = []
        if args.bucket:
            buckets = list(args.bucket)
        elif args.from_state and created_buckets:
            buckets = list(created_buckets)
        elif args.bucket_prefix:
            buckets = [b for b in existing_names if b.startswith(args.bucket_prefix)]
        buckets = [b for b in buckets if b not in set(args.exclude_bucket or [])]
        buckets = _unique(buckets)

        # --- select delete-data targets from state or explicit flags ---
        data_targets: List[Tuple[str, str]] = []  # (bucket, measurement)
        run_ids: Set[str] = set(args.run_id or [])
        measurements: Set[str] = set(args.measurement or [])

        if args.from_state:
            for item in written_data:
                if not isinstance(item, dict):
                    continue
                if item.get("target") != target:
                    continue
                b = item.get("bucket")
                m = item.get("measurement")
                r = item.get("run_id")
                if b and m and r:
                    data_targets.append((b, m))
                    run_ids.add(r)

        # If user passed measurement but no bucket: use buckets selection
        if measurements and buckets:
            for b in buckets:
                for m in measurements:
                    data_targets.append((b, m))

        data_targets = list(dict.fromkeys(data_targets))

        print(f"Target: {target} url={conn.url} org={conn.org}")
        print(f"Mode: {'delete-buckets' if delete_buckets else 'delete-data'}")
        if delete_buckets:
            print("Buckets:")
            for b in buckets:
                print(f"  - {b}")
        else:
            print("Data targets:")
            for b, m in data_targets:
                print(f"  - bucket={b} measurement={m}")
            print("Run IDs:")
            for r in sorted(run_ids):
                print(f"  - {r}")

        if args.list or not args.yes:
            print("\nDry-run only. Use --yes to apply.")
            return 0

        if delete_buckets:
            for name in buckets:
                bucket_obj = buckets_api.find_bucket_by_name(name)
                if not bucket_obj:
                    print(f"[skip] bucket not found: {name}")
                    continue
                print(f"[delete-bucket] {name}")
                buckets_api.delete_bucket(bucket_obj)
            return 0

        # delete-data:
        delete_api = client.delete_api()

        if not run_ids:
            raise RuntimeError(
                "Refusing delete-data without run_id(s). "
                "Use --from-state (recommended) or pass --run-id explicitly."
            )

        extra = (args.predicate or "").strip()
        for b, m in data_targets:
            # base predicate: measurement + run_id
            for rid in run_ids:
                pred = f'_measurement="{m}" AND run_id="{rid}"'
                if extra:
                    pred = f"({pred}) AND ({extra})"
                print(f"[delete-data] bucket={b} start={args.start} stop={stop} predicate={pred}")
                delete_api.delete(args.start, stop, pred, bucket=b, org=conn.org)

        return 0

