import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple, List

def load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def rel_regress(new: float, base: float) -> float:
    if base == 0:
        return 0.0 if new == 0 else float("inf")
    return (new - base) / base

def get(d: Dict[str, Any], path: str):
    cur: Any = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def require_same_meta(base: Dict[str, Any], new: Dict[str, Any], keys: List[str]) -> Tuple[bool, str]:
    for k in keys:
        bv = get(base, f"meta.{k}")
        nv = get(new, f"meta.{k}")
        if bv != nv:
            return False, f"meta mismatch on '{k}': base={bv!r}, new={nv!r}"
    return True, ""

def check_latency_lower_is_better(name: str, base_v: float, new_v: float, up_limit: float) -> Tuple[bool, Dict[str, Any]]:
    r = rel_regress(new_v, base_v)
    ok = r <= up_limit
    return ok, {"metric": name, "base": base_v, "new": new_v, "regress": r, "limit": up_limit, "direction": "lower_better"}

def check_throughput_higher_is_better(name: str, base_v: float, new_v: float, down_limit: float) -> Tuple[bool, Dict[str, Any]]:
    # fail if new drops more than down_limit
    r = rel_regress(new_v, base_v)
    ok = r >= -down_limit
    return ok, {"metric": name, "base": base_v, "new": new_v, "regress": r, "limit": -down_limit, "direction": "higher_better"}

def must_equal(name: str, base_v: Any, new_v: Any) -> Tuple[bool, Dict[str, Any]]:
    ok = base_v == new_v
    return ok, {"metric": name, "base": base_v, "new": new_v}

def classify(file_name: str) -> str:
    # based on your report naming
    if file_name.startswith("write-"):
        return "write"
    if file_name.startswith("multi-write-"):
        return "multi_write"
    if file_name.startswith("query-"):
        return "query"
    if file_name.startswith("delete-"):
        return "delete"
    if file_name.startswith("user-me-") or file_name.startswith("user-lifecycle-"):
        return "user"
    if file_name.startswith("memory-"):
        return "memory"
    if file_name.startswith("storage-"):
        return "storage"
    return "unknown"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base_dir")
    ap.add_argument("new_dir")
    ap.add_argument("--latency_up", type=float, default=0.10)
    ap.add_argument("--throughput_down", type=float, default=0.10)
    args = ap.parse_args()

    base_dir = Path(args.base_dir)
    new_dir = Path(args.new_dir)

    base_files = sorted([p.name for p in base_dir.glob("*.json")])
    new_files = sorted([p.name for p in new_dir.glob("*.json")])

    missing_in_new = sorted(set(base_files) - set(new_files))
    missing_in_base = sorted(set(new_files) - set(base_files))
    if missing_in_new or missing_in_base:
        raise SystemExit(
            f"Report set mismatch.\n"
            f"Missing in new: {missing_in_new}\n"
            f"Missing in base: {missing_in_base}"
        )

    checks: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for fn in base_files:
        b = load_json(base_dir / fn)
        n = load_json(new_dir / fn)
        kind = classify(fn)

        # Common: enforce same meta config for key benchmarks
        if kind in ("write", "multi_write", "query", "delete", "user"):
            meta_keys = ["sut_url", "sut_influx_url", "org", "sut_org", "bucket", "sut_bucket", "scenario_id"]
            ok, msg = require_same_meta(b, n, [k for k in meta_keys if get(b, f"meta.{k}") is not None])
            if not ok:
                item = {"file": fn, "kind": kind, "check": "meta_equal", "detail": msg}
                checks.append(item)
                failures.append(item)
                continue

        # Now metric-specific checks
        if kind == "write":
            # errors must be 0
            base_err = get(b, "summary.errors_count")
            new_err = get(n, "summary.errors_count")
            ok, item = must_equal("errors_count==0", 0, new_err)
            item.update({"file": fn, "kind": kind, "base_errors": base_err})
            checks.append(item)
            if not ok:
                failures.append(item)

            # latency median
            bm = get(b, "summary.latency_stats.median")
            nm = get(n, "summary.latency_stats.median")
            if bm is None or nm is None:
                failures.append({"file": fn, "kind": kind, "metric": "latency_stats.median", "error": "missing"})
            else:
                ok2, item2 = check_latency_lower_is_better("latency_median_s", float(bm), float(nm), args.latency_up)
                item2.update({"file": fn, "kind": kind})
                checks.append(item2)
                if not ok2:
                    failures.append(item2)

            # throughput
            bt = get(b, "summary.throughput.points_per_s")
            nt = get(n, "summary.throughput.points_per_s")
            if bt is None or nt is None:
                failures.append({"file": fn, "kind": kind, "metric": "throughput.points_per_s", "error": "missing"})
            else:
                ok3, item3 = check_throughput_higher_is_better("throughput_points_per_s", float(bt), float(nt), args.throughput_down)
                item3.update({"file": fn, "kind": kind})
                checks.append(item3)
                if not ok3:
                    failures.append(item3)

        elif kind == "multi_write":
            new_err = get(n, "summary.errors_count")
            ok, item = must_equal("errors_count==0", 0, new_err)
            item.update({"file": fn, "kind": kind})
            checks.append(item)
            if not ok:
                failures.append(item)

            bm = get(b, "summary.latency_stats.median")
            nm = get(n, "summary.latency_stats.median")
            if bm is not None and nm is not None:
                ok2, item2 = check_latency_lower_is_better("latency_median_s", float(bm), float(nm), args.latency_up)
                item2.update({"file": fn, "kind": kind})
                checks.append(item2)
                if not ok2:
                    failures.append(item2)

            bt = get(b, "summary.throughput.points_per_s")
            nt = get(n, "summary.throughput.points_per_s")
            if bt is not None and nt is not None:
                ok3, item3 = check_throughput_higher_is_better("throughput_points_per_s", float(bt), float(nt), args.throughput_down)
                item3.update({"file": fn, "kind": kind})
                checks.append(item3)
                if not ok3:
                    failures.append(item3)

            # config sanity
            for k in ["bucket_count", "duration_s", "batch_size", "parallel_writers_per_bucket"]:
                bv = get(b, f"meta.{k}")
                nv = get(n, f"meta.{k}")
                if bv is not None and nv is not None and bv != nv:
                    failures.append({"file": fn, "kind": kind, "metric": f"meta.{k}", "base": bv, "new": nv, "error": "config_changed"})

        elif kind == "query":
            new_err = get(n, "summary.errors_count")
            ok, item = must_equal("errors_count==0", 0, new_err)
            item.update({"file": fn, "kind": kind})
            checks.append(item)
            if not ok:
                failures.append(item)

            for mname, path in [
                ("ttf_median_s", "summary.latency_stats.ttf_median"),
                ("total_median_s", "summary.latency_stats.total_median"),
            ]:
                bm = get(b, path); nm = get(n, path)
                if bm is None or nm is None:
                    failures.append({"file": fn, "kind": kind, "metric": mname, "error": "missing"})
                else:
                    ok2, item2 = check_latency_lower_is_better(mname, float(bm), float(nm), args.latency_up)
                    item2.update({"file": fn, "kind": kind})
                    checks.append(item2)
                    if not ok2:
                        failures.append(item2)

            for mname, path in [
                ("throughput_rows_per_s", "summary.throughput.rows_per_s"),
                ("throughput_bytes_per_s", "summary.throughput.bytes_per_s"),
            ]:
                bt = get(b, path); nt = get(n, path)
                if bt is None or nt is None:
                    failures.append({"file": fn, "kind": kind, "metric": mname, "error": "missing"})
                else:
                    ok3, item3 = check_throughput_higher_is_better(mname, float(bt), float(nt), args.throughput_down)
                    item3.update({"file": fn, "kind": kind})
                    checks.append(item3)
                    if not ok3:
                        failures.append(item3)

        elif kind == "delete":
            okb = get(n, "summary.ok")
            pa = get(n, "summary.points_after")
            # must be ok and points_after==0
            if okb is not True:
                failures.append({"file": fn, "kind": kind, "metric": "summary.ok", "new": okb, "error": "not_ok"})
            if pa != 0:
                failures.append({"file": fn, "kind": kind, "metric": "summary.points_after", "new": pa, "error": "not_zero"})

            # optional latency regression
            bl = get(b, "summary.delete_latency_s")
            nl = get(n, "summary.delete_latency_s")
            if bl is not None and nl is not None:
                ok2, item2 = check_latency_lower_is_better("delete_latency_s", float(bl), float(nl), args.latency_up)
                item2.update({"file": fn, "kind": kind})
                checks.append(item2)
                if not ok2:
                    failures.append(item2)

        elif kind == "user":
            errs = get(n, "summary.errors")
            if errs != 0:
                failures.append({"file": fn, "kind": kind, "metric": "summary.errors", "new": errs, "error": "nonzero_errors"})

            bt = get(b, "summary.throughput_ops_s")
            nt = get(n, "summary.throughput_ops_s")
            if bt is not None and nt is not None:
                ok3, item3 = check_throughput_higher_is_better("throughput_ops_s", float(bt), float(nt), args.throughput_down)
                item3.update({"file": fn, "kind": kind})
                checks.append(item3)
                if not ok3:
                    failures.append(item3)

            bm = get(b, "summary.stats_s.median")
            nm = get(n, "summary.stats_s.median")
            if bm is not None and nm is not None:
                ok2, item2 = check_latency_lower_is_better("latency_median_s", float(bm), float(nm), args.latency_up)
                item2.update({"file": fn, "kind": kind})
                checks.append(item2)
                if not ok2:
                    failures.append(item2)

        elif kind == "memory":
            bt = get(b, "result.throughput_mib_s")
            nt = get(n, "result.throughput_mib_s")
            if bt is not None and nt is not None:
                ok3, item3 = check_throughput_higher_is_better("throughput_mib_s", float(bt), float(nt), args.throughput_down)
                item3.update({"file": fn, "kind": kind})
                checks.append(item3)
                if not ok3:
                    failures.append(item3)

            bl = get(b, "result.total_time_s")
            nl = get(n, "result.total_time_s")
            if bl is not None and nl is not None:
                ok2, item2 = check_latency_lower_is_better("total_time_s", float(bl), float(nl), args.latency_up)
                item2.update({"file": fn, "kind": kind})
                checks.append(item2)
                if not ok2:
                    failures.append(item2)

        elif kind == "storage":
            # higher is better
            for mname in ["read_bw_kib_s", "write_bw_kib_s", "read_iops", "write_iops"]:
                bt = get(b, f"result.metrics.{mname}")
                nt = get(n, f"result.metrics.{mname}")
                if bt is not None and nt is not None:
                    ok3, item3 = check_throughput_higher_is_better(mname, float(bt), float(nt), args.throughput_down)
                    item3.update({"file": fn, "kind": kind})
                    checks.append(item3)
                    if not ok3:
                        failures.append(item3)

            # lower is better (if exists)
            for mname in ["read_lat_ns_mean", "write_lat_ns_mean"]:
                bl = get(b, f"result.metrics.{mname}")
                nl = get(n, f"result.metrics.{mname}")
                if bl is not None and nl is not None:
                    ok2, item2 = check_latency_lower_is_better(mname, float(bl), float(nl), args.latency_up)
                    item2.update({"file": fn, "kind": kind})
                    checks.append(item2)
                    if not ok2:
                        failures.append(item2)

        else:
            # unknown reports: strict fail (so "全部 features 都比较" 不漏)
            failures.append({"file": fn, "kind": kind, "error": "unknown_report_type"})

    report = {
        "base_dir": str(base_dir),
        "new_dir": str(new_dir),
        "latency_up": args.latency_up,
        "throughput_down": args.throughput_down,
        "checks_count": len(checks),
        "failures_count": len(failures),
        "failures": failures,
        "checks": checks[:200],  # 避免太大
    }

    with open("regression_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if failures:
        print(f"FAIL: {len(failures)} regression failures. See regression_report.json")
        for x in failures[:30]:
            print(x)
        raise SystemExit(1)

    print("PASS: no regressions detected.")

if __name__ == "__main__":
    main()

