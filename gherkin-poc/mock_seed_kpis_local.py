# mock_seed_kpis_local.py
import csv, time, random, statistics, uuid
from pathlib import Path

out = Path("mock/main_kpis.csv")
out.parent.mkdir(parents=True, exist_ok=True)

def synth_run(base_ms: float, spread: float, n: int, tags: dict):
    samples = [random.gauss(base_ms, spread) for _ in range(n)]
    return {
        "_time": int(time.time()),
        "_measurement": "bddbench_summary",
        "_field": "mean_ms",                       # write one row per field
        "_value": statistics.mean(samples),
        **tags,
        "_n": n,
        "_median_ms": statistics.median(samples),
        "_p95_ms": sorted(samples)[int(0.95*(n-1))],
        "_p99_ms": sorted(samples)[int(0.99*(n-1))],
        "_std_ms": statistics.pstdev(samples),
        "_read_ok": 1,
    }

rows = []
# Group A (e.g., config_id=v1)
for i in range(10):
    tags = {
        "run_id": str(uuid.uuid4()),
        "scenario": "write_basic",
        "measurement": "bddbench_write",
        "config_id": "v1",
        "sut_host": "dsp25-efe",
        "build_id": "sha_v1",
    }
    rows.append(synth_run(base_ms=10.05, spread=0.06, n=20, tags=tags))

# Group B (e.g., config_id=v2) a bit faster
for i in range(10):
    tags = {
        "run_id": str(uuid.uuid4()),
        "scenario": "write_basic",
        "measurement": "bddbench_write",
        "config_id": "v2",
        "sut_host": "dsp25-efe",
        "build_id": "sha_v2",
    }
    rows.append(synth_run(base_ms=9.95, spread=0.05, n=20, tags=tags))

# Write CSV shaped like an Influx CSV export (flat, one value column)
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=[
            "_time","_measurement","_field","_value",
            "run_id","scenario","measurement","config_id","sut_host","build_id",
            "_n","_median_ms","_p95_ms","_p99_ms","_std_ms","_read_ok"
        ],
    )
    w.writeheader()
    for r in rows: w.writerow(r)

print(f"Wrote {len(rows)} mock KPI rows -> {out}")
