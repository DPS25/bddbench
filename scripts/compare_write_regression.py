import os
from influxdb_client import InfluxDBClient

def need(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing env: {name}")
    return v

url = need("INFLUXDB_MAIN_URL")
token = need("INFLUXDB_MAIN_TOKEN")
org = need("INFLUXDB_MAIN_ORG")
bucket = need("INFLUXDB_MAIN_BUCKET")

sha_a = need("SHA_A")
sha_b = need("SHA_B")
scenario_id = os.getenv("SCENARIO_ID", "smoke")
measurement = os.getenv("MEASUREMENT", "bddbench_write_result")

def fetch(sha: str) -> dict:
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["scenario_id"] == "{scenario_id}")
  |> filter(fn: (r) => r["git_sha"] == "{sha}")
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
    tables = c.query_api().query(query=flux, org=org)
    if not tables or not tables[0].records:
        raise SystemExit(f"NOT FOUND in MAIN: measurement={measurement} scenario_id={scenario_id} git_sha={sha}")
    return tables[0].records[0].values

with InfluxDBClient(url=url, token=token, org=org) as c:
    a = fetch(sha_a)
    b = fetch(sha_b)

def f(v, k, default=None):
    x = v.get(k, default)
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

a_thr = f(a, "throughput_points_per_s")
b_thr = f(b, "throughput_points_per_s")
a_med = f(a, "latency_median_s")
b_med = f(b, "latency_median_s")

print("=== Regression Compare (MAIN) ===")
print("measurement:", measurement)
print("scenario_id:", scenario_id)
print("A git_sha:", sha_a, "time:", a.get("_time"))
print("B git_sha:", sha_b, "time:", b.get("_time"))

print("\n--- Key Metrics ---")
print("throughput_points_per_s:", a_thr, "->", b_thr, "delta:", (b_thr - a_thr) if (a_thr is not None and b_thr is not None) else None)
print("latency_median_s:", a_med, "->", b_med, "delta:", (b_med - a_med) if (a_med is not None and b_med is not None) else None)

print("\n--- Tags (B) ---")
for k in ["env_name","git_ref","pipeline_id","suite_run_id","sut_influx_url","sut_org","sut_bucket"]:
    print(f"{k}:", b.get(k))

