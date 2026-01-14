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

measurement = os.getenv("MEASUREMENT", "bddbench_write_result")
scenario_id = os.getenv("SCENARIO_ID", "smoke")
git_sha = os.getenv("BDD_GIT_SHA")

filters = [f'r["_measurement"] == "{measurement}"', f'r["scenario_id"] == "{scenario_id}"']
if git_sha:
    filters.append(f'r["git_sha"] == "{git_sha}"')

where = " and ".join(filters)

flux = f'''
from(bucket: "{bucket}")
  |> range(start: -30d)
  |> filter(fn: (r) => {where})
  |> last()
'''

with InfluxDBClient(url=url, token=token, org=org) as c:
    tables = c.query_api().query(query=flux, org=org)
    if not tables or not tables[0].records:
        raise SystemExit(f"NOT FOUND in MAIN: measurement={measurement} scenario_id={scenario_id} git_sha={git_sha or '(any)'}")
    r = tables[0].records[0].values
    print("FOUND _time:", r.get("_time"))
    print("measurement:", measurement)
    print("scenario_id:", r.get("scenario_id"))
    for k in ["env_name","git_sha","git_ref","pipeline_id","suite_run_id","scenario_run_id","sut_influx_url","sut_org","sut_bucket"]:
        print(f"{k}:", r.get(k))

