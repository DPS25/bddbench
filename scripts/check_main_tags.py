import os
from influxdb_client import InfluxDBClient

MEASUREMENT = os.getenv("BDD_RESULT_MEASUREMENT", "bddbench_write_result")
SCENARIO_ID = os.getenv("BDD_SCENARIO_ID", "smoke")

url = os.getenv("INFLUXDB_MAIN_URL")
token = os.getenv("INFLUXDB_MAIN_TOKEN")
org = os.getenv("INFLUXDB_MAIN_ORG")
bucket = os.getenv("INFLUXDB_MAIN_BUCKET")

assert url and token and org and bucket, "MAIN Influx env missing (INFLUXDB_MAIN_*)"

flux = f'''
from(bucket: "{bucket}")
  |> range(start: -2d)
  |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
  |> filter(fn: (r) => r["scenario_id"] == "{SCENARIO_ID}")
  |> last()
'''

with InfluxDBClient(url=url, token=token, org=org) as c:
    tables = c.query_api().query(query=flux, org=org)
    if not tables or not tables[0].records:
        raise SystemExit(f"NOT FOUND: no point for measurement={MEASUREMENT}, scenario_id={SCENARIO_ID} in last 2d")

    r = tables[0].records[0].values

    print("FOUND _time:", r.get("_time"))
    print("scenario_id:", r.get("scenario_id"))
    print("env_name:", r.get("env_name"))
    print("git_sha:", r.get("git_sha"))
    print("git_ref:", r.get("git_ref"))
    print("pipeline_id:", r.get("pipeline_id"))
    print("suite_run_id:", r.get("suite_run_id"))
    print("scenario_run_id:", r.get("scenario_run_id"))

