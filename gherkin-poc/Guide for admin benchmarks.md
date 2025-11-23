

## 1. Clone the repository

```bash
git clone https://github.com/DPS25/bddbench.git
cd bddbench/gherkin-poc
```

---

## 2. Enter a Nix shell (recommended)



```bash
nix-shell -p python3 python3Packages.pip python3Packages.numpy python3Packages.scipy python3Packages.influxdb-client
```



---

## 3. Create and activate a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```


```bash
pip install -r requirements.txt
```

---

## 4. Configure InfluxDB environment variables

Set the environment variables so the scripts know how to connect to your InfluxDB instance:

```bash
export INFLUX_URL="http://localhost:8086"
export INFLUX_ORG="3S"
export INFLUX_BUCKET="dsp25"
export INFLUX_TOKEN="<YOUR_INFLUX_TOKEN>"
export BDD_ENV="dev"
```

> Replace `<YOUR_INFLUX_TOKEN>` with a valid token for your InfluxDB instance.
> Adjust `INFLUX_ORG` and `INFLUX_BUCKET` as needed for your environment.

---

## 5. Run the three benchmark scripts

All commands below are executed from `bddbench/gherkin-poc`.

### 5.1 `/health` benchmark – `benchmark_health.py`

Standard run: **60 seconds at 5 requests/second**

```bash
python scripts/benchmark_health.py
```

This is equivalent to:

```bash
python scripts/benchmark_health.py \
  --duration-sec 60 \
  --rate-per-sec 5.0 \
  --scenario health_under_load \
  --env-tag dev
```

Optional low-rate example: **30 seconds at 1 request/second**

```bash
python scripts/benchmark_health.py \
  --duration-sec 30 \
  --rate-per-sec 1.0 \
  --scenario health_low_rate \
  --env-tag dev
```

This script sends requests to `/health`, computes latency percentiles and error rates, and writes a single KPI point to the Influx measurement `influx_admin_kpi`.

---

### 5.2 `/api/v2/delete` benchmark – `benchmark_delete.py`

> **Warning:** This script performs real deletions. Only run it on test data / test measurements.

**Dry run (no delete is executed):**

```bash
python scripts/benchmark_delete.py \
  --range 5m \
  --measurement bddbench_write \
  --dry-run
```

This shows which time range and predicate would be used, but does not call the delete API.

**Real delete + health probes before and after:**

```bash
python scripts/benchmark_delete.py \
  --range 5m \
  --measurement bddbench_write \
  --probe-health
```

Example with an additional predicate:

```bash
python scripts/benchmark_delete.py \
  --range 1h \
  --measurement bddbench_write \
  --predicate 'host="test-host"' \
  --probe-health
```

This script calls `/api/v2/delete` for the specified time range and predicate, measures `delete_duration_ms`, and optionally runs small `/health` probes before and after the delete. It writes a KPI point to `influx_admin_kpi` (endpoint = `/api/v2/delete`).

---

### 5.3 `/api/v2/users` benchmark – `benchmark_user_mgmt.py`

**Safe mode: dry run (no actual users created or deleted):**

```bash
python scripts/benchmark_user_mgmt.py \
  --num-users 5 \
  --dry-run
```

**Real run (requires a token with `write:users` permissions):**

```bash
python scripts/benchmark_user_mgmt.py \
  --num-users 20 \
  --scenario user_mgmt_bulk \
  --env-tag dev
```

This script benchmarks user-management endpoints (`/api/v2/users`) by bulk-creating, listing, and deleting users. It records latency percentiles and error counts and writes a KPI point to `influx_admin_kpi` (endpoint = `/api/v2/users`).

> If your token does not have `write:users` permissions, `POST /api/v2/users` will return `401` and the KPI will show non-zero error counts. The script still runs and records this as part of the results.

---

## 6. Viewing results in the InfluxDB UI

1. Open a browser on the same machine and visit:
   **[http://localhost:8086](http://localhost:8086)**
2. Log in to the InfluxDB UI.
3. Go to **“Data Explorer” / “Explore”**.
4. In the query builder:

   * Select **Bucket**: `dsp25` (or your configured bucket).
   * Select **_measurement**: `influx_admin_kpi`.
5. Filter by **endpoint** depending on which benchmark you ran:

   * `/health` – health benchmark KPIs
   * `/api/v2/delete` – delete benchmark KPIs
   * `/api/v2/users` – user-management benchmark KPIs
6. Select the fields you are interested in, for example:

   * `/health`: `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms`, `error_rate`, `total_requests`, `req_per_min`
   * `/api/v2/delete`: `delete_duration_ms`, `delete_range_sec`, `health_before_avg_ms`, `health_after_avg_ms`
   * `/api/v2/users`: `create_p50_ms`, `create_errors`, `list_latency_ms`, `total_errors`, etc.
7. Click **SUBMIT** to see the raw data or charts for the KPI points created by the scripts.

---

If you’d like, I can also turn this into a ready-to-paste `docs/admin_benchmarks.md` file structure (with headings and bullet points styled for the repo).
