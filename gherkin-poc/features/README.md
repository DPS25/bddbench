# Benchmark Metrics in InfluxDB (MAIN)

This document explains the data points we write into the **MAIN InfluxDB instance** from our Behave benchmarks.

Right now we have three main measurements:

- `bddbench_query_result` – aggregated metrics from the **query benchmark**
- `bddbench_write_result` – aggregated metrics from the **single-bucket write benchmark**
- `bddbench_multi_write_result` – aggregated metrics from the **multi-bucket write benchmark**

Each Behave scenario writes **exactly one point per run** into these measurements.  
Most fields are already **aggregated statistics** (min/max/avg/median) over multiple runs or batches – they are *not* raw per-request values.

---

## 1. Query benchmark (`bddbench_query_result`)

This measurement holds the **result of the query benchmark** (what we do in `influx_query_steps.py`).

### 1.1 Tags (dimensions)

These tags describe *which scenario / configuration* produced the metrics:

- `source_measurement` – the measurement name in the SUT (e.g. `bddbench_generic`)
- `time_range` – the Flux time range (e.g. `10s`, `1h`)
- `query_type` – type of query (`filter`, `aggregate`, `group_by`, `pivot`, `join`, …)
- `result_size` – target size of the result set (`small` / `large`, controls `limit(n=…)`)
- `output_format` – response format used by the client (`csv` or tables/JSON)
- `compression` – HTTP compression (`none` / `gzip`)
- `sut_bucket` – bucket of the SUT we read from
- `sut_org` – org of the SUT
- `sut_influx_url` – base URL of the SUT (e.g. `http://localhost:8086`)
- `scenario_id` – logical scenario id derived from the report file name (e.g. `smoke`, `load` from `query-load.json`)

### 1.2 Fields (values)

These are the actual performance metrics.

#### 1.2.1 General

- `total_runs` – how many client runs we executed in this benchmark (usually = `concurrent_clients`)
- `errors_count` – how many of those runs failed
- `error_rate` – `errors_count / total_runs` (0.0 means everything was fine)

#### 1.2.2 Latency / duration

All times are in **seconds**.

- `ttf_min_s`, `ttf_max_s`, `ttf_avg_s`, `ttf_median_s`  
  **Time to first result (TTF)**: time from sending the request until the client sees the **first row/record**.  
  → This is basically our **latency**.

- `total_time_min_s`, `total_time_max_s`, `total_time_avg_s`, `total_time_median_s`  
  Total runtime per query: time from sending the request until **all data has been consumed**.  
  → This is the **end-to-end duration** of a query run.

#### 1.2.3 Result size

- `bytes_min`, `bytes_max`, `bytes_avg`, `bytes_median`  
  Number of bytes returned per run (approx., based on CSV lines or JSON size).

- `rows_min`, `rows_max`, `rows_avg`, `rows_median`  
  Number of rows/records per run.

#### 1.2.4 Throughput

These are derived metrics based on the average duration and result size:

- `throughput_bytes_per_s`  
  Average **byte throughput** in bytes per second  
  → roughly `bytes_avg / total_time_avg_s`

- `throughput_rows_per_s`  
  Average **row throughput** in rows per second  
  → roughly `rows_avg / total_time_avg_s`

**So if you’re looking for:**

- “How fast do we get the **first** result?” → `ttf_avg_s` / `ttf_median_s`
- “How long does the **whole** query take?” → `total_time_avg_s` / `total_time_median_s`
- “What’s our **throughput per second**?” → `throughput_bytes_per_s`, `throughput_rows_per_s`

---

## 2. Single-bucket write benchmark (`bddbench_write_result`)

This measurement contains the aggregated result of the **single-bucket write benchmark** (what we do in `influx_write_steps.py`).

### 2.1 Tags (dimensions)

- `source_measurement` – measurement name we write into (e.g. `bddbench_write_poc`)
- `compression` – client compression (`none` / `gzip`)
- `precision` – timestamp precision (`ns`, `ms`, `s`)
- `point_complexity` – `low` = few fields per point, `high` = more fields (`aux1`, `aux2`, `aux3`, …)
- `time_ordering` – `in_order` = monotonic timestamps, `out_of_order` = jitter/out-of-order writes
- `sut_bucket` – target bucket in the SUT
- `sut_org` – org of the SUT
- `sut_influx_url` – base URL of the SUT
- `scenario_id` – scenario id from the report file name (e.g. `smoke`, `load` from `write-load.json`)

### 2.2 Fields (values)

#### 2.2.1 Volume and duration

- `total_points` – total number of points written  
  (= `batch_size * batches * parallel_writers`)

- `total_batches` – total number of batches across all writers

- `total_duration_s` – wall-clock time of the whole benchmark run (start → all writers finished)

- `throughput_points_per_s` – effective **write throughput** in points per second  
  → `total_points / total_duration_s`

#### 2.2.2 Errors

- `errors_count` – number of failed batch writes (exceptions in the client)
- `error_rate` – `errors_count / total_batches`

#### 2.2.3 Batch latency

This is what we mean by “latency per batch” – but aggregated:

- `latency_min_s`, `latency_max_s`, `latency_avg_s`, `latency_median_s`  
  Latency per batch: time from calling `write_api.write(...)` until it returns or fails, aggregated over **all batches**.

So:

- **Latency per batch** → `latency_avg_s`, `latency_median_s`
- **Throughput per second** → `throughput_points_per_s`
- **How much did we write?** → `total_points`
- **How many writes failed?** → `errors_count`, `error_rate`

---

## 3. Multi-bucket write benchmark (`bddbench_multi_write_result`)

The multi-bucket benchmark is similar to the single-bucket one, but it **creates multiple buckets** and writes into all of them in parallel for a fixed duration.

Conceptually:

- we create `bucket_count` buckets with a given `bucket_prefix`
- we spawn `parallel_writers` per bucket
- each writer keeps writing batches until the configured duration is over
- we aggregate everything across all buckets/writers into one result point

### 3.1 Tags (dimensions)

Typical tags for the multi-bucket result:

- `source_measurement` – measurement name used for the points (e.g. `bddbench_multiwrite`)
- `bucket_prefix` – prefix used to create the buckets (actual buckets are `${bucket_prefix}_0`, `${bucket_prefix}_1`, …)
- `compression` – client compression (`none` / `gzip`)
- `precision` – timestamp precision (`ns`, `ms`, `s`)
- `point_complexity` – `low` / `high` (same meaning as in the single-bucket benchmark)
- `time_ordering` – `in_order` / `out_of_order`
- `sut_bucket_pattern` (or similar) – logical representation of all buckets, e.g. `bddbench_mb_load_*`
- `sut_org` – org of the SUT
- `sut_influx_url` – base URL of the SUT
- `scenario_id` – scenario id from the report file name (e.g. `smoke`, `load` from `multi-write-load.json`)

(Exact tag names may differ slightly depending on the implementation, but this is the idea.)

### 3.2 Fields (values)

The fields are basically the same idea as for the single-bucket write benchmark, just **aggregated across all buckets**:

- `bucket_count` – how many buckets we actually targeted in this scenario
- `total_points` – total number of points written across all buckets and writers
- `total_duration_s` – total wall-clock time of the benchmark (start → all writers across all buckets finished)
- `throughput_points_per_s` – overall **write throughput** (points per second across all buckets)

Error and latency metrics:

- `errors_count` – number of failed batch writes (sum over all buckets/writers)
- `error_rate` – `errors_count / total_batches` (where `total_batches` is the total number of batches across all buckets/writers)

- `latency_min_s`, `latency_max_s`, `latency_avg_s`, `latency_median_s`  
  Aggregated batch latency metrics across all buckets and writers (same semantics as in the single-bucket benchmark).

So if you compare **single-bucket** vs **multi-bucket**:

- single-bucket = throughput and latency for “one bucket under load”
- multi-bucket = throughput and latency when we **fan out** to multiple buckets in parallel

---

## 4. TL;DR

The very short version I use when explaining this:

- **Query benchmark (`bddbench_query_result`)**
  - Latency (first result) → `ttf_*_s`
  - Latency (full query) → `total_time_*_s`
  - Throughput per second → `throughput_bytes_per_s`, `throughput_rows_per_s`
  - Errors → `error_rate`, `errors_count`

- **Single-bucket write benchmark (`bddbench_write_result`)**
  - Latency **per batch** → `latency_*_s`
  - Write throughput (points per second) → `throughput_points_per_s`
  - Volume → `total_points`, `total_batches`
  - Errors → `error_rate`, `errors_count`

- **Multi-bucket write benchmark (`bddbench_multi_write_result`)**
  - Same metrics as the single-bucket write benchmark,
  - but aggregated across **N buckets** in parallel.
  - Good for answering “how does the system behave when I hit multiple buckets at once?”
