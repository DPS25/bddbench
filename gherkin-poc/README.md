This folder contins a proof of concept to run Gherkin features with Python (behave) for InfluxDB-like benchmarks  

**How to install:**

```
PowerShell (Windows):

git clone https://github.com/DPS25/bddbench.git

cd .\bddbench\gherkin-poc

python -m venv .venv

.\.venv\Scripts\Activate.ps1

pip install -r .\requirements.txt

behave
```

```  
Bash (Linx/macOS/WSL):

git clone https://github.com/DPS25/bddbench.git

cd bddbench/gherkin-poc

nix-shell -p python3 python3Packages.pip python3Packages.behave python3Packages.influxdb-client

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

behave
```

the following console output should be generated:

```
Feature: InfluxDB v2 write path performance (POC) # features/write_latency.feature:1
  In order to express performance benchmarks in BDD
  As the team
  I want to define a write workload and check basic latency rules
  Background:   # features/write_latency.feature:6

  @poc @m1 @write
  Scenario Outline: Write load with basic thresholds -- @1.1                        # features/write_latency.feature:18
    Given an InfluxDB endpoint is configured                                        # features/steps/write_steps.py:8
    And a bucket "bdbench" is defined                                               # features/steps/write_steps.py:14
    When I write 200 points per second for 5 seconds                                # features/steps/write_steps.py:19
    Then the median latency shall be <= 50 ms                                       # features/steps/write_steps.py:43
    And I store the benchmark result as "gherkin-poc/reports/write-latency-p1.json" # features/steps/write_steps.py:50

1 feature passed, 0 failed, 0 skipped
1 scenario passed, 0 failed, 0 skipped
5 steps passed, 0 failed, 0 skipped, 0 undefined
Took 0m0.056s
```

--------------------------------

## First real Influx Test 
# Influx BDD Benchmark

This repo contains the first **real** BDD feature that writes data to our InfluxDB and reads it back. This lets us test end-to-end: token valid, org correct, bucket reachaable, Flux query works

---

## idea

the feature `features/influx_basic_benchmark.feature` does:

1. connects via Env-Vars with the influx
2. 10 points with measurement `bddbench_write` and a specific `run_id` wrote into the bucket
3. the influx reads the 10 points
4. (optional) check latency
   
We are able to test it directly from our BDD environment

---

## 1. get influx tokens directly from UI

1. in Influx, go to Load Data on the left
2. Tab "API Tokens"
3. Either click an existing token or **Generate API Token --> Custom API Token**
4. for bucket `dsp25` check both `read` and `write` 
5. Copy the token --> later use it as  `INFLUX_TOKEN` 

> Note: the token must really come from the UI

---

## 2. Find Org name

Query the server once with teh token

```bash
curl -s -H "Authorization: Token <TOKEN_FROM_UI>" http://localhost:8086/api/v2/orgs
```
------------------------------
## 3. How to run
```
# 1. fetch repo
git clone -b POC https://github.com/DPS25/bddbench.git
cd bddbench/gherkin-poc

# 2. (optional) get org name from Influx
curl -s -H "Authorization: Token <TOKEN_FROM_UI>" http://localhost:8086/api/v2/orgs

# 3. open NixOS shell with Python + behave
nix-shell -p python3 python3Packages.pip python3Packages.behave python3Packages.influxdb-client

# 4. start the benchmark
INFLUX_URL=http://localhost:8086 \
INFLUX_ORG=<S3> \
INFLUX_BUCKET=dsp25 \
INFLUX_TOKEN="<TOKEN_FROM_UI>" \
behave -v --tags @influx
```


# Offline KPI Mock + Single $t$-Test

This bundle generates a mock CSV of KPI rows and then run a one-sample, two-sided t-test against a target latency.

## Files

- `mock_seed_kpis_local.py`: Generates realistic mock KPI rows into `mock/main_kpis.csv` (two groups: `config_id=v1` and `config_id=v2`).

- `analyze_ttest_single_offline.py`: Loads KPI values from the CSV, filters by scenario/tag, runs a _two-sided one-sample t-test_ vs `TARGET_MS`, and plots a histogram + mean with 95% confidence interval.

## Start

```shell
# 1) Create mock data
python mock_seed_kpis_local.py

# 2) Run a single t-test on a subset (e.g., scenario=write_basic, config_id=v1)
export KPI_CSV=mock/main_kpis.csv
export KPI_SCENARIO=write_basic
export KPI_GROUP_TAG=config_id
export KPI_GROUP_VAL=v1
export KPI_FIELD=mean_ms          # or median_ms, p95_ms, ...
export KPI_TARGET_MS=15
export KPI_MEASUREMENT=bddbench_summary

python analyze_ttest_single_offline.py
```

The script prints statistical results:

- sample size (`n`)
- sample mean and standard deviation
- t-statistic
- two-sided p-value
- 95% confidence interval for the mean

It produces two plots:

- Histogram of per-run KPI values
- Mean with 95% confidence interval

## What the mock seeder produces

`mock_seed_kpis_local.py` writes a CSV that mimics an Influx query export (flat records with `_measurement`, `_field`, `_value`, and tags). It creates:

- 10 runs for `config_id=v1` (slightly slower; base ~10.05 ms, spread ~0.06)
- 10 runs for `config_id=v2` (slightly faster; base ~9.95 ms, spread ~0.05)
- Each "run" aggregates ~20 synthetic samples into summary fields.
