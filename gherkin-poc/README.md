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
