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

--------------------------------

# First real Influx Test 

```
# Influx BDD Benchmark

Dieses Repo/Unterverzeichnis enthält ein erstes **echtes** BDD-Feature, das gegen unsere InfluxDB schreibt und die Daten wieder ausliest. Damit testen wir End-to-End: Token gültig, Org korrekt, Bucket erreichbar, Flux-Query funktioniert.

---

## Kurze Idee

Das Feature `features/influx_basic_benchmark.feature` macht:

1. mit Env-Vars zu Influx verbinden
2. 10 Punkte mit Measurement `bddbench_write` und einer eindeutigen `run_id` ins Bucket schreiben
3. per Flux genau diese 10 Punkte wieder lesen
4. (optional) Latenz prüfen

So können wir unsere Influx-Umgebung direkt aus dem BDD-Setup heraus testen.

---

## 1. Influx-Token im UI holen

1. In Influx links auf **Load Data** gehen
2. Tab **API Tokens**
3. Entweder bestehenden Token anklicken oder **Generate API Token → Custom API Token**
4. Für Bucket **`dsp25`** `read` **und** `write` anhaken
5. Token kopieren → später als `INFLUX_TOKEN` benutzen

> Hinweis: Der Token muss wirklich aus dem UI kommen. Der in irgendwelchen System-Env-Vars hinterlegte `INFLUX_ADMIN_TOKEN=...` wurde von Influx bei unseren Tests nicht akzeptiert.

---

## 2. Org-Namen herausfinden (empfohlen)

Mit dem Token einmal die Orgs vom Server abfragen:

```bash
curl -s -H "Authorization: Token <TOKEN_AUS_UI>" http://localhost:8086/api/v2/orgs
```

```
# 1. Repo holen
git clone -b POC https://github.com/DPS25/bddbench.git
cd bddbench/gherkin-poc

# 2. (optional) Org-Namen aus Influx holen
curl -s -H "Authorization: Token <TOKEN_AUS_UI>" http://localhost:8086/api/v2/orgs

# 3. NixOS-Shell mit Python + behave öffnen
nix-shell -p python3 python3Packages.pip python3Packages.behave python3Packages.influxdb-client

# 4. Benchmark starten
INFLUX_URL=http://localhost:8086 \
INFLUX_ORG=<ORG_AUS_INFLUX> \
INFLUX_BUCKET=dsp25 \
INFLUX_TOKEN="<TOKEN_AUS_UI>" \
behave -v --tags @influx
```
