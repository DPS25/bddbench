#!/usr/bin/env bash
set -euo pipefail

cd /home/nixos/bddbench/gherkin-poc

VENV_PY="./.venv/bin/python"
VENV_BEHAVE="./.venv/bin/behave"

echo "[nightly] CWD: $(pwd)"
echo "[nightly] Using python: $VENV_PY"


export INFLUX_URL="http://localhost:8086"
export INFLUX_ORG="3S"
export INFLUX_BUCKET="dsp25"
export INFLUX_TOKEN='sIba9El67Uhzjnk2vGoCTO6l7Jp3b8Eu_WmEnL4UsNXiXG8wO_EyAR2mcT14vbfnLCYVzfL9FvnHKNAm7IJTkg=='


"$VENV_BEHAVE" -v --tags @influx

# 2) generate mock KPI CSV
echo "[nightly] Seeding mock KPI CSV..."
"$VENV_PY" mock_seed_kpis_local.py

# 3) single sample t-test + image（save PNG）
echo "[nightly] Single-sample t-test (config_id=v1)..."
export KPI_CSV="mock/main_kpis.csv"
export KPI_SCENARIO="write_basic"
export KPI_GROUP_TAG="config_id"
export KPI_GROUP_VAL="v1"
export KPI_FIELD="mean_ms"
export KPI_TARGET_MS="15"
export KPI_MEASUREMENT="bddbench_summary"
export KPI_PLOT_DIR="reports/plots"

"$VENV_PY" analyze_ttest_single_offline.py

# 4) tow sample t-test（v1 vs v2）
echo "[nightly] Two-sample t-test (v1 vs v2)..."
export KPI_GROUP_VAL_A="v1"
export KPI_GROUP_VAL_B="v2"

"$VENV_PY" analyze_ttest_two_sample_offline.py

echo "[nightly] Done."

