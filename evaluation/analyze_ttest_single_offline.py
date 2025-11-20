import os, math, csv
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

CSV_PATH   = os.getenv("KPI_CSV", "mock/main_kpis.csv")
SCENARIO   = os.getenv("KPI_SCENARIO", "write_basic")
GROUP_TAG  = os.getenv("KPI_GROUP_TAG")      # e.g., "config_id"
GROUP_VAL  = os.getenv("KPI_GROUP_VAL")      # e.g., "v1" (optional)
FIELD_NAME = os.getenv("KPI_FIELD", "mean_ms") # e.g., mean_ms or median_ms
TARGET_MS  = float(os.getenv("KPI_TARGET_MS", "15"))
MEAS       = os.getenv("KPI_MEASUREMENT", "bddbench_summary")

def load_values():
    vals = []
    with open(CSV_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["_measurement"] != MEAS: continue
            if r["_field"] != FIELD_NAME: continue
            if r["scenario"] != SCENARIO: continue
            if GROUP_TAG and r.get(GROUP_TAG) != GROUP_VAL: continue
            vals.append(float(r["_value"]))
    return np.array(vals, dtype=float)

def main():
    values = load_values()
    n = len(values)
    if n < 2:
        print(f"Not enough observations (n={n}). Need 2 or more per t-test.")
        return

    mean_val = float(np.mean(values))
    sd = float(np.std(values, ddof=1))

    # two-sided t-test
    t_stat, p_two_tailed = stats.ttest_1samp(values, popmean=TARGET_MS, alternative="two-sided")

    alpha = 0.05
    df = n - 1
    se = sd / math.sqrt(n)
    tcrit = stats.t.ppf(1 - alpha/2, df)
    ci_low, ci_high = mean_val - tcrit * se, mean_val + tcrit * se


    print(f"n={n}, mean={mean_val:.3f} ms, sd={sd:.3f} ms")
    print(f"t={t_stat:.3f}, p(two)={p_two_tailed:.4g}")
    print(f"95% CI: [{ci_low:.3f}, {ci_high:.3f}] ms")

    # histogram
    plt.figure()
    plt.title(f"Per-run {FIELD_NAME} (ms)")
    plt.hist(values, bins=10)
    plt.xlabel(f"{FIELD_NAME} (ms)")
    plt.ylabel("Count")
    plt.show()

    # mean + confidence interval (zoom to CI)
    plt.figure()
    plt.title(f"{FIELD_NAME} with 95% CI")
    plt.errorbar(
        [0], [mean_val],
        yerr=[[mean_val - ci_low], [ci_high - mean_val]],
        fmt='o', capsize=6, elinewidth=2, markeredgewidth=2, markersize=8
    )

    pad = max(0.02 * (ci_high - ci_low), 0.05)
    ymin = ci_low - pad
    ymax = ci_high + pad

    if ymin <= TARGET_MS <= ymax:
        plt.axhline(TARGET_MS, linestyle='--')

    plt.ylim(ymin, ymax)
    plt.xticks([0], [f"{SCENARIO}"])
    plt.ylabel("Latency (ms)")
    plt.grid(True, axis='y', alpha=0.3)
    plt.show()

if __name__ == "__main__":
    main()
