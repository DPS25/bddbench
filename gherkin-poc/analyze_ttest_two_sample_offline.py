import os, math, csv
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy import stats
from datetime import datetime


CSV_PATH    = os.getenv("KPI_CSV", "mock/main_kpis.csv")
SCENARIO    = os.getenv("KPI_SCENARIO", "write_basic")
GROUP_TAG   = os.getenv("KPI_GROUP_TAG")             
GROUP_VAL_A = os.getenv("KPI_GROUP_VAL_A")           
GROUP_VAL_B = os.getenv("KPI_GROUP_VAL_B")           
FIELD_NAME  = os.getenv("KPI_FIELD", "mean_ms")      
MEAS        = os.getenv("KPI_MEASUREMENT", "bddbench_summary")
ALPHA       = float(os.getenv("KPI_ALPHA", "0.05"))  
OUT_DIR     = Path(os.getenv("KPI_PLOT_DIR", "reports/plots"))


def load_group_values(group_val: str) -> np.ndarray:
    """
    Load all _value entries for a specific group value (e.g. config_id=v1)
    matching the given measurement, field, scenario and GROUP_TAG.
    """
    if not GROUP_TAG:
        raise RuntimeError("GROUP_TAG env var must be set for two-sample test.")

    vals = []
    with open(CSV_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("_measurement") != MEAS:
                continue
            if r.get("_field") != FIELD_NAME:
                continue
            if r.get("scenario") != SCENARIO:
                continue
            if r.get(GROUP_TAG) != group_val:
                continue
            vals.append(float(r["_value"]))
    return np.array(vals, dtype=float)


def timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not GROUP_VAL_A or not GROUP_VAL_B:
        raise RuntimeError("KPI_GROUP_VAL_A and KPI_GROUP_VAL_B must be set.")

    values_a = load_group_values(GROUP_VAL_A)
    values_b = load_group_values(GROUP_VAL_B)

    n_a = len(values_a)
    n_b = len(values_b)

    if n_a < 2 or n_b < 2:
        print(
            f"Not enough observations: "
            f"{GROUP_VAL_A} has n={n_a}, {GROUP_VAL_B} has n={n_b}. "
            f"Need at least 2 per group."
        )
        return

    mean_a = float(np.mean(values_a))
    mean_b = float(np.mean(values_b))
    sd_a   = float(np.std(values_a, ddof=1))
    sd_b   = float(np.std(values_b, ddof=1))

    # Welch's two-sample t-test (unequal variances, two-sided)
    t_stat, p_two_tailed = stats.ttest_ind(
        values_a, values_b, equal_var=False, alternative="two-sided"
    )

    # 95% confidence interval for each group's mean (Student t)
    df_a = n_a - 1
    df_b = n_b - 1
    se_a = sd_a / math.sqrt(n_a)
    se_b = sd_b / math.sqrt(n_b)

    tcrit_a = stats.t.ppf(1 - ALPHA / 2, df_a)
    tcrit_b = stats.t.ppf(1 - ALPHA / 2, df_b)

    ci_a_low, ci_a_high = mean_a - tcrit_a * se_a, mean_a + tcrit_a * se_a
    ci_b_low, ci_b_high = mean_b - tcrit_b * se_b, mean_b + tcrit_b * se_b

    # CI for difference in means (A - B) using Welch approximation
    diff_mean = mean_a - mean_b
    se_diff = math.sqrt(sd_a**2 / n_a + sd_b**2 / n_b)

    df_num = (sd_a**2 / n_a + sd_b**2 / n_b) ** 2
    df_den = ((sd_a**2 / n_a) ** 2) / (n_a - 1) + ((sd_b**2 / n_b) ** 2) / (n_b - 1)
    df_diff = df_num / df_den
    tcrit_diff = stats.t.ppf(1 - ALPHA / 2, df_diff)
    diff_ci_low, diff_ci_high = diff_mean - tcrit_diff * se_diff, diff_mean + tcrit_diff * se_diff

    print(f"Scenario: {SCENARIO}, field={FIELD_NAME}, measurement={MEAS}")
    print(f"Group tag: {GROUP_TAG}")
    print(f"Group A: {GROUP_VAL_A}, n={n_a}, mean={mean_a:.3f} ms, sd={sd_a:.3f} ms")
    print(f"Group B: {GROUP_VAL_B}, n={n_b}, mean={mean_b:.3f} ms, sd={sd_b:.3f} ms")
    print()
    print("Welch two-sample t-test (A - B):")
    print(f"t={t_stat:.3f}, p(two)={p_two_tailed:.4g}")
    print(f"Mean difference A-B = {diff_mean:.3f} ms")
    print(f"95% CI for A mean: [{ci_a_low:.3f}, {ci_a_high:.3f}] ms")
    print(f"95% CI for B mean: [{ci_b_low:.3f}, {ci_b_high:.3f}] ms")
    print(f"95% CI for difference (A-B): [{diff_ci_low:.3f}, {diff_ci_high:.3f}] ms")

    # filename 
    base_name = f"{SCENARIO}_{GROUP_TAG}-{GROUP_VAL_A}_vs_{GROUP_VAL_B}_{FIELD_NAME}"

    # --- 1) Histograms of the two groups ---
    plt.figure()
    plt.title(f"Per-run {FIELD_NAME} (ms) by {GROUP_TAG} for scenario={SCENARIO}")
    bins = 10
    plt.hist(values_a, bins=bins, alpha=0.6, label=f"{GROUP_VAL_A}")
    plt.hist(values_b, bins=bins, alpha=0.6, label=f"{GROUP_VAL_B}")
    plt.xlabel(f"{FIELD_NAME} (ms)")
    plt.ylabel("Count")
    plt.legend()
    plt.grid(True, axis='y', alpha=0.3)


    ts = timestamp()
    hist_path = OUT_DIR / f"{ts}_{base_name}_hist.png"
    plt.tight_layout()
    plt.savefig(hist_path)
    plt.close()
    print(f"[two-sample] Saved histogram to: {hist_path}")

    # --- 2) Means + 95% CI by group ---
    plt.figure()
    plt.title(f"{FIELD_NAME} with 95% CI by {GROUP_TAG} (scenario={SCENARIO})")

    x_pos = [0, 1]
    means = [mean_a, mean_b]
    yerr_lower = [mean_a - ci_a_low, mean_b - ci_b_low]
    yerr_upper = [ci_a_high - mean_a, ci_b_high - mean_b]

    plt.errorbar(
        x_pos, means,
        yerr=[yerr_lower, yerr_upper],
        fmt='o', capsize=6, elinewidth=2, markeredgewidth=2, markersize=8
    )

    labels = [f"{GROUP_VAL_A}", f"{GROUP_VAL_B}"]
    plt.xticks(x_pos, labels)
    plt.ylabel("Latency (ms)")
    plt.grid(True, axis='y', alpha=0.3)

    all_ci_vals = [ci_a_low, ci_a_high, ci_b_low, ci_b_high]
    ymin = min(all_ci_vals)
    ymax = max(all_ci_vals)
    pad = max(0.02 * (ymax - ymin), 0.05)
    plt.ylim(ymin - pad, ymax + pad)


    ci_path = OUT_DIR / f"{ts}_{base_name}_ci.png"
    plt.tight_layout()
    plt.savefig(ci_path)
    plt.close()
    print(f"[two-sample] Saved CI plot to: {ci_path}")


if __name__ == "__main__":
    main()

