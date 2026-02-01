import os
import sys
import subprocess
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from urllib.parse import urlparse
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)
plt.style.use('bmh')

# --- DEFINED LOGICAL ORDER ---
SCENARIO_ORDER = ["smoke", "average", "load", "stress", "soak", "spike", "breakpoint"]

BASE_MAPPING = {
    "smoke": "#2ECC71", "average": "#3498DB", "load": "#3498DB",
    "stress": "#E67E22", "soak": "#9B59B6", "spike": "#E74C3C",
    "breakpoint": "#C0392B"
}


def to_single_df(raw_data):
    if raw_data is None: return pd.DataFrame()
    if isinstance(raw_data, list):
        return pd.concat(raw_data, ignore_index=True) if raw_data else pd.DataFrame()
    return raw_data


def get_sut_hostname():
    sut_url = os.getenv("INFLUXDB_SUT_URL")
    if not sut_url: return None
    target_ip = urlparse(sut_url).hostname
    try:
        result = subprocess.check_output(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3", f"nixos@{target_ip}", "hostname"],
            stderr=subprocess.STDOUT, text=True
        )
        return result.strip()
    except Exception:
        return None


def fetch_data(start, end, measurement, sut_hostname):
    url = os.getenv("INFLUXDB_MAIN_URL")
    token = os.getenv("INFLUXDB_MAIN_TOKEN")
    org = os.getenv("INFLUXDB_MAIN_ORG")
    client = InfluxDBClient(url=url, token=token, org=org)

    bench_query = f'''
    from(bucket: "dsp25")
      |> range(start: {start}, stop: {end})
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    host_filter = f'|> filter(fn: (r) => r.host == "{sut_hostname}")' if sut_hostname else ""
    res_query = f'''
    from(bucket: "telegraf")
      |> range(start: {start}, stop: {end})
      {host_filter}
      |> filter(fn: (r) => r._measurement == "influxdb_metrics" or r._measurement == "cpu" or r._measurement == "procstat")
      |> filter(fn: (r) => r._field == "usage_user" or r._field == "storage_wal_size" or r._field == "memory_rss" or r._field == "cpu_usage" or r._field == "go_goroutines")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    try:
        bench_df = to_single_df(client.query_api().query_data_frame(bench_query))
        res_df = to_single_df(client.query_api().query_data_frame(res_query))

        if not res_df.empty:
            res_df['_time'] = pd.to_datetime(res_df['_time']).dt.tz_localize(None)
            if not bench_df.empty:
                bench_df['_time'] = pd.to_datetime(bench_df['_time']).dt.tz_localize(None)
                res_df = pd.merge_asof(
                    res_df.sort_values('_time'),
                    bench_df[['_time', 'scenario_id']].sort_values('_time'),
                    on='_time', direction='nearest', tolerance=pd.Timedelta("45s")
                ).dropna(subset=['scenario_id'])

        return bench_df, res_df
    finally:
        client.close()


def generate_report(df, res_df, feature_name):
    if df.empty: return

    found_scenarios = df["scenario_id"].unique()
    plot_order = [s for s in SCENARIO_ORDER if s in found_scenarios]
    plot_order += [s for s in found_scenarios if s not in SCENARIO_ORDER]

    palette = {s: BASE_MAPPING.get(next((k for k in BASE_MAPPING if k in str(s).lower()), ""), "#95a5a6") for s in
               plot_order}

    # Setup 2x3 Grid (6 plots)
    fig, axes = plt.subplots(2, 3, figsize=(24, 14))
    axes = axes.flatten()

    def finalize_ax(ax, title, ylabel):
        ax.set_title(title, fontweight='bold', fontsize=14)
        ax.set_ylabel(ylabel)
        ax.set_ylim(0, None)

    # 1. Performance (Throughput)
    perf_col = next((c for c in ["throughput_points_per_s", "latency_s"] if c in df.columns), None)
    if perf_col:
        sns.barplot(x="scenario_id", y=perf_col, data=df, ax=axes[0], palette=palette, order=plot_order,
                    hue="scenario_id", legend=False)
        finalize_ax(axes[0], "Workload Performance", perf_col)

    # 2. Latency Stability
    stab_col = next((c for c in ["latency_avg_s", "total_avg_s"] if c in df.columns), None)
    if stab_col:
        sns.boxplot(x="scenario_id", y=stab_col, data=df, ax=axes[1], palette=palette, order=plot_order,
                    hue="scenario_id", legend=False, showfliers=False)
        p99 = df.groupby("scenario_id")[stab_col].quantile(0.99)
        for i, s in enumerate(plot_order):
            if s in p99: axes[1].plot(i, p99[s], marker='D', color='red', markersize=10)
        finalize_ax(axes[1], "Latency (Red=P99)", "Seconds")

    # 3. CPU Usage
    cpu_f = next((f for f in ['usage_user', 'cpu_usage'] if f in res_df.columns), None)
    if not res_df.empty and cpu_f:
        sns.barplot(x="scenario_id", y=cpu_f, data=res_df, ax=axes[2], palette=palette, order=plot_order,
                    hue="scenario_id", alpha=0.7)
        finalize_ax(axes[2], "SUT CPU Utilization", "CPU %")

    # 4. Memory Usage (RSS)
    mem_f = 'memory_rss' if 'memory_rss' in res_df.columns else None
    if not res_df.empty and mem_f:
        res_df["mem_gb"] = res_df[mem_f] / (1024 ** 3)
        sns.barplot(x="scenario_id", y="mem_gb", data=res_df, ax=axes[3], palette=palette, order=plot_order,
                    hue="scenario_id", alpha=0.7)
        finalize_ax(axes[3], "SUT Memory Usage", "Memory (GB)")

    # 5. Go Goroutines
    if not res_df.empty and 'go_goroutines' in res_df.columns:
        sns.lineplot(x="scenario_id", y="go_goroutines", data=res_df, ax=axes[4], marker="o", color="#8E44AD",
                     linewidth=3, errorbar=None)
        finalize_ax(axes[4], "InfluxDB Threading", "Active Goroutines")

    # 6. Database Internals (WAL Size)
    if not res_df.empty and 'storage_wal_size' in res_df.columns:
        res_df["wal_mb"] = res_df["storage_wal_size"] / (1024 ** 2)
        sns.barplot(x="scenario_id", y="wal_mb", data=res_df, ax=axes[5], palette=palette, order=plot_order,
                    hue="scenario_id", alpha=0.5)
        finalize_ax(axes[5], "Write-Ahead Log Size", "WAL Size (MB)")

    plt.suptitle(f"FEATURE REPORT: {feature_name.upper()}", fontsize=24, fontweight='bold', y=0.98)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f"{feature_name}.png", dpi=150)
    print(f"✅ Six-panel report generated: {feature_name}.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True);
    parser.add_argument("--end", required=True)
    parser.add_argument("--measurement", required=True);
    parser.add_argument("--feature", required=True)
    args = parser.parse_args()

    hn = get_sut_hostname()
    b_data, r_data = fetch_data(args.start, args.end, args.measurement, hn)
    generate_report(b_data, r_data, args.feature)