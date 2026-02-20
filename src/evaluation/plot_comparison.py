import os
import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from influxdb_client import InfluxDBClient

def fetch_data(start, end, measurement):
    client = InfluxDBClient(
        url=os.getenv("INFLUXDB_MAIN_URL"),
        token=os.getenv("INFLUXDB_MAIN_TOKEN"),
        org=os.getenv("INFLUXDB_MAIN_ORG")
    )

    query = f'''
    from(bucket: "dsp25")
      |> range(start: {start}, stop: {end})
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        df = client.query_api().query_data_frame(query)
        if isinstance(df, list):
            df = pd.concat(df, ignore_index=True)
        return df
    finally:
        client.close()

def plot_version_comparison(df, kpi, feature_name):
    if df.empty:
        print("⚠️ No data found.")
        return

    # 1. Force KPI to numeric (this fixes the "2.0" issue)
    if kpi in df.columns:
        # errors='coerce' turns non-numeric strings into NaN
        df[kpi] = pd.to_numeric(df[kpi], errors='coerce')
    else:
        print(f"⚠️ Skipping: Field '{kpi}' not found in columns.")
        return

    # 2. Drop missing data
    df = df.dropna(subset=[kpi])

    if df.empty:
        print(f"⚠️ Skipping: No valid numeric data for '{kpi}'.")
        return

    # 3. Detect X-axis column
    version_col = next((c for c in ["sut_version", "sut_host", "version", "host"] if c in df.columns), None)
    if not version_col:
        print(f"❌ Error: No version column found.")
        return

    # Debug: Print the actual values to the console so you can see them
    print(f"\n--- Data Summary for {kpi} ---")
    print(df.groupby([version_col, 'scenario_id'], observed=True)[kpi].mean())

    # 4. Plotting
    plt.figure(figsize=(14, 8))
    sns.set_style("whitegrid")

    order = ["smoke", "average", "load", "stress", "soak", "spike", "breakpoint"]
    df['scenario_id'] = pd.Categorical(df['scenario_id'], categories=order, ordered=True)

    ax = sns.barplot(
        data=df,
        x=version_col,
        y=kpi,
        hue="scenario_id",
        palette="viridis",
        errorbar="sd" # Shows standard deviation instead of just a count
    )

    plt.title(f"Comparison: {kpi.replace('_', ' ').upper()}", fontsize=16, fontweight='bold')
    plt.xlabel("InfluxDB Version (sut_version)", fontsize=12)
    plt.ylabel(kpi.replace("_", " ").title(), fontsize=12)
    plt.legend(title="Scenario", bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()
    plt.savefig(f"{feature_name}_comparison.png", dpi=150)
    print(f"📈 Comparison Plot Generated: {feature_name}_comparison.png")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--measurement", required=True)
    parser.add_argument("--kpi", required=True)
    parser.add_argument("--feature", required=True)
    args = parser.parse_args()

    df = fetch_data(args.start, args.end, args.measurement)
    plot_version_comparison(df, args.kpi, args.feature)