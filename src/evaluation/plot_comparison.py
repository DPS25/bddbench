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

    # Note: We include 'operation' in the pivot to distinguish
    # between 'me' and 'lifecycle_crud' in the summary table
    query = f'''
    from(bucket: "{os.getenv("INFLUXDB_MAIN_BUCKET", "dsp25")}")
      |> range(start: {start}, stop: {end})
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> pivot(rowKey:["_time", "operation", "scenario_id"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        df = client.query_api().query_data_frame(query)
        if isinstance(df, list):
            df = pd.concat(df, ignore_index=True)
        return df
    finally:
        client.close()


def plot_version_comparison(df, kpi, feature_name):
    if df is None or df.empty:
        print(f"⚠️ No data found for {feature_name}.")
        return

    # Ensure KPI is numeric
    df[kpi] = pd.to_numeric(df[kpi], errors='coerce')
    df = df.dropna(subset=[kpi])

    # Find version column
    version_col = next((c for c in ["sut_version", "sut_host", "version"] if c in df.columns), None)
    if not version_col:
        # Fallback: if no version tag, we use the run_id or time
        df['version_label'] = "Run " + df['run_id'].str[:4]
        version_col = 'version_label'

    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")

    # If it's the User Summary, we might have multiple operations (me vs crud)
    # Use 'operation' as a sub-hue if it exists
    hue_col = "scenario_id"
    if "operation" in df.columns and df["operation"].nunique() > 1:
        df["group"] = df["operation"] + " (" + df["scenario_id"].astype(str) + ")"
        hue_col = "group"

    ax = sns.barplot(
        data=df,
        x=version_col,
        y=kpi,
        hue=hue_col,
        palette="magma",
        errorbar="sd"
    )

    plt.title(f"User Benchmark: {kpi.replace('_', ' ').upper()}", fontsize=14)
    plt.ylabel(kpi.replace("_", " ").title())
    plt.xticks(rotation=15)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()
    plt.savefig(f"{feature_name}.png", dpi=150)
    print(f"📈 Plot saved: {feature_name}.png")


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