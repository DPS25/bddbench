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

    # We use a broader pivot rowKey to ensure we don't lose data,
    # but keep it simple to avoid the 'column does not exist' error.
    query = f'''
    from(bucket: "{os.getenv("INFLUXDB_MAIN_BUCKET", "dsp25")}")
      |> range(start: {start}, stop: {end})
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        df = client.query_api().query_data_frame(query)
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            return pd.DataFrame()
        if isinstance(df, list):
            df = pd.concat(df, ignore_index=True)
        return df
    except Exception as e:
        print(f"⚠️ Query failed for {measurement}: {e}")
        return pd.DataFrame()
    finally:
        client.close()

def plot_version_comparison(df, kpi, feature_name):
    if df.empty:
        print(f"⚠️ No data found for {feature_name}.")
        return

    # 1. Force KPI to numeric
    if kpi in df.columns:
        df[kpi] = pd.to_numeric(df[kpi], errors='coerce')
    else:
        print(f"⚠️ Skipping: Field '{kpi}' not found in columns: {df.columns.tolist()}")
        return

    df = df.dropna(subset=[kpi])

    # 2. Detect Version Column
    version_col = next((c for c in ["sut_version", "sut_host", "version", "host"] if c in df.columns), None)
    if not version_col:
        print(f"❌ Error: No version column found in {df.columns.tolist()}")
        return

    # 3. Create a unique Hue label
    # If 'operation' exists (for User Summary), we combine it with scenario_id
    if "operation" in df.columns:
        df["group_label"] = df["operation"].astype(str) + " (" + df["scenario_id"].astype(str) + ")"
    else:
        df["group_label"] = df["scenario_id"].astype(str)

    # 4. Debugging: This will show you exactly what versions are in the data
    print(f"\n--- Multi-Version Summary for {kpi} ---")
    print(df.groupby([version_col, 'group_label'], observed=True)[kpi].mean())

    # 5. Plotting
    plt.figure(figsize=(14, 8))
    sns.set_style("whitegrid")

    # Define scenario order for consistent coloring
    order = ["smoke", "average", "load", "stress", "soak", "spike", "breakpoint"]
    if "scenario_id" in df.columns:
        df['scenario_id'] = pd.Categorical(df['scenario_id'], categories=order, ordered=True)
        df = df.sort_values([version_col, 'scenario_id'])

    ax = sns.barplot(
        data=df,
        x=version_col, # Versions on X-axis
        y=kpi,
        hue="group_label", # Combined Op + Scenario in Legend
        palette="viridis",
        errorbar="sd"
    )

    plt.title(f"Comparison: {kpi.replace('_', ' ').upper()}", fontsize=16, fontweight='bold')
    plt.xlabel("InfluxDB Version", fontsize=12)
    plt.ylabel(kpi.replace("_", " ").title(), fontsize=12)
    plt.legend(title="Operation (Scenario)", bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()
    os.makedirs(os.path.dirname(feature_name), exist_ok=True)
    plt.savefig(f"{feature_name}.png", dpi=150)
    print(f"📈 Comparison Plot Generated: {feature_name}.png")

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