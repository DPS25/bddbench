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
        print(f"⚠️ Skipping: Field '{kpi}' not found.")
        return

    df = df.dropna(subset=[kpi])

    # 2. Detect Version Column
    version_col = next((c for c in ["sut_version", "sut_host", "version", "host"] if c in df.columns), None)
    if not version_col:
        print(f"❌ Error: No version column found.")
        return

    # 3. Apply Categorical Ordering
    scenario_order = ["smoke", "average", "load", "stress", "soak", "spike", "breakpoint"]
    available_scenarios = [s for s in scenario_order if s in df['scenario_id'].unique()]

    df['scenario_id'] = pd.Categorical(
        df['scenario_id'],
        categories=available_scenarios,
        ordered=True
    )

    # 4. Create Labeling
    if "operation" in df.columns:
        df["group_label"] = df["operation"].astype(str) + " (" + df["scenario_id"].astype(str) + ")"
    else:
        df["group_label"] = df["scenario_id"].astype(str)

    # 5. Presentation Styling Configuration
    # 'talk' context scales up all elements (lines, points, labels) for visibility
    sns.set_context("talk", font_scale=1.2)
    sns.set_style("whitegrid")

    # 16:9 Aspect ratio is standard for digital presentations
    plt.figure(figsize=(16, 9))

    # 6. Sorting
    df = df.sort_values(by=[version_col, "scenario_id"])

    # 7. Plotting with "Presentation" enhancements
    ax = sns.barplot(
        data=df,
        x=version_col,
        y=kpi,
        hue="group_label",
        palette="viridis",
        errorbar="sd",
        capsize=.05,  # Makes error bars more visible
        edgecolor=".2"  # High contrast borders for low-res screens
    )

    # Title and Axes with large, bold fonts
    plt.title(f"{kpi.replace('_', ' ').upper()}", fontsize=32, fontweight='bold', pad=25)
    plt.xlabel("Version / Host", fontsize=24, labelpad=15)
    plt.ylabel(kpi.replace("_", " ").title(), fontsize=24, labelpad=15)

    # Tick labels (numbers on axes)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)

    # Legend optimization - moved outside to prevent overlapping data
    plt.legend(
        title="Scenario",
        title_fontsize=22,
        fontsize=18,
        bbox_to_anchor=(1.02, 1),
        loc='upper left',
        borderaxespad=0
    )

    plt.tight_layout()

    # Ensure directory exists
    if "/" in feature_name:
        os.makedirs(os.path.dirname(feature_name), exist_ok=True)

    output_path = f"{feature_name}.png"
    # Using 300 DPI for high-quality projection/sharing
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"📈 Presentation Plot Generated: {output_path}")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start time (e.g., -24h)")
    parser.add_argument("--end", required=True, help="End time (e.g., now())")
    parser.add_argument("--measurement", required=True)
    parser.add_argument("--kpi", required=True)
    parser.add_argument("--feature", required=True)
    args = parser.parse_args()

    df = fetch_data(args.start, args.end, args.measurement)

    if not df.empty and "operation" in df.columns:
        ops = df["operation"].unique()
        if len(ops) > 1:
            for op in ops:
                op_df = df[df["operation"] == op].copy()
                op_feature_name = f"{args.feature}_{op}"
                print(f"🔍 Splitting plot for operation: {op}")
                plot_version_comparison(op_df, args.kpi, op_feature_name)
        else:
            plot_version_comparison(df, args.kpi, args.feature)
    else:
        plot_version_comparison(df, args.kpi, args.feature)