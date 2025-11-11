import os
import time
import uuid
import json
import math
import statistics
from pathlib import Path
from typing import List

from behave import given, when, then
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def _ensure_client_on_context(context):
    """
    Stellt sicher, dass auf dem Behave-Context ein InfluxDB-Client, Write-API
    und Organisation vorhanden ist. Erwartet Umgebungsvariablen:
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG
    """
    if getattr(context, "influx_client", None) and getattr(context, "write_api", None):
        return

    url = os.getenv("INFLUX_URL")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("INFLUX_ORG")
    if not url or not token or not org:
        raise RuntimeError(
            "INFLUX_URL / INFLUX_TOKEN / INFLUX_ORG müssen als Environment Variablen gesetzt sein."
        )

    client = InfluxDBClient(url=url, token=token, org=org)
    context.influx_client = client
    context.influx_org = org
    context.write_api = client.write_api(write_options=SYNCHRONOUS)
    context.query_api = client.query_api()


def _median_ms(values_ms: List[float]) -> float:
    if not values_ms:
        return float("nan")
    return statistics.median(values_ms)


def _percentile_ms(values_ms: List[float], p: float) -> float:
    """
    Einfache Perzentil-Berechnung (p in [0,100])
    """
    if not values_ms:
        return float("nan")
    if p <= 0:
        return min(values_ms)
    if p >= 100:
        return max(values_ms)
    sorted_vals = sorted(values_ms)
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[int(f)] * (c - k)
    d1 = sorted_vals[int(c)] * (k - f)
    return d0 + d1


#@given("an InfluxDB v2 endpoint is configured from environment")
@given("an InfluxDB endpoint is configured")
def step_config_influx(context):
    """
    Akzeptiert beide Formulierungen (kompatibel zu deinem bestehenden POC).
    """
    _ensure_client_on_context(context)
    # Hilfswerte initialisieren
    if not hasattr(context, "run_id"):
        context.run_id = str(uuid.uuid4())
    context.write_latencies_ms = []


@given('a bucket "{bucket}" is defined')
def step_bucket_is_defined(context, bucket):
    """
    Setzt den Ziel-Bucket im Context. Existenz wird nicht erstellt, sondern vorausgesetzt.
    """
    context.influx_bucket = bucket


@when("I write {rate:d} points per second for {duration:d} seconds")
def step_write_at_rate(context, rate: int, duration: int):
    """
    Schreibt SYNCHRONOUS genau 'rate' Punkte pro Sekunde über 'duration' Sekunden.
    Für jeden einzelnen Write wird die Latenz gemessen (Start->Response).
    """
    _ensure_client_on_context(context)
    bucket = getattr(context, "influx_bucket", None)
    if not bucket:
        raise RuntimeError("Kein Bucket gesetzt. Nutze z.B.: And a bucket \"bdbench\" is defined")

    # Messungsname 
    measurement = "bddbench_latency"

    latencies_ms: List[float] = []
    total_points = rate * duration
    started_at = time.perf_counter()

    for s in range(duration):
        sec_window_start = time.perf_counter()
        for i in range(rate):
            # Zielzeitpunkt für dieses Event innerhalb der aktuellen Sekunde
            target_ts = sec_window_start + (i / max(rate, 1))
            now = time.perf_counter()
            sleep_for = target_ts - now
            if sleep_for > 0:
                time.sleep(sleep_for)

            p = Point(measurement) \
                .tag("run_id", context.run_id) \
                .field("value", (s * rate) + i) \
                .time(time.time_ns(), WritePrecision.NS)

            t0 = time.perf_counter()
            context.write_api.write(
                bucket=bucket,
                org=context.influx_org,
                record=p
            )
            t1 = time.perf_counter()
            lat_ms = (t1 - t0) * 1000.0
            latencies_ms.append(lat_ms)
          
        elapsed_in_window = time.perf_counter() - sec_window_start
        if elapsed_in_window < 1.0:
            time.sleep(1.0 - elapsed_in_window)

    duration_s = time.perf_counter() - started_at

    context.benchmark_meta = {
        "rate": rate,
        "duration_s": duration,
        "actual_duration_s": duration_s,
        "total_points": total_points,
        "bucket": bucket,
        "run_id": context.run_id,
        "measurement": measurement,
        "started_epoch_s": time.time() - duration_s,
    }
    context.write_latencies_ms = latencies_ms


@then("the median latency shall be <= {max_p50_ms:d} ms")
def step_check_median_latency(context, max_p50_ms: int):
    lats = getattr(context, "write_latencies_ms", [])
    if not lats:
        raise AssertionError("Es wurden keine Schreiblatenzen gemessen.")

    p50 = _median_ms(lats)
    if p50 > max_p50_ms:
        raise AssertionError(
            f"Median-Latenz {p50:.2f} ms überschreitet Schwellwert {max_p50_ms} ms"
        )


@then('I store the benchmark result as "{path_template}"')
def step_store_result(context, path_template: str):
    """
    Persistiert die Benchmark-Metriken als JSON.
    Unterstützt Platzhalter <id> im Pfad, falls ein Scenario-Outline-Wert verwendet wird.
    """
    if "<id>" in path_template:
        example_id = getattr(context, "example_id", None)
        if not example_id:
            example_id = getattr(context, "run_id", "run")[:8]
        path_template = path_template.replace("<id>", str(example_id))

    out_path = Path(path_template)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lats = getattr(context, "write_latencies_ms", [])
    meta = getattr(context, "benchmark_meta", {})

    result = {
        "meta": meta,
        "count": len(lats),
        "latency_ms": {
            "min": min(lats) if lats else None,
            "p50": _median_ms(lats),
            "p90": _percentile_ms(lats, 90),
            "p95": _percentile_ms(lats, 95),
            "p99": _percentile_ms(lats, 99),
            "max": max(lats) if lats else None,
            "avg": statistics.mean(lats) if lats else None,
        },
        "raw_latencies_ms": lats, 
        "created_at_epoch_s": time.time(),
    }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
