# features/environment.py
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

logger = logging.getLogger("bddbench.environment")


# ---------------------------
# Helpers
# ---------------------------

def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise AssertionError(f"{name} environment variable must be set")
    return v


def _api_exc_to_msg(prefix: str, exc: ApiException) -> str:
    # ApiException may contain status/reason/body
    status = getattr(exc, "status", None)
    reason = getattr(exc, "reason", None)
    body = getattr(exc, "body", None)
    parts = [prefix]
    if status is not None:
        parts.append(f"HTTP {status}")
    if reason:
        parts.append(str(reason))
    if body:
        parts.append(str(body))
    return " - ".join(parts)


# ---------------------------
# Data models
# ---------------------------

@dataclass
class InfluxEndpoint:
    url: str
    token: str
    org: str
    bucket: str
    client: Optional[InfluxDBClient] = None
    write_api: Optional[object] = None  # influx write api


# ---------------------------
# Init logic
# ---------------------------

def _init_influx_endpoint(
    *,
    name: str,
    url: str,
    token: str,
    org: str,
    bucket: str,
    create_write_api: bool,
) -> InfluxEndpoint:
    """
    Connect to InfluxDB and validate that the configured bucket exists.
    Raises on any failure.
    """
    ep = InfluxEndpoint(url=url, token=token, org=org, bucket=bucket)

    client = InfluxDBClient(url=url, token=token, org=org)

    # 1) Basic reachability/auth check
    try:
        # ping() returns bool, but may raise in some setups; be defensive
        ok = client.ping()
        if ok is False:
            raise RuntimeError(f"{name}: ping() returned False")
    except ApiException as exc:
        client.close()
        raise RuntimeError(_api_exc_to_msg(f"{name}: ping failed", exc)) from exc
    except Exception as exc:
        client.close()
        raise

    # 2) Bucket existence check
    try:
        b = client.buckets_api().find_bucket_by_name(bucket)
        if b is None:
            raise RuntimeError(f"{name}: configured bucket '{bucket}' does not exist.")
    except ApiException as exc:
        client.close()
        raise RuntimeError(_api_exc_to_msg(f"{name}: bucket lookup failed", exc)) from exc
    except Exception:
        client.close()
        raise

    ep.client = client
    if create_write_api:
        ep.write_api = client.write_api(write_options=SYNCHRONOUS)

    return ep


def _setup_logging() -> None:
    """
    Log to reports/behave.log with the same style you already see.
    """
    log_dir = Path("reports")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "behave.log"

    # Avoid duplicate handlers if behave reloads
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    level_name = os.getenv("BDD_LOG_LEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.DEBUG)

    fmt = "%(asctime)s | %(name)s | %(levelname)8s | %(message)s"

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt))

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(logging.Formatter(fmt))

    root.setLevel(level)
    root.addHandler(fh)
    root.addHandler(sh)


# ---------------------------
# Behave hooks
# ---------------------------

def before_all(context):
    _setup_logging()

    logger.info("------------------------------------------------")
    logger.info("Starting BDD tests...")

    # Export strict semantics (used by steps)
    export_strict = _env_bool("INFLUXDB_EXPORT_STRICT", "0")

    # Create container on context
    if not hasattr(context, "influxdb"):
        context.influxdb = SimpleNamespace()

    context.influxdb.export_strict = export_strict

    # suite_run_id for tagging
    context.suite_run_id = uuid.uuid4().hex
    logger.info(f"suite_run_id={context.suite_run_id}")

    # ------------------------
    # SUT (required)
    # ------------------------
    sut_url = _require_env("INFLUXDB_SUT_URL")
    sut_token = _require_env("INFLUXDB_SUT_TOKEN")
    sut_org = _require_env("INFLUXDB_SUT_ORG")
    sut_bucket = _require_env("INFLUXDB_SUT_BUCKET")

    try:
        sut = _init_influx_endpoint(
            name="SUT InfluxDB",
            url=sut_url,
            token=sut_token,
            org=sut_org,
            bucket=sut_bucket,
            create_write_api=False,  # benchmark uses its own client in steps
        )
        context.influxdb.sut = sut
        logger.info("successfully connected and authenticated to SUT InfluxDB")
        logger.debug(f"SUT InfluxDB URL: {sut.url}")
        logger.debug(f"SUT InfluxDB ORG: {sut.org}")
        logger.debug(f"SUT InfluxDB BUCKET: {sut.bucket}")
    except Exception as exc:
        # SUT must be available
        raise AssertionError(f"SUT InfluxDB init failed: {exc}") from exc

    # ------------------------
    # MAIN (optional unless strict)
    # ------------------------
    main_url = os.getenv("INFLUXDB_MAIN_URL")
    main_token = os.getenv("INFLUXDB_MAIN_TOKEN")
    main_org = os.getenv("INFLUXDB_MAIN_ORG")
    main_bucket = os.getenv("INFLUXDB_MAIN_BUCKET")

    main_configured = all([main_url, main_token, main_org, main_bucket])

    if not main_configured:
        msg = "MAIN InfluxDB not fully configured (missing INFLUXDB_MAIN_*)."
        if export_strict:
            raise AssertionError(msg + " INFLUXDB_EXPORT_STRICT=1 -> aborting.")
        logger.warning(msg + " Export to MAIN will be skipped.")
        context.influxdb.main = None
        return

    try:
        main = _init_influx_endpoint(
            name="MAIN InfluxDB",
            url=str(main_url),
            token=str(main_token),
            org=str(main_org),
            bucket=str(main_bucket),
            create_write_api=True,
        )
        context.influxdb.main = main

        # Compatibility: some utils may look for these
        context.influxdb.main_client = main.client
        context.influxdb.main_write_api = main.write_api

        logger.info("successfully connected and authenticated to MAIN InfluxDB")
    except Exception as exc:
        msg = f"MAIN InfluxDB init failed ({exc})."
        if export_strict:
            # ✅ strict=1 -> FAIL PIPELINE
            raise AssertionError(msg + " INFLUXDB_EXPORT_STRICT=1 -> aborting.") from exc

        # non-strict -> warn + disable MAIN export
        logger.warning(msg + " Export to MAIN will be skipped.")
        context.influxdb.main = None
        context.influxdb.main_client = None
        context.influxdb.main_write_api = None


def before_scenario(context, scenario):
    # scenario_run_id for tagging (your base point builder will pick this up)
    context.run_id = uuid.uuid4().hex


def after_all(context):
    # Close any clients we created
    try:
        main = getattr(getattr(context, "influxdb", None), "main", None)
        if main and getattr(main, "client", None):
            main.client.close()
    except Exception:
        pass

    try:
        sut = getattr(getattr(context, "influxdb", None), "sut", None)
        if sut and getattr(sut, "client", None):
            sut.client.close()
    except Exception:
        pass

