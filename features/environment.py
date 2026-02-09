import logging
import os
import socket
import uuid
from pathlib import Path
from types import SimpleNamespace

from dotenv import load_dotenv

from behave.model import Feature, Scenario, Step
from behave.runner import Context
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from src.utils import _run_on_sut
from src.formatter.AnsiColorFormatter import AnsiColorFormatter

logger = logging.getLogger("bddbench.environment")

# Tags that require full InfluxDB connectivity and SUT metadata
_INFLUX_REQUIRED_TAGS = {
    "query", "write", "delete", "multibucket", "user",
    "cpu", "disk", "net", "mem", "benchmark"
}

# =====================================================================
# Helpers
# =====================================================================

def _env_truthy(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_strip(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = str(v).strip()
    return v if v != "" else None


def _feature_requires_influx(feature: Feature) -> bool:
    tags = {t.strip().lower() for t in (feature.tags or [])}
    return bool(tags & _INFLUX_REQUIRED_TAGS)


def _local_hostname() -> str:
    """
    Get a hostname without relying on external binaries (works in minimal Nix/CI envs).
    """
    h = (socket.gethostname() or "").strip()
    return h or "unknown-host"


def _sut_hostname() -> str:
    """
    Determine SUT host identifier:
      - If SUT_SSH is set: ask the remote host via SSH (using _run_on_sut)
      - Else: return local hostname without calling external `hostname`
    """
    if (os.getenv("SUT_SSH") or "").strip():
        # Remote SUT: try common commands (remote usually has them, but keep fallbacks)
        for cmd in (["hostname"], ["uname", "-n"], ["cat", "/etc/hostname"]):
            try:
                out = _run_on_sut(cmd).stdout.strip()
                if out:
                    return out
            except Exception:
                continue
        return "unknown-host"

    # Local run: do NOT call external commands
    return _local_hostname()


# =====================================================================
# Core Logic
# =====================================================================

def _load_env(context: Context):
    """
    Full initialization of InfluxDB namespaces and SUT metadata.
    """
    if not hasattr(context, "influxdb"):
        context.influxdb = SimpleNamespace()

    context.influxdb.main = SimpleNamespace()
    context.influxdb.sut = SimpleNamespace()

    # SUT Configuration
    context.influxdb.sut.url = _env_strip("INFLUXDB_SUT_URL", "http://localhost:8086")
    context.influxdb.sut.token = _env_strip("INFLUXDB_SUT_TOKEN", None)
    context.influxdb.sut.org = _env_strip("INFLUXDB_SUT_ORG", None)
    context.influxdb.sut.bucket = _env_strip("INFLUXDB_SUT_BUCKET", None)

    try:
        sut_client = InfluxDBClient(
            url=context.influxdb.sut.url,
            token=context.influxdb.sut.token,
            org=context.influxdb.sut.org
        )
        context.influxdb.sut.client = sut_client
        context.influxdb.sut.write_api = sut_client.write_api(write_options=SYNCHRONOUS)
        context.influxdb.sut.query_api = sut_client.query_api()

        # Populate Host info for tagging (NO external `hostname` for local runs)
        context.influxdb.sut.host = _sut_hostname()

        health = sut_client.health()
        context.influxdb.sut.version = health.version
        context.influxdb.sut.commit = health.commit

    except Exception as e:
        logger.warning(f"SUT InfluxDB initialization partially failed: {e}")
        # Fallback to ensure context.influxdb.sut.host exists
        if not hasattr(context.influxdb.sut, "host"):
            context.influxdb.sut.host = "unknown-host"


def _ensure_influx_stub(context: Context):
    """
    Ensures that context.influxdb.sut exists even if InfluxDB is not initialized.
    Prevents AttributeError in step definitions.
    """
    if not hasattr(context, "influxdb") or not isinstance(context.influxdb, SimpleNamespace):
        context.influxdb = SimpleNamespace()

    if not hasattr(context.influxdb, "sut"):
        context.influxdb.sut = SimpleNamespace(host="localhost", version="stub", commit="stub")

    if not hasattr(context.influxdb, "main"):
        context.influxdb.main = SimpleNamespace()


# =====================================================================
# Behave Hooks
# =====================================================================

def before_all(context: Context):
    # Pre-initialize to avoid ContextMaskWarning
    context.run_id = None

    # Setup Logging
    root = logging.getLogger("bddbench")
    context.config.logging_level = context.config.logging_level or logging.DEBUG
    os.makedirs("reports", exist_ok=True)

    formatter = AnsiColorFormatter('%(asctime)s | %(name)s | %(levelname)8s | %(message)s')
    file_handler = logging.FileHandler("reports/behave.log")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    root.setLevel(context.config.logging_level)

    # Load environment variables
    load_dotenv(Path(".env"), override=False)
    load_dotenv(Path(".env.generated"), override=False)

    context.stress = context.config.userdata.get("stress") == "true"
    context._stress_active = False


def before_feature(context: Context, feature: Feature):
    logger.debug(f"=== Starting Feature: {feature.name} ===")

    if _feature_requires_influx(feature):
        _load_env(context)
    else:
        logger.info(f"Feature '{feature.name}' skip InfluxDB init. Loading stubs.")
        _ensure_influx_stub(context)


def before_scenario(context: Context, scenario: Scenario):
    # Unique ID per scenario run
    context.run_id = uuid.uuid4().hex

    # Defensive check: Ensure sut namespace is present even in Scenario Outlines
    _ensure_influx_stub(context)
    logger.debug(f"-- Scenario: {scenario.name} (ID: {context.run_id})")


def before_step(context: Context, step: Step):
    if context.stress and "benchmark" in (step.name or "").lower():
        # Example stress logic
        _run_on_sut(["sudo", "systemctl", "start", "stress@cpu4.service"])
        context._stress_active = True


def after_step(context: Context, step: Step):
    if context.stress and getattr(context, "_stress_active", False):
        _run_on_sut(["sudo", "systemctl", "stop", "stress@cpu4.service"])
        context._stress_active = False


def after_feature(context: Context, feature: Feature):
    logger.debug(f"=== Finished Feature: {feature.name} -> {feature.status.name} ===")

