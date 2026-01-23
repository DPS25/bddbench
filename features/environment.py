import logging
import os
import uuid
from types import SimpleNamespace
from dotenv import load_dotenv
from pathlib import Path
from influxdb_client.rest import ApiException

from behave.model import Feature, Scenario, Step
from influxdb_client.client.write_api import SYNCHRONOUS
from behave.runner import Context
from influxdb_client import InfluxDBClient
from src.utils import _run_on_sut
from src.formatter.AnsiColorFormatter import AnsiColorFormatter

logger = logging.getLogger("bddbench.environment")


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------

def _env_truthy(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_strip(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = str(v).strip()
    return v if v != "" else None


def _should_stress_step(step) -> bool:
    if step is None:
        return False
    if step.keyword.strip().lower() != "when":
        return False
    name = (step.name or "").lower()
    return ("benchmark" in name) and ("i run" in name)


def _load_dotenv_files():
    """
    Load dotenv files (repo defaults + generated/secret env) into process env.
    This must NOT require any Influx settings.
    """
    load_dotenv(dotenv_path=Path(".env"), override=False)
    load_dotenv(dotenv_path=Path(".env.generated"), override=False)


def _validate_influx_auth(client: InfluxDBClient, label: str) -> None:
    try:
        _ = client.buckets_api().find_buckets()
    except ApiException as exc:
        raise AssertionError(
            f"{label} Influx auth failed: HTTP {exc.status} {exc.reason}. "
            f"Most likely token/org/url mismatch or token has insufficient permissions."
        ) from exc
    except Exception as exc:
        raise AssertionError(f"{label} Influx auth failed with unexpected error: {exc}") from exc


def _validate_bucket_exists(client: InfluxDBClient, bucket: str, label: str) -> None:
    try:
        b = client.buckets_api().find_bucket_by_name(bucket_name=bucket)
    except Exception as exc:
        raise AssertionError(f"{label}: failed to lookup bucket '{bucket}': {exc}") from exc
    if b is None:
        raise AssertionError(f"{label}: configured bucket '{bucket}' does not exist.")


# ----------------------------------------------------------------------
# Influx initialization (lazy)
# ----------------------------------------------------------------------

_INFLUX_REQUIRED_TAGS = {"query", "write", "delete", "multibucket", "user"}

def _feature_requires_influx(feature: Feature) -> bool:
    tags = {t.strip().lower() for t in (feature.tags or [])}
    return bool(tags & _INFLUX_REQUIRED_TAGS)


def _load_env(context: Context):
    """
    Initialize context.influxdb.{main,sut} clients if configured.

    NOTE:
    - MAIN is optional (export can be skipped).
    - SUT is required for influx benchmarks; if missing -> fail those features.
    """
    context.influxdb = getattr(context, "influxdb", SimpleNamespace())
    context.influxdb.main = getattr(context.influxdb, "main", SimpleNamespace())
    context.influxdb.sut = getattr(context.influxdb, "sut", SimpleNamespace())

    context.influxdb.export_strict = _env_truthy("INFLUXDB_EXPORT_STRICT", "0")

    # ----- MAIN (optional) -----
    require_main = _env_truthy("INFLUXDB_REQUIRE_MAIN", "0")
    skip_main = _env_truthy("INFLUXDB_SKIP_MAIN", "0")

    context.influxdb.main.url = _env_strip("INFLUXDB_MAIN_URL", "http://localhost:8086")
    context.influxdb.main.token = _env_strip("INFLUXDB_MAIN_TOKEN", None)
    context.influxdb.main.org = _env_strip("INFLUXDB_MAIN_ORG", None)
    context.influxdb.main.bucket = _env_strip("INFLUXDB_MAIN_BUCKET", None)

    main_cfg_complete = bool(
        (context.influxdb.main.url or "").strip()
        and (context.influxdb.main.token or "").strip()
        and (context.influxdb.main.org or "").strip()
        and (context.influxdb.main.bucket or "").strip()
    )

    if skip_main:
        logger.info("INFLUXDB_SKIP_MAIN=1 -> skipping MAIN InfluxDB initialization")
    elif not main_cfg_complete:
        msg = "MAIN InfluxDB is not fully configured (INFLUXDB_MAIN_*). Export to MAIN will be skipped."
        if require_main:
            raise AssertionError(msg + " (Set INFLUXDB_REQUIRE_MAIN=0 to allow running without MAIN.)")
        logger.warning(msg)
    else:
        try:
            context.influxdb.main.client = InfluxDBClient(
                url=context.influxdb.main.url,
                token=context.influxdb.main.token,
                org=context.influxdb.main.org,
            )
            if not context.influxdb.main.client.ping():
                raise AssertionError("Cannot reach MAIN InfluxDB endpoint (ping failed).")
            _validate_influx_auth(context.influxdb.main.client, label="MAIN")
            _validate_bucket_exists(context.influxdb.main.client, bucket=context.influxdb.main.bucket, label="MAIN")

            context.influxdb.main.write_api = context.influxdb.main.client.write_api(write_options=SYNCHRONOUS)
            context.influxdb.main.query_api = context.influxdb.main.client.query_api()
            logger.info("successfully connected and authenticated to MAIN InfluxDB")
        except Exception as exc:
            if require_main:
                raise
            logger.warning(f"MAIN InfluxDB init failed ({exc}). Export to MAIN will be skipped.")
            context.influxdb.main.client = None
            context.influxdb.main.write_api = None
            context.influxdb.main.query_api = None

    # ----- SUT (required for influx benchmarks) -----
    context.influxdb.sut.url = _env_strip("INFLUXDB_SUT_URL", "http://localhost:8086")
    context.influxdb.sut.token = _env_strip("INFLUXDB_SUT_TOKEN", None)
    context.influxdb.sut.org = _env_strip("INFLUXDB_SUT_ORG", None)
    context.influxdb.sut.bucket = _env_strip("INFLUXDB_SUT_BUCKET", None)

    if context.influxdb.sut.url is None:
        raise AssertionError("INFLUXDB_SUT_URL environment variable must be set")
    if not (context.influxdb.sut.token or "").strip():
        raise AssertionError("INFLUXDB_SUT_TOKEN environment variable must be set")
    if not (context.influxdb.sut.org or "").strip():
        raise AssertionError("INFLUXDB_SUT_ORG environment variable must be set")
    if not (context.influxdb.sut.bucket or "").strip():
        raise AssertionError("INFLUXDB_SUT_BUCKET environment variable must be set")

    context.influxdb.sut.client = InfluxDBClient(
        url=context.influxdb.sut.url,
        token=context.influxdb.sut.token,
        org=context.influxdb.sut.org,
    )
    if not context.influxdb.sut.client.ping():
        raise AssertionError("Cannot reach SUT InfluxDB endpoint (ping failed).")

    _validate_influx_auth(context.influxdb.sut.client, label="SUT")
    _validate_bucket_exists(context.influxdb.sut.client, bucket=context.influxdb.sut.bucket, label="SUT")

    context.influxdb.sut.write_api = context.influxdb.sut.client.write_api(write_options=SYNCHRONOUS)
    context.influxdb.sut.query_api = context.influxdb.sut.client.query_api()

    # optional info (used for tagging in other benchmarks)
    context.influxdb.sut.host = _run_on_sut(["hostname"]).stdout.strip()
    health = context.influxdb.sut.client.health()
    context.influxdb.sut.commit = health.commit
    context.influxdb.sut.version = health.version

    logger.info("successfully connected and authenticated to SUT InfluxDB")
    logger.debug(f"SUT InfluxDB URL: {context.influxdb.sut.url}")
    logger.debug(f"SUT InfluxDB ORG: {context.influxdb.sut.org}")
    logger.debug(f"SUT InfluxDB BUCKET: {context.influxdb.sut.bucket}")
    logger.debug(f"SUT InfluxDB commit: {context.influxdb.sut.commit}")
    logger.debug(f"SUT InfluxDB version: {context.influxdb.sut.version}")
    logger.debug(f"SUT host identifier: {context.influxdb.sut.host}")


def _setup_logging(context: Context):
    root = logging.getLogger("bddbench")
    context.config.logging_level = context.config.logging_level or logging.DEBUG
    context.config.logfile = context.config.userdata.get("logfile", None) or "reports/behave.log"
    context.config.logdir = os.path.dirname(os.path.abspath(context.config.logfile)) or os.getcwd()
    try:
        os.makedirs(context.config.logdir, exist_ok=True)
    except Exception:
        pass

    root.setLevel(context.config.logging_level)
    formatter = AnsiColorFormatter('%(asctime)s | %(name)s | %(levelname)8s | %(message)s')
    file_handler = logging.FileHandler(context.config.logfile)
    file_handler.setLevel(context.config.logging_level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def _ensure_influx_initialized(context: Context):
    if getattr(context, "_influx_initialized", False):
        return
    _load_env(context)
    context._influx_initialized = True


# ----------------------------------------------------------------------
# stress logic (unchanged)
# ----------------------------------------------------------------------

def run_stress_logic(context: Context, action: str, step: Step | None) -> None:
    if step is not None and step.keyword != "When":
        return

    raw = getattr(context, "_stress_presets", "cpu4")
    presets = (
        [p.strip() for p in raw.split(",") if p.strip()]
        if isinstance(raw, str)
        else list(raw)
    )

    if action == "start":
        if getattr(context, "_stress_active", False):
            return
    elif action == "stop":
        if not getattr(context, "_stress_active", False):
            return
    else:
        raise AssertionError(f"Invalid stress action: {action!r}")

    for preset in presets:
        unit = f"stress@{preset}.service"
        try:
            _run_on_sut(["sudo", "systemctl", action, unit])
        except Exception as exc:
            if action == "stop":
                logger.warning("Failed to stop %s: %s", unit, exc)
            else:
                raise AssertionError(f"Failed to start {unit}: {exc}") from exc

    context._stress_active = (action == "start")


# ----------------------------------------------------------------------
# behave hooks
# ----------------------------------------------------------------------

def before_all(context: Context):
    context.run_id = None
    context._influx_initialized = False

    _setup_logging(context)
    logger.info("------------------------------------------------")
    logger.info("Starting BDD tests...")
    _load_dotenv_files()

    context.stress = context.config.userdata.get("stress") == "true"
    context._stress_active = False
    context._stress_presets = (context.config.userdata.get("stress_presets") or "cpu4")


def before_feature(context: Context, feature: Feature):
    logger.debug(f"=== starting feature: {feature.name} ===")

    # Lazy init Influx only for influx benchmarks, NOT for @network VM health checks.
    if _feature_requires_influx(feature):
        _ensure_influx_initialized(context)
    else:
        logger.info("Skipping Influx init for feature '%s' (tags=%s)", feature.name, feature.tags)


def after_feature(context: Context, feature: Feature):
    logger.debug(f"=== finished feature: {feature.name} -> {feature.status.name} ===")


def before_step(context: Context, step: Step):
    if context.stress and _should_stress_step(step):
        run_stress_logic(context, "start", step)


def after_step(context: Context, step: Step):
    if context.stress and _should_stress_step(step):
        run_stress_logic(context, "stop", step)


def before_scenario(context: Context, scenario: Scenario):
    context.run_id = uuid.uuid4().hex
    logger.debug(f"-- starting scenario: {scenario.name}")


def after_scenario(context: Context, scenario: Scenario):
    logger.debug(f"-- finished scenario: {scenario.name} -> {scenario.status.name}")

