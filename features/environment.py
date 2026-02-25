import logging
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

from dotenv import load_dotenv
from influxdb_client.rest import ApiException

from behave.model import Feature, Scenario, Step
from behave.runner import Context
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient
from src.utils import _run_on_sut
from src.formatter.AnsiColorFormatter import AnsiColorFormatter

logger = logging.getLogger("bddbench.environment")

_INFLUX_REQUIRED_TAGS = {
    "query", "write", "delete", "multibucket", "user",
    "cpu", "disk", "net", "network", "mem", "memory", "storage",
    "benchmark",
}

def _env_truthy(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def _env_strip(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = str(v).strip()
    return v if v != "" else None

def _should_stress_step(step: Step | None) -> bool:
    if step is None:
        return False
    if step.keyword.strip().lower() != "when":
        return False
    name = (step.name or "").lower()
    return ("benchmark" in name) and ("i run" in name)

def _load_dotenv_files() -> None:
    """
    Load dotenv files (repo defaults + generated/secret env) into process env.
    This must NOT require any Influx settings.
    """
    load_dotenv(dotenv_path=Path(".env"), override=False)
    load_dotenv(dotenv_path=Path(".env.generated"), override=False)


def _validate_influx_auth(client: InfluxDBClient, label: str) -> None:
    """
    Validate that token/org actually works (not just ping).
    We call an auth-protected endpoint. If token is wrong for this URL/org,
    Influx returns 401.
    """
    try:
        # Buckets listing is auth protected; enough to prove token works.
        _ = client.buckets_api().find_buckets()
    except ApiException as exc:
        # Provide a clean, actionable message
        raise AssertionError(
            f"{label} Influx auth failed: HTTP {exc.status} {exc.reason}. "
            f"Most likely token/org/url mismatch or token has insufficient permissions."
        ) from exc
    except Exception as exc:
        raise AssertionError(
            f"{label} Influx auth failed with unexpected error: {exc}"
        ) from exc

def _validate_bucket_exists(client: InfluxDBClient, bucket: str, label: str) -> None:
    """
    Optional: ensure the configured bucket exists. This prevents confusion later.
    """
    try:
        b = client.buckets_api().find_bucket_by_name(bucket_name=bucket)
    except Exception as exc:
        raise AssertionError(f"{label}: failed to lookup bucket '{bucket}': {exc}") from exc
    if b is None:
        raise AssertionError(f"{label}: configured bucket '{bucket}' does not exist.")

def _collect_feature_and_scenario_tags(feature: Feature) -> set[str]:
    """
    Behave tags may live on:
      - feature level (@tag above Feature)
      - scenario level (@tag above Scenario/Scenario Outline)
      - example level (tags above Examples:)
    We must consider scenario/example tags too
    """
    tags: set[str] = set()
    tags |= {t.strip().lower() for t in (feature.tags or [])}

    for sc in getattr(feature, "scenarios", []) or []:
        tags |= {t.strip().lower() for t in (getattr(sc, "tags", None) or [])}
        for ex in getattr(sc, "examples", []) or []:
            tags |= {t.strip().lower() for t in (getattr(ex, "tags", None) or [])}

    return tags

def _feature_requires_influx(feature: Feature) -> bool:
    tags = _collect_feature_and_scenario_tags(feature)
    return bool(tags & _INFLUX_REQUIRED_TAGS)

def _ensure_namespaces(context: Context) -> None:
    context.influxdb = getattr(context, "influxdb", SimpleNamespace())
    context.influxdb.main = getattr(context.influxdb, "main", SimpleNamespace())
    context.influxdb.sut = getattr(context.influxdb, "sut", SimpleNamespace())

def _ensure_influx_stub(context: Context) -> None:
    """
    Ensures that context.influxdb.sut exists even if InfluxDB is not initialized.
    Prevents AttributeError in step definitions.
    Host is derived from SUT_SSH if possible.
    """
    _ensure_namespaces(context)

    # Ensure SUT host identifier exists for tagging/reports
    if not getattr(context.influxdb.sut, "host", None):
        try:
            context.influxdb.sut.host = _run_on_sut(["hostname"]).stdout.strip() or "unknown-host"
        except Exception:
            context.influxdb.sut.host = "unknown-host"

    context.influxdb.sut.version = getattr(context.influxdb.sut, "version", "stub")
    context.influxdb.sut.commit = getattr(context.influxdb.sut, "commit", "stub")

def _init_main_influx(context: Context) -> None:
    """
    MAIN Influx init
    """
    _ensure_namespaces(context)

    context.influxdb.export_strict = _env_truthy("INFLUXDB_EXPORT_STRICT", "0")

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
        context.influxdb.main.client = None
        context.influxdb.main.write_api = None
        context.influxdb.main.query_api = None
        return

    if not main_cfg_complete:
        msg = "MAIN InfluxDB is not fully configured (INFLUXDB_MAIN_*). Export to MAIN will be skipped."
        if require_main:
            raise AssertionError(msg + " (Set INFLUXDB_REQUIRE_MAIN=0 to allow running without MAIN.)")
        logger.warning(msg)
        context.influxdb.main.client = None
        context.influxdb.main.write_api = None
        context.influxdb.main.query_api = None
        return

    try:
        context.influxdb.main.client = InfluxDBClient(
            url=context.influxdb.main.url,
            token=context.influxdb.main.token,
            org=context.influxdb.main.org,
        )

        if not context.influxdb.main.client.ping():
            raise AssertionError("Cannot reach MAIN InfluxDB endpoint (ping failed).")

        _validate_influx_auth(context.influxdb.main.client, label="MAIN")
        _validate_bucket_exists(
            context.influxdb.main.client,
            bucket=context.influxdb.main.bucket,
            label="MAIN",
        )

        context.influxdb.main.write_api = context.influxdb.main.client.write_api(
            write_options=SYNCHRONOUS
        )
        context.influxdb.main.query_api = context.influxdb.main.client.query_api()

        logger.info("successfully connected and authenticated to MAIN InfluxDB")
    except Exception as exc:
        if require_main:
            raise
        logger.warning(f"MAIN InfluxDB init failed ({exc}). Export to MAIN will be skipped.")
        context.influxdb.main.client = None
        context.influxdb.main.write_api = None
        context.influxdb.main.query_api = None

def _init_sut_influx(context: Context) -> None:
    """
    SUT Influx init
    """
    _ensure_namespaces(context)

    context.influxdb.sut.url = _env_strip("INFLUXDB_SUT_URL", "http://localhost:8086")
    context.influxdb.sut.token = _env_strip("INFLUXDB_SUT_TOKEN", None)
    context.influxdb.sut.org = _env_strip("INFLUXDB_SUT_ORG", None)
    context.influxdb.sut.bucket = _env_strip("INFLUXDB_SUT_BUCKET", None)

    if context.influxdb.sut.url is None:
        text = "INFLUXDB_SUT_URL environment variable must be set"
        logger.error(text)
        raise AssertionError(text)
    if not (context.influxdb.sut.token or "").strip():
        text = "INFLUXDB_SUT_TOKEN environment variable must be set"
        logger.error(text)
        raise AssertionError(text)
    if not (context.influxdb.sut.org or "").strip():
        text = "INFLUXDB_SUT_ORG environment variable must be set"
        logger.error(text)
        raise AssertionError(text)
    if not (context.influxdb.sut.bucket or "").strip():
        text = "INFLUXDB_SUT_BUCKET environment variable must be set"
        logger.error(text)
        raise AssertionError(text)

    context.influxdb.sut.client = InfluxDBClient(
        url=context.influxdb.sut.url,
        token=context.influxdb.sut.token,
        org=context.influxdb.sut.org,
    )
    if not context.influxdb.sut.client.ping():
        raise AssertionError("Cannot reach SUT InfluxDB endpoint (ping failed).")

    _validate_influx_auth(context.influxdb.sut.client, label="SUT")
    _validate_bucket_exists(
        context.influxdb.sut.client,
        bucket=context.influxdb.sut.bucket,
        label="SUT",
    )
    logger.info("successfully connected and authenticated to SUT InfluxDB")

    context.influxdb.sut.write_api = context.influxdb.sut.client.write_api(
        write_options=SYNCHRONOUS
    )
    context.influxdb.sut.query_api = context.influxdb.sut.client.query_api()

    # SUT metadata
    context.influxdb.sut.host = _run_on_sut(["hostname"]).stdout.strip()
    health = context.influxdb.sut.client.health()
    context.influxdb.sut.commit = health.commit
    context.influxdb.sut.version = health.version

    logger.debug(f"SUT InfluxDB URL: {context.influxdb.sut.url}")
    logger.debug(f"SUT InfluxDB ORG: {context.influxdb.sut.org}")
    logger.debug(f"SUT InfluxDB BUCKET: {context.influxdb.sut.bucket}")
    logger.debug(f"SUT InfluxDB commit: {context.influxdb.sut.commit}")
    logger.debug(f"SUT InfluxDB version: {context.influxdb.sut.version}")
    logger.debug(f"SUT host identifier: {context.influxdb.sut.host}")

def _ensure_influx_initialized(context: Context) -> None:
    """
    Ensure at least SUT is initialized once.
    MAIN is optional but should be attempted once (for exporters).
    """
    _ensure_namespaces(context)

    # Always attempt MAIN, but only once per process.
    if getattr(getattr(context.influxdb, "main", None), "client", "UNSET") == "UNSET":
        _init_main_influx(context)

    # Ensure SUT client exists when needed.
    if getattr(getattr(context.influxdb, "sut", None), "client", None) is None:
        _init_sut_influx(context)

def run_stress_logic(context: Context, action: str, step: Step | None) -> None:
    """
    Start/stop stress-ng systemd presets on the SUT.
    Uses SUT_SSH via src.utils._run_on_sut().
    """
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
            # start must fail the run; stop should not block cleanup
            if action == "stop":
                logger.warning("Failed to stop %s: %s", unit, exc)
            else:
                raise AssertionError(f"Failed to start {unit}: {exc}") from exc

    context._stress_active = (action == "start")

def _setup_logging(context: Context) -> None:
    root = logging.getLogger("bddbench")
    context.config.logging_level = context.config.logging_level or logging.DEBUG
    context.config.logfile = (
        context.config.userdata.get("logfile", None) or "reports/behave.log"
    )
    context.config.logdir = (
        os.path.dirname(os.path.abspath(context.config.logfile)) or os.getcwd()
    )
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


def before_all(context: Context) -> None:
    context.run_id = None

    _setup_logging(context)
    logger.info("------------------------------------------------")
    logger.info("Starting BDD tests...")

    _load_dotenv_files()

    _ensure_influx_stub(context)
    _init_main_influx(context)

    _ensure_influx_initialized(context)

    context.stress = (context.config.userdata.get("stress") == "true")
    context._stress_active = False
    context._stress_presets = (context.config.userdata.get("stress_presets") or "cpu4")

def before_feature(context: Context, feature: Feature) -> None:
    logger.debug(f"=== starting feature: {feature.name} ===")

    if _feature_requires_influx(feature):
        _ensure_influx_initialized(context)
    else:
        _ensure_influx_stub(context)

def after_feature(context: Context, feature: Feature) -> None:
    logger.debug(f"=== finished feature: {feature.name} -> {feature.status.name} ===")

def before_scenario(context: Context, scenario: Scenario) -> None:
    context.run_id = uuid.uuid4().hex
    logger.debug(f"-- starting scenario: {scenario.name}")

    _ensure_influx_stub(context)

def after_scenario(context: Context, scenario: Scenario) -> None:
    logger.debug(f"-- finished scenario: {scenario.name} -> {scenario.status.name}")

def before_step(context: Context, step: Step) -> None:
    if context.stress and _should_stress_step(step):
        run_stress_logic(context, "start", step)


def after_step(context: Context, step: Step) -> None:
    if context.stress and _should_stress_step(step):
        run_stress_logic(context, "stop", step)
