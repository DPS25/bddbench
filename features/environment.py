import sys
from pathlib import Path

# ----------------------------------------------------------------------
# Bootstrap: ensure repo root is importable so `import src.*` works
# (Fixes: ModuleNotFoundError: No module named 'src.utils')
# ----------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[1]  # repo root (features/..)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# If another installed package named "src" shadows our local one, drop it.
if "src" in sys.modules:
    _m = sys.modules.get("src")
    _m_file = getattr(_m, "__file__", None) or ""
    _m_paths = [str(p) for p in getattr(_m, "__path__", [])] if hasattr(_m, "__path__") else []
    _is_ours = (str(_REPO_ROOT) in _m_file) or any(str(_REPO_ROOT) in p for p in _m_paths)
    if not _is_ours:
        del sys.modules["src"]
# ----------------------------------------------------------------------

import logging
import os
import uuid
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
    Also try envs/<ENV_NAME>.env for per-user configs.
    """
    load_dotenv(dotenv_path=Path(".env"), override=False)
    load_dotenv(dotenv_path=Path(".env.generated"), override=True)

    env_name = (os.getenv("ENV_NAME") or "").strip()
    if env_name:
        p = Path("envs") / f"{env_name}.env"
        if p.exists():
            load_dotenv(dotenv_path=p, override=True)


def _validate_influx_auth(client: InfluxDBClient, label: str) -> None:
    """
    Validate that token/org actually works (not just ping).
    We call an auth-protected endpoint. If token is wrong for this URL/org,
    Influx returns 401.
    """
    try:
        _ = client.buckets_api().find_buckets()
    except ApiException as exc:
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
    Compatible with slightly different influxdb_client versions.
    """
    try:
        try:
            b = client.buckets_api().find_bucket_by_name(bucket_name=bucket)
        except TypeError:
            b = client.buckets_api().find_bucket_by_name(bucket)
    except Exception as exc:
        raise AssertionError(f"{label}: failed to lookup bucket '{bucket}': {exc}") from exc

    if b is None:
        raise AssertionError(f"{label}: configured bucket '{bucket}' does not exist.")


def _load_env(context: Context) -> None:
    """
    Load environment variables into the context.
    """

    context.influxdb = getattr(context, "influxdb", SimpleNamespace())
    context.influxdb.main = getattr(context.influxdb, "main", SimpleNamespace())
    context.influxdb.sut = getattr(context.influxdb, "sut", SimpleNamespace())

    context.influxdb.export_strict = _env_truthy("INFLUXDB_EXPORT_STRICT", "0")

    # ----- MAIN influx DB -----
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

    elif not main_cfg_complete:
        msg = "MAIN InfluxDB is not fully configured (INFLUXDB_MAIN_*). Export to MAIN will be skipped."
        if require_main:
            raise AssertionError(msg + " (Set INFLUXDB_REQUIRE_MAIN=0 to allow running without MAIN.)")
        logger.warning(msg)
        context.influxdb.main.client = None
        context.influxdb.main.write_api = None
        context.influxdb.main.query_api = None

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

    # ----- SUT influx DB -----
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

    logger.debug(f"SUT InfluxDB URL: {context.influxdb.sut.url}")
    logger.debug(f"SUT InfluxDB ORG: {context.influxdb.sut.org}")
    logger.debug(f"SUT InfluxDB BUCKET: {context.influxdb.sut.bucket}")


def _setup_logging(context: Context) -> None:
    """
    Setup logging for behave tests.
    """
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
    formatter = AnsiColorFormatter("%(asctime)s | %(name)s | %(levelname)8s | %(message)s")

    file_handler = logging.FileHandler(context.config.logfile)
    file_handler.setLevel(context.config.logging_level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def _ensure_influx_initialized(context: Context) -> None:

    # Network-only/local health checks: do NOT touch ANY Influx (MAIN/SUT).

    if _env_truthy("BDD_DISABLE_INFLUX", "0"):
        logger.info("BDD_DISABLE_INFLUX=1 -> skipping all InfluxDB initialization")
        return
    if getattr(context, "influxdb", None) is None:
        _load_env(context)
        return
    if getattr(getattr(context.influxdb, "sut", None), "client", None) is None:
        _load_env(context)
        return


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
            if action == "stop":
                logger.warning("Failed to stop %s: %s", unit, exc)
            else:
                raise AssertionError(f"Failed to start {unit}: {exc}") from exc

    context._stress_active = (action == "start")


# ---------------- behave hooks ----------------

def before_all(context: Context) -> None:
    """
    Setup logging & environment before all tests.
    """
    # One run_id for the whole behave run (for regression tests across commits)
    context.suite_run_id = os.getenv("BDD_RUN_ID") or uuid.uuid4().hex
    context.run_id = context.suite_run_id

    _setup_logging(context)
    logger.info("------------------------------------------------")
    logger.info("Starting BDD tests...")
    _load_dotenv_files()
    _ensure_influx_initialized(context)

    context.stress = context.config.userdata.get("stress") == "true"
    context._stress_active = False
    context._stress_presets = (context.config.userdata.get("stress_presets") or "cpu4")


def before_feature(context: Context, feature: Feature) -> None:
    logger.debug(f"=== starting feature: {feature.name} ===")


def after_feature(context: Context, feature: Feature) -> None:
    logger.debug(f"=== finished feature: {feature.name} -> {feature.status.name} ===")


def before_step(context: Context, step: Step) -> None:
    if context.stress and _should_stress_step(step):
        run_stress_logic(context, "start", step)


def after_step(context: Context, step: Step) -> None:
    if context.stress and _should_stress_step(step):
        run_stress_logic(context, "stop", step)


def before_scenario(context: Context, scenario: Scenario) -> None:
    """
    Reuse suite_run_id so all scenarios share the same run_id in this behave run.
    """
    context.run_id = getattr(context, "suite_run_id", None) or uuid.uuid4().hex
    logger.debug(f"-- starting scenario: {scenario.name}")


def after_scenario(context: Context, scenario: Scenario) -> None:
    logger.debug(f"-- finished scenario: {scenario.name} -> {scenario.status.name}")

