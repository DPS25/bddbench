import logging, os
from types import SimpleNamespace
from dotenv import load_dotenv
from pathlib import Path

from behave.model import Feature, Scenario, Step
from influxdb_client.client.write_api import SYNCHRONOUS
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision

from src.formatter.AnsiColorFormatter import AnsiColorFormatter

logger = logging.getLogger("bddbench.environment")

def _load_dotenv_files():
    """
    Load dotenv files (repo defaults + generated/secret env) into process env.
    This must NOT require any Influx settings.
    """
    load_dotenv(dotenv_path=Path(".env"), override=False)
    load_dotenv(dotenv_path=Path(".env.generated"), override=False)
    
def _load_env(context: Context):
    """
    Load environment variables into the context.
    :param context:
    :return:
    """

    # dotenv files are loaded in before_all(). Keep this function focused on Influx init.

    context.influxdb = getattr(context, "influxdb", SimpleNamespace())
    context.influxdb.main = getattr(context.influxdb, "main", SimpleNamespace())
    context.influxdb.sut = getattr(context.influxdb, "sut", SimpleNamespace())
    context.influxdb.main.url = None
    context.influxdb.main.token = None
    context.influxdb.main.org = None
    context.influxdb.main.bucket = None
    context.influxdb.main.client = None
    context.influxdb.main.write_api = None
    context.influxdb.main.query_api = None
    context.influxdb.sut.url = None
    context.influxdb.sut.token = None
    context.influxdb.sut.org = None
    context.influxdb.sut.bucket = None
    context.influxdb.sut.client = None
    context.influxdb.sut.write_api = None
    context.influxdb.sut.query_api = None

    # ----- main influx DB -----
    context.influxdb.main.url = os.getenv("INFLUXDB_MAIN_URL", "http://localhost:8086")
    context.influxdb.main.token = os.getenv("INFLUXDB_MAIN_TOKEN", None)
    context.influxdb.main.org = os.getenv("INFLUXDB_MAIN_ORG", None)
    context.influxdb.main.bucket = os.getenv("INFLUXDB_MAIN_BUCKET", None)

    if context.influxdb.main.url is None:
        text = "INFLUXDB_MAIN_URL environment variable must be set"
        logger.error(text)
        raise AssertionError(text)
    if not (context.influxdb.main.token or "").strip():
        text = "INFLUXDB_MAIN_TOKEN environment variable must be set"
        logger.error(text)
        raise AssertionError(text)
    if not (context.influxdb.main.org or "").strip():
        text = "INFLUXDB_MAIN_ORG environment variable must be set"
        logger.error(text)
        raise AssertionError(text)
    if not (context.influxdb.main.bucket or "").strip():
        text = "INFLUXDB_MAIN_BUCKET environment variable must be set"
        logger.error(text)
        raise AssertionError(text)

    context.influxdb.main.client = InfluxDBClient(
        url=context.influxdb.main.url,
        token=context.influxdb.main.token,
        org=context.influxdb.main.org,
    )
    if not context.influxdb.main.client.ping():
        text = "Cannot reach main INfluxDB endpoint"
        logger.error(text)
        raise AssertionError(text)

    logger.info(
        "successfully connected to main InfluxDB endpoint"
    )

    context.influxdb.main.write_api = context.influxdb.main.client.write_api(
        write_options=SYNCHRONOUS
    )
    context.influxdb.main.query_api = context.influxdb.main.client.query_api()
    # ----- SUT influx DB -----
    context.influxdb.sut.url = os.getenv("INFLUXDB_SUT_URL", "http://localhost:8086")
    context.influxdb.sut.token = os.getenv("INFLUXDB_SUT_TOKEN", None)
    context.influxdb.sut.org = os.getenv("INFLUXDB_SUT_ORG", None)
    context.influxdb.sut.bucket = os.getenv("INFLUXDB_SUT_BUCKET", None)

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
        text = "Cannot reach SUT InfluxDB endpoint"
        logger.error(text)
        raise AssertionError(text)
    logger.info(
        "successfully connected to SUT InfluxDB endpoint"
    )

    context.influxdb.sut.write_api = context.influxdb.sut.client.write_api(
        write_options=SYNCHRONOUS
    )
    context.influxdb.sut.query_api = context.influxdb.sut.client.query_api()


def _setup_logging(context: Context):
    """
    Setup logging for behave tests, directing logs to a file and the systemd journal.
    :param context:
    :return:
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
        # ignore creation errors (handlers will raise later if there's a real problem)
        pass

    root.setLevel(context.config.logging_level)
    formatter = AnsiColorFormatter('%(asctime)s | %(name)s | %(levelname)8s | %(message)s')
    file_handler = logging.FileHandler(context.config.logfile)
    file_handler.setLevel(context.config.logging_level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

def _ensure_influx_initialized(context: Context):
    if getattr(context, "influxdb", None) is None:
        _load_env(context)

def before_all(context: Context):
    """
    Setup logging before all tests.
    :param context:
    :return:
    """

    _setup_logging(context)
    logger.info("------------------------------------------------")
    logger.info("Starting BDD tests...")
    _load_dotenv_files()


def before_feature(context: Context, feature: Feature):
    """
    Log the start of a feature.
    :param context:
    :param feature:
    :return:
    """
    logger.debug(f"=== starting feature: {feature.name} ===")
    #Only initialize influx when a feature actually needs it
    if "influx" in getattr(feature, "tags", []):
        _ensure_influx_initialized(context)

def after_feature(context: Context, feature: Feature):
    """
    Log the end of a feature.
    :param context:
    :param feature:
    :return:
    """
    logger.debug(
        f"=== finished feature: {feature.name} -> {feature.status.name} ==="
    )

def before_step(context: Context, step: Step):
    pass

def after_step(context: Context, step: Step):
    pass


def before_scenario(context: Context, scenario: Scenario):
    """
    Log the start of a scenario.
    :param context:
    :param scenario:
    :return:
    """
    logger.debug(f"-- starting scenario: {scenario.name}")
    if "influx" in getattr(scenario, "tags", []):
        _ensure_influx_initialized(context)

def after_scenario(context: Context, scenario: Scenario):
    """
    Log the end of a scenario.
    :param context:
    :param scenario:
    :return:
    """
    logger.debug(
        f"-- finished scenario: {scenario.name} -> {scenario.status.name}"
    )
