import logging, os
from types import SimpleNamespace
from dotenv import load_dotenv
from pathlib import Path

from behave.model import Feature, Scenario
from influxdb_client.client.write_api import SYNCHRONOUS
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision


def _load_env(context: Context):
    """
    Load environment variables into the context.
    :param context:
    :return:
    """

    dotenv_path = Path("../.env")
    load_dotenv(dotenv_path=dotenv_path)

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

    assert context.influxdb.main.url is not None, (
        "INFLUXDB_MAIN_URL environment variable must be set"
    )
    assert context.influxdb.main.token is not None, (
        "INFLUXDB_MAIN_TOKEN environment variable must be set"
    )
    assert context.influxdb.main.org is not None, (
        "INFLUXDB_MAIN_ORG environment variable must be set"
    )
    assert context.influxdb.main.bucket is not None, (
        "INFLUXDB_MAIN_BUCKET environment variable must be set"
    )

    context.influxdb.main.client = InfluxDBClient(
        url=context.influxdb.main.url,
        token=context.influxdb.main.token,
        org=context.influxdb.main.org,
    )
    if not context.influxdb.main.client.ping():
        logging.getLogger("file_handler").error("Cannot reach main InfluxDB endpoint")
        exit(1)

    logging.getLogger("file_handler").info(
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
    context.influxdb.sut.client = InfluxDBClient(
        url=context.influxdb.sut.url,
        token=context.influxdb.sut.token,
        org=context.influxdb.sut.org,
    )
    assert context.influxdb.sut.url is not None, (
        "INFLUXDB_SUT_URL environment variable must be set"
    )
    assert context.influxdb.sut.token is not None, (
        "INFLUXDB_SUT_TOKEN environment variable must be set"
    )
    assert context.influxdb.sut.org is not None, (
        "INFLUXDB_SUT_ORG environment variable must be set"
    )
    assert context.influxdb.sut.bucket is not None, (
        "INFLUXDB_SUT_BUCKET environment variable must be set"
    )

    if not context.influxdb.sut.client.ping():
        logging.getLogger("bddbench").error("Cannot reach SUT InfluxDB endpoint")
        exit(1)
    logging.getLogger("bddbench").info(
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
    root = logging.getLogger()
    context.config.logging_level = context.config.logging_level or logging.DEBUG
    context.config.logging_format = (
        context.config.logging_format
        or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    context.config.logfile = (
        context.config.userdata.get("logfile", None) or "../reports/behave.log"
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
    formatter = logging.Formatter(context.config.logging_format)
    # File handler
    file_handler = logging.FileHandler(context.config.logfile)
    file_handler.setLevel(context.config.logging_level)
    file_handler.setFormatter(formatter)
    file_handler.set_name("bddbench")
    root.addHandler(file_handler)


def before_all(context: Context):
    """
    Setup logging before all tests.
    :param context:
    :return:
    """

    _setup_logging(context)
    _load_env(context)


def before_feature(context: Context, feature: Feature):
    """
    Log the start of a feature.
    :param context:
    :param feature:
    :return:
    """
    logging.getLogger("bddbench").debug(f"=== starting feature: {feature.name} ===")


def after_feature(context: Context, feature: Feature):
    """
    Log the end of a feature.
    :param context:
    :param feature:
    :return:
    """
    logging.getLogger("bddbench").debug(
        f"=== finished feature: {feature.name} -> {feature.status.name} ==="
    )


def before_scenario(context: Context, scenario: Scenario):
    """
    Log the start of a scenario.
    :param context:
    :param scenario:
    :return:
    """
    logging.getLogger("bddbench").debug(f"-- starting scenario: {scenario.name}")


def after_scenario(context: Context, scenario: Scenario):
    """
    Log the end of a scenario.
    :param context:
    :param scenario:
    :return:
    """
    logging.getLogger("bddbench").debug(
        f"-- finished scenario: {scenario.name} -> {scenario.status.name}"
    )
