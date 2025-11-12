import logging, os

from behave.model import Feature, Scenario
from influxdb_client.client.write_api import SYNCHRONOUS
from systemd.journal import JournalHandler
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision

def _load_env(context: Context):
    """
    Load environment variables into the context.
    :param context:
    :return:
    """

    # ----- main influx DB -----
    context.config.inlufxdb.main.url = os.getenv("INFLUXDB_MAIN_URL", "http://localhost:8086")
    context.config.influxdb.main.token = os.getenv("INFLUXDB_MAIN_TOKEN", None)
    context.config.influxdb.main.org = os.getenv("INFLUXDB_MAIN_ORG", None)
    context.config.influxdb.main.bucket = os.getenv("INFLUXDB_MAIN_BUCKET", None)

    assert context.config.inlufxdb.main.url is not None, "INFLUXDB_MAIN_URL environment variable must be set"
    assert context.config.influxdb.main.token is not None, "INFLUXDB_MAIN_TOKEN environment variable must be set"
    assert context.config.influxdb.main.org is not None, "INFLUXDB_MAIN_ORG environment variable must be set"
    assert context.config.influxdb.main.bucket is not None, "INFLUXDB_MAIN_BUCKET environment variable must be set"

    # ----- SUT influx DB -----
    context.config.influxdb.sut.url = os.getenv("INFLUXDB_SUT_URL", "http://localhost:8086")
    context.config.influxdb.sut.token = os.getenv("INFLUXDB_SUT_TOKEN", None)
    context.config.influxdb.sut.org = os.getenv("INFLUXDB_SUT_ORG", None)
    context.config.influxdb.sut.bucket = os.getenv("INFLUXDB_SUT_BUCKET", None)

    assert context.config.influxdb.sut.url is not None, "INFLUXDB_SUT_URL environment variable must be set"
    assert context.config.influxdb.sut.token is not None, "INFLUXDB_SUT_TOKEN environment variable must be set"
    assert context.config.influxdb.sut.org is not None, "INFLUXDB_SUT_ORG environment variable must be set"
    assert context.config.influxdb.sut.bucket is not None, "INFLUXDB_SUT_BUCKET environment variable must be set"



def _setup_logging(context: Context):
    """
    Setup logging for behave tests, directing logs to a file and the systemd journal.
    :param context:
    :return:
    """
    root = logging.getLogger()
    context.config.logging_level = context.config.logging_level or logging.DEBUG
    context.config.logging_format = context.config.logging_format or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    context.config.logfile = context.config.logfile or "../reports/behave.log"
    context.config.logdir = os.path.dirname(os.path.abspath(context.config.logfile)) or os.getcwd()
    try:
        os.makedirs(context.config.logdir, exist_ok=True)
    except Exception:
        # ignore creation errors (handlers will raise later if there's a real problem)
        pass

    root.setLevel(context.config.logging_level)
    formatter = logging.Formatter(context.config.logging_format)


    try:
        jh = JournalHandler()
        jh.setLevel(context.config.logging_level or logging.DEBUG)
        jh.setFormatter(formatter)
        jh.set_name("bdd_journal")
        root.addHandler(jh)
    except Exception:
        if not root.handlers:
            logging.basicConfig(level=context.logging_level,
                                format=context.config.logging_format,
                                filename=context.config.logfile)
        else:
            # attach a file handler explicitly if needed
            fh = logging.FileHandler(context.config.logfile)
            fh.setLevel(context.logging_level)
            fh.setFormatter(formatter)
            root.addHandler(fh)


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
    logging.getLogger("bdd_journal").debug(f"=== starting feature: {feature.name} ===")

def after_feature(context: Context, feature: Feature):
    """
    Log the end of a feature.
    :param context:
    :param feature:
    :return:
    """
    logging.getLogger("bdd_journal").debug(f"=== finished feature: {feature.name} -> {feature.status.name} ===")

def before_scenario(context: Context, scenario: Scenario):
    """
    Log the start of a scenario.
    :param context:
    :param scenario:
    :return:
    """
    logging.getLogger("bdd_journal").debug(f"-- starting scenario: {scenario.name}")

def after_scenario(context: Context, scenario: Scenario):
    """
    Log the end of a scenario.
    :param context:
    :param scenario:
    :return:
    """
    logging.getLogger("bdd_journal").debug(f"-- finished scenario: {scenario.name} -> {scenario.status.name}")
