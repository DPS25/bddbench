import logging, os

from behave.model import Feature, Scenario
from influxdb_client.client.write_api import SYNCHRONOUS
from systemd.journal import JournalHandler
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision

def _config_influx_from_env(context: Context, env_prefix: str, ctx_prefix: str) -> None:
    """
    Generic InfluxDB configuration loader.

    - env_prefix: prefix for environment variables, e.g. "SUT_INFLUX" or "MAIN_INFLUX"
    - ctx_prefix: prefix for context attributes, e.g. "sut_influx" or "main_influx"
    """
    url = os.getenv(f"{env_prefix}_URL")
    token = os.getenv(f"{env_prefix}_TOKEN")
    org = os.getenv(f"{env_prefix}_ORG")

    if not url or not token or not org:
        raise RuntimeError(
            f"{env_prefix}_URL, {env_prefix}_TOKEN oder {env_prefix}_ORG nicht gesetzt – bitte beim behave-Aufruf mitgeben!"
        )

    client = InfluxDBClient(url=url, token=token, org=org)
    setattr(context, f"{ctx_prefix}_client", client)
    setattr(context, f"{ctx_prefix}_org", org)
    setattr(context, f"{ctx_prefix}_url", url)
    setattr(context, f"{ctx_prefix}_write_api", client.write_api(write_options=SYNCHRONOUS))
    setattr(context, f"{ctx_prefix}_query_api", client.query_api())


def _config_sut_influx_from_env(context: Context) -> None:
    """
    Setup SUT InfluxDB configuration from environment variables.
    :param context:
    :return:
    """
    _config_influx_from_env(context, env_prefix="SUT_INFLUX", ctx_prefix="sut_influx")


def _config_main_influx_from_env(context: Context) -> None:
    """
    Setup main InfluxDB configuration from environment variables.
    :param context:
    :return:
    """
    _config_influx_from_env(context, env_prefix="MAIN_INFLUX", ctx_prefix="main_influx")

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
    _config_main_influx_from_env(context)
    _config_sut_influx_from_env(context)

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
