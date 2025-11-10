import logging, os

def before_all(context):
    level = os.getenv("BDD_LOG_LEVEL", "DEBUG").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.DEBUG),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("bdd").debug("booting behave…")

def before_feature(context, feature):
    logging.getLogger("bdd").debug(f"=== starting feature: {feature.name} ===")

def after_feature(context, feature):
    logging.getLogger("bdd").debug(f"=== finished feature: {feature.name} -> {feature.status.name} ===")

def before_scenario(context, scenario):
    logging.getLogger("bdd").debug(f"-- starting scenario: {scenario.name}")

def after_scenario(context, scenario):
    logging.getLogger("bdd").debug(f"-- finished scenario: {scenario.name} -> {scenario.status.name}")
