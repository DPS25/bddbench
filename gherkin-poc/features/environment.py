import time
import logging
import os
import contextvars
from behave.model_core import Status

_feat = contextvars.ContextVar("feat", default="-")
_scn  = contextvars.ContextVar("scn", default="-")
_run  = contextvars.ContextVar("run", default="-")

class CtxFilter(logging.Filter):
    def filter(self, record):
        record.feat = _feat.get()
        record.scn  = _scn.get()
        record.run  = _run.get()
        return True

def _setup_logging():
    level = os.getenv("BDD_LOG_LEVEL", "DEBUG").upper()
    fmt   = os.getenv("BDD_LOG_FMT", "%(asctime)s %(levelname)s %(name)s: %(message)s")
    date  = os.getenv("BDD_LOG_DATEFMT", "%H:%M:%S")
    logging.basicConfig(level=getattr(logging, level, logging.DEBUG),
                        format=fmt, datefmt=date)
    for h in logging.getLogger().handlers:
        h.addFilter(CtxFilter())

def before_all(context):
    _setup_logging()
    logging.getLogger("bench").debug("booting behave…")

def before_feature(context, feature):
    logging.getLogger("bench").debug(f"=== starting feature: {feature.name} ({feature.filename}:{feature.line}) ===")
    logging.getLogger("bench").debug(f"tags: {feature.tags}")

def after_feature(context, feature):
    logging.getLogger("bench").debug(f"=== finished feature: {feature.name} -> {feature.status.name} ===")

def before_scenario(context, scenario):
    logging.getLogger("bench").debug(f"-- starting scenario: {scenario.name} | tags={scenario.tags}")

def after_scenario(context, scenario):
    logging.getLogger("bench").debug(f"-- finished scenario: {scenario.name} -> {scenario.status.name}")

def before_step(context, step):
    if hasattr(context, "run_id"):
        _run.set(context.run_id)
    logging.getLogger("bench").debug(f"> {step.keyword}{step.name}")

def after_step(context, step):
    status = step.status.name if isinstance(step.status, Status) else str(step.status)
    logging.getLogger("bench").debug(f"< {step.keyword}{step.name} -> {status}")
    if step.exception:
        logging.getLogger("bench").exception(step.exception)
