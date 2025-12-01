from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


def state_path() -> Path:
    return Path(os.getenv("BDDBENCH_STATE_FILE", "reports/bddbench_state.json"))


def _load() -> Dict[str, Any]:
    p = state_path()
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _save(state: Dict[str, Any]) -> None:
    p = state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def ensure_run_id(context=None) -> str:
    """
    Provides a stable run_id for the current process/test run.
    Priority:
      1) context.run_id (if present)
      2) env BDDBENCH_RUN_ID
      3) generated uuid4
    """
    if context is not None and getattr(context, "run_id", None):
        return str(context.run_id)

    env = os.getenv("BDDBENCH_RUN_ID")
    rid = env if env else str(uuid.uuid4())

    if context is not None:
        context.run_id = rid
    return rid


def register_created_bucket(target: str, bucket_name: str) -> None:
    if not bucket_name:
        return
    st = _load()
    created = st.setdefault("created_buckets", {})
    lst: List[str] = created.setdefault(target, [])
    if bucket_name not in lst:
        lst.append(bucket_name)
    _save(st)


def register_written_data(target: str, bucket: str, measurement: str, run_id: str) -> None:
    if not bucket or not measurement or not run_id:
        return
    st = _load()
    lst: List[Dict[str, str]] = st.setdefault("written_data", [])
    item = {"target": target, "bucket": bucket, "measurement": measurement, "run_id": run_id}
    if item not in lst:
        lst.append(item)
    _save(st)


def register_main_result(measurement: str, bucket: str, run_id: str) -> None:
    # main results are also just “written_data”, but we keep it explicit/readable
    register_written_data("main", bucket=bucket, measurement=measurement, run_id=run_id)
