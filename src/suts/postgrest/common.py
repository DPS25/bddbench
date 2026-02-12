from __future__ import annotations

import json
import os
import random
import string
import subprocess
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_any(names: List[str], default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and v != "":
            return v
    return default


def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    return str(uuid.uuid4())


def git_sha_short() -> str:
    sha = _env("GITHUB_SHA", "")
    if sha:
        return sha[:8]
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except Exception:
        return "unknown"


def percentile(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    xs = sorted(values)
    if p <= 0:
        return xs[0]
    if p >= 100:
        return xs[-1]
    k = int(round((p / 100.0) * (len(xs) - 1)))
    k = max(0, min(k, len(xs) - 1))
    return xs[k]


def random_payload(n_bytes: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n_bytes))


@dataclass
class PostgrestConfig:
    url: str
    token: str
    table: str
    timeout_s: float

    @staticmethod
    def from_env() -> "PostgrestConfig":
        return PostgrestConfig(
            url=_env_any(["POSTGREST_SUT_URL", "POSTGREST_URL"], "http://localhost:3000").rstrip("/"),
            token=_env_any(["POSTGREST_SUT_TOKEN", "POSTGREST_TOKEN"], ""),
            table=_env_any(["POSTGREST_TABLE"], "bench_events"),
            timeout_s=float(_env_any(["POSTGREST_TIMEOUT_S"], "20")),
        )


@dataclass
class InfluxConfig:
    url: str
    token: str
    org: str
    bucket: str
    measurement: str
    enabled: bool

    @staticmethod
    def from_env() -> "InfluxConfig":
        url = _env_any(["INFLUXDB_MAIN_URL", "MAIN_INFLUX_URL"], "")
        org = _env_any(["INFLUXDB_MAIN_ORG", "MAIN_INFLUX_ORG"], "")
        bucket = _env_any(["INFLUXDB_MAIN_BUCKET", "MAIN_INFLUX_BUCKET"], "")

        token = _env_any(
            [
                "INFLUXDB_MAIN_TOKEN",
                "INFLUXDB_MAIN_API_TOKEN",
                "INFLUXDB_TOKEN",
                "INFLUXDB_API_TOKEN",
                "MAIN_INFLUX_TOKEN",
            ],
            "",
        )

        measurement = _env_any(["INFLUXDB_MAIN_MEASUREMENT", "MAIN_INFLUX_MEASUREMENT"], "bddbench")
        enabled = bool(url and org and bucket)
        return InfluxConfig(url=url.rstrip("/"), token=token, org=org, bucket=bucket, measurement=measurement, enabled=enabled)


def postgrest_table_url(cfg: PostgrestConfig, table: Optional[str] = None) -> str:
    t = table or cfg.table
    return f"{cfg.url}/{t}"


# ---- tiny HTTP helper (stdlib only) ----

class HttpResponse(Tuple[int, bytes, Dict[str, str]]):
    """(status, body, headers)"""


def _http_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[bytes] = None,
    timeout_s: float = 20.0,
) -> Tuple[int, bytes, Dict[str, str]]:
    req = urllib.request.Request(url=url, data=body, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read() if resp else b""
            hdrs = {k.lower(): v for k, v in resp.headers.items()} if resp else {}
            return status, data, hdrs
    except urllib.error.HTTPError as e:
        data = e.read() if e.fp else b""
        hdrs = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        return e.code, data, hdrs
    except Exception as e:
        # 用 status=0 表示网络/连接层错误
        return 0, repr(e).encode("utf-8"), {}


def _auth_headers(token: str) -> Dict[str, str]:
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def postgrest_get(cfg: PostgrestConfig, url: str, extra_headers: Optional[Dict[str, str]] = None) -> Tuple[int, bytes]:
    headers = {"User-Agent": "bddbench-postgrest-sut/1.0"}
    headers.update(_auth_headers(cfg.token))
    if extra_headers:
        headers.update(extra_headers)
    status, body, _ = _http_request("GET", url, headers=headers, timeout_s=cfg.timeout_s)
    return status, body


def postgrest_post_json(cfg: PostgrestConfig, url: str, obj: Any, extra_headers: Optional[Dict[str, str]] = None) -> Tuple[int, bytes]:
    headers = {"User-Agent": "bddbench-postgrest-sut/1.0", "Content-Type": "application/json", "Prefer": "return=minimal"}
    headers.update(_auth_headers(cfg.token))
    if extra_headers:
        headers.update(extra_headers)
    body = json.dumps(obj).encode("utf-8")
    status, data, _ = _http_request("POST", url, headers=headers, body=body, timeout_s=cfg.timeout_s)
    return status, data


def postgrest_delete(cfg: PostgrestConfig, url: str, extra_headers: Optional[Dict[str, str]] = None) -> Tuple[int, bytes]:
    headers = {"User-Agent": "bddbench-postgrest-sut/1.0", "Prefer": "return=minimal"}
    headers.update(_auth_headers(cfg.token))
    if extra_headers:
        headers.update(extra_headers)
    status, data, _ = _http_request("DELETE", url, headers=headers, timeout_s=cfg.timeout_s)
    return status, data


# ---- Influx line protocol write (stdlib only) ----

def _escape_tag(s: str) -> str:
    return s.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _format_field_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        if v != v:  # NaN
            return "0.0"
        return f"{v}"
    sv = str(v).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{sv}"'


def influx_write_point(
    influx: InfluxConfig,
    measurement: str,
    tags: Dict[str, str],
    fields: Dict[str, Any],
    timestamp_ns: Optional[int] = None,
) -> None:
    if not influx.enabled:
        return

    ts = timestamp_ns or int(time.time() * 1e9)
    tag_str = ",".join(f"{_escape_tag(k)}={_escape_tag(v)}" for k, v in sorted(tags.items()) if v is not None)
    field_str = ",".join(f"{_escape_tag(k)}={_format_field_value(v)}" for k, v in fields.items())

    line = f"{measurement}"
    if tag_str:
        line += f",{tag_str}"
    line += f" {field_str} {ts}"

    url = f"{influx.url}/api/v2/write"
    params = {"org": influx.org, "bucket": influx.bucket, "precision": "ns"}
    full_url = url + "?" + urllib.parse.urlencode(params)

    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if influx.token:
        headers["Authorization"] = f"Token {influx.token}"

    status, body, _ = _http_request("POST", full_url, headers=headers, body=line.encode("utf-8"), timeout_s=10)
    if status >= 300 or status == 0:
        raise RuntimeError(f"Influx write failed: status={status} body={body[:300]!r}")

