import json
import logging
import os
import re
import shlex
import socket
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from behave import when, then

from src.utils import write_json_report

logger = logging.getLogger("bddbench.vm_network_steps")

SUT_PLACEHOLDER = "__SUT__"
_ALLOWED_DIRECTIONS = ("runner->target", "target->runner")

# MAIN export gating:
# - Default: OFF (so local dev won't try to write MAIN)
# - Enable by setting ONE of these env vars to truthy: 1/true/yes/on
_EXPORT_MAIN_ENV_KEYS = (
    "BDD_EXPORT_MAIN_INFLUX",
    "BDD_MAIN_INFLUX_EXPORT",
    "EXPORT_MAIN_INFLUX",
    "EXPORT_TO_MAIN_INFLUX",
    "BDD_EXPORT_MAIN",
    "EXPORT_MAIN",
)


def _is_truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _main_export_enabled() -> bool:
    for k in _EXPORT_MAIN_ENV_KEYS:
        if _is_truthy(os.getenv(k, "")):
            return True
    return False


# ============================================================
# Target host selection (derive from INFLUXDB_SUT_URL)
# ============================================================

def _sut_host_from_influxdb_sut_url() -> str:
    """
    Parse host from INFLUXDB_SUT_URL.
    Example: http://192.168.8.116:8086 -> 192.168.8.116
    """
    sut_url = (os.getenv("INFLUXDB_SUT_URL") or "").strip()
    if not sut_url:
        raise AssertionError(
            "INFLUXDB_SUT_URL is not set. It is required to derive the default network benchmark target. "
            "Set it via envs/<ENV_NAME>.env or export it before running behave."
        )

    # Allow values like "192.168.8.116:8086" (no scheme)
    if "://" not in sut_url:
        sut_url = "http://" + sut_url

    u = urlparse(sut_url)
    host = (u.hostname or "").strip()
    if not host:
        raise AssertionError(f"Could not parse host from INFLUXDB_SUT_URL={sut_url!r}")
    return host


def _pick_target_host(feature_value: str) -> str:
    """
    Default target_host is derived from INFLUXDB_SUT_URL.
    Feature may override by providing an explicit host/IP in the Examples table.
    """
    fv = (feature_value or "").strip()
    if not fv or fv == SUT_PLACEHOLDER:
        return _sut_host_from_influxdb_sut_url()
    return fv


def _normalize_direction(direction: str) -> str:
    v = (direction or "").strip()
    if v in _ALLOWED_DIRECTIONS:
        return v
    raise AssertionError(
        f"Invalid direction={v!r}. Direction must be one of: {_ALLOWED_DIRECTIONS}. "
        "Direction must be specified in the feature (Examples table), not via environment variables."
    )


# ============================================================
# Small helpers
# ============================================================

_IPV4_RE = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")


def _is_literal_ipv4(s: str) -> bool:
    if not s:
        return False
    if not _IPV4_RE.match(s.strip()):
        return False
    try:
        return all(0 <= int(x) <= 255 for x in s.strip().split("."))
    except Exception:
        return False


def _guess_runner_ip() -> Optional[str]:
    """
    Best-effort: figure out primary outbound IP. We avoid external traffic
    and just open a UDP "connection" to a well-known IP.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except Exception:
        return None


def _run_local(cmd: list, timeout_s: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def _ssh_run(host: str, remote_cmd: str, timeout_s: int = 60) -> subprocess.CompletedProcess:
    """
    Run a remote command via SSH.
    IMPORTANT: do NOT use "bash -lc" (login shell) because it can hang on some Nix setups.
    Use: ssh host -- bash -c "<cmd>"
    """
    connect_timeout = (os.getenv("BDD_NET_SSH_CONNECT_TIMEOUT_S") or "6").strip()
    extra_opts = (os.getenv("BDD_NET_SSH_OPTS") or "").strip()

    ssh_cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", f"ConnectTimeout={connect_timeout}",
        "-o", "StrictHostKeyChecking=accept-new",
    ]
    if extra_opts:
        ssh_cmd += shlex.split(extra_opts)

    ssh_cmd += [host, "--", "bash", "-c", remote_cmd]

    return subprocess.run(
        ssh_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def _resolve_target_ip(target_host: str) -> Tuple[str, str]:
    """
    Resolve peer IP. If target_host already looks like an IPv4 literal, use it.
    Returns (ip, origin) where origin is a short marker.
    """
    if _is_literal_ipv4(target_host):
        return target_host.strip(), "literal"
    try:
        return socket.gethostbyname(target_host.strip()), "dns"
    except Exception:
        return target_host.strip(), "fallback(target_host)"


def _extract_json_from_messy_output(text: str) -> Optional[Dict[str, Any]]:
    """
    iperf3 -J should output pure JSON, but wrappers sometimes add banners.
    Extract the first {...} block.
    """
    if not text:
        return None
    s = text.strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    start = s.find("{")
    if start < 0:
        return None

    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                candidate = s[start:i + 1]
                try:
                    obj = json.loads(candidate)
                    return obj if isinstance(obj, dict) else None
                except Exception:
                    return None
    return None


def _port_in_use_local(port: int) -> bool:
    try:
        proc = _run_local(["ss", "-lnt"], timeout_s=3)
        out = (proc.stdout or "") + (proc.stderr or "")
        return f":{port} " in out or f":{port}\n" in out or re.search(rf":{port}\b", out) is not None
    except Exception:
        return False


def _candidate_ports(default_port: int, explicit_port: Optional[int]) -> Tuple[int, ...]:
    """
    If user pinned a port via env -> only that port.
    Else try a small fixed range (firewall-friendly) starting from default.
    """
    if explicit_port is not None:
        return (explicit_port,)
    return tuple(range(default_port, default_port + 10))  # 5201..5210 by default


# ============================================================
# SSH target selection (still allowed via env; not part of FR#1)
# ============================================================

def _pick_target_ssh_host(peer_host: str) -> str:
    """
    Derive SSH destination for the peer.

    Defaults to nixos@<peer>.
    Can override with:
      - BDD_NET_TARGET_SSH (full user@host)
      - BDD_NET_SSH_USER (default nixos)
      - BDD_NET_USE_SUT_SSH_AS_IS=1 (use SUT_SSH literally)
    """
    ssh_override = (os.getenv("BDD_NET_TARGET_SSH") or "").strip()
    if ssh_override:
        return ssh_override

    ssh_user = (os.getenv("BDD_NET_SSH_USER") or "nixos").strip() or "nixos"
    use_sut_ssh_as_is = (os.getenv("BDD_NET_USE_SUT_SSH_AS_IS") or "").strip().lower() in ("1", "true", "yes")

    sut_ssh = (os.getenv("SUT_SSH") or "").strip()
    if sut_ssh:
        if use_sut_ssh_as_is:
            return sut_ssh
        sut_host = sut_ssh.split("@")[-1].strip()
        if sut_host:
            return f"{ssh_user}@{sut_host}"

    h = (peer_host or "").strip()
    if "@" in h:
        if use_sut_ssh_as_is:
            return h
        host_part = h.split("@")[-1].strip()
        return f"{ssh_user}@{host_part}"

    return f"{ssh_user}@{h}"


# ============================================================
# iperf3 server helpers
# ============================================================

_LOCAL_IPERF3_SERVER_PROC: Optional[subprocess.Popen] = None


def _start_local_iperf3_server(port: int) -> Dict[str, Any]:
    """
    Start local one-shot server: iperf3 -s -1 -p <port>
    """
    global _LOCAL_IPERF3_SERVER_PROC

    if _port_in_use_local(port):
        return {"ok": False, "error": f"port {port} already in use", "port": port}

    cmd = ["iperf3", "-s", "-1", "-p", str(port)]
    try:
        _LOCAL_IPERF3_SERVER_PROC = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        time.sleep(0.25)
        ok = (_LOCAL_IPERF3_SERVER_PROC.poll() is None)
        if not ok:
            stderr = ""
            try:
                _out, _err = _LOCAL_IPERF3_SERVER_PROC.communicate(timeout=1)
                stderr = (_err or "").strip()
            except Exception:
                pass
            return {"ok": False, "error": stderr or "failed to start", "port": port}

        return {"ok": True, "port": port, "cmd": cmd}
    except Exception as exc:
        return {"ok": False, "error": repr(exc), "port": port}


def _stop_local_iperf3_server() -> None:
    global _LOCAL_IPERF3_SERVER_PROC
    if not _LOCAL_IPERF3_SERVER_PROC:
        return
    try:
        if _LOCAL_IPERF3_SERVER_PROC.poll() is None:
            _LOCAL_IPERF3_SERVER_PROC.terminate()
            try:
                _LOCAL_IPERF3_SERVER_PROC.wait(timeout=3)
            except Exception:
                _LOCAL_IPERF3_SERVER_PROC.kill()
    finally:
        _LOCAL_IPERF3_SERVER_PROC = None


def _start_remote_iperf3_server_oneoff(ssh_target: str, port: int) -> Dict[str, Any]:
    """
    Start one-off iperf3 server on remote via SSH.
    Use nohup + (optional) timeout to avoid lingering listener.
    """
    log_path = f"/tmp/bddbench_iperf3_remote_{port}.log"

    script = (
        "set -e; "
        f"LOG={shlex.quote(log_path)}; "
        "RUN_SRV() { "
        f"  if command -v timeout >/dev/null 2>&1; then "
        f"    nohup timeout 40s iperf3 -s -1 -p {port} >$LOG 2>&1 & "
        f"  else "
        f"    nohup iperf3 -s -1 -p {port} >$LOG 2>&1 & "
        f"  fi "
        "}; "
        "(command -v iperf3 >/dev/null 2>&1 && RUN_SRV) "
        " || "
        f"(command -v nix >/dev/null 2>&1 && nohup nix run nixpkgs#iperf3 -- -s -1 -p {port} >$LOG 2>&1 &) "
        " || "
        "(echo 'iperf3 not available on target and nix not found' >&2; exit 1); "
        "sleep 0.25; "
        f"ss -lnt | grep -E ':{port}\\b' >/dev/null; "
        "echo LISTEN_OK"
    )

    timeout_s = int(os.getenv("BDD_NET_REMOTE_SERVER_TIMEOUT_S", "60"))

    try:
        proc = _ssh_run(ssh_target, script, timeout_s=timeout_s)
        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        ok = (proc.returncode == 0) and ("LISTEN_OK" in combined)
        return {
            "ok": ok,
            "rc": proc.returncode,
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
            "log_path": log_path,
            "ssh_target": ssh_target,
            "port": port,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "rc": None,
            "stdout": "",
            "stderr": f"TimeoutExpired({exc})",
            "log_path": log_path,
            "ssh_target": ssh_target,
            "port": port,
        }
    except Exception as exc:
        return {
            "ok": False,
            "rc": None,
            "stdout": "",
            "stderr": repr(exc),
            "log_path": log_path,
            "ssh_target": ssh_target,
            "port": port,
        }


# ============================================================
# iperf3 client runners
# ============================================================

def _iperf3_client_args(
    server_ip: str,
    port: int,
    protocol: str,
    duration_s: int,
    parallel_streams: int,
    udp_bitrate: str,
    connect_timeout_ms: int,
) -> list:
    args = [
        "iperf3",
        "-c", server_ip,
        "-p", str(port),
        "-t", str(duration_s),
        "-P", str(parallel_streams),
        "-J",
        "--connect-timeout", str(connect_timeout_ms),
    ]
    if protocol.lower() == "udp":
        args += ["-u", "-b", str(udp_bitrate)]
    return args


def _run_iperf3_client_local(
    server_ip: str,
    port: int,
    protocol: str,
    duration_s: int,
    parallel_streams: int,
    udp_bitrate: str,
    connect_timeout_ms: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    cmd = _iperf3_client_args(
        server_ip=server_ip,
        port=port,
        protocol=protocol,
        duration_s=duration_s,
        parallel_streams=parallel_streams,
        udp_bitrate=udp_bitrate,
        connect_timeout_ms=connect_timeout_ms,
    )

    meta = {"cmd": cmd, "timeout_s": 60}

    try:
        proc = _run_local(cmd, timeout_s=60)
        meta.update(
            {
                "returncode": proc.returncode,
                "stdout_len": len(proc.stdout or ""),
                "stderr": (proc.stderr or "").strip(),
            }
        )
        raw = _extract_json_from_messy_output(proc.stdout or "")
        err = None
        if proc.returncode != 0:
            err = (raw or {}).get("error") or (proc.stderr or "").strip() or "iperf3 client failed"
        return raw, err, meta
    except subprocess.TimeoutExpired:
        meta["timeout"] = True
        return None, "iperf3 client timeout", meta
    except Exception as exc:
        meta["exception"] = repr(exc)
        return None, repr(exc), meta


def _run_iperf3_client_remote_via_ssh(
    client_ssh: str,
    server_ip: str,
    port: int,
    protocol: str,
    duration_s: int,
    parallel_streams: int,
    udp_bitrate: str,
    connect_timeout_ms: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    cmd = _iperf3_client_args(
        server_ip=server_ip,
        port=port,
        protocol=protocol,
        duration_s=duration_s,
        parallel_streams=parallel_streams,
        udp_bitrate=udp_bitrate,
        connect_timeout_ms=connect_timeout_ms,
    )

    meta = {"cmd": cmd, "timeout_s": 90}

    remote = (
        "set -e; "
        f"(command -v iperf3 >/dev/null 2>&1 && {' '.join(map(shlex.quote, cmd))})"
        " || "
        f"(command -v nix >/dev/null 2>&1 && nix run nixpkgs#iperf3 -- {' '.join(map(shlex.quote, cmd[1:]))})"
    )

    try:
        proc = _ssh_run(client_ssh, remote, timeout_s=90)
        meta.update(
            {
                "returncode": proc.returncode,
                "stdout_len": len(proc.stdout or ""),
                "stderr": (proc.stderr or "").strip(),
            }
        )
        raw = _extract_json_from_messy_output(proc.stdout or "")
        err = None
        if proc.returncode != 0:
            err = (raw or {}).get("error") or (proc.stderr or "").strip() or "iperf3 remote client failed"
        return raw, err, meta
    except subprocess.TimeoutExpired:
        meta["timeout"] = True
        return None, "iperf3 remote client timeout", meta
    except Exception as exc:
        meta["exception"] = repr(exc)
        return None, repr(exc), meta


def _extract_iperf3_metrics(protocol: str, raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not raw or not isinstance(raw, dict):
        return {"throughput_mbps": None, "jitter_ms": None, "packet_loss_pct": None}

    end = raw.get("end") or {}
    if protocol.lower() == "udp":
        s = (end.get("sum") or end.get("sum_received") or end.get("sum_sent") or {})
        bps = s.get("bits_per_second")
        jitter_ms = s.get("jitter_ms")
        loss_pct = s.get("lost_percent")
        return {
            "throughput_mbps": (bps / 1_000_000.0) if isinstance(bps, (int, float)) else None,
            "jitter_ms": jitter_ms if isinstance(jitter_ms, (int, float)) else None,
            "packet_loss_pct": loss_pct if isinstance(loss_pct, (int, float)) else None,
        }

    s = (end.get("sum_received") or end.get("sum_sent") or end.get("sum") or {})
    bps = s.get("bits_per_second")
    return {
        "throughput_mbps": (bps / 1_000_000.0) if isinstance(bps, (int, float)) else None,
        "jitter_ms": None,
        "packet_loss_pct": None,
    }


# ============================================================
# ping helpers
# ============================================================

def _run_ping(target_ip: str, packet_count: int) -> Tuple[str, Optional[str]]:
    cmd = ["ping", "-n", "-c", str(packet_count), target_ip]
    try:
        proc = _run_local(cmd, timeout_s=max(10, packet_count + 5))
        out = (proc.stdout or "") + (proc.stderr or "")
        err = None
        if proc.returncode != 0:
            err = out.strip().splitlines()[-1] if out.strip() else "ping failed"
        return out, err
    except subprocess.TimeoutExpired:
        return "", "ping timeout"
    except Exception as exc:
        return "", repr(exc)


def _parse_ping_metrics(out: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "rtt_min_ms": None,
        "rtt_avg_ms": None,
        "rtt_max_ms": None,
        "rtt_mdev_ms": None,
        "packet_loss_pct": None,
    }

    if not out:
        return metrics

    m = re.search(r"(\d+(?:\.\d+)?)%\s+packet loss", out)
    if m:
        try:
            metrics["packet_loss_pct"] = float(m.group(1))
        except Exception:
            pass

    m = re.search(r"rtt .* =\s*([\d\.]+)/([\d\.]+)/([\d\.]+)/([\d\.]+)\s*ms", out)
    if m:
        try:
            metrics["rtt_min_ms"] = float(m.group(1))
            metrics["rtt_avg_ms"] = float(m.group(2))
            metrics["rtt_max_ms"] = float(m.group(3))
            metrics["rtt_mdev_ms"] = float(m.group(4))
        except Exception:
            pass

    return metrics


# ============================================================
# MAIN Influx export (NEW)
# ============================================================

def _env_first(*keys: str) -> str:
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _get_main_bucket_org_from_context_or_env(context) -> Tuple[str, str]:
    # Try context.influxdb.main.{bucket,org}
    bucket = ""
    org = ""
    influxdb = getattr(context, "influxdb", None)
    main = getattr(influxdb, "main", None) if influxdb is not None else None
    if main is not None:
        bucket = (
            (getattr(main, "bucket", None) or "")
            or (getattr(main, "bucket_name", None) or "")
        ).strip()
        org = (
            (getattr(main, "org", None) or "")
            or (getattr(main, "org_name", None) or "")
        ).strip()

    # Fallback to env (names are guessed; keep multiple aliases)
    if not org:
        org = _env_first("INFLUXDB_MAIN_ORG", "MAIN_INFLUXDB_ORG", "INFLUX_MAIN_ORG")
    if not bucket:
        bucket = _env_first("INFLUXDB_MAIN_BUCKET", "MAIN_INFLUXDB_BUCKET", "INFLUX_MAIN_BUCKET")

    return bucket, org


def _network_result_to_point(context, data: Dict[str, Any], outfile: str):
    """
    Build an InfluxDB point for MAIN export.
    We try to use generate_base_point() if available; otherwise fall back to Point().
    """
    # Lazy import to avoid hard dependency for users who never export.
    try:
        from src.utils import generate_base_point  # type: ignore
    except Exception:
        generate_base_point = None

    try:
        from influxdb_client import Point  # type: ignore
    except Exception as exc:
        raise AssertionError(f"influxdb_client is required for MAIN export, but not available: {exc!r}")

    params = data.get("params") or {}
    metrics = ((data.get("result") or {}).get("metrics")) or {}
    meta = ((data.get("result") or {}).get("meta")) or {}
    err = (meta.get("error") or "").strip()

    measurement = "bddbench_network_result"
    if generate_base_point is not None:
        try:
            p = generate_base_point(context=context, measurement=measurement)
        except Exception:
            p = Point(measurement)
    else:
        p = Point(measurement)

    # Tags (dimensions)
    p.tag("mode", str(params.get("mode", "")))
    p.tag("protocol", str(params.get("protocol", "")))
    p.tag("direction", str(params.get("direction", "")))
    p.tag("target_host", str(params.get("target_host", "")))

    # Fields
    p.field("ok", err == "")
    p.field("report_file", str(outfile))

    if err:
        # Keep error as field (not tag) to avoid huge tag cardinality
        p.field("error", err)

    # Numeric fields: only write if value is not None
    for k, v in (metrics or {}).items():
        if v is None:
            continue
        if isinstance(v, (int, float)):
            p.field(k, float(v) if isinstance(v, float) else v)

    # Some metrics dictionaries include packet_count etc (ints) - above handles.

    return p


def _get_main_write_api_and_cfg(context):
    """
    Returns (client, write_api, bucket, org, created_client_bool)
    """
    # 1) Try project-native helper first (works when context.influxdb is initialized)
    try:
        from src.utils import get_main_influx_write_api  # type: ignore
    except Exception:
        get_main_influx_write_api = None

    if get_main_influx_write_api is not None and hasattr(context, "influxdb"):
        try:
            client, write_api = get_main_influx_write_api(context, create_client_if_missing=False)
            bucket, org = _get_main_bucket_org_from_context_or_env(context)
            if write_api is not None and bucket and org:
                return client, write_api, bucket, org, False
        except Exception:
            # We'll fallback to env-based client below
            pass

    # 2) Fallback: build MAIN client from env vars (allows running network feature standalone)
    try:
        from influxdb_client import InfluxDBClient  # type: ignore
        from influxdb_client.client.write_api import SYNCHRONOUS  # type: ignore
    except Exception as exc:
        raise AssertionError(f"influxdb_client is required for MAIN export, but not available: {exc!r}")

    url = _env_first("INFLUXDB_MAIN_URL", "MAIN_INFLUXDB_URL", "INFLUX_MAIN_URL", "INFLUXDB_URL_MAIN")
    token = _env_first("INFLUXDB_MAIN_TOKEN", "MAIN_INFLUXDB_TOKEN", "INFLUX_MAIN_TOKEN")
    bucket = _env_first("INFLUXDB_MAIN_BUCKET", "MAIN_INFLUXDB_BUCKET", "INFLUX_MAIN_BUCKET")
    org = _env_first("INFLUXDB_MAIN_ORG", "MAIN_INFLUXDB_ORG", "INFLUX_MAIN_ORG")

    missing = [k for k, v in {
        "INFLUXDB_MAIN_URL": url,
        "INFLUXDB_MAIN_TOKEN": token,
        "INFLUXDB_MAIN_BUCKET": bucket,
        "INFLUXDB_MAIN_ORG": org,
    }.items() if not v]
    if missing:
        raise AssertionError(
            "MAIN export is enabled, but MAIN Influx env config is missing. "
            f"Missing: {missing}. "
            "Set e.g. INFLUXDB_MAIN_URL/INFLUXDB_MAIN_TOKEN/INFLUXDB_MAIN_ORG/INFLUXDB_MAIN_BUCKET (or their aliases)."
        )

    client = InfluxDBClient(url=url, token=token, org=org, timeout=30_000)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    return client, write_api, bucket, org, True


def _export_network_result_to_main_influx(context, data: Dict[str, Any], outfile: str) -> Dict[str, str]:
    """
    Export one network result point to MAIN Influx (optional).
    - Default: skipped
    - If enabled: must succeed, otherwise raise (so CI can catch config issues)
    """
    if not _main_export_enabled():
        return {"status": "skipped", "reason": "MAIN export disabled (set BDD_EXPORT_MAIN_INFLUX=1 to enable)."}

    client, write_api, bucket, org, created = _get_main_write_api_and_cfg(context)
    p = _network_result_to_point(context, data, outfile)

    try:
        write_api.write(bucket=bucket, org=org, record=p)
    finally:
        if created and client is not None:
            try:
                client.close()
            except Exception:
                pass

    # Use print so it shows up in behave output (some setups don't surface logger.info)
    print("Exported network benchmark result to MAIN Influx")
    return {"status": "exported", "reason": ""}


# ============================================================
# behave steps
# ============================================================

@when('I run an iperf3 "{protocol}" benchmark "{direction}" to "{target_host}" with {parallel_streams:d} streams for {duration_s:d} seconds')
def step_run_iperf3_benchmark(context, protocol, direction, target_host, parallel_streams, duration_s):
    target_host = _pick_target_host(target_host)
    direction = _normalize_direction(direction)

    udp_bitrate = os.getenv("BDD_NET_UDP_BITRATE", "25G")
    connect_timeout_ms = int(os.getenv("BDD_NET_CONNECT_TIMEOUT_MS", "3000"))

    runner_host = socket.gethostname()
    runner_ip = os.getenv("BDD_NET_RUNNER_IP") or _guess_runner_ip() or ""
    peer_ip, origin = _resolve_target_ip(target_host)
    peer_ssh = _pick_target_ssh_host(target_host)

    port_raw = (os.getenv("BDD_NET_IPERF_PORT") or "").strip()
    explicit_port = int(port_raw) if port_raw else None
    ports_to_try = _candidate_ports(default_port=5201, explicit_port=explicit_port)

    raw = None
    err = None
    meta_extra: Dict[str, Any] = {}
    server_info: Dict[str, Any] = {}
    chosen_port: Optional[int] = None

    try:
        if direction == "runner->target":
            # start server on peer, run client locally
            for p in ports_to_try:
                chosen_port = p
                server_info = _start_remote_iperf3_server_oneoff(ssh_target=peer_ssh, port=chosen_port)
                if server_info.get("ok"):
                    break
                if explicit_port is not None:
                    break  # pinned port -> do not retry

            if not server_info.get("ok"):
                err = f"iperf3 server start failed on {peer_ssh}: {server_info.get('stderr') or server_info}"
                meta_extra = {"skipped_client": True}
            else:
                raw, err, meta_extra = _run_iperf3_client_local(
                    server_ip=peer_ip,
                    port=chosen_port,
                    protocol=protocol,
                    duration_s=duration_s,
                    parallel_streams=parallel_streams,
                    udp_bitrate=udp_bitrate,
                    connect_timeout_ms=connect_timeout_ms,
                )

        else:
            # direction == "target->runner": start server locally, run client on peer via ssh
            if not runner_ip:
                raise AssertionError("Cannot determine runner IP. Set BDD_NET_RUNNER_IP=...")

            for p in ports_to_try:
                chosen_port = p
                server_info = _start_local_iperf3_server(port=chosen_port)
                if server_info.get("ok"):
                    break
                if explicit_port is not None:
                    break  # pinned port -> do not retry

            if not server_info.get("ok"):
                err = f"Failed to start local iperf3 server: {server_info}"
                meta_extra = {"skipped_client": True}
            else:
                raw, err, meta_extra = _run_iperf3_client_remote_via_ssh(
                    client_ssh=peer_ssh,
                    server_ip=runner_ip,
                    port=chosen_port,
                    protocol=protocol,
                    duration_s=duration_s,
                    parallel_streams=parallel_streams,
                    udp_bitrate=udp_bitrate,
                    connect_timeout_ms=connect_timeout_ms,
                )
    finally:
        if direction == "target->runner":
            _stop_local_iperf3_server()

    metrics = _extract_iperf3_metrics(protocol=protocol, raw=raw)
    if metrics.get("throughput_mbps") is None and not err:
        err = "missing throughput in iperf3 JSON (stdout may be polluted)"

    context.network_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": runner_host,
        "env_name": os.getenv("ENV_NAME"),
        "params": {
            "mode": "iperf3",
            "protocol": protocol.lower(),
            "direction": direction,
            "target_host": target_host,
            "parallel_streams": parallel_streams,
            "duration_s": duration_s,
            "iperf_port": chosen_port,
            "udp_bitrate": udp_bitrate if protocol.lower() == "udp" else None,
            "connect_timeout_ms": connect_timeout_ms,
        },
        "result": {
            "meta": {
                "run_uuid": str(uuid.uuid4()),
                "runner_host": runner_host,
                "runner_ip": runner_ip,
                "peer_host": target_host,
                "peer_ip": peer_ip,
                "peer_ssh_target": peer_ssh,
                "peer_ip_origin": origin,
                "server_start": server_info,
                "client_meta": meta_extra,
                "error": err,
            },
            "metrics": metrics,
            "raw": raw,
        },
    }


@when('I run a ping benchmark to "{target_host}" with {packet_count:d} packets')
def step_run_ping_benchmark(context, target_host, packet_count):
    target_host = _pick_target_host(target_host)

    runner_host = socket.gethostname()
    runner_ip = os.getenv("BDD_NET_RUNNER_IP") or _guess_runner_ip() or ""

    peer_ip, origin = _resolve_target_ip(target_host)

    out, err = _run_ping(target_ip=peer_ip, packet_count=packet_count)
    m = _parse_ping_metrics(out)

    context.network_benchmark = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "host": runner_host,
        "env_name": os.getenv("ENV_NAME"),
        "params": {
            "mode": "ping",
            "protocol": "icmp",
            "direction": "runner->target",
            "target_host": target_host,
            "packet_count": packet_count,
        },
        "result": {
            "meta": {
                "run_uuid": str(uuid.uuid4()),
                "runner_host": runner_host,
                "runner_ip": runner_ip,
                "peer_host": target_host,
                "peer_ip": peer_ip,
                "peer_ip_origin": origin,
                "error": err,
            },
            "metrics": {**m, "packet_count": packet_count},
            "raw": out,
        },
    }


@then('I store the network benchmark result as "{outfile}"')
def step_store_network_result(context, outfile):
    data = getattr(context, "network_benchmark", None)
    if not isinstance(data, dict):
        raise AssertionError("No network benchmark found in context (did the When step run?)")

    write_json_report(
        outfile,
        data,
        logger_=logger,
        log_prefix="Stored network benchmark result to ",
    )

    # Optional MAIN export (gated)
    export_status = _export_network_result_to_main_influx(context, data, outfile)
    if export_status.get("status") == "skipped":
        logger.info("MAIN export skipped: %s", export_status.get("reason"))

    # Keep existing behavior: fail scenario if benchmark itself failed
    err = ((data.get("result") or {}).get("meta") or {}).get("error")
    if err:
        raise AssertionError(f"Network benchmark failed (report written): {err}")
