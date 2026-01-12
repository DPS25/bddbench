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
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from behave import when, then

from src.utils import write_json_report

logger = logging.getLogger("bddbench.vm_network_steps")

def _infer_default_peer_host() -> Optional[str]:
    """
    Infer peer host from envs used across the repo.

    Priority:
      1) INFLUXDB_SUT_URL 
      2) SUT_SSH          
    """
    sut_url = (os.getenv("INFLUXDB_SUT_URL") or "").strip()
    m = re.search(r"\d+\.\d+\.\d+\.\d+", sut_url)
    if m:
        return m.group(0)

    sut_ssh = (os.getenv("SUT_SSH") or "").strip()
    if sut_ssh:
        return sut_ssh.split("@")[-1].strip()

    return None

def _pick_target_host(feature_value: str) -> str:
    """
    Resolve the network benchmark peer host robustly.

    - If BDD_NET_TARGET_HOST is set => use it
    - Else if feature_value is real => use it
    - Else infer from INFLUXDB_SUT_URL / SUT_SSH
    """
    env_override = (os.getenv("BDD_NET_TARGET_HOST") or "").strip()
    if env_override:
        return env_override

    fv = (feature_value or "").strip()
    if fv and fv != "__SET_BY_ENV__":
        return fv

    inferred = _infer_default_peer_host()
    if inferred:
        return inferred

    raise AssertionError(
        "Network benchmark peer not configured. Set BDD_NET_TARGET_HOST=<ip/host> "
        "or configure INFLUXDB_SUT_URL (or SUT_SSH on older branches)."
    )

def _pick_target_ssh_host(peer_host: str) -> str:
    """
    Derive SSH destination for the peer

    Defaults to nixos@<peer> 
    """
    ssh_override = (os.getenv("BDD_NET_TARGET_SSH") or "").strip()
    if ssh_override:
        return ssh_override

    sut_ssh = (os.getenv("SUT_SSH") or "").strip()
    if sut_ssh:
        return sut_ssh

    h = (peer_host or "").strip()
    if "@" in h:
        return h

    ssh_user = (os.getenv("BDD_NET_SSH_USER") or "nixos").strip() or "nixos"
    return f"{ssh_user}@{h}"

# ----------------------------
# small helpers
# ----------------------------

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
    just open a UDP "connection" to a well-known IP.
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
    Use BatchMode/ConnectTimeout to avoid hanging forever.
    """
    return subprocess.run(
        [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=6",
            "-o", "StrictHostKeyChecking=accept-new",
            host,
            "bash",
            "-lc",
            remote_cmd,
        ],
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
    iperf3 -J should output pure JSON, but wrappers (nix/ssh) sometimes
    add warnings or banners on stdout. This extracts the first {...} block.
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

# ----------------------------
# iperf3 server helpers
# ----------------------------

_LOCAL_IPERF3_SERVER_PROC: Optional[subprocess.Popen] = None

def _start_local_iperf3_server(port: int) -> Dict[str, Any]:
    global _LOCAL_IPERF3_SERVER_PROC
    if _LOCAL_IPERF3_SERVER_PROC and _LOCAL_IPERF3_SERVER_PROC.poll() is None:
        return {"ok": True, "already_running": True, "port": port}

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
        return {"ok": ok, "already_running": False, "port": port}
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

def _start_remote_iperf3_server_oneoff(target_host: str, port: int) -> Dict[str, Any]:
    """
    Start one-off iperf3 server on target_host
    Uses nohup so SSH returns immediately.
    """
    log_path = f"/tmp/bddbench_iperf3_remote_{port}.log"
    script = (
        f"set -e; "
        f"LOG={shlex.quote(log_path)}; "
        f"(command -v iperf3 >/dev/null 2>&1 && "
        f" nohup iperf3 -s -1 -p {port} >$LOG 2>&1 &)"
        f" || "
        f"(command -v nix >/dev/null 2>&1 && "
        f" nohup nix run nixpkgs#iperf3 -- -s -1 -p {port} >$LOG 2>&1 &)"
        f" || "
        f"(echo 'iperf3 not available on target and nix not found' >&2; exit 1); "
        f"sleep 0.25; "
        f"ss -lntp | grep -E ':{port}\\b' || true"
    )
    try:
        proc = _ssh_run(target_host, script, timeout_s=25)
        ok = (proc.returncode == 0) and (f":{port}" in (proc.stdout + proc.stderr))
        return {
            "ok": ok,
            "rc": proc.returncode,
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
            "log_path": log_path,
        }
    except Exception as exc:
        return {
            "ok": False,
            "rc": None,
            "stdout": "",
            "stderr": repr(exc),
            "log_path": log_path,
        }

# ----------------------------
# iperf3 client runners
# ----------------------------

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

    meta = {
        "cmd": cmd,
        "timeout_s": 45,
    }

    try:
        proc = _run_local(cmd, timeout_s=45)
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
    client_host: str,
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

    meta = {
        "cmd": cmd,
        "timeout_s": 60,
    }

    remote = (
        f"set -e; "
        f"(command -v iperf3 >/dev/null 2>&1 && {' '.join(map(shlex.quote, cmd))})"
        f" || "
        f"(command -v nix >/dev/null 2>&1 && nix run nixpkgs#iperf3 -- {' '.join(map(shlex.quote, cmd[1:]))})"
    )

    try:
        proc = _ssh_run(client_host, remote, timeout_s=60)
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
        return {
            "throughput_mbps": None,
            "jitter_ms": None,
            "packet_loss_pct": None,
        }

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

# ----------------------------
# ping helpers
# ----------------------------

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
    """
    Parse ping summary line. Example:
      rtt min/avg/max/mdev = 0.123/0.234/0.345/0.012 ms
      20 packets transmitted, 20 received, 0% packet loss, time 19016ms
    """
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

# ----------------------------
# behave steps
# ----------------------------

@when('I run an iperf3 "{protocol}" benchmark to "{target_host}" with {parallel_streams:d} streams for {duration_s:d} seconds')
def step_run_iperf3_benchmark(context, protocol, target_host, parallel_streams, duration_s):
    target_host = _pick_target_host(target_host)

    direction = os.getenv("BDD_NET_DIRECTION", "runner->target")
    direction = (direction or "").strip()
    if direction not in ("runner->target", "target->runner"):
        raise AssertionError(
            f"Invalid BDD_NET_DIRECTION={direction!r} "
            f"(use runner->target or target->runner). "
            f"Tip: if you see 'target-' your env line is truncated; set BDD_NET_DIRECTION=\"target->runner\""
        )

    iperf_port = int(os.getenv("BDD_NET_IPERF_PORT", "5201"))
    udp_bitrate = os.getenv("BDD_NET_UDP_BITRATE", "25G")
    connect_timeout_ms = int(os.getenv("BDD_NET_CONNECT_TIMEOUT_MS", "3000"))

    runner_host = socket.gethostname()
    runner_ip = os.getenv("BDD_NET_RUNNER_IP") or _guess_runner_ip() or ""
    if not runner_ip:
        raise AssertionError("Cannot determine runner IP. Set BDD_NET_RUNNER_IP=...")

    peer_ip, origin = _resolve_target_ip(target_host)
    peer_ssh = _pick_target_ssh_host(target_host)

    raw = None
    err = None
    meta_extra: Dict[str, Any] = {}
    server_info: Dict[str, Any] = {}

    try:
        if direction == "runner->target":
            server_info = _start_remote_iperf3_server_oneoff(target_host=peer_ssh, port=iperf_port)

            raw, err, meta_extra = _run_iperf3_client_local(
                server_ip=peer_ip,
                port=iperf_port,
                protocol=protocol,
                duration_s=duration_s,
                parallel_streams=parallel_streams,
                udp_bitrate=udp_bitrate,
                connect_timeout_ms=connect_timeout_ms,
            )

        else:
            server_info = _start_local_iperf3_server(port=iperf_port)
            if not server_info.get("ok"):
                raise AssertionError(f"Failed to start local iperf3 server: {server_info}")

            raw, err, meta_extra = _run_iperf3_client_remote_via_ssh(
                client_host=peer_ssh,
                server_ip=runner_ip,
                port=iperf_port,
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
        err = "missing throughput in iperf3 JSON (likely stdout polluted; check metrics.raw._raw_stdout/_raw_stderr)"

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
            "iperf_port": iperf_port,
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

    write_json_report(outfile, data, logger_=logger)
