import json
import logging
import os
import re
import shlex
import socket
import subprocess
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from behave import when, then

logger = logging.getLogger("bddbench.influx_network_steps")


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


def _guess_runner_ip(prefer_prefix: str = "192.168.8.") -> Optional[str]:
    """
    Pick a stable 'scope global' IPv4 for this VM.
    Prefer 192.168.8.* (your OpenStack LAN), else first global IPv4.
    """
    try:
        out = subprocess.check_output(
            ["bash", "-lc", "ip -4 -o addr show scope global | awk '{print $4}'"],
            text=True,
        )
        ips = [x.split("/")[0].strip() for x in out.splitlines() if x.strip()]
        for ip in ips:
            if ip.startswith(prefer_prefix):
                return ip
        return ips[0] if ips else None
    except Exception:
        return None


def _run_local(cmd: list[str], timeout_s: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_s,
    )


def _ssh_run(host: str, remote_cmd: str, timeout_s: int = 30) -> subprocess.CompletedProcess:
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
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_s,
    )


def _resolve_target_ip(target_host: str) -> Tuple[str, str]:
    """
    Resolve target_host into an IPv4.
    If user passes literal IP, keep it.
    Otherwise try DNS (may be wrong if /etc/hosts maps to loopback).
    """
    if _is_literal_ipv4(target_host):
        return target_host, "literal-ip"

    try:
        ip = socket.gethostbyname(target_host)
        return ip, "socket.gethostbyname()"
    except Exception:
        return target_host, "fallback(target_host)"


def _extract_json_from_messy_output(text: str) -> Optional[Dict[str, Any]]:
    """
    iperf3 -J should output pure JSON, but wrappers (nix/ssh) sometimes
    add warnings or banners on stdout. This extracts the first {...} block.
    """
    if not text:
        return None
    s = text.strip()
    # fast path
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    # messy path: slice from first { to last }
    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j > i:
        candidate = s[i:j+1]
        try:
            obj = json.loads(candidate)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None


# ----------------------------
# iperf3 server control (LOCAL on runner)
# ----------------------------

def _start_local_iperf3_server(port: int) -> Dict[str, Any]:
    """
    Start persistent iperf3 server on runner (this VM).
    No sudo required.
    """
    log_path = f"/tmp/bddbench_iperf3_srv_{port}.log"
    cmd = [
        "bash", "-lc",
        (
            f"set -e; "
            f"LOG={shlex.quote(log_path)}; "
            f"pkill -x iperf3 >/dev/null 2>&1 || true; "
            f"nohup iperf3 -s -p {port} >$LOG 2>&1 & "
            f"sleep 0.25; "
            f"ss -lntp | grep -E ':{port}\\b' || true"
        ),
    ]
    try:
        proc = _run_local(cmd, timeout_s=12)
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


def _stop_local_iperf3_server() -> None:
    try:
        _run_local(["bash", "-lc", "pkill -x iperf3 >/dev/null 2>&1 || true"], timeout_s=5)
    except Exception:
        pass


# ----------------------------
# iperf3 server control (REMOTE on target) - runner->target mode
# ----------------------------

def _start_remote_iperf3_server_oneoff(target_host: str, port: int) -> Dict[str, Any]:
    """
    Start one-off iperf3 server on target_host (accept one test then exit).
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
) -> list[str]:
    args = [
        "-c", server_ip,
        "-p", str(port),
        "-t", str(duration_s),
        "-P", str(parallel_streams),
        "-J",
        "--connect-timeout", str(connect_timeout_ms),
    ]
    if protocol.lower() == "udp":
        args += ["-u", "-b", udp_bitrate]
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
    cmd = ["iperf3"] + _iperf3_client_args(
        server_ip, port, protocol, duration_s, parallel_streams, udp_bitrate, connect_timeout_ms
    )
    timeout_s = duration_s + (90 if protocol.lower() == "udp" else 35)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        return None, f"TimeoutExpired({timeout_s}s): {exc}", {"cmd": cmd, "timeout_s": timeout_s}

    raw = _extract_json_from_messy_output(proc.stdout or "")
    if raw is None:
        # sometimes wrappers write to stderr; try combined
        raw = _extract_json_from_messy_output((proc.stdout or "") + "\n" + (proc.stderr or ""))

    if raw is None and (proc.stdout or proc.stderr):
        raw = {"_raw_stdout": proc.stdout, "_raw_stderr": proc.stderr}

    err = None
    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        if not err and isinstance(raw, dict):
            err = raw.get("error")
        if not err:
            err = f"rc={proc.returncode}"

    return raw, err, {
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout_len": len(proc.stdout or ""),
        "stderr": (proc.stderr or "").strip(),
        "timeout_s": timeout_s,
    }


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
    args = _iperf3_client_args(server_ip, port, protocol, duration_s, parallel_streams, udp_bitrate, connect_timeout_ms)
    args_str = " ".join(shlex.quote(a) for a in args)

    script = (
        "set -e; "
        "if command -v iperf3 >/dev/null 2>&1; then "
        f"  iperf3 {args_str}; "
        "elif command -v nix >/dev/null 2>&1; then "
        f"  nix run nixpkgs#iperf3 -- {args_str}; "
        "else "
        "  echo 'NO iperf3 on client and no nix' >&2; exit 1; "
        "fi"
    )

    timeout_s = duration_s + (110 if protocol.lower() == "udp" else 50)

    try:
        proc = _ssh_run(client_host, script, timeout_s=timeout_s)
    except subprocess.TimeoutExpired as exc:
        return None, f"ssh TimeoutExpired({timeout_s}s): {exc}", {"client_host": client_host, "timeout_s": timeout_s}

    raw = _extract_json_from_messy_output(proc.stdout or "")
    if raw is None:
        raw = _extract_json_from_messy_output((proc.stdout or "") + "\n" + (proc.stderr or ""))

    if raw is None and (proc.stdout or proc.stderr):
        raw = {"_raw_stdout": proc.stdout, "_raw_stderr": proc.stderr}

    err = None
    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        if not err and isinstance(raw, dict):
            err = raw.get("error")
        if not err:
            err = f"rc={proc.returncode}"

    return raw, err, {
        "client_host": client_host,
        "returncode": proc.returncode,
        "stdout_len": len(proc.stdout or ""),
        "stderr": (proc.stderr or "").strip(),
        "timeout_s": timeout_s,
    }


def _extract_iperf3_metrics(protocol: str, raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"throughput_mbps": None, "jitter_ms": None, "packet_loss_pct": None}

    end = raw.get("end", {}) or {}

    def _get_nested(d, *keys):
        cur = d
        for k in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    if protocol.lower() == "tcp":
        bps = _get_nested(end, "sum_received", "bits_per_second")
        if bps is None:
            bps = _get_nested(end, "sum_sent", "bits_per_second")
        mbps = (bps / 1e6) if isinstance(bps, (int, float)) else None
        return {"throughput_mbps": mbps, "jitter_ms": None, "packet_loss_pct": None}

    # UDP
    bps = _get_nested(end, "sum", "bits_per_second")
    if bps is None:
        bps = _get_nested(end, "sum_received", "bits_per_second")
    mbps = (bps / 1e6) if isinstance(bps, (int, float)) else None

    jitter_ms = _get_nested(end, "sum", "jitter_ms")
    if jitter_ms is None:
        jitter_ms = _get_nested(end, "sum_received", "jitter_ms")

    loss_pct = _get_nested(end, "sum", "lost_percent")
    if loss_pct is None:
        loss_pct = _get_nested(end, "sum_received", "lost_percent")

    return {"throughput_mbps": mbps, "jitter_ms": jitter_ms, "packet_loss_pct": loss_pct}


# ----------------------------
# ping
# ----------------------------

def _run_ping(target_ip: str, packet_count: int) -> Tuple[str, Optional[str]]:
    cmd = ["ping", "-n", "-c", str(packet_count), target_ip]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=packet_count + 20)
    except subprocess.TimeoutExpired as exc:
        return "", f"TimeoutExpired: {exc}"

    out = proc.stdout or ""
    if proc.returncode != 0:
        return out, (proc.stderr.strip() or f"rc={proc.returncode}")

    return out, None


def _parse_ping_metrics(ping_output: str) -> Dict[str, Any]:
    loss = None
    rtt_min = rtt_avg = rtt_max = rtt_mdev = None

    m_loss = re.search(r"(\d+(?:\.\d+)?)%\s+packet loss", ping_output)
    if m_loss:
        loss = float(m_loss.group(1))

    m_rtt = re.search(
        r"rtt\s+min/avg/max/(?:mdev|stddev)\s*=\s*([\d\.]+)/([\d\.]+)/([\d\.]+)/([\d\.]+)\s*ms",
        ping_output,
    )
    if m_rtt:
        rtt_min = float(m_rtt.group(1))
        rtt_avg = float(m_rtt.group(2))
        rtt_max = float(m_rtt.group(3))
        rtt_mdev = float(m_rtt.group(4))

    return {
        "rtt_min_ms": rtt_min,
        "rtt_avg_ms": rtt_avg,
        "rtt_max_ms": rtt_max,
        "rtt_mdev_ms": rtt_mdev,
        "packet_loss_pct": loss,
    }


# ----------------------------
# Export to main influx + verify
# ----------------------------

def _export_and_verify_main_influx(result: Dict[str, Any]) -> None:
    """
    Writes a compact point to MAIN influx and verifies by querying for run_uuid.
    Writes meta.main_influx = {...} into the result dict.
    """
    meta = result.get("meta", {}) or {}
    metrics = result.get("metrics", {}) or {}

    mi: Dict[str, Any] = {
        "enabled": False,
        "write_ok": False,
        "verify_ok": False,
        "verify_rows": 0,
        "error": None,
        "measurement": "bddbench_network_result",
    }
    meta["main_influx"] = mi  # always attach

    try:
        from influxdb_client import InfluxDBClient, Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS
    except Exception:
        mi["error"] = "influxdb_client not available"
        return

    url = os.getenv("INFLUXDB_MAIN_URL")
    token = os.getenv("INFLUXDB_MAIN_TOKEN")
    org = os.getenv("INFLUXDB_MAIN_ORG")
    bucket = os.getenv("INFLUXDB_MAIN_BUCKET")

    if not url or not token or not org or not bucket:
        mi["error"] = "MAIN influx env not set (INFLUXDB_MAIN_URL/TOKEN/ORG/BUCKET)"
        return

    mi["enabled"] = True

    run_uuid = meta.get("run_uuid") or meta.get("result_uuid") or str(uuid.uuid4())
    meta["run_uuid"] = run_uuid

    # Build point
    p = (
        Point(mi["measurement"])
        .tag("run_uuid", str(run_uuid))
        .tag("mode", str(meta.get("mode", "")))
        .tag("protocol", str(meta.get("protocol", "")))
        .tag("direction", str(meta.get("direction", "")))
        .tag("peer_host", str(meta.get("peer_host", "")))
        .tag("peer_ip", str(meta.get("peer_ip", "")))
        .tag("runner_host", str(meta.get("runner_host", "")))
        .tag("runner_ip", str(meta.get("runner_ip", "")))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )

    # numeric fields
    for k in [
        "throughput_mbps",
        "jitter_ms",
        "packet_loss_pct",
        "rtt_min_ms",
        "rtt_avg_ms",
        "rtt_max_ms",
        "rtt_mdev_ms",
    ]:
        v = metrics.get(k)
        if isinstance(v, (int, float)):
            p = p.field(k, float(v))

    # meta numeric fields
    for k in ["parallel_streams", "duration_s", "packet_count", "iperf_port"]:
        v = meta.get(k)
        if isinstance(v, int):
            p = p.field(k, int(v))

    # write + verify
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        if not client.ping():
            mi["error"] = "MAIN influx ping failed"
            client.close()
            return

        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket, org=org, record=p)
        mi["write_ok"] = True

        # verify by run_uuid within last 10 minutes
        start = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        flux = f'''
from(bucket: "{bucket}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "{mi["measurement"]}")
  |> filter(fn: (r) => r.run_uuid == "{run_uuid}")
  |> limit(n: 20)
'''
        mi["verify_query"] = flux.strip()

        tables = client.query_api().query(flux, org=org)
        rows = 0
        for t in tables:
            rows += len(t.records)
        mi["verify_rows"] = rows
        mi["verify_ok"] = rows > 0

        client.close()
    except Exception as exc:
        mi["error"] = repr(exc)
        return


# ----------------------------
# Behave steps
# ----------------------------

@when('I run an iperf3 "{protocol}" benchmark to "{target_host}" with {parallel_streams:d} streams for {duration_s:d} seconds')
def step_run_iperf3_benchmark(context, protocol, target_host, parallel_streams, duration_s):
    target_host = os.getenv("BDD_NET_TARGET_HOST", target_host).strip()

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

    raw = None
    err = None
    meta_extra: Dict[str, Any] = {}
    server_info: Dict[str, Any] = {}

    try:
        if direction == "runner->target":
            server_info = _start_remote_iperf3_server_oneoff(target_host=target_host, port=iperf_port)

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
                client_host=target_host,
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

    # stable run_uuid across scenarios
    if not hasattr(context, "run_uuid"):
        context.run_uuid = str(uuid.uuid4())

    context.network_result = {
        "meta": {
            "run_uuid": context.run_uuid,

            "mode": "iperf3",
            "protocol": protocol.lower(),
            "direction": direction,

            "runner_host": runner_host,
            "runner_ip": runner_ip,
            "peer_host": target_host,
            "peer_ip": peer_ip,
            "peer_ip_origin": origin,

            "parallel_streams": parallel_streams,
            "duration_s": duration_s,
            "iperf_port": iperf_port,
            "udp_bitrate": udp_bitrate if protocol.lower() == "udp" else None,
            "connect_timeout_ms": connect_timeout_ms,

            "server_start": server_info,
            "client_meta": meta_extra,
            "error": err,
        },
        "metrics": {
            **metrics,
            "raw": raw,
        },
        "created_at_epoch_s": time.time(),
        "created_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@when('I run a ping benchmark to "{target_host}" with {packet_count:d} packets')
def step_run_ping_benchmark(context, target_host, packet_count):
    target_host = os.getenv("BDD_NET_TARGET_HOST", target_host).strip()
    peer_ip, origin = _resolve_target_ip(target_host)

    runner_host = socket.gethostname()
    runner_ip = os.getenv("BDD_NET_RUNNER_IP") or _guess_runner_ip() or ""

    out, err = _run_ping(target_ip=peer_ip, packet_count=packet_count)
    m = _parse_ping_metrics(out)

    if not hasattr(context, "run_uuid"):
        context.run_uuid = str(uuid.uuid4())

    context.network_result = {
        "meta": {
            "run_uuid": context.run_uuid,

            "mode": "ping",
            "protocol": "icmp",
            "direction": "runner->target",

            "runner_host": runner_host,
            "runner_ip": runner_ip,

            "peer_host": target_host,
            "peer_ip": peer_ip,
            "peer_ip_origin": origin,

            "packet_count": packet_count,
            "error": err,
        },
        "metrics": {
            **m,
            "raw": out,
        },
        "created_at_epoch_s": time.time(),
        "created_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@then('I store the network benchmark result as "{outfile}"')
def step_store_network_result(context, outfile):
    result = getattr(context, "network_result", None)
    if not isinstance(result, dict):
        raise AssertionError("network_result missing in context (did you run a benchmark step first?)")

    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # export to main influx + verify (writes meta.main_influx)
    _export_and_verify_main_influx(result)

    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    logger.info("Stored network benchmark report: %s", str(out_path))

