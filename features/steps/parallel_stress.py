#!/usr/bin/env python3
import argparse
import subprocess
import sys
from typing import List


def run_remote_systemctl(host: str, action: str, preset: str) -> None:
    """
    Run 'sudo systemctl <action> stress@<preset>.service' on the remote host via ssh.
    Raises CalledProcessError on failure.
    """
    unit = f"stress@{preset}.service"
    cmd = ["ssh", host, "sudo", "systemctl", action, unit]
    print(f"[INFO] Running remote command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def start_presets(host: str, presets: List[str]) -> None:
    for preset in presets:
        if not preset:
            continue
        run_remote_systemctl(host, "start", preset)


def stop_presets(host: str, presets: List[str]) -> None:
    # in reverse order, just to be nice
    for preset in reversed(presets):
        if not preset:
            continue
        try:
            run_remote_systemctl(host, "stop", preset)
        except subprocess.CalledProcessError as exc:
            # don't kill the whole script if stopping fails, just warn
            print(f"[WARN] Failed to stop preset {preset} on {host}: {exc}", file=sys.stderr)


def run_behave(feature: str, tags: str | None, name: str | None) -> int:
    """
    Run behave with the given feature and optional tag / name filters.
    Returns the behave exit code.
    """
    cmd = ["behave", "-i", feature]

    # if a scenario name is given, prefer that
    if name:
        cmd.extend(["-n", name])
    elif tags:
        cmd.extend(["-t", tags])

    print(f"[INFO] Running behave: {' '.join(cmd)}")
    proc = subprocess.run(cmd)
    return proc.returncode


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run systemd stress-ng presets in parallel with a Behave benchmark."
    )
    parser.add_argument(
        "--host",
        default="dsp25-sut-influx",
        help="Host on which stress@*.service runs (default: dsp25-sut-influx)",
    )
    parser.add_argument(
        "--presets",
        default="",
        help='Comma-separated list of stress presets, e.g. "cpu4,mem1g". '
             "These must exist under /etc/stress-ng/<preset> on the host.",
    )
    parser.add_argument(
        "--feature",
        required=True,
        help="Path to the Behave feature file, e.g. features/influx_query_benchmark.feature",
    )
    parser.add_argument(
        "--tags",
        default=None,
        help="Optional Behave tag filter, e.g. 'influx_query' or 'influx_write and not slow'",
    )
    parser.add_argument(
        "--name",
        default=None,
        help='Optional Behave scenario name filter for "-n", e.g. '
             '"Influx query benchmark under load". If set, overrides --tags.',
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    presets = [p.strip() for p in args.presets.split(",") if p.strip()]
    host = args.host

    print(f"[INFO] Stress host: {host}")
    print(f"[INFO] Stress presets: {presets if presets else 'none'}")
    print(f"[INFO] Feature: {args.feature}")

    exit_code = 1
    try:
        if presets:
            print("[INFO] Starting stress presets...")
            start_presets(host, presets)
        else:
            print("[INFO] No stress presets configured, running benchmark without host stress.")

        exit_code = run_behave(args.feature, args.tags, args.name)
    finally:
        if presets:
            print("[INFO] Stopping stress presets...")
            stop_presets(host, presets)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
