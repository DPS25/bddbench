#!/usr/bin/env python3
import argparse
import subprocess
import sys
from typing import List, Optional


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
    """
    Start all configured stress-ng presets on the given host.
    If one preset fails to start, the function raises and the caller decides what to do.
    """
    for preset in presets:
        try:
            run_remote_systemctl(host, "start", preset)
        except subprocess.CalledProcessError as exc:
            print(
                f"[ERROR] Failed to start preset {preset} on {host}: "
                f"Command exited with {exc.returncode}"
            )
            raise


def stop_presets(host: str, presets: List[str]) -> None:
    """
    Try to stop all configured stress-ng presets on the given host.
    Errors are logged as warnings but do not abort the script.
    """
    for preset in presets:
        try:
            run_remote_systemctl(host, "stop", preset)
        except subprocess.CalledProcessError as exc:
            print(
                f"[WARN] Failed to stop preset {preset} on {host}: "
                f"Command exited with {exc.returncode}"
            )


def run_behave(feature: str, tags: Optional[str], name: Optional[str]) -> int:
    """
    Run behave for the given feature file and either a scenario name OR tag expression.
    Returns the behave exit code.
    """
    cmd = ["behave", "-i", feature]

    if name:
        cmd.extend(["-n", name])
        print(f"[INFO] Running behave with scenario name filter: {name!r}")
    elif tags:
        cmd.extend(["-t", tags])
        print(f"[INFO] Running behave with tag filter: {tags!r}")
    else:
        print("[INFO] Running behave without name or tag filters.")

    print(f"[INFO] Running behave command: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run systemd-based stress-ng presets on a SUT host in parallel with "
            "Behave write/query benchmarks."
        )
    )

    parser.add_argument(
        "--host",
        required=True,
        help=(
            "SSH target for the SUT host, e.g. 'nixos@192.168.8.132' or a host alias "
            "that works from dsp25-main-influx."
        ),
    )
    parser.add_argument(
        "--presets",
        default="",
        help=(
            "Comma-separated list of stress-ng presets to start via systemd, "
            "e.g. 'cpu4,mem1g,io4'. These map to /etc/stress-ng/<preset> "
            "and stress@<preset>.service on the SUT."
        ),
    )
    parser.add_argument(
        "--feature",
        required=True,
        help="Path to the Behave feature file to run, e.g. "
             "'features/influx_query_benchmark.feature'.",
    )
    parser.add_argument(
        "--tags",
        default="",
        help=(
            "Optional Behave tag expression used with '-t'. Ignored if --name "
            "is provided."
        ),
    )
    parser.add_argument(
        "--name",
        default="",
        help=(
            "Optional Behave scenario name used with '-n'. "
            "If set, this takes precedence over --tags."
        ),
    )

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    host = args.host
    presets = [p.strip() for p in args.presets.split(",") if p.strip()]

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

        exit_code = run_behave(
            feature=args.feature,
            tags=args.tags or None,
            name=args.name or None,
        )

    finally:
        if presets:
            print("[INFO] Stopping stress presets...")
            stop_presets(host, presets)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
