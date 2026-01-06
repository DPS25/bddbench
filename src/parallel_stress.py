#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from typing import List, Optional


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run behave benchmarks with optional stress-ng via behave hooks (before_step/after_step)."
    )
    p.add_argument("--feature", default="", help="Optional feature file (e.g. features/influx_write_benchmark.feature)")
    p.add_argument("--tags", default="", help='Behave tag expression (e.g. "write and normal and singlebucket")')
    p.add_argument("--name", default="", help="Optional scenario name (-n). Takes precedence over --tags if set.")
    p.add_argument("--formatter", default="progress3", help="Behave formatter (default: progress3)")
    p.add_argument("--host", default="", help='SSH target for SUT (sets env SUT_SSH), e.g. "nixos@192.168.8.132"')
    p.add_argument("--presets", default="cpu4", help='Stress presets passed as define stress_presets (default: "cpu4")')
    p.add_argument("--no-stress", action="store_true", help="Run without --define stress=true")
    p.add_argument("extra", nargs=argparse.REMAINDER, help="Extra behave args (e.g. --no-capture)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    if args.host:
        os.environ["SUT_SSH"] = args.host

    cmd = ["behave", "-f", args.formatter]

    if args.feature:
        cmd += ["-i", args.feature]

    if args.name:
        cmd += ["-n", args.name]
    elif args.tags:
        cmd += ["-t", args.tags]

    if not args.no_stress:
        cmd += ["--define", "stress=true"]
        if args.presets:
            cmd += ["--define", f"stress_presets={args.presets}"]

    if args.extra:
        cmd += args.extra

    print("[INFO] Running:", " ".join(cmd))
    raise SystemExit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
