from __future__ import annotations

import argparse
import sys

from .cleanup import run_cleanup_cli


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bddbench")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("cleanup", help="Clean up bddbench test artifacts (dry-run by default).")
    c.add_argument("--target", choices=["sut", "main"], required=True, help="Where to clean up.")
    c.add_argument("--allow-main", action="store_true", help="Allow destructive operations on MAIN.")

    mode = c.add_mutually_exclusive_group()
    mode.add_argument("--delete-buckets", action="store_true", help="Delete buckets.")
    mode.add_argument("--delete-data", action="store_true", help="Delete data in buckets (keeps buckets).")

    c.add_argument("--from-state", action="store_true", help="Use reports/bddbench_state.json as source of targets.")
    c.add_argument("--state-file", default="reports/bddbench_state.json", help="State file path.")

    c.add_argument("--bucket", action="append", default=[], help="Bucket(s) to target explicitly (repeatable).")
    c.add_argument("--bucket-prefix", default=None, help="Bucket prefix fallback (lists & targets buckets by name).")
    c.add_argument("--exclude-bucket", action="append", default=[], help="Bucket(s) to exclude (repeatable).")

    c.add_argument("--measurement", action="append", default=[], help="Measurement(s) used for delete-data (repeatable).")
    c.add_argument("--run-id", action="append", default=[], help="Run-id(s) used for delete-data (repeatable).")

    c.add_argument("--start", default="1970-01-01T00:00:00Z", help="RFC3339 start for delete-data.")
    c.add_argument("--stop", default=None, help="RFC3339 stop for delete-data (default: now).")
    c.add_argument("--predicate", default="", help="Extra delete predicate, appended with AND. Usually leave empty.")

    c.add_argument("--list", action="store_true", help="Only list what would be deleted.")
    c.add_argument("--yes", action="store_true", help="Perform deletion. Without --yes it's dry-run.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "cleanup":
        return run_cleanup_cli(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

