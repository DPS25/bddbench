#!/usr/bin/env python3
import argparse
import logging
import shutil
import subprocess
import sys
from typing import List
from behave import given, when, then

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

class StressNGError(RuntimeError):
    pass

def ensure_stress_ng() -> None:
    """Check that stress-ng is available on the system PATH."""
    if shutil.which("stress-ng") is None:
        raise StressNGError("stress-ng not found on PATH. Please install it.")

def run_stress(cmd: List[str], description: str) -> None:
    """Run a single stress-ng command and raise an exception on failure."""
    logging.info("start: %s", description)
    logging.info("command: %s", " ".join(cmd))

    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        raise StressNGError(
            f"stress-ng failed with exit code {result.returncode} for: {description}"
        )

    logging.info("finished: %s", description)

def run_vm_stress_test(
    *,
    duration: str = "60s",
    vm_workers: int = 1,
    vm_bytes: str = "70%",
    hdd_workers: int = 1,
    hdd_bytes: str = "1G",
    network_all: int = 1,
    enable_memory: bool = True,
    enable_storage: bool = True,
    enable_network: bool = True,
) -> None:
    """
    Run a configurable host stress test using stress-ng.

    This function is intentionally generic so it can be used
    directly from Behave step definitions or from other Python code.
    """
    ensure_stress_ng()

    # Memory stress
    if enable_memory and vm_workers > 0 and vm_bytes:
        run_stress(
            [
                "stress-ng",
                "--vm",
                str(vm_workers),
                "--vm-bytes",
                str(vm_bytes),
                "--timeout",
                duration,
            ],
            f"Memory stress (vm={vm_workers}, vm-bytes={vm_bytes}, duration={duration})",
        )
    else:
        logging.info("Skipping memory stress (disabled or zero workers).")

    # Storage stress
    if enable_storage and hdd_workers > 0 and hdd_bytes:
        run_stress(
            [
                "stress-ng",
                "--hdd",
                str(hdd_workers),
                "--hdd-bytes",
                str(hdd_bytes),
                "--timeout",
                duration,
            ],
            f"Storage stress (hdd={hdd_workers}, hdd-bytes={hdd_bytes}, duration={duration})",
        )
    else:
        logging.info("Skipping storage stress (disabled or zero workers).")

    # Network stress
    if enable_network and network_all > 0:
        run_stress(
            [
                "stress-ng",
                "--class",
                "network",
                "--all",
                str(network_all),
                "--timeout",
                duration,
            ],
            f"Network stress (network class, all={network_all}, duration={duration})",
        )
    else:
        logging.info("Skipping network stress (disabled or zero workers).")

    logging.info("VM host stress test (memory, storage, network) finished.")


# ---------- CLI entry point (manual shell usage) -------------------------


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Configurable VM host stress test using stress-ng.",
    )

    parser.add_argument(
        "--duration",
        default="60s",
        help='Duration for each stress-ng run, e.g. "60s", "2m". Default: 60s',
    )

    mem = parser.add_argument_group("memory")
    mem.add_argument("--vm-workers", type=int, default=1)
    mem.add_argument("--vm-bytes", default="70%")
    mem.add_argument("--disable-memory", action="store_true")

    hdd = parser.add_argument_group("storage")
    hdd.add_argument("--hdd-workers", type=int, default=1)
    hdd.add_argument("--hdd-bytes", default="1G")
    hdd.add_argument("--disable-storage", action="store_true")

    net = parser.add_argument_group("network")
    net.add_argument("--network-all", type=int, default=1)
    net.add_argument("--disable-network", action="store_true")

    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    try:
        run_vm_stress_test(
            duration=args.duration,
            vm_workers=args.vm_workers,
            vm_bytes=args.vm_bytes,
            hdd_workers=args.hdd_workers,
            hdd_bytes=args.hdd_bytes,
            network_all=args.network_all,
            enable_memory=not args.disable_memory,
            enable_storage=not args.disable_storage,
            enable_network=not args.disable_network,
        )
    except StressNGError as exc:
        logging.error(str(exc))
        sys.exit(1)


# ------------------------ Behave step definitions ---------------------------


@given("stress-ng is installed on the host")
def step_stress_ng_installed(context):
    if shutil.which("stress-ng") is None:
        raise AssertionError("stress-ng is not installed on the host")

@when(
    'I run the vm stress test for "{duration}" with vm {vm_workers:d}, "{vm_bytes}", '
    'hdd {hdd_workers:d}, "{hdd_bytes}" and network {network_all:d}'
)
def step_run_vm_stress(context, duration, vm_workers, vm_bytes, hdd_workers, hdd_bytes, network_all):
    """
    Take the values directly from the Scenario Outline
    and call the generic vm stress test function.
    """
    try:
        run_vm_stress_test(
            duration=duration,
            vm_workers=vm_workers,
            vm_bytes=vm_bytes,
            hdd_workers=hdd_workers,
            hdd_bytes=hdd_bytes,
            network_all=network_all,
        )
        context.stress_failed = False
    except StressNGError as exc:
        context.stress_failed = True
        context.stress_error = str(exc)


@then("the vm stress test should complete successfully")
def step_vm_stress_success(context):
    if getattr(context, "stress_failed", False):
        raise AssertionError(
            f"vm_stress-ng test failed: {getattr(context, 'stress_error', 'unknown error')}"
        )


if __name__ == "__main__":
    main()
