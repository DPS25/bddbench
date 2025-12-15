import os
import subprocess
from typing import Any, Dict, List, Optional

def _get_sut_ssh_target() -> str:
    """
    Determine where to run your stress tool (stress-ng, fio, ...) via SSH.

    Expected env vars (in .env, loaded by environment.py):
      - SUT_SSH: hostname or IP of the SUT (e.g. 192.168.8.34)
    """
    host = os.getenv("SUT_SSH")

    if not host:
        raise AssertionError(
            "SUT_SSH is not set. "
            "Add it to your .env so we know which SUT to run fio on."
        )

    return host


def _run_on_sut(cmd: list[str]) -> subprocess.CompletedProcess:
    """
    Run a command on the SUT via ssh and return the CompletedProcess.
    """
    ssh_target = _get_sut_ssh_target()
    ssh_cmd = ["ssh", ssh_target, "--"] + cmd
    return _run(ssh_cmd)


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    """
    Run a command, capture stdout/stderr, and raise AssertionError on non-zero exit.
    """
    p = subprocess.run(cmd, text=True, capture_output=True)
    if p.returncode != 0:
        out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        raise AssertionError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n{out}")
    return p
