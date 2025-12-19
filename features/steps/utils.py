import os
import platform
import subprocess
import logging
import re
from typing import List, Optional

logger = logging.getLogger("bddbench.utils")

# SUT helper:

def _run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    logger.debug("Running command: %s", " ".join(cmd))
    p = subprocess.run(cmd, text=True, capture_output=True)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    if p.returncode != 0:
        raise AssertionError(
            f"Command failed ({p.returncode}): {' '.join(cmd)}\n{out}"
        )
    return p


def _get_sut_ssh_target() -> Optional[str]:
    return os.getenv("SUT_SSH")


def _run_on_sut(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    target = _get_sut_ssh_target()
    if target:
        ssh_cmd = ["ssh", target, "--", *cmd]
        logger.debug("Running on SUT (%s): %s", target, " ".join(cmd))
        return _run(ssh_cmd)
    else:
        logger.debug("Running locally (no SUT_SSH): %s", " ".join(cmd))
        return _run(cmd)


def _sut_host_identifier() -> str:
    target = _get_sut_ssh_target()
    if target:
        return target
    return platform.node()


# Size-Parsing Helpers:

_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMG]?)\s*$", re.IGNORECASE)


def _size_to_bytes(value: str) -> int:
    m = _SIZE_RE.match(value)
    if not m:
        raise AssertionError(
            f"Invalid size format: {value!r} (expected e.g. 1K/1M/1G)"
        )
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[suffix]
    return int(num * mult)
