from behave import given
from src.utils import _run_on_sut

@given("sysbench is installed")
def step_sysbench_installed(context):
    try:
        _run_on_sut(["sysbench", "--version"])
    except Exception as e:
        raise AssertionError(
            "sysbench not found on SUT. Install sysbench there or add pkgs.sysbench to flake.nix"
        ) from e
