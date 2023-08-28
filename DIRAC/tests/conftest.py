import os
from pathlib import Path

import pytest

import DIRAC


# Adds the --runslow command line arg based on the example in the docs
# https://docs.pytest.org/en/stable/example/simple.html
# #control-skipping-of-tests-according-to-command-line-option
def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption(
        "--no-check-dirac-environment",
        action="store_false",
        dest="check_dirac_environment",
        help="Allow pytest to be ran when credentials and a dirac.cfg file are available",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)

    if config.getoption("check_dirac_environment"):
        _check_environment()


def _check_environment():
    """Ensure the environment is safe for running tests"""
    errors = []

    dirac_cfg = Path(DIRAC.rootPath) / "etc" / "dirac.cfg"
    if dirac_cfg.exists():
        errors += [f"    * Found dirac.cfg file {dirac_cfg}\n"]
    user_proxy_path = Path("/tmp") / f"x509up_u{os.getuid()}"
    if user_proxy_path.exists():
        errors += [f"    * Found possible proxy file at {user_proxy_path}\n"]
    if "X509_USER_PROXY" in os.environ:
        errors += [f"    * X509_USER_PROXY is set\n"]

    if errors:
        pytest.exit(
            f"ERROR: Found potential issues with your environment which are "
            f"likely to cause test failures.\n{''.join(errors)}If you want to "
            "bypass this check pass --no-check-dirac-environment to pytest.",
            returncode=42,
        )
