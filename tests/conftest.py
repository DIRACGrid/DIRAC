import os
from pathlib import Path

import pytest
from packaging.version import Version

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


@pytest.fixture(name="serverIsOlderThan")
def fixtureServerIsOlderThan():
    """Return a callable telling whether the server behind a client predates a given version.

    These integration tests are also run by the "Backward Compatibility" CI job, which points
    the tests of a release branch at a server installed from a more recent branch. Use this to
    guard the assertions covering an API that a later release is known to have dropped::

        def test_something(someClient, serverIsOlderThan):
            if serverIsOlderThan(someClient, "9.1"):
                assert someClient.methodDroppedIn91()["OK"]

    :param client: any :class:`~DIRAC.Core.Base.Client.Client` talking to the service of interest
    :param str version: the version to compare the server against
    """

    def _serverIsOlderThan(client, version):
        res = client.ping()
        assert res["OK"], res["Message"]
        return Version(res["Value"]["version"]) < Version(version)

    return _serverIsOlderThan
