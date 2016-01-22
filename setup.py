""" Basic setuptools script for DIRAC
"""

# Python version check - not yet know if this can be handled in the
# setup() call.
import sys
if sys.version_info < (2, 7):
    sys.exit("Dirac requires Python 2.7 or above, running version is:\n"+sys.executable+"\n"+sys.version)

# Actual setuptools
from setuptools import setup, find_packages

setup(
    # Rough and ready metadata
    name = "DIRAC",
    version = "6.15.0",
    url="https://github.com/DIRACGRID/DIRAC",
    license = "GPLv3",

    # Exclude tests from install
    packages = find_packages(exclude=["*.tests", "*.tests.*"]),

    # Integrate testing as recommended on pytest site:
    # https://pytest.org/latest/goodpractises.html#integrating-with-setuptools-python-setup-py-test-pytest-runner
    setup_requires = ["pytest-runner"],
    tests_require = ["pytest", "mock"],

    # NOT COMPLETE
    # Also not totally clear all are needed from survey of import statements
    # No split yet between client/server sides
    install_requires = ["simplejson", "pyparsing", "GSI"]
    )
