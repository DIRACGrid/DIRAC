""" Basic setuptools script for DIRAC
"""

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
