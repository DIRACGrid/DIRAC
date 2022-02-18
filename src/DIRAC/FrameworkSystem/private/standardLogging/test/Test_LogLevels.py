"""
Test LogLevels module
"""
import logging
import pytest

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels


@pytest.mark.parametrize(
    "logLevel, value",
    [
        ("debug", logging.DEBUG),
        ("verbose", 15),
        ("info", 20),
        ("warn", logging.WARN),
        ("notice", 35),
        ("error", logging.ERROR),
        ("always", 45),
        ("fatal", logging.CRITICAL),
        ("unknown", None),
    ],
)
def test_getLevelValue(logLevel, value):
    """
    Test getLevelValue
    """
    assert LogLevels.getLevelValue(logLevel) == value


@pytest.mark.parametrize(
    "value, logLevel",
    [
        (logging.DEBUG, "debug"),
        (15, "verbose"),
        (20, "info"),
        (logging.WARN, "warn"),
        (35, "notice"),
        (logging.ERROR, "error"),
        (45, "always"),
        (logging.CRITICAL, "fatal"),
        (-1, None),
        ("error", None),
    ],
)
def test_getLevel(value, logLevel):
    """
    Test getLevel
    """
    if logLevel:
        logLevel = logLevel.upper()
    assert LogLevels.getLevel(value) == logLevel


def test_getLevelNames():
    """
    Test getLevelNames
    """
    levels = ["DEBUG", "VERBOSE", "INFO", "WARN", "NOTICE", "ERROR", "ALWAYS", "FATAL"]
    assert sorted(LogLevels.getLevelNames()) == sorted(levels)
