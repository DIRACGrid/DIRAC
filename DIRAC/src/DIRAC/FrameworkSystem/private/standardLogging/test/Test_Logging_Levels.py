"""
Test Levels

- shown
- getLevel
- setLevel

"""
import pytest

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, gLoggerReset, cleaningLog


def test_getAllPossibleLevels():
    """
    Make sure getAllPossibleLevels returns every existing level
    """
    _, log, sublog = gLoggerReset()
    levels = ["DEBUG", "VERBOSE", "INFO", "WARN", "NOTICE", "ERROR", "ALWAYS", "FATAL"]
    assert sorted(gLogger.getAllPossibleLevels()) == sorted(levels)
    assert sorted(log.getAllPossibleLevels()) == sorted(levels)
    assert sorted(sublog.getAllPossibleLevels()) == sorted(levels)


def test_setLevelInit():
    """
    Test setLevel and getLevel: initialization of gLogger
    """
    _, log, sublog = gLoggerReset()

    # make sure gLogger and its subloggers have a common level when initialized
    # we don't set the level of gLogger
    assert gLogger.getLevel() == "NOTICE"
    assert log.getLevel() == "NOTICE"
    assert sublog.getLevel() == "NOTICE"


def test_setLevelgLogger():
    """
    Test setLevel and getLevel: set gLogger level
    """
    _, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    # as log and sublog levels has not been changed, they should inherit from the same level
    for level in levels:
        gLogger.setLevel(level)
        assert gLogger.getLevel() == level.upper()
        assert log.getLevel() == level.upper()
        assert sublog.getLevel() == level.upper()


def test_setLevelLog():
    """
    Test setLevel and getLevel: set log level
    """
    _, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    # set gLogger level
    gLogger.setLevel("notice")
    # by changing log level, gLogger should not be affected, subLogger should be
    for level in levels:
        log.setLevel(level)
        assert gLogger.getLevel() == "NOTICE"
        assert log.getLevel() == level.upper()
        assert sublog.getLevel() == level.upper()


def test_setLevelSublog():
    """
    Test setLevel and getLevel: set sublog level
    """
    _, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    # set gLogger and log level
    gLogger.setLevel("notice")
    log.setLevel("warn")
    # by changing sublog level, gLogger and log should not be affected
    for level in levels:
        sublog.setLevel(level)
        assert gLogger.getLevel() == "NOTICE"
        assert log.getLevel() == "WARN"
        assert sublog.getLevel() == level.upper()


def test_setLevelStopPropagation():
    """
    Test setLevel and getLevel: set gLogger level while log and sublog have already be set
    """
    _, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    # by changing gLogger level, log and sublog should not be affected anymore as they have been changed manually
    log.setLevel("warn")
    sublog.setLevel("verbose")
    for level in levels:
        gLogger.setLevel(level)
        assert gLogger.getLevel() == level.upper()
        assert log.getLevel() == "WARN"
        assert sublog.getLevel() == "VERBOSE"


@pytest.mark.parametrize(
    "loggerLevel, isSuperiorTo, logRecordLevel,",
    [("debug", False, "error"), ("error", True, "debug"), ("info", False, "info")],
)
def test_setLevelShowngLogger(loggerLevel, isSuperiorTo, logRecordLevel):
    """
    Set gLogger level: check whether a log record should be displayed
    """
    capturedBackend, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    gLogger.setLevel(loggerLevel)

    # convert level name into its integer value
    logRecordLevelValue = LogLevels.getLevelValue(logRecordLevel)
    res = gLogger._createLogRecord(logRecordLevelValue, "message", "")

    # clean the log to remove unecessary information
    logstring = cleaningLog(capturedBackend.getvalue())

    # if loggerLevel is superior to logRecordLevel then:
    # - log record should not appear
    # - shown should return False as the log doesn't appear
    # - value returned by createLogRecord should be False too
    isLoggerLvlSupToLogRecordLvl = LogLevels.getLevelValue(loggerLevel) > logRecordLevelValue
    assert isLoggerLvlSupToLogRecordLvl == isSuperiorTo

    if isLoggerLvlSupToLogRecordLvl:
        assert not gLogger.shown(logRecordLevel)
        assert not res
        assert logstring == ""
    else:
        assert gLogger.shown(logRecordLevel)
        assert res
        assert logstring == f"Framework {logRecordLevel.upper()}: message\n"
        capturedBackend.truncate(0)
        capturedBackend.seek(0)


@pytest.mark.parametrize(
    "loggerLevel, isSuperiorTo, logRecordLevel,",
    [("debug", False, "error"), ("error", True, "debug"), ("info", False, "info")],
)
def test_setLevelShownLog(loggerLevel, isSuperiorTo, logRecordLevel):
    """
    Set log level: check whether a log record should be displayed
    """
    capturedBackend, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    gLogger.setLevel("notice")
    log.setLevel(loggerLevel)

    # convert level name into its integer value
    logRecordLevelValue = LogLevels.getLevelValue(logRecordLevel)
    res = log._createLogRecord(logRecordLevelValue, "message", "")

    # clean the log to remove unecessary information
    logstring = cleaningLog(capturedBackend.getvalue())

    # if loggLevel is superior to logRecordLevel then:
    # - log record should not appear
    # - shown should return False as the log doesn't appear
    # - value returned by createLogRecord should be False too
    isLoggerLvlSupToLogRecordLvl = LogLevels.getLevelValue(loggerLevel) > logRecordLevelValue
    assert isLoggerLvlSupToLogRecordLvl == isSuperiorTo

    if LogLevels.getLevelValue(loggerLevel) > logRecordLevelValue:
        assert not log.shown(logRecordLevel)
        assert not res
        assert logstring == ""
    else:
        assert log.shown(logRecordLevel)
        assert res
        assert logstring == f"Framework/log {logRecordLevel.upper()}: message\n"
        capturedBackend.truncate(0)
        capturedBackend.seek(0)


@pytest.mark.parametrize(
    "loggerLevel, isSuperiorTo, logRecordLevel,",
    [("debug", False, "error"), ("error", True, "debug"), ("info", False, "info")],
)
def test_setLevelShownSubLog(loggerLevel, isSuperiorTo, logRecordLevel):
    """
    Set sublog level: check whether a log record should be displayed
    """
    capturedBackend, log, sublog = gLoggerReset()
    levels = gLogger.getAllPossibleLevels()

    gLogger.setLevel("notice")
    log.setLevel("warn")
    sublog.setLevel(loggerLevel)

    # convert level name into its integer value
    logRecordLevelValue = LogLevels.getLevelValue(logRecordLevel)
    res = sublog._createLogRecord(logRecordLevelValue, "message", "")

    # clean the log to remove unecessary information
    logstring = cleaningLog(capturedBackend.getvalue())

    # if logLevel is superior to logRecordLevel then:
    # - log record should not appear
    # - shown should return False as the log doesn't appear
    # - value returned by createLogRecord should be False too
    isLoggerLvlSupToLogRecordLvl = LogLevels.getLevelValue(loggerLevel) > logRecordLevelValue
    assert isLoggerLvlSupToLogRecordLvl == isSuperiorTo

    if LogLevels.getLevelValue(loggerLevel) > logRecordLevelValue:
        assert not sublog.shown(logRecordLevel)
        assert not res
        assert logstring == ""
    else:
        assert sublog.shown(logRecordLevel)
        assert res
        assert logstring == f"Framework/log/sublog {logRecordLevel.upper()}: message\n"
    capturedBackend.truncate(0)
    capturedBackend.seek(0)
