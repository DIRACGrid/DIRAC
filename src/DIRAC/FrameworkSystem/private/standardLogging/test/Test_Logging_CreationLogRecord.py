"""
Test LogRecord Creation

- createLogRecord
- debug, verbose, info, warn, notice, error, fatal, always

"""
import pytest

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, gLoggerReset, cleaningLog


@pytest.mark.parametrize(
    "sMsg, sVarMsg, exc_info, expected",
    [
        ("message", "", False, "message"),
        ("this\nis\na\nmessage\non\nmultiple\nlines.", "", False, "this\nis\na\nmessage\non\nmultiple\nlines."),
        ("mess", "age", False, "mess age"),
        ("this\nis\na\nmessage\non\n", "multiple\nlines.", False, "this\nis\na\nmessage\non\n multiple\nlines."),
    ],
)
def test_createLogRecord(sMsg, sVarMsg, exc_info, expected):
    """
    Create logs of different levels with multiple logs
    """
    capturedBackend, log, sublog = gLoggerReset()

    # Set the level to debug
    gLogger.setLevel("debug")

    # dictionary of key = logger to use, value = output associated to the logger
    logDict = {gLogger: "", log: "/log", sublog: "/log/sublog"}

    # get list of existing levels, for each of them, a log record is created
    levels = gLogger.getAllPossibleLevels()
    for level in levels:
        for logger, logInfo in logDict.items():

            # createLogRecord is the method in charge of creating the log record
            # debug, ..., always methods wrap the following method
            # we use logLevels to get the int value corresponding to the level name
            logger._createLogRecord(LogLevels.getLevelValue(level), sMsg, sVarMsg, exc_info)

            # clean the log to remove unecessary information
            logstring = cleaningLog(capturedBackend.getvalue())
            logExpected = f"Framework{logInfo} {level}: {expected}\n"
            assert logExpected == logstring
            capturedBackend.truncate(0)
            capturedBackend.seek(0)


def test_showStack():
    """
    Get the showStack
    """
    capturedBackend, log, sublog = gLoggerReset()

    # dictionary of key = logger to use, value = output associated to the logger
    logDict = {gLogger: "", log: "/log", sublog: "/log/sublog"}
    for logger, logInfo in logDict.items():
        # By default, should not appear as the level is NOTICE
        logger.showStack()

        # clean the log to remove unecessary information
        logstring = cleaningLog(capturedBackend.getvalue())
        assert logstring == ""
        capturedBackend.truncate(0)
        capturedBackend.seek(0)

    # Set level to debug
    gLogger.setLevel("debug")

    for logger, logInfo in logDict.items():
        # The debug message should appear
        logger.showStack()

        # clean the log to remove unecessary information
        logstring = cleaningLog(capturedBackend.getvalue())
        assert logstring == "Framework%s DEBUG: \n" % logInfo
        capturedBackend.truncate(0)
        capturedBackend.seek(0)
