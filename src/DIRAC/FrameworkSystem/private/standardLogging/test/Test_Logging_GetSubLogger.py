"""
Test SubLogger
"""
import pytest
from flaky import flaky
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels


from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, gLoggerReset


def test_getSubLoggerLogRecord():
    """
    Create some subloggers and create a log record
    """
    capturedBackend, log, sublog = gLoggerReset()

    # Send a first log with a simple sublogger
    log.always("message")
    assert " Framework/log " in capturedBackend.getvalue()

    # Reinitialize the buffer and send a log with a child of the sublogger
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    sublog.always("message")
    assert " Framework/log/sublog " in capturedBackend.getvalue()

    # Generate a new sublogger from sublog
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    subsublog = sublog.getSubLogger("subsublog")
    subsublog.always("message")
    assert " Framework/log/sublog/subsublog " in capturedBackend.getvalue()


def test_getSubLoggerObject():
    """
    Create a sublogger, set its level, get a sublogger with the same name and check that is it the same object
    """
    _, _, _ = gLoggerReset()

    log = gLogger.getSubLogger("log")
    log.setLevel("notice")
    anotherLog = gLogger.getSubLogger("log")

    assert log.getLevel() == anotherLog.getLevel()
    assert log == anotherLog


# Run the tests for all the log levels and exceptions
# We may need to rerun the test if we are unlucky and the timestamps
# don't match
@flaky(max_runs=3)
@pytest.mark.parametrize("logLevel", ["exception"] + [lvl.lower() for lvl in LogLevels.getLevelNames()])
def test_localSubLoggerObject(logLevel):
    """
    Create a local subLogger and compare its output with the standard subLogger
    for all the log levels
    """
    capturedBackend, log, _ = gLoggerReset()
    # Set the level to debug to always make sure that something is printed
    log.setLevel("debug")

    # Create a real subLogger and a localSubLogger
    # with the same "name"
    subLog = log.getSubLogger("child")
    localSubLog = log.getSubLogger("child")

    # Print and capture a message with the real sublogger
    capturedBackend.truncate(0)
    capturedBackend.seek(0)
    getattr(subLog, logLevel)(logLevel)
    subMsg = capturedBackend.getvalue()

    # Print and capture a message with the local sublogger
    capturedBackend.truncate(0)
    capturedBackend.seek(0)
    getattr(localSubLog, logLevel)(logLevel)
    locMsg = capturedBackend.getvalue()

    # Compare the output
    assert subMsg == locMsg
