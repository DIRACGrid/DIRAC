"""
Test Config for External Libraries:

- External libs may use the standard logging library with a different configuration
- We want to make sure this does not affect the DIRAC logging system based on it

"""
import logging
from io import StringIO
import pytest

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, cleaningLog, gLoggerReset


@pytest.mark.parametrize(
    "isEnabled, loggingLevel, numberHandlers, handlerType",
    [
        (True, logging.DEBUG, 1, logging.StreamHandler),
        (False, logging.DEBUG, 1, logging.NullHandler),
    ],
)
def test_logsFromExtLibsHandlers(isEnabled, loggingLevel, numberHandlers, handlerType):
    """
    Check that handlers attached are different according to the value of enableLogsFromExternalLibs()
    """
    gLoggerReset()
    # when enable, Logging should also report logs from external library
    # a StreamHandler should be attached to the root logger by default
    if isEnabled:
        gLogger.enableLogsFromExternalLibs()
    # in the other case, Logging should not report logs from external library
    # a NullHandler disables the emission of the log records going to the root logger
    else:
        gLogger.disableLogsFromExternalLibs()

    handlers = logging.getLogger().handlers

    assert logging.getLogger().getEffectiveLevel() == loggingLevel

    assert numberHandlers == len(handlers)
    assert isinstance(handlers[0], handlerType)


@pytest.mark.parametrize(
    "isEnabled, loggerName, message, expected",
    [
        (True, "", "message", "ExternalLibrary/root INFO: message\n"),
        (True, "sublog", "message", "ExternalLibrary/sublog INFO: message\n"),
        (False, "", "message", ""),
        (False, "sublog", "message", ""),
    ],
)
def test_logsFromExtLibsLogs(isEnabled, loggerName, message, expected):
    """
    Check whether logs are displayed according to the value of enableLogsFromExternalLibs()
    """
    gLoggerReset()
    # when enable, Logging should also report logs from external library
    # logs from external libs should appear
    if isEnabled:
        gLogger.enableLogsFromExternalLibs()
    # in the other case, Logging should not report logs from external library
    # logs from external libs shouldn't appear
    else:
        gLogger.disableLogsFromExternalLibs()

    # modify the output to capture logs of the root logger
    bufferRoot = StringIO()
    logging.getLogger().handlers[0].stream = bufferRoot

    logging.getLogger(loggerName).info(message)
    logstring = cleaningLog(bufferRoot.getvalue())

    assert expected == logstring


def test_logsFromExtLibsPropag():
    """
    Test the no propagation of the logs from the Logging objects to the root logger of 'logging'
    """
    capturedBackend, _, _ = gLoggerReset()
    gLogger.enableLogsFromExternalLibs()

    # modify the output to capture logs of the root logger
    bufferRoot = StringIO()
    logging.getLogger().handlers[0].stream = bufferRoot

    gLogger.error("message")

    assert capturedBackend.getvalue() != ""
    assert bufferRoot.getvalue() == ""


def test_logsFromExtLibsMultCalls():
    """
    Calls the method several times to see whether we have duplication of the logs
    """
    gLoggerReset()
    for i in range(5):
        gLogger.enableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.StreamHandler)

    for i in range(5):
        gLogger.disableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.NullHandler)

    gLogger.enableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.NullHandler)

    gLogger.disableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    gLogger.disableLogsFromExternalLibs()
    gLogger.enableLogsFromExternalLibs()
    handlers = logging.getLogger().handlers

    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.StreamHandler)
