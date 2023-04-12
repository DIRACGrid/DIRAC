"""
Test Logger Wrapper
"""
import logging
from io import StringIO

from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging


gLogger = LoggingRoot()


def cleaningLog(log):
    """
    Remove date and space from the log string
    """
    log = log.split("Z ", 1)[-1]
    return log


def captureBackend():
    """
    Dirac logger is wrapped by LoggingRoot and represent the root of the DIRAC logging system
    Modify the output to capture logs of LoggingRoot
    """
    bufferDirac = StringIO()
    if logging.getLogger("dirac").handlers:
        logging.getLogger("dirac").handlers[0].stream = bufferDirac
    return bufferDirac


def gLoggerReset():
    """
    Reinitialize gLogger as only one instance exists
    It avoids any unexpected behaviour due to multiple different usages
    """
    # Reinitialize the system/component name after other tests
    # because LoggingRoot is a singleton and can not be reinstancied
    Logging._componentName = "Framework"

    # reset gLogger
    gLogger.setLevel("notice")
    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)
    gLogger.showContexts(True)
    gLogger.showTimeStamps(True)

    # modify the output to capture the log records into a buffer
    bufferDirac = captureBackend()

    del logging.getLogger("dirac").handlers[1:]
    del gLogger._backendsList[1:]

    # reset log
    logging.getLogger("dirac").getChild("log").setLevel(logging.NOTSET)
    log = gLogger.getSubLogger("log")

    log.showHeaders(True)
    log.showThreadIDs(False)
    log.showContexts(True)
    log.showTimeStamps(True)
    for option in log._optionsModified:
        log._optionsModified[option] = False

    del logging.getLogger("dirac.log").handlers[:]
    del log._backendsList[:]

    # reset sublog
    logging.getLogger("dirac.log").getChild("sublog").setLevel(logging.NOTSET)
    sublog = log.getSubLogger("sublog")

    sublog.showHeaders(True)
    sublog.showThreadIDs(False)
    sublog.showContexts(True)
    sublog.showTimeStamps(True)
    for option in sublog._optionsModified:
        sublog._optionsModified[option] = False

    del logging.getLogger("dirac.log.sublog").handlers[:]
    del sublog._backendsList[:]

    return (bufferDirac, log, sublog)
