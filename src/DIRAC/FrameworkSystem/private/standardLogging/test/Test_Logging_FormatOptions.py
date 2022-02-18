"""
Test properties of log records
"""
import _thread
import pytest

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, gLoggerReset
from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import cleaningLog


def test_showFormatOptionsInit():
    """
    Check showHeaders and showThreadIDs methods at initialization
    """
    capturedBackend, log, sublog = gLoggerReset()

    # first, make sure options are inherited from gLogger
    assert gLogger._options["headerIsShown"]
    assert gLogger._options["timeStampIsShown"]
    assert gLogger._options["contextIsShown"]
    assert not gLogger._options["threadIDIsShown"]
    assert log._options["headerIsShown"] == gLogger._options["headerIsShown"]
    assert log._options["timeStampIsShown"] == gLogger._options["timeStampIsShown"]
    assert log._options["contextIsShown"] == gLogger._options["contextIsShown"]
    assert log._options["threadIDIsShown"] == gLogger._options["threadIDIsShown"]
    assert sublog._options["headerIsShown"] == log._options["headerIsShown"]
    assert sublog._options["timeStampIsShown"] == log._options["timeStampIsShown"]
    assert sublog._options["contextIsShown"] == log._options["contextIsShown"]
    assert sublog._options["threadIDIsShown"] == log._options["threadIDIsShown"]

    # create log records and check that the format is correct
    gLogger.notice("me")
    logstring = cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    log.notice("ss")
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    sublog.notice("age")
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    assert logstring == "Framework NOTICE: me\nFramework/log NOTICE: ss\nFramework/log/sublog NOTICE: age\n"


@pytest.mark.parametrize(
    "header, threadID, timeStamp, context, msg,\
                          expectedLog, isThreadIDAvailable, isTimeStampAvailable",
    [
        (False, False, False, False, "msg", "msg\nmsg\nmsg\n", False, False),
        (True, False, False, False, "m", "NOTICE: m\nNOTICE: m\nNOTICE: m\n", False, False),
        (False, True, False, False, "msg", "msg\nmsg\nmsg\n", False, False),
        (True, True, False, False, "m", "[%s] NOTICE: m\n[%s] NOTICE: m\n[%s] NOTICE: m\n", True, False),
        (False, False, True, False, "msg", "msg\nmsg\nmsg\n", False, False),
        (True, False, True, False, "m", "NOTICE: m\nNOTICE: m\nNOTICE: m\n", False, True),
        (False, True, True, False, "msg", "msg\nmsg\nmsg\n", False, False),
        (True, True, True, False, "m", "[%s] NOTICE: m\n[%s] NOTICE: m\n[%s] NOTICE: m\n", True, True),
        (False, False, False, True, "msg", "msg\nmsg\nmsg\n", False, False),
        (
            True,
            False,
            False,
            True,
            "m",
            "Framework NOTICE: m\nFramework/log NOTICE: m\nFramework/log/sublog NOTICE: m\n",
            False,
            False,
        ),
        (False, True, False, True, "msg", "msg\nmsg\nmsg\n", False, False),
        (
            True,
            True,
            False,
            True,
            "m",
            "Framework [%s] NOTICE: m\nFramework/log [%s] NOTICE: m\n" + "Framework/log/sublog [%s] NOTICE: m\n",
            True,
            False,
        ),
        (False, False, True, True, "msg", "msg\nmsg\nmsg\n", False, False),
        (
            True,
            False,
            True,
            True,
            "m",
            "Framework NOTICE: m\nFramework/log NOTICE: m\nFramework/log/sublog NOTICE: m\n",
            False,
            True,
        ),
        (False, True, True, True, "msg", "msg\nmsg\nmsg\n", False, False),
        (
            True,
            True,
            True,
            True,
            "m",
            "Framework [%s] NOTICE: m\nFramework/log [%s] NOTICE: m\n" + "Framework/log/sublog [%s] NOTICE: m\n",
            True,
            True,
        ),
    ],
)
def test_showFormatOptionsgLogger(
    header, threadID, timeStamp, context, msg, expectedLog, isThreadIDAvailable, isTimeStampAvailable
):
    """
    Set gLogger options, check that options are inherited in log and sublog
    """
    capturedBackend, log, sublog = gLoggerReset()

    # setting these values should modify the way the log record is displayed
    gLogger.showHeaders(header)
    gLogger.showThreadIDs(threadID)
    gLogger.showTimeStamps(timeStamp)
    gLogger.showContexts(context)

    # log and sublog should inherit from the changes
    assert gLogger._options["headerIsShown"] == header
    assert gLogger._options["threadIDIsShown"] == threadID
    assert log._options["headerIsShown"] == gLogger._options["headerIsShown"]
    assert log._options["timeStampIsShown"] == gLogger._options["timeStampIsShown"]
    assert log._options["contextIsShown"] == gLogger._options["contextIsShown"]
    assert log._options["threadIDIsShown"] == gLogger._options["threadIDIsShown"]
    assert sublog._options["headerIsShown"] == log._options["headerIsShown"]
    assert sublog._options["timeStampIsShown"] == log._options["timeStampIsShown"]
    assert sublog._options["contextIsShown"] == log._options["contextIsShown"]
    assert sublog._options["threadIDIsShown"] == log._options["threadIDIsShown"]

    # create log records and check the format is correct
    gLogger.notice(msg)
    logValue = capturedBackend.getvalue()
    # check that timestamp is available if it has to be available
    assert ("UTC" in logValue) == isTimeStampAvailable
    logstring = cleaningLog(logValue)
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    log.notice(msg)
    logValue = capturedBackend.getvalue()
    assert ("UTC" in logValue) == isTimeStampAvailable
    logstring += cleaningLog(logValue)
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    sublog.notice(msg)
    logValue = capturedBackend.getvalue()
    assert ("UTC" in logValue) == isTimeStampAvailable
    logstring += cleaningLog(logValue)
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    # check that threadID is present in the log when it should be present
    threadIDValue = str(_thread.get_ident())
    assert (threadIDValue in logstring) == isThreadIDAvailable
    # as thread ID depends on the execution, we have to add it to the expected results
    if isThreadIDAvailable:
        expectedLog = expectedLog % (threadIDValue, threadIDValue, threadIDValue)
    assert expectedLog == logstring


@pytest.mark.parametrize(
    "header, threadID, msg, expectedLog, isThreadIDAvailable",
    [
        (False, False, "message", "message\nmessage\n", False),
        (True, False, "message", "Framework/log NOTICE: message\nFramework/log/sublog NOTICE: message\n", False),
        (False, True, "message", "message\nmessage\n", False),
        (
            True,
            True,
            "message",
            "Framework/log [%s] NOTICE: message\nFramework/log/sublog [%s] NOTICE: message\n",
            True,
        ),
    ],
)
def test_showFormatOptionsLog(header, threadID, msg, expectedLog, isThreadIDAvailable):
    """
    Set log (child of gLogger) options, check that options are inherited in sublog
    """
    capturedBackend, log, sublog = gLoggerReset()

    # set gLogger options, they should not be modified
    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)

    # set log options
    log.showHeaders(header)
    log.showThreadIDs(threadID)

    # sublog should inherit from the changes, gLogger should not be affected
    assert gLogger._options["headerIsShown"]
    assert not gLogger._options["threadIDIsShown"]
    assert log._options["headerIsShown"] == header
    assert log._options["threadIDIsShown"] == threadID
    assert sublog._options["headerIsShown"] == log._options["headerIsShown"]
    assert sublog._options["threadIDIsShown"] == log._options["threadIDIsShown"]

    # create log records and check the format is correct
    gLogger.notice(msg)
    logstring = cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    log.notice(msg)
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    sublog.notice(msg)
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    expectedLog = "Framework NOTICE: message\n" + expectedLog

    threadIDValue = str(_thread.get_ident())
    assert (threadIDValue in logstring) == isThreadIDAvailable
    # as thread ID depends on the execution, we have to add it to the expected results
    if isThreadIDAvailable:
        expectedLog = expectedLog % (threadIDValue, threadIDValue)
    assert expectedLog == logstring


@pytest.mark.parametrize(
    "header, threadID, msg, expectedLog, isThreadIDAvailable",
    [
        (False, False, "message", "message\n", False),
        (True, False, "message", "Framework/log/sublog NOTICE: message\n", False),
        (False, True, "message", "message\n", False),
        (True, True, "message", "Framework/log/sublog [%s] NOTICE: message\n", True),
    ],
)
def test_showFormatOptionsSubLog(header, threadID, msg, expectedLog, isThreadIDAvailable):
    """
    Set sublog (child of log) options
    """
    capturedBackend, log, sublog = gLoggerReset()

    # set gLogger and log options, sublog options should not be modified
    gLogger.showHeaders(True)
    gLogger.showThreadIDs(False)
    log.showHeaders(False)
    log.showThreadIDs(False)

    # set sublog options
    sublog.showHeaders(header)
    sublog.showThreadIDs(threadID)

    # log should inherit from the options of gLogger, subLog shoud not inherit from log
    assert gLogger._options["headerIsShown"]
    assert not gLogger._options["threadIDIsShown"]
    assert not log._options["headerIsShown"]
    assert not log._options["threadIDIsShown"]
    assert sublog._options["headerIsShown"] == header
    assert sublog._options["threadIDIsShown"] == threadID

    # create log records and check the format is correct
    gLogger.notice(msg)
    logstring = cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    log.notice(msg)
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    sublog.notice(msg)
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    expectedLog = "Framework NOTICE: message\nmessage\n" + expectedLog

    threadIDValue = str(_thread.get_ident())
    assert (threadIDValue in logstring) == isThreadIDAvailable
    # as thread ID depends on the execution, we have to add it to the expected results
    if isThreadIDAvailable:
        expectedLog = expectedLog % threadIDValue
    assert expectedLog == logstring


def test_showFormatOptionsPropag():
    """
    Make sure log and sublog don't inherit from gLogger options if already set
    """
    capturedBackend, log, sublog = gLoggerReset()

    # set logger options
    log.showHeaders(False)
    log.showThreadIDs(False)

    # then, set gLogger options again
    gLogger.showHeaders(True)
    gLogger.showThreadIDs(True)

    # log should not inherit from the options of gLogger as it has been modified by a developer
    # subLog shoud inherit from log, it has not been modified
    assert gLogger._options["headerIsShown"]
    assert gLogger._options["threadIDIsShown"]
    assert log._options["headerIsShown"] != gLogger._options["headerIsShown"]
    assert log._options["threadIDIsShown"] != gLogger._options["threadIDIsShown"]
    assert sublog._options["headerIsShown"] == log._options["headerIsShown"]
    assert sublog._options["threadIDIsShown"] == log._options["threadIDIsShown"]

    # a log record is sent, we then get the result to see if options have been taken into account
    gLogger.notice("me")
    logstring = cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    log.notice("ss")
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    sublog.notice("age")
    logstring += cleaningLog(capturedBackend.getvalue())
    capturedBackend.truncate(0)
    capturedBackend.seek(0)

    threadID = str(_thread.get_ident())
    expectedLog = "Framework [%s] NOTICE: me\nss\nage\n" % threadID

    assert expectedLog == logstring
