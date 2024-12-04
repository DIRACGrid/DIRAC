""" Test the context variable logger """

from DIRAC import gLogger
from DIRAC.FrameworkSystem.private.standardLogging.Logging import Logging
from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLoggerReset
from DIRAC.FrameworkSystem.private.standardLogging.LoggingContext import contextLogger, setContextLogger


class A:
    def __init__(self):
        # Get the logger from the context variable
        self._defaultLogger = gLogger.getSubLogger("A")

    # Use a property to get and set the logger, this is necessary to use the context variable
    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value: Logging):
        self._defaultLogger = value

    def do_something(self):
        self.log.notice("A is doing something")


class B:
    def __init__(self, a: A, pilotRef: str = None):
        self.a = A()

        # Get the logger from the context variable
        if pilotRef:
            self.log = gLogger.getLocalSubLogger(f"[{pilotRef}]B")
            contextLogger.set(self.log)
        else:
            self.log = gLogger.getSubLogger("B")

    def do_something_else(self):
        with setContextLogger(self.log):
            self.a.do_something()
            self.log.notice("B is doing something else")


def test_contextvar_logger():
    capturedBackend, log, sublog = gLoggerReset()

    # Create an instance of A
    a = A()

    # Create an instance of B and call its method without setting the pilotRef
    # Log signature coming from A and B should be different
    b1 = B(a)
    b1.do_something_else()
    assert "Framework/B NOTICE: A is doing something" in capturedBackend.getvalue()
    assert "Framework/B NOTICE: B is doing something else" in capturedBackend.getvalue()

    # Create an instance of B and call its method with setting the pilotRef
    # Log signature coming from A and B should be similar because of the pilotRef
    capturedBackend.truncate(0)

    b2 = B(a, "pilotRef")
    b2.do_something_else()
    assert "Framework/[pilotRef]B NOTICE: A is doing something" in capturedBackend.getvalue()
    assert "Framework/[pilotRef]B NOTICE: B is doing something else" in capturedBackend.getvalue()

    # Now we check that the logger of b1 is not the same as the logger of b2 (b1 should still use its own logger)
    capturedBackend.truncate(0)

    b1.do_something_else()
    assert "Framework/B NOTICE: A is doing something" in capturedBackend.getvalue()
    assert "Framework/B NOTICE: B is doing something else" in capturedBackend.getvalue()
