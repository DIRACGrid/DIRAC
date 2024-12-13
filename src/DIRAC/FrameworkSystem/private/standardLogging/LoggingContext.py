""" Logging context module"""

# Context variable for the logger (adapted to the request of the pilot reference)
import contextvars
from contextlib import contextmanager

contextLogger = contextvars.ContextVar("Logger", default=None)


@contextmanager
def setContextLogger(logger_name):
    token = contextLogger.set(logger_name)
    try:
        yield
    finally:
        contextLogger.reset(token)
