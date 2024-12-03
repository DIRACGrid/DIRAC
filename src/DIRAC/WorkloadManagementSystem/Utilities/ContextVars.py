""" Context variables for the Workload Management System """

# Context variable for the logger (adapted to the request of the pilot reference)
import contextvars
from contextlib import contextmanager

pilotRefLogger = contextvars.ContextVar("PilotRefLogger", default=None)


@contextmanager
def setPilotRefLogger(logger_name):
    token = pilotRefLogger.set(logger_name)
    try:
        yield
    finally:
        pilotRefLogger.reset(token)
