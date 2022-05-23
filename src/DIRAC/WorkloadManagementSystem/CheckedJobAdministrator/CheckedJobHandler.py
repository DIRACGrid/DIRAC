"""
CheckedJobHandler abstract class

It provides the ability to make a chain of responsability to change the state of a job
from CHECKED to another state.
"""


from __future__ import annotations
from abc import ABC, abstractmethod

from DIRAC.Core.Utilities.ReturnValues import S_ERROR
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class CheckedJobHandler(ABC):
    """
    CheckedJobHandler abstract class

    It provides the ability to make a chain of responsability.
    """

    _nextHandler: CheckedJobHandler = None

    @abstractmethod
    def __init__(self, jobState: JobState):
        self.jobState = jobState

    @abstractmethod
    def handle(self):
        """Method that needs to be overriden to change the state of the job"""
        if self._nextHandler:
            return self._nextHandler.handle()

        return S_ERROR("No more handlers to run")

    def setNext(self, handler: CheckedJobHandler) -> CheckedJobHandler:
        """
        Returning a handler from here will let us link handlers in a
        convenient way like this:
        >>> monkey.setNext(squirrel).setNext(dog)
        """
        self._nextHandler = handler

        return handler
