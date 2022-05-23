"""
Optimizer is an abstract class that all concrete optimizers must inherit
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from DIRAC import gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class Optimizer(ABC):
    """
    Optimizer class

    It provides the ability to make a queue of optimizers in the Invoker class.
    """

    _nextHandler: Optimizer = None

    def __init__(self, jobState: JobState):
        self.jobState = jobState
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")

    @abstractmethod
    def optimize(self):
        """Method that needs to be overriden to optimize the job"""
        if self._nextHandler:
            result = self.jobState.setStatus(
                JobStatus.CHECKING,
                minorStatus=self._nextHandler.__class__.__name__,
                appStatus="Unknown",
                source=self.__class__.__name__,
            )
            if not result["OK"]:
                return result

            return self._nextHandler.optimize()

        return S_ERROR("No more optimizer to run")

    def setNext(self, handler: Optimizer) -> Optimizer:
        """
        Returning a handler from here will let us link handlers in a
        convenient way like this:
        >>> monkey.setNext(squirrel).setNext(dog)
        """
        self._nextHandler = handler
        #
        return handler

    def storeOptimizerParam(self, name, value):
        """Store an optimizer parameter in jobState"""
        valenc = DEncode.encode(value)
        return self.jobState.setOptParameter(name, valenc)

    def retrieveOptimizerParam(self, name):
        """Retreive an optimizer parameter from jobState"""
        result = self.jobState.getOptParameter(name)
        if not result["OK"]:
            return result
        valenc = result["Value"]
        try:
            if not isinstance(valenc, bytes):
                valenc = valenc.encode()
            value, encLength = DEncode.decode(valenc)
            if encLength == len(valenc):
                return S_OK(value)
        except NotImplementedError:
            self.log.exception(f"Opt param {name} doesn't seem to be dencoded {valenc}")
        return S_OK(eval(valenc))
