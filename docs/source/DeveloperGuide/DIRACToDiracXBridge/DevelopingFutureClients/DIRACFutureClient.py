# DIRAC/WorkloadManagementSystem/FutureClient/JobAnnotatorClient.py
from __future__ import annotations

from DIRAC.Core.Security.DiracX import DiracXClient, FutureClient, addRPCStub, stripValueIfOK
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


class JobAnnotatorClient(FutureClient):
    @addRPCStub
    @stripValueIfOK
    @convertToReturnValue
    def addAnnotation(self, jobID: str | int, annotation: str):
        with DiracXClient() as api:
            # Legacy accepts str or int, DiracX expects an int:
            api.jobs.add_annotations({int(jobID): annotation})

    @convertToReturnValue
    def getAnnotations(self, jobID: str | int):
        with DiracXClient() as api:
            return api.jobs.get_annotations(int(jobID))
