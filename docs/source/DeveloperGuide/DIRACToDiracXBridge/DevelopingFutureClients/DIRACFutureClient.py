# DIRAC/WorkloadManagementSystem/FutureClient/JobAnnotatorClient.py
from __future__ import annotations

from DIRAC.Core.Security.DiracX import DiracXClient, FutureClient, addRPCStub
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


class JobAnnotatorClient(FutureClient):
    @addRPCStub
    @convertToReturnValue
    def addAnnotation(self, jobID: str | int, annotation: str):
        with DiracXClient() as api:
            # Legacy accepts str or int, DiracX expects an int:
            api.jobs.add_annotation({int(jobID): annotation})

    @convertToReturnValue
    def getAnnotations(self, jobID: str | int):
        with DiracXClient() as api:
            return api.jobs.get_annotations(int(jobID))
