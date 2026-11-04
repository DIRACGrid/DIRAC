# DIRAC/WorkloadManagementSystem/Client/JobAnnotatorClient.py
from DIRAC.WorkloadManagementSystem.FutureClient.JobAnnotatorClient import (
    JobAnnotatorClient as futureJobAnnotatorClient,
)

from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/JobAnnotator")
class JobAnnotatorClient(Client):
    diracxClient = futureJobAnnotatorClient  # <-- the only change

    def __init__(self, url=None, **kwargs):
        super().__init__(**kwargs)
        self.serverURL = url or "WorkloadManagement/JobAnnotator"
