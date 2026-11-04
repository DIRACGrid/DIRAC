# DIRAC/WorkloadManagementSystem/Client/JobAnnotatorClient.py
from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/JobAnnotator")
class JobAnnotatorClient(Client):
    def __init__(self, url=None, **kwargs):
        super().__init__(**kwargs)
        self.serverURL = url or "WorkloadManagement/JobAnnotator"
