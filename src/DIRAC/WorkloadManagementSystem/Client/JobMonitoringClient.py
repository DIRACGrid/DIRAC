""" Class that contains client access to the job monitoring handler. """

from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.JEncode import strToIntDict

try:
    from DIRAC.WorkloadManagementSystem.FutureClient.JobMonitoringClient import (
        JobMonitoringClient as futureJobMonitoringClient,
    )
except ImportError:
    futureJobMonitoringClient = None


@createClient("WorkloadManagement/JobMonitoring")
class JobMonitoringClient(Client):
    # Set to None to raise an error if this service is set as "legacy adapted"
    # See ClientSelector
    diracxClient = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setServer("WorkloadManagement/JobMonitoring")

    @ignoreEncodeWarning
    def getJobsStatus(self, jobIDs):
        res = self._getRPC().getJobsStatus(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobParameters(self, jobIDs, parName=None):
        res = self._getRPC().getJobParameters(jobIDs, parName)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsMinorStatus(self, jobIDs):
        res = self._getRPC().getJobsMinorStatus(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsApplicationStatus(self, jobIDs):
        res = self._getRPC().getJobsApplicationStatus(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsSites(self, jobIDs):
        res = self._getRPC().getJobsSites(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    def getJobsStates(self, jobIDs):
        res = self._getRPC().getJobsStates(jobIDs)
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    def getInputData(self, jobIDs):
        res = self._getRPC().getInputData(jobIDs)
        if res["OK"] and isinstance(res["Value"], dict):
            res["Value"] = strToIntDict(res["Value"])
        return res
