import os

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.BundlerClient import BundlerClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.DB.BundleDB import BundleDB
from DIRAC.WorkloadManagementSystem.Utilities.BundlerTemplates import generate_template
from DIRAC.WorkloadManagementSystem.Client import JobStatus

class BundleManagerAgent(AgentModule):
    def __init__(self, agentName, loadName, baseAgentName=False, properties=None):
        if not properties:
            properties = {}
        super().__init__(agentName, loadName, baseAgentName, properties)

        self.bundleDB = None

    #############################################################################

    def initialize(self):
        self.bundleDB = BundleDB()
        self.jobMonitor = JobMonitoringClient()
        self.bundler = BundlerClient()

    def execute(self):
        self._sendStalledBundles()
        self._cleanFinishedBundles()
        self._removeKilledJobs()

    def finalize(self):
        pass

    #############################################################################

    def _cleanFinishedBundles(self):
        self.log.info("Cleaning inputs of finished bundles bundles")

        result = self.bundleDB.getFinishedBundles()
        if not result["OK"]:
            return result
        bundleIDs = result["Value"]

        for bundleId in bundleIDs:
            result = self.getJobIDsOfBundle(bundleId) 
            if not result["OK"]:
                return result
            jobIDs = result["Value"]

            for jobId in jobIDs:
                result = self.bundleDB.removeJobInputs(jobId)
                if not result["OK"]:
                    self.log.error(f"Failed to remove inputs of job {jobId} from bundle {bundleId}, skipping...")
                    self.log.error(result)

        return S_OK()

    def _removeKilledJobs(self):
        killedJobs = []

        result = self.bundleDB.getWaitingBundles()
        if not result["OK"]:
            return result

        for bundleId in result["Value"]:
            result = self.bundleDB.getJobsOfBundle(bundleId)
            if not result["OK"]:
                return result

            result = self.jobMonitor.getJobsStatus(result["Value"])
            if not result["OK"]:
                return result

            statusDict = result["Value"]
            for job, status in statusDict.items():
                if status == JobStatus.KILLED:
                    killedJobs.append(job)

        result = self.bundleDB.removeJobs(killedJobs)
        return result

    def _sendStalledBundles(self):
        pass