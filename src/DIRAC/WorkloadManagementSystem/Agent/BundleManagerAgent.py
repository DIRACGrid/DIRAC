import os
from datetime import datetime, timedelta, timezone

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import PilotStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.BundlerClient import BundlerClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.DB.BundleDB import BundleDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


class BundleManagerAgent(AgentModule):
    def __init__(self, agentName, loadName, baseAgentName=False, properties=None):
        if not properties:
            properties = {}
        super().__init__(agentName, loadName, baseAgentName, properties)

        self.bundleDB = None
        self.jobDB = None

    #############################################################################

    def initialize(self):
        self.bundleDB = BundleDB()
        self.jobDB = JobDB()
        self.jobMonitor = JobMonitoringClient()
        self.bundler = BundlerClient()
        self.maxMinsInBundle = self.am_getOption("MaxMinutesInBundle", defaultValue=10)
        return S_OK()

    def execute(self):
        self.log.info("Sending stalled Bundles")
        result = self._sendStalledBundles()
        if not result["OK"]:
            self.log.warn(f"Failed send the bundles: {result}")

        self.log.info("Cleaning inputs of finished bundles bundles")
        result = self._cleanFinishedBundles()
        if not result["OK"]:
            self.log.warn(f"Failed to clean the inputs: {result}")

        self.log.info("Deleting killed jobs from bundles")
        result = self._removeKilledJobs()
        if not result["OK"]:
            self.log.warn(f"Failed to delete the inputs: {result}")

        self._checkHeartBeat()

        return S_OK()

    def finalize(self):
        return S_OK()

    #############################################################################

    def _cleanFinishedBundles(self):
        result = self.bundleDB.getUnpurgedBundles()
        if not result["OK"]:
            return result

        bundleIDs = result["Value"]
        self.log.verbose(f"> Found {len(bundleIDs)} finished and unpurged bundles")

        for bundleId in bundleIDs:
            success = True
            result = self.bundleDB.getJobIDsOfBundle(bundleId)
            if not result["OK"]:
                self.log.error(f"Failed to obtain the jobs of the bundle {bundleId}")
                return result

            jobIDs = result["Value"]

            self.log.verbose(f"> Purging inputs of bundle with ID '{bundleId}'")

            for jobId in jobIDs:
                result = self.bundleDB.removeJobInputs(jobId)
                if not result["OK"]:
                    success = False
                    self.log.error(f"Failed to remove inputs of job {jobId} from bundle {bundleId}, skipping...")
                    self.log.error(result)

            if success:
                self.log.info(f"> Inputs of bundle with ID '{bundleId}' were removed from DB")
                self.bundleDB.setBundleAsPurged(bundleId)

        return S_OK()

    def _removeKilledJobs(self):
        killedJobs = []

        result = self.bundleDB.getWaitingBundles()
        if not result["OK"]:
            return result

        bundles = result["Value"]
        self.log.debug(f"> Found {len(bundles)} waiting bundles")

        for bundleInfo in bundles:
            bundleId = bundleInfo["BundleID"]

            result = self.bundleDB.getJobsOfBundle(bundleId, noInputs=True)
            if not result["OK"]:
                self.log.error(f"Failed to get the jobs of the bundle '{bundleId}'")
                return result

            jobs = result["Value"]
            jobIds = list(jobs.keys())

            diracIds = []
            diracIdToJobId = {}
            for jobId in jobIds:
                if "DiracID" not in jobs[jobId]:
                    continue

                diracId = jobs[jobId]["DiracID"]
                if diracId:
                    diracIds.append(diracId)
                    diracIdToJobId[diracId] = jobId

            result = self.jobMonitor.getJobsStatus(diracIds)
            if not result["OK"]:
                self.log.error(f"Failed to get the status of the jobs with ids: {diracIds}")
                self.log.error(result)
                return result

            statusDict = result["Value"]
            for diracId, status in statusDict.items():
                if status == JobStatus.KILLED:
                    self.log.info(f"> Status of job '{diracId}' is 'Killed', adding it to the deletion list")
                    killedJobs.append(diracIdToJobId[diracId])

        if not killedJobs:
            self.log.verbose("Nothing to delete...")
            return S_OK()

        result = self.bundleDB.removeJobsFromBundle(killedJobs)
        if not result["OK"]:
            return result

        deletedDict = result["Value"]

        failedDeletions = {}
        for jobId, jobResult in deletedDict.items():
            if not jobResult["OK"]:
                failedDeletions[jobId] = jobResult

        if failedDeletions:
            return S_ERROR(f"Failed to delete the following jobs: {failedDeletions}")

        return S_OK()

    def _sendStalledBundles(self):
        result = self.bundleDB.getWaitingBundles()
        if not result["OK"]:
            return result

        bundles = result["Value"]
        self.log.debug(f"> Found {len(bundles)} waiting bundles")

        bundleIds = []
        currentTime = datetime.now(tz=timezone.utc).replace(tzinfo=None)

        for bundleInfo in bundles:
            elapsedTime: timedelta = currentTime - bundleInfo["LastTimestamp"]
            elapsedMinutes = elapsedTime.total_seconds() // 60

            if elapsedMinutes > self.maxMinsInBundle:
                _id = bundleInfo["BundleID"]
                bundleIds.append(bundleInfo["BundleID"])

        if bundleIds:
            self.log.info(f"> Force-Submitting {len(bundleIds)} bundles due to timeout, IDs: ({bundleIds})")
            result = self.bundler.forceSubmitBundles(bundleIds)

        return S_OK()

    def _checkHeartBeat(self):
        """Hack to avoid stalled jobs when they are not"""
        self.log.info("Sending heartbeats to running bundles")
        result = self.bundleDB.getRunningBundles()
        if not result["OK"]:
            return result

        for bundleInfo in result["Value"]:
            if bundleInfo["Status"] == PilotStatus.RUNNING:
                result = self.bundleDB.getJobsOfBundle(bundleInfo["BundleID"], noInputs=True)
                if not result["OK"]:
                    continue

                for _, jobDesc in result["Value"].items():
                    diracId = jobDesc["DiracID"]
                    if not diracId:
                        continue

                    self.jobDB.setHeartBeatData(diracId, {})
