"""The Bundler service provides an interface for bundling jobs into a a big job

It connects to a BundleDB to store and retrive bundles.
"""

import os
import shutil
from ast import literal_eval

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.DB.BundleDB import BundleDB
from DIRAC.WorkloadManagementSystem.Utilities.BundlerTemplates import BASH_RUN_TASK, generate_template


class BundlerHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.BundleDB", "BundleDB")
            if not result["OK"]:
                return result
            cls.bundleDB: BundleDB = result["Value"](parentLogger=cls.log)

            # Dictionaries entries should be removed afer some time
            cls.jobToCE = {}
            cls.bundleToCE = {}
            cls.jobToBundle = {}

            cls.ceFactory = ComputingElementFactory()

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        return S_OK()

    def initialize(self):
        self.killBundleOnError = self.getCSOption("KillBundleOnError", True)
        self.bundlesBaseDir = self.getCSOption("/LocalSite/BundlesBaseDir", "/tmp/bundles")

        if not os.path.exists(self.bundlesBaseDir):
            os.mkdir(self.bundlesBaseDir)

    #############################################################################

    types_storeInBundle = [str, str, list, list, str, int, dict, [int, type(None)]]

    def export_storeInBundle(self, jobId, executable, inputs, outputs, proxyPath, processors, ceDict, diracId):
        result = self._setupCE(ceDict, proxyPath)

        if not result["OK"]:
            return result

        # Insert the Job into the DB
        result = self.bundleDB.insertJobToBundle(
            jobId, executable, inputs, outputs, processors, ceDict, proxyPath, diracId
        )
        if not result["OK"]:
            self.log.error("Failed to insert into a bundle the job with id ", str(jobId))
            return result

        bundleId = result["Value"]["BundleId"]
        readyForSubmission = result["Value"]["Ready"]

        self.log.info("Job inserted in bundle successfully")

        if readyForSubmission:
            self._submitBundle(bundleId)

        return S_OK({"BundleID": bundleId, "Executing": readyForSubmission})

    #############################################################################

    types_getTaskInfo = [str]

    def export_getTaskInfo(self, bundleId):
        return self._getTaskInfo(bundleId)

    def _getTaskInfo(self, bundleId):
        result = self.bundleDB.getBundleStatus(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain status of bundle ", str(bundleId))
            return result

        resultDict = {"Status": result["Value"]}

        # If it hasn't been uploaded yet
        if resultDict["Status"] == PilotStatus.WAITING:
            return S_OK(resultDict)

        result = self.bundleDB.getTaskId(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain taskId of bundle ", str(bundleId))
            return result

        resultDict["TaskID"] = result["Value"]

        return S_OK(resultDict)

    #############################################################################

    types_bundleIdFromJobId = [str]

    def export_bundleIdFromJobId(self, jobId):
        return self._getBundleIdFromJobId(jobId)

    #############################################################################

    types_tryToKillJob = [str]

    def export_tryToKillJob(self, jobId):
        result = self._killJob(jobId)
        if result["OK"]:
            self.log.info(f"Job {jobId} killed successfully")
            return result

        self.log.warn("Failed to ONLY kill the job with id ", str(jobId))

        if self.killBundleOnError:
            self.log.warn("KillBundleOnError is on, killing the WHOLE bundle containing the job")
            result = self._killBundleOfJob(jobId)
            if not result["OK"]:
                return result

            bundleId = result["Value"]
            self.log.info(f"Bundle {bundleId} of Job {jobId} killed successfully")
            return S_OK()

        else:
            self.log.warn("KillBundleOnError is off, doing nothing")
            return S_ERROR(message="KillBundleOnError is off, won't kill the bundle")

    def _killBundleOfJob(self, jobId):
        result = self._getJobCE(jobId)
        if not result["OK"]:
            return result
        ce = result["Value"]["CE"]
        result = self._getBundleIdFromJobId(jobId)

        if not result["OK"]:
            return result

        bundleId = result["Value"]
        result = self._getTaskInfo(bundleId)
        if not result["OK"]:
            return result

        if result["Value"]["Status"] in PilotStatus.PILOT_FINAL_STATES:
            return S_ERROR("Cannot kill finished jobs")

        result = ce.killJob([result["Value"]["TaskID"]])

        if not result["OK"]:
            return result

        self.bundleDB.setBundleAsFailed()
        return

    def _killJob(self, jobId):
        return S_ERROR("CAN'T STOP JOBS")

    #############################################################################

    types_cleanJob = [str]

    def export_cleanJob(self, jobId):
        result = self._getBundleIdFromJobId(jobId)
        if not result["OK"]:
            return result
        bundleId = result["Value"]

        result = self.bundleDB.isBundleCleaned(bundleId)

        if not result["OK"]:
            return result

        # Bundle already got cleaned
        if result["Value"]:
            return S_OK()

        result = self._getTaskInfo(bundleId)

        if not result["OK"]:
            return result
        status = result["Value"]["Status"]

        if status not in PilotStatus.PILOT_FINAL_STATES:
            return S_ERROR(f"The bundle hasn't finished, cleaning is not permitted. Current Status: {status}")

        taskId = result["Value"]["TaskID"]

        result = self._getJobCE(jobId)
        if not result["OK"]:
            return result
        ce = result["Value"]["CE"]

        try:
            result = ce.cleanJob(taskId)
            if result["OK"]:
                self.bundleDB.setBundleAsCleaned(bundleId)
        except AttributeError as e:  # If the CE has no method 'cleanJob'
            return S_ERROR(e)

        # Remove bundle specific files (NOT THE OUTPUTS OF THE JOBS)
        bundlePath = os.path.join(self.bundlesBaseDir, bundleId)
        for item in os.listdir(bundlePath):
            itemPath = os.path.join(bundlePath, item)
            if os.path.isfile(item):
                os.remove(itemPath)

        return S_OK()

    #############################################################################

    types_getBundleStatus = [str]

    def export_getBundleStatus(self, bundleId):
        result = self._getTaskInfo(bundleId)

        if not result["OK"]:
            return result

        status = result["Value"]["Status"]

        if status == PilotStatus.RUNNING:
            task = result["Value"]["TaskID"]

            if ":::" in task:
                task = task.split(":::")[0]

            result = self._getBundleCE(bundleId)

            if not result["OK"]:
                return result

            ce = result["Value"]["CE"]

            result = ce.getJobStatus(task)

            if not result["OK"]:
                return result

            status = result["Value"][task]

            if status == PilotStatus.DONE:
                self.bundleDB.setBundleAsFinalized(bundleId)
            elif status in PilotStatus.PILOT_FINAL_STATES:  # ABORTED, DELETED or FAILED
                self.bundleDB.setBundleAsFailed(bundleId)

        return S_OK(status)

    #############################################################################

    types_forceSubmitBundles = [list]

    def export_forceSubmitBundles(self, bundleIds):
        resultDict = {}

        if not isinstance(bundleIds, list):
            bundleIds = [bundleIds]

        for bundleId in bundleIds:
            result = self._submitBundle(bundleId)
            resultDict[bundleId] = result

        return S_OK(resultDict)

    def _submitBundle(self, bundleId):
        result = self._getBundleCE(bundleId)

        if not result["OK"]:
            return result

        ce = result["Value"]["CE"]
        proxy = result["Value"]["Proxy"]

        result = self._wrapBundle(bundleId)
        if not result["OK"]:
            return result

        jobIds, bundle_exe, bundle_inputs, bundle_outputs = result["Value"]
        extra_outputs = [item for job_id in jobIds for item in [f"{job_id}.out", f"{job_id}.status"]]
        bundle_outputs.extend(extra_outputs)

        self.log.info(f"Submitting bundle '{bundleId}' to CE '{ce.ceName}'")

        ce.ceParameters["NumberOfProcessors"] = len(jobIds)
        result = ce.submitJob(bundle_exe, proxy=proxy, inputs=bundle_inputs, outputs=bundle_outputs)

        if not result["OK"]:
            self.bundleDB.setBundleAsFailed(bundleId)
            return result

        innerJobId = result["Value"][0]
        taskId = innerJobId + ":::" + result["PilotStampDict"][innerJobId]

        result = self.bundleDB.setTaskId(bundleId, taskId)

        if not result["OK"]:
            return S_ERROR("Failed to set the task id of the Bundle")

        return S_OK()

    #############################################################################

    def _getBundleIdFromJobId(self, jobId):
        if jobId in self.jobToBundle:
            return S_OK(self.jobToBundle[jobId])

        result = self.bundleDB.getBundleIdFromJobId(jobId)
        if not result["OK"]:
            return result

        self.jobToBundle[jobId] = result["Value"]
        return result

    def _wrapBundle(self, bundleId):
        result = self.bundleDB.getWholeBundle(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain bundle while wrapping. BundleID ", str(bundleId))
            return result

        bundle = result["Value"]

        result = self.bundleDB.getJobsOfBundle(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain bundled job while wrapping. BundleID=", str(bundleId))
            return result

        jobs: dict = result["Value"]

        template = bundle["ExecTemplate"]
        executables = []
        inputs = []
        outputs = []
        jobIds = []

        bundlePath = os.path.join(self.bundlesBaseDir, bundleId)
        os.mkdir(bundlePath)

        for jobId, jobInfo in jobs.items():
            jobIds.append(jobId)

            # Copy the original file in a new location with the rest
            job_executable = jobInfo["ExecutablePath"]
            job_executable_dst = os.path.join(bundlePath, jobId + "_" + os.path.basename(job_executable))

            shutil.copy(job_executable, job_executable_dst)

            executables.append(os.path.basename(job_executable_dst))
            inputs.append(job_executable_dst)

            for job_input in jobInfo["Inputs"]:
                inputBasename = os.path.basename(job_input)
                job_input_dst = os.path.join(bundlePath, jobId + "_" + inputBasename)
                shutil.copy(job_input, job_input_dst)
                inputs.append(job_input_dst)

            outputs.extend(list(set(jobInfo["Outputs"])))  # Remove duplicated entries

        result = generate_template(template, executables, bundleId)

        if not result["OK"]:
            self.log.error("Error while generating wrapper")
            return result

        wrappedBundle = result["Value"]
        wrapperPath = os.path.join(bundlePath, "bundle_wrapper")
        runnerPath = os.path.join(bundlePath, "run_task.sh")

        with open(wrapperPath, "x") as f:
            f.write(wrappedBundle)

        with open(runnerPath, "x") as f:
            f.write(BASH_RUN_TASK)

        inputs.append(runnerPath)

        return S_OK((jobIds, wrapperPath, inputs, outputs))

    def _getBundleCEDict(self, bundleId):
        result = self.bundleDB.getBundleCE(bundleId)
        if not result["OK"]:
            return result

        # Convert the CEDict from string to a dictionary
        ceDict = literal_eval(result["Value"]["CEDict"])

        return S_OK({"CEDict": ceDict, "ProxyPath": result["Value"]["ProxyPath"]})

    def _setupCE(self, ceDict, proxyPath):
        result = getProxyInfo(proxy=proxyPath)

        if not result["OK"]:
            self.log.error("Failed to obtain proxy from path")
            return result

        proxy = result["Value"]["chain"]

        # Setup CE
        result = self.ceFactory.getCE(ceType=ceDict["CEType"], ceName=ceDict["GridCE"], ceParametersDict=ceDict)

        if not result["OK"]:
            self.log.error("Failed obtain the CE with configuration: ", str(ceDict))
            return result

        ce = result["Value"]

        ce.setProxy(proxy)

        return S_OK({"CE": ce, "Proxy": proxy})

    def _getBundleCE(self, bundleId):
        if bundleId not in self.bundleToCE:
            result = self._getBundleCEDict(bundleId)

            if not result["OK"]:
                return result

            result = self._setupCE(result["Value"]["CEDict"], result["Value"]["ProxyPath"])

            if not result["OK"]:
                return result

            self.bundleToCE[bundleId] = result["Value"]  # CE + Proxy

        return S_OK(self.bundleToCE[bundleId])

    def _getJobCE(self, jobId):
        if jobId not in self.jobToCE:
            result = self._getBundleIdFromJobId(jobId)

            if not result["OK"]:
                self.log.error("Failed to obtain BundleId with JobId ", str(jobId))
                return result

            bundleId = result["Value"]

            result = self._getBundleCE(bundleId)

            if not result["OK"]:
                return result

            self.jobToCE[jobId] = result["Value"]

        return S_OK(self.jobToCE[jobId])
