""" The Bundler service provides an interface for bundling jobs into a a big job

    It connects to a BundleDB to store and retrive bundles.
"""
from ast import literal_eval

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.DB.BundleDB import BundleDB
from DIRAC.WorkloadManagementSystem.Utilities.BundlerTemplates import generate_template


class BundlerHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.BundleDB", "BundleDB")
            if not result["OK"]:
                return result
            cls.bundleDB : BundleDB = result["Value"](parentLogger=cls.log)
            
            # Dictionaries entries should be removed afer some time
            cls.jobToCE = {}
            cls.bundleToCE = {}
            cls.jobToBundle = {}
            
            cls.ceFactory = ComputingElementFactory()
            cls.killBundleOnError = True

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        return S_OK()

    #############################################################################

    types_storeInBundle = [str, str, list, str, int, dict]

    def export_storeInBundle(self, jobId, executable, inputs, proxyDict, processors, ceDict):
        self.log.debug(f"Received: \n\tjobID={jobId}\n\texecutable={executable}\n\tinputs={inputs}\n\tprocessors={processors}\n\tceDict={ceDict}")
        
        proxy = X509Chain()
        result = proxy.loadChainFromString(proxy)
        if not result["OK"]:
            self.log.error("Failed to obtain proxy from the input string")
            self.log.debug(f"Obtained proxy string:\n{proxy}")
            return result

        result = self.ceFactory.getCE(ceType=ceDict["CEType"], ceName=ceDict["CEName"] ,ceParametersDict=ceDict)

        if not result["OK"]:
            self.log.error("Failed obtain the CE with configuration: ", str(ceDict))
            return result

        ce = result["Value"]
        self.jobToCE[jobId] = ce

        result = self.bundleDB.insertJobToBundle(jobId, executable, inputs, processors, ceDict)
        if not result["OK"]:
            self.log.error("Failed to insert into a bundle the job with id ", str(jobId))
            return result

        bundleId = result["Value"]["BundleId"]
        readyForSubmission = result["Value"]["Ready"]
        self.bundleToCE[bundleId] = ce

        self.log.info("Job inserted in bundle successfully")

        if readyForSubmission:
            self.log.info(f"Submitting bundle '{bundleId}' to CE '{ce.ceName}'")

            result = self._wrapBundle(bundleId)
            if not result["OK"]:
                return result
            bundle_exe, bundle_inputs = result["Value"]

            result = ce.submitJob(bundle_exe, inputs=bundle_inputs, proxy=proxy)

            if not result["OK"]:
                self.log.error("Failed to submit job to with id ", str(jobId))
                return result

            taskID = result["Value"]
            result = self.bundleDB.setTaskId(bundleId, taskID)

            if not result["OK"]:
                self.log.error("Failed to set task id of JobId ", str(jobId))
                return result

        return S_OK({"BundleID": bundleId, "Executing": readyForSubmission})

    #############################################################################

    types_getJobTask = [str]

    def export_getJobTask(self, jobId):
        result = self._getBundleIdFromJobId(jobId)

        if not result["OK"]:
            self.log.error("Failed to obtain Bundle of JobId ", str(jobId))
            return result

        bundleId = result["Value"]

        result = self.bundleDB.getBundleStatus(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain status of bundle ", str(bundleId))
            return result

        status = result["Value"]
        if status == "Storing":
            return S_OK()

        result = self.bundleDB.getTaskId(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain Job Output of JobId ", str(jobId))
        else:
            self.bundleDB.setBundleAsFinalized(bundleId)

        return S_OK((result["Value"]))
    
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
    
    def _killJob(self, jobId):
        return S_ERROR()

    def _killBundleOfJob(self, jobId):
        ce = self.__getJobCE(jobId)
        result = self._getBundleIdFromJobId(jobId)
        if not result["OK"]:
            return result
        return ce.killJob(result["Value"])

    #############################################################################

    types_cleanJob = [str]

    def export_cleanJob(self, jobId):
        result = self._getBundleIdFromJobId(jobId)
        if not result["OK"]:
            return result
        bundleId = result["Value"]

        result = self.bundleDB.getBundleStatus(jobId)
        if not result["OK"]:
            return result
        status = result["Value"]
        
        if status != "Finalized":
            return S_OK("There are jobs running, cleaning is not permitted")

        ce = self.__getJobCE(jobId)
        return self._cleanBundle(ce, bundleId)
    
    def _cleanBundle(self, ce, bundleId):
        try:
            ce.cleanJob(bundleId)
        except AttributeError as e: # If the CE has no method 'cleanJob'
            return S_ERROR(e)
        return S_OK()

    #############################################################################

    types_getJobStatus = [str]

    def export_getJobStatus(self, jobId):
        return self._getJobStatus(jobId)

    def _getJobStatus(self, jobId):
        result = self._getBundleIdFromJobId(jobId)

        if not result["OK"]:
            return result
        
        bundleId = result["Value"]

        result = self.bundleDB.getBundleStatus(bundleId)

        if not result["OK"]:
            return result

        status=result["Value"]

        if status == "Sent":
            ce = self.__getJobCE(jobId)
            result = ce.getJobStatus(bundleId)

            if not result["OK"]:
                return result
            
            if result["Value"] == PilotStatus.DONE:
                self.bundleDB.setBundleAsFinalized()
                status = "Finalized"
            
            elif result["Value"] == PilotStatus.FAILED | result["Value"] == PilotStatus.ABORTED:
                self.bundleDB.setBundleAsFailed()
                status = "Failed"

        return S_OK(status)

    #############################################################################

    def _getBundleIdFromJobId(self, jobId):
        if self.jobToBundle[jobId]:
            return self.jobToBundle[jobId]

        result = self.bundleDB.getBundleIdFromJobId(jobId)
        if not result["OK"]:
            return result
        
        self.jobToBundle[jobId] = result["Value"]
        return result

    def _wrapBundle(self, bundleId):
        result = self.bundleDB.getBundle(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain bundle while wrapping. BundleID ", str(bundleId))
            return result

        bundle = result["Value"]

        result = self.bundleDB.getJobsOfBundle(bundleId)

        if not result["OK"]:
            self.log.error("Failed to obtain bundled job while wrapping. BundleID=", str(bundleId))
            return result

        jobs = result["Value"]

        template = bundle["ExecTemplate"]
        inputs = []

        for job in jobs:
            inputs.append(job["ExecutablePath"])
            inputs.append(job["Inputs"])

        result = generate_template(template, inputs)

        if not result["OK"]:
            self.log.error("Error while generating wrapper")
            return result

        wrappedBundle = result["Value"]
        wrapperPath = f"/tmp/bundle_wrapper_{bundleId}"

        with open(wrapperPath, "x") as f:
            f.write(wrappedBundle)

        return S_OK((wrapperPath, inputs))

    def _getCeDict(self, jobId):
        result = self._getBundleIdFromJobId(jobId)
        if not result["OK"]:
            return result
        bundleId = result["Value"]

        result = self.bundleDB.getBundleCE(bundleId)
        if not result["OK"]:
            return result

        # Convert the CEDict from string to a dictionary
        ceDict = literal_eval(result["Value"])
        return S_OK(ceDict)

    def __getJobCE(self, jobId):
        if jobId not in self.jobToCE:
            # Look for it in the DB
            result = self._getCeDict(jobId)

            if not result["OK"]:
                self.log.error("Failed to obtain CE Dict of Bundle with JobId ", str(jobId))
                return result

            ceDict = result["Value"]

            # Build the ce obtained from the DB
            result = self.ceFactory.getCE(ceType=ceDict["CEType"], ceName=ceDict["GridCE"], ceParametersDict=ceDict)

            if not result["OK"]:
                self.log.error("Failed to CE of JobId ", str(jobId))
                return result

            self.jobToCE[jobId] = result["Value"]

        return self.jobToCE[jobId]
