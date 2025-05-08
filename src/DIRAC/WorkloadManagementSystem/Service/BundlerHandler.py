""" The Bundler service provides an interface for bundling jobs into a a big job

    It connects to a BundleDB to store and retrive bundles.
"""
from ast import literal_eval

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Utilities.BundlerTemplates import generate_template


class BundlerHandler(RequestHandler):

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.BundleDB", "BundleDB")
            if not result["OK"]:
                return result
            cls.bundleDB = result["Value"](parentLogger=cls.log)
            cls.jobToCE = {}
            cls.ceFactory = ComputingElementFactory()
        
        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        return S_OK()
    
    types_storeInBundle = [str, str, list, str, int, dict]

    def export_storeInBundle(self, jobId, executable, inputs, proxy, processors, ceDict):
        result = self.ceFactory.getCE(ceType=ceDict["CEType"], ceParametersDict=ceDict)

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
        self.log.info("Job inserted in bundle successfully")

        if readyForSubmission:
            self.log.info(f"Submitting bundle '{bundleId}' to CE '{ce.ceName}'")

            bundle_exe, bundle_inputs = self.__wrapBundle(bundleId)
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

    types_getOutput = [str]

    def export_getOutput(self, jobId):
        result = self.bundleDB.getBundleIdFromJobId(jobId)

        if not result["OK"]:
            self.log.error("Failed to obtain Bundle of JobId ", str(jobId))
            return result

        bundleID = result["Value"]        

        # TODO: THIS CAN BE CACHED
        ce = self.__getJobCE(jobId)
        result = ce.getJobOutput(bundleID)

        if not result["OK"]:
            self.log.error("Failed to obtain Job Output of JobId ", str(jobId))
        
        return result

    def __getJobBundle(self, jobId):
        result = self.bundleDB.getBundleIdFromJobId(jobId)
        
        if not result["OK"]:
            self.log.error("Failed to obtain BundleId of JobId ", str(jobId))
            return result

        bundleId = result["Value"]

        result = self.bundleDB.getBundle(bundleId)

        if not result["OK"]:
            self.log.error

        return result

    def __getJobCE(self, jobId):
        if jobId not in self.jobToCE:
            # Look for it in the DB
            result = self.__getJobBundle(jobId)
            
            if not result["OK"]:
                self.log.error("Failed to obtain Bundle of JobId ", str(jobId))
                return result

            # Convert the CEDict from string to a dictionary
            ceDict = literal_eval(result["Value"]["CEDict"])
            # Build the ce obtained from the DB
            result = self.ceFactory.getCE(ceParametersDict=ceDict)
            
            if not result["OK"]:
                self.log.error("Failed to CE of JobId ", str(jobId))
                return result
            
            self.jobToCE[jobId] = result["Value"]
        
        return self.jobToCE[jobId]

    def __getJobTask(self, jobId):
        result = self.__getJobBundle(jobId)

        if not result["OK"]:
            self.log.error("Failed to obtain task id of Job ", str(jobId))
            return result

        return result["Value"]["TaskID"]

    def __wrapBundle(self, bundleId):
        result = self.bundleDB.getBundle(bundleId)
        
        if not result["OK"]:
            self.log.error("Failed to obtain bundle while wrapping. BundleID=", str(bundleId))
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
            return result
        
        wrappedBundle = result["Value"]
        wrapperPath = f"/tmp/bundle_wrapper_{bundleId}"

        with open(wrapperPath, "x") as f:
            f.write(wrappedBundle)

        return wrapperPath, inputs