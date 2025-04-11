""" The Bundler service provides an interface for bundling jobs into a a big job

    It connects to a BundleDB to store and retrive bundles.
"""
from ast import literal_eval

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory


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
    
    types_storeInBundle = [int, str, list, str, int, dict]

    def export_storeInBundle(self, jobId, executable, inputs, proxy, processors, ceDict):
        ce = self.ceFactory.getCE(ceParametersDict=ceDict)
        self.jobToCE[jobId] = ce

        result = self.bundleDB.insertJobToBundle(jobId, executable, inputs, processors, ceDict)
        if not result["OK"]:
            return S_ERROR()

        bundleID = result["Value"]["BundleId"]
        readyForSubmission = result["Value"]["Ready"]

        if readyForSubmission:
            bundle_exe, bundle_inputs = self.__wrapBundle(bundleID)
            result = ce.submitJob(bundle_exe, inputs=bundle_inputs, proxy=proxy)

            if not result["OK"]:
                return result

            taskID = result["Value"]
            result = self.bundleDB.setTaskId(bundleID, taskID)

            if not bundleID["OK"]:
                return result

        return S_OK({"BundleID": bundleID, "Executing": readyForSubmission})


    types_getOutput = [int]

    def export_getOutput(self, jobID):
        result = self.bundleDB.getBundleIdFromJobId(jobID)

        if not result["OK"]:
            return result
        bundleID = result["Value"]        

        ce = self.__getJobCE(jobID)
        result = ce.getJobOutput(bundleID)

        if not result["OK"]:
            return result
        
        return result["Value"]

    def __getJobBundle(self, jobID):
        result = self.bundleDB.getBundleIdFromJobId(jobID)
        
        if not result["OK"]:
            return result

        bundleId = result["Value"]

        result = self.bundleDB.getBundle(bundleId)

        if not result["OK"]:
            return S_ERROR()
        
        return S_OK(result["Value"])

    def __getJobCE(self, jobID):
        if jobID not in self.jobToCE:
            # Look for it in the DB
            result = self.__getJobBundle(jobID)
            
            if not result["OK"]:
                return S_ERROR("Job not in a bundle")

            # Convert the CEDict from string to a dictionary
            ceDict = literal_eval(result["Value"]["CEDict"])
            # Build the ce obtained from the DB
            result = self.ceFactory.getCE(ceParametersDict=ceDict)
            
            if not result["OK"]:
                return result
            
            self.jobToCE[jobID] = result["Value"]
        
        return self.jobToCE[jobID]

    def __getJobTask(self, jobId):
        result = self.bundleDB.getBundleIdFromJobId(jobId)

        if not result["OK"]:
            return result
        
        bundleId = result["Value"]

        result = self.bundleDB.getBundle(bundleId)

        if not result["OK"]:
            return result

        return result["Value"]["TaskID"]

    def __wrapBundle(self, bundleId):
        result = self.bundleDB.getBundle(bundleId)
        
        if not result["OK"]:
            return result

        bundle = result["Value"]

        result = self.bundleDB.getJobsOfBundle(bundleId)
        
        if not result["OK"]:
            return result

        jobs = result["Value"]
        
        wrapper = bundle["ExecTemplate"]
        inputs = []
        execs = []

        for job in jobs:
            execs.append(job["ExecutablePath"])
            inputs.append(job["Inputs"])
        
        wrappedBundle = wrapper.format(inputs=','.join(execs))
        wrapperPath = f"/tmp/bundle_wrapper_{bundleId}"

        with open(wrapperPath, "x") as f:
            f.write(wrappedBundle)

        return wrapperPath, inputs