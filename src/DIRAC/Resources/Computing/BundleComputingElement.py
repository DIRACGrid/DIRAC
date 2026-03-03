"""Bundle Computing Elemenet

Allows grouping jobs in a single big job prior to their submission in an actual CE.

**Configuration Parameters**

Configuration for the BundleComputingElemenet submission can be done via the configuration system.
Below, you can find a list of parameters specific to the BundleCE.

InnerCEType:
    Type of the CE that will end up executing the templated wrapper.

**CE Configuration**

This CE must be configure in the same way as the one that will execute the jobs, the only
difference is that the CEType will become InnerCEType and it must have configured the template
to be used.

For example:

CEs
{
    host
    {
        CEType = SSH
        SSHHost = host
        SSHUser = user
        SSHPassword = password
        ...
        Queues
        {
            dirac
            {
                ...
            }
        }
    }
}

Will become:

CEs
{
    host
    {
        CEType = BUNDLE
        InnerCEType = SSH

        SSHHost = host
        SSHUser = user
        SSHPassword = password
        ...
        Queues
        {
            dirac
            {
                ...
            }
        }
    }
}

**Code Documentation**
"""

import copy
import inspect
import os
import uuid

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.BundlerClient import BundlerClient


class BundleTaskDict(dict):
    def __init__(self, getProperty):
        self.getProperty = getProperty

    def __contains__(self, jobId):
        if super().__contains__(jobId):
            return True

        res = self.getProperty(jobId)
        if res:
            self.__setitem__(jobId, res)
            return True

        return False

    def __getitem__(self, jobId):
        if jobId in self:
            return super().__getitem__(jobId)

        res = self.getProperty(jobId)
        if res:
            super().__setitem__(jobId, res)

        return res


class BundleComputingElement(ComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        if not ceUniqueID.startswith("bundled-"):
            ceUniqueID = f"bundled-{ceUniqueID}"

        super().__init__(ceUniqueID)

        self.mandatoryParameters = ["InnerCEType"]

        self.innerCE = None
        self.innerCEParams = {}

        self.bundler = BundlerClient()
        self.ceFactory = ComputingElementFactory()

        self.taskResults = BundleTaskDict(self.__getTraskResult)

    #############################################################################

    def _reset(self):
        self.taskResults = BundleTaskDict(self.__getTraskResult)

        # Force the CE to make the job submissions asynchronous
        self.ceParameters["AsyncSubmission"] = True

        # Create the InnerCE from the config obtained from the BundleCE
        innerCEParams = copy.deepcopy(self.ceParameters)
        innerCEType = innerCEParams.pop("InnerCEType")
        innerCEParams["CEType"] = innerCEType

        innerCeName = self.ceParameters["GridCE"][len("bundled-") :]

        innerCEParams["GridCE"] = innerCeName

        # Building of the InnerCE
        result = self.ceFactory.getCE(ceType=innerCEType, ceName=innerCeName, ceParametersDict=innerCEParams)

        if not result["OK"]:
            self.log.error("Failure while creating the InnerCE")
            return result

        self.innerCE = result["Value"]
        self.innerCE.setParameters(innerCEParams)
        self.innerCEParams = innerCEParams

        self.innerCEMethods = [
            name for name, _ in inspect.getmembers(self.innerCE, predicate=inspect.ismethod) if name[0] != "_"
        ]

        self.bundlesBaseDir = gConfig.getValue("/LocalSite/BundlesBaseDir", "/tmp/bundles")

        return S_OK()

    #############################################################################

    def submitJob(self, executableFile, proxy=None, numberOfProcessors=1, inputs=[], outputs=[], **kwargs):
        jobId = str(uuid.uuid4().hex)

        proxy = self.proxy if self.proxy else proxy

        if not proxy:
            self.log.error("Proxy not defined. Use setProxy or send proxy during job submission")
            return S_ERROR("PROXY NOT DEFINED")

        # Store the job in a bundle using the ceDict of the InnerCE (containing the template)
        if isinstance(proxy, str):
            return S_ERROR("PROXY CANNOT BE IN A STRING FORMAT")

        proxyStr = proxy.dumpAllToString()["Value"]
        result = self.writeProxyToFile(proxyStr)

        if not result["OK"]:
            return result

        proxyPath = result["Value"]

        diracId = kwargs.get("jobDesc", {}).get("jobID", None)
        if diracId:
            diracId = int(diracId)

        result = self.bundler.storeInBundle(
            jobId, executableFile, inputs, outputs, proxyPath, numberOfProcessors, self.innerCEParams, diracId
        )

        if not result["OK"]:
            self.log.error(f"Failure while storing in the Bundle: {result}")
            return result

        bundleId = result["Value"]["BundleID"]
        submitted = result["Value"]["Executing"]  # For logging purposes

        result = S_OK([jobId])
        result["PilotStampDict"] = {jobId: bundleId}

        if not submitted:
            self.log.info(f"Job {jobId} stored successfully in bundle: ", bundleId)
        else:
            self.log.info("Submitting job to CE: ", self.innerCE.ceName)

        # Return the id of the job, setting the "PilotStamp" to the BundleID
        return result

    def getJobOutput(self, jobId, workingDirectory="."):
        bundleId = None
        if ":::" in jobId:
            jobId, bundleId = jobId.split(":::")

        if not bundleId:
            bundleId = self.bundler.bundleIdFromJobId(jobId)

        result = self.bundler.getTaskInfo(bundleId)

        if not result["OK"]:
            return result

        if result["Value"]["Status"] not in PilotStatus.PILOT_FINAL_STATES:
            return S_ERROR("Output not ready yet")

        taskId = result["Value"]["TaskID"]

        result = self.innerCE.getJobOutput(taskId, workingDirectory=workingDirectory, path=jobId)

        error = os.path.join(workingDirectory, f"{bundleId}.err")
        output = os.path.join(workingDirectory, f"{bundleId}.out")

        if not os.path.exists(output) or not os.path.exists(error):
            return S_ERROR("Outputs unable to be obtained")

        with open(output) as f:
            output = f.read()

        with open(error) as f:
            error = f.read()

        return S_OK((output, error))

    def getJobStatus(self, jobIDList):
        resultDict = {}

        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]

        for job in jobIDList:
            jobId = job
            bundleId = None
            if ":::" in job:
                jobId, bundleId = job.split(":::")

            if not bundleId:
                result = self.bundler.bundleIdFromJobId(jobId)
                if not result["OK"]:
                    return result
                bundleId = result["Value"]

            self.log.debug(f"Obtaining the status of job: '{jobId}' with bundleID: '{bundleId}'")
            result = self.bundler.getBundleStatus(bundleId)

            if not result["OK"]:
                return result

            # Default Value: The one from the Bundle
            resultDict[jobId] = result["Value"]
            self.log.debug(f"Status of bundle '{bundleId}': {result['Value']}")

        return S_OK(resultDict)

    #############################################################################

    def getCEStatus(self):
        return self.innerCE.getCEStatus()

    def setProxy(self, proxy):
        super().setProxy(proxy)
        self.innerCE.setProxy(proxy)

    def setToken(self, token, valid=0):
        super().setToken(token, valid)
        self.innerCE.setToken(token, valid)

    def cleanJob(self, jobIDList):
        if "cleanJob" not in self.innerCEMethods:
            self.log.error(f"Inner CE {self.innerCE.ceName} has no function called 'cleanJob'")
            return S_ERROR(f"Inner CE {self.innerCE.ceName} has no function called 'cleanJob'")

        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]

        for job in jobIDList:
            if ":::" in job:
                job, bundleId = job.split(":::")

            return self.bundler.cleanJob(job)

    def killJob(self, jobIDList):
        resultDict = {}

        for job in jobIDList:
            if ":::" in job:
                jobId, bundleId = job.split(":::")

            result = self.bundler.tryToKillJob(jobId)
            resultDict[jobId] = result

        return resultDict

    #############################################################################

    def __getTraskResult(self, jobId):
        self.log.debug(f"Obtaining the task results of {jobId}")

        result = self.getJobStatus(jobId)

        if not result["OK"]:
            return result

        if ":::" in jobId:
            jobId, _ = jobId.split(":::")

        status = result["Value"][jobId]

        if status not in PilotStatus.PILOT_FINAL_STATES:
            return S_OK()

        if status == PilotStatus.DONE:
            return S_OK(0)

        return S_OK(1)
