"""Bundle Computing Elemenet

Allows grouping jobs in a single big job prior to their submission in an actual CE.

**Configuration Parameters**

Configuration for the BundleComputingElemenet submission can be done via the configuration system.
Below, you can find a list of parameters specific to the BundleCE.

ExecTemplate:
    Name of the execution template to be used to bundle the jobs.
    This template will the one that be passed to the CE to be executed alongside
        each jobExecutable file and input as the inputs of the template.

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
        ExecTemplate = BASH

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
import uuid

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.BundlerClient import BundlerClient


class BundleComputingElement(ComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.mandatoryParameters = ["ExecTemplate", "InnerCEType"]

        self.innerCE = None
        self.innerCEParams = {}

        self.bundler = BundlerClient()
        self.ceFactory = ComputingElementFactory()

    def _reset(self):
        # Force the CE to make the job submissions asynchronous
        self.ceParameters["AsyncSubmission"] = True

        # Create the InnerCE from the config obtained from the BundleCE
        innerCEParams = copy.deepcopy(self.ceParameters)
        innerCEType = innerCEParams.pop("InnerCEType")
        innerCEParams["CEType"] = innerCEType

        innerCeName = self.ceParameters["GridCE"].split("bundled-")[1]
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
            name
            for name, _ in 
            self.inspect.getmembers(self.innerCE, predicate=inspect.ismethod)
            if name[0] != "_"
        ]

        return S_OK()

    def submitJob(self, executableFiles, proxy=None, numberOfProcessors=1, inputs=None, outputs=[]):
        jobId = f"BUNDLE_{self.ceName}_{uuid.uuid4().hex}"

        # Store the job in a bundle using the ceDict of the InnerCE (containing the template)
        result = proxy.dumpAllToString()

        if not result["OK"]:
            self.log.error("Error while encoding proxy as string")
            return result

        result = self.bundler.storeInBundle(
            jobId, 
            executableFiles, 
            inputs, 
            result["Value"], 
            numberOfProcessors, 
            self.innerCEParams
        )

        if not result["OK"]:
            self.log.error("Failure while storing in the Bundle")
            return result

        bundleId = result["Value"]["BundleID"]
        submitted = result["Value"]["Executing"]

        # The bundle is not being executed in the InnerCE
        if not submitted:
            self.log.info(f"Job {jobId} stored successfully in bundle: ", bundleId)
            # Return the bundle id as if it was the task id of the asynchronous executing job
            return S_OK([jobId])

        else:
            self.log.info("Submitting job to CE: ", self.ce.ceName)

        # Return the id of the job (NOT THE BUNDLE)
        return S_OK(jobId)

    def getJobOutput(self, jobId, workingDirectory=None):
        if ":::" in jobId:
            jobId = jobId.split(":::")[0]

        result = self.bundler.getJobTask(jobId)

        if not result["OK"]:
            return result

        bundleId, taskId = result["Value"]
        self.innerCE.getJobOutput(taskId)
        
        return ()

    def getJobStatus(self, jobIDList):
        resultDict = {}

        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]

        for job in jobIDList:
            if ":::" in job:
                job = job.split(":::")[0]

            result = self.bundler.getBundleStatusOfJob(job)
            
            if not result["OK"]:
                self.log.error(result["Message"])
                resultDict[job] = PilotStatus.FAILED
            else:
                if result["Value"] == "Finalized":
                    resultDict[job] = PilotStatus.DONE 
                elif result["Value"] == "Failed":
                    resultDict[job] = PilotStatus.DONE 
                else:
                    resultDict[job] = PilotStatus.RUNNING

        return S_OK(resultDict)

    def killJob(self, jobIDList):
        resultDict = {}

        for jobId in jobIDList:
            result = self.bundler.tryToKillJob(jobId)
            resultDict[jobId] = result

        return resultDict

    def getCEStatus(self):
        return self.innerCE.getCEStatus()
    
    def setProxy(self, proxy):
        super().setProxy(proxy)
        self.innerCE.setProxy(proxy)
    
    def setToken(self, token):
        super().setToken(token)
        self.innerCE.setToken(token)

    def cleanJob(self, jobIDList):
        if "cleanJob" not in self.innerCEMethods:
           self.log.error(f"Inner CE {self.innerCE.ceName} has no function called 'cleanJob'")
           return S_ERROR()

        for job in jobIDList:
            if ":::" in job:
                job = job.split(":::")[0]
            self.bundler.cleanJob(job)
