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

import uuid

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.BundlerClient import BundlerClient


class BundleComputingElement(ComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.mandatoryParameters = ["ExecTemplate", "InnerCEType"]

        self.innerCE = None

        self.bundler = BundlerClient()
        self.ceFactory = ComputingElementFactory()

    def _reset(self):
        # Force the CE to make the job submissions asynchronous
        self.ceParameters["AsyncSubmission"] = True

        # Create the InnerCE from the config obtained from the BundleCE
        innerCEParams = self.ceParameters.copy()
        innerCEType = innerCEParams.pop("InnerCEType")
        innerCEParams["CEType"] = innerCEType

        # Building of the InnerCE
        self.innerCE = self.ceFactory.getCE(ceType=innerCEType, ceParametersDict=innerCEParams)

    def submitJob(self, executableFiles, proxy=None, numberOfProcessors=1, inputs=None):
        # Create a unique ID that cannot clash with other BundleCEs and Jobs in the database
        jobId = f"BUNDLE_{self.ceUniqueID}_{uuid.uuid4()}"

        # Store the job in a bundle using the ceDict of the InnerCE (containing the template)
        ceDict = self.innerCE.getDescription()
        result = self.bundler.storeInBundle(jobId, executableFiles, inputs, proxy, numberOfProcessors, ceDict)

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
        return S_OK([jobId])

    def getJobOutput(self, jobIDList):
        resultDict = {}

        for jobId in jobIDList:
            result = self.bundler.getJobOutput(jobId)

            if not result["OK"]:
                return result

            resultDict[jobId] = result["Value"]

        return resultDict

    # def getJobStatus(self, jobIDList):
    #     pass

    #
    # CAN THIS BE IMPLEMENETED ??
    #
    def killJob(self, jobIDList):
        resultDict = {}

        for jobId in jobIDList:
            resultDict[jobId] = S_ERROR("Bundled jobs cannot be killed at the moment")

        return resultDict

    def getDescription(self):
        return self.innerCE.getDescription()

    def getCEStatus(self):
        return self.innerCE.getCEStatus()
