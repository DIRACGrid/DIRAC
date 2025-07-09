import os
import sys
import time
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Resources.Computing.AREXComputingElement import AREXComputingElement

# AREXComputingElement redefinition
import os
import json
import requests
import shutil
from DIRAC import S_OK, S_ERROR


class AREXEnhancedComputingElement(AREXComputingElement):
    def _getListOfAvailableOutputs(self, jobID, arcJobID, path=None):
        """Request a list of outputs available for a given jobID.

        :param str jobID: job reference without the DIRAC stamp
        :param str arcJobID: ARC job ID
        :param str path: remote path
        :return list: names of the available outputs
        """
        query = self._urlJoin(os.path.join("jobs", arcJobID, "session", path or ''))

        # Submit the GET request to retrieve the names of the outputs
        #self.log.debug(f"Retrieving the names of the outputs for {jobID}")
        self.log.debug(f"Retrieving the names of the outputs with {query}")
        result = self._request("get", query)
        if not result["OK"]:
            self.log.error("Failed to retrieve at least some outputs", f"for {jobID}: {result['Message']}")
            return S_ERROR(f"Failed to retrieve at least some outputs for {jobID}")
        response = result["Value"]

        if not response.text:
            return S_ERROR(f"There is no output for job {jobID}")

        #return S_OK(response.json()["file"])
        return S_OK(response.json())
    
    def getJobOutput(self, jobID, workingDirectory=None, path=None):
        """Get the outputs of the given job reference.

        Outputs and stored in workingDirectory if present, else in a new directory named <ARC JobID>.

        :param str jobID: job reference followed by the DIRAC stamp.
        :param str workingDirectory: name of the directory containing the retrieved outputs.
        :param str path: remote path
        :return: content of stdout and stderr
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot get job outputs", result["Message"])
            return result

        # Extract stamp from the Job ID
        if ":::" in jobID:
            jobRef, stamp = jobID.split(":::")
        else:
            return S_ERROR(f"DIRAC stamp not defined for {jobID}")
        arcJob = self._jobReferenceToArcID(jobRef)

        # Get the list of available outputs
        result = self._getListOfAvailableOutputs(jobRef, arcJob, path)
        if not result["OK"]:
            return result
        remoteOutputs = result["Value"]
        self.log.debug("Outputs to get are", remoteOutputs)

        remoteOutputsFiles = []
        if 'file' in remoteOutputs:
            remoteOutputsFiles = remoteOutputs["file"]

        remoteOutputsDirs  = [] 
        if 'dir' in remoteOutputs:
            remoteOutputsDirs = remoteOutputs["dir"]

        if not workingDirectory:
            if "WorkingDirectory" in self.ceParameters:
                # We assume that workingDirectory exists
                workingDirectory = os.path.join(self.ceParameters["WorkingDirectory"], arcJob)
            else:
                workingDirectory = arcJob
        
        if not os.path.exists(workingDirectory):
            os.mkdir(workingDirectory)
            
        # Directories
        for remoteOutput in remoteOutputsDirs:
            self.getJobOutput(jobID, 
                    workingDirectory=os.path.join(workingDirectory, remoteOutput),
                    path=os.path.join(path or '',  remoteOutput))

        # Files
        stdout = None
        stderr = None
        for remoteOutput in remoteOutputsFiles:
            # Prepare the command
            #query = self._urlJoin(os.path.join("jobs", arcJob, "session", remoteOutput))
            query = self._urlJoin(os.path.join("jobs", arcJob, "session", path or '', remoteOutput))

            # Submit the GET request to retrieve outputs
            result = self._request("get", query, stream=True)
            if not result["OK"]:
                self.log.error("Error downloading", f"{remoteOutput} for {arcJob}: {result['Message']}")
                return S_ERROR(f"Error downloading {remoteOutput} for {jobID}")
            response = result["Value"]

            localOutput = os.path.join(workingDirectory, remoteOutput)
            with open(localOutput, "wb") as f:
                shutil.copyfileobj(response.raw, f)

            if remoteOutput == f"{stamp}.out":
                with open(localOutput) as f:
                    stdout = f.read()
            if remoteOutput == f"{stamp}.err":
                with open(localOutput) as f:
                    stderr = f.read()


        return S_OK((stdout, stderr))
