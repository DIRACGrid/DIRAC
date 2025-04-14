""" LocalComputingElement is a class to handle non-grid computing clusters

Allows direct submission to underlying Batch Systems.

**Configuration Parameters**

Configuration for the LocalComputingElement submission can be done via the configuration system.

BatchError:
   Area where the job errors are stored.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

BatchOutput:
   Area where the job outputs are stored.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

BatchSystem:
   Underlying batch system that is going to be used to orchestrate executable files. The Batch System has to be
   accessible from the LocalCE. By default, the LocalComputingElement submits directly on the host via the Host class.

ExecutableArea:
   Area where the executable files are stored if necessary.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

SharedArea:
   Area used to store executable/output/error files if they are not aready defined via BatchOutput, BatchError,
   InfoArea, ExecutableArea and/or WorkArea. The path should be absolute.

**Code Documentation**
"""
import os
import stat
import shutil
import tempfile
import getpass
import errno
import uuid
from urllib.parse import urlparse

from DIRAC import S_OK, S_ERROR

from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Core.Utilities.Subprocess import systemCall


class LocalComputingElement(ComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.execution = "Local"
        self.submittedJobs = 0
        self.userName = getpass.getuser()

    def _reset(self):
        """Process CE parameters and make necessary adjustments"""
        batchSystemName = self.ceParameters.get("BatchSystem", "Host")
        result = self.loadBatchSystem(batchSystemName)
        if not result["OK"]:
            self.log.error("Failed to load the batch system plugin %s", batchSystemName)
            return result

        self.queue = self.ceParameters["Queue"]
        self.log.info("Using queue: ", self.queue)

        self.sharedArea = self.ceParameters["SharedArea"]
        self.batchOutput = self.ceParameters["BatchOutput"]
        if not self.batchOutput.startswith("/"):
            self.batchOutput = os.path.join(self.sharedArea, self.batchOutput)
        self.batchError = self.ceParameters["BatchError"]
        if not self.batchError.startswith("/"):
            self.batchError = os.path.join(self.sharedArea, self.batchError)
        self.infoArea = self.ceParameters["InfoArea"]
        if not self.infoArea.startswith("/"):
            self.infoArea = os.path.join(self.sharedArea, self.infoArea)
        self.executableArea = self.ceParameters["ExecutableArea"]
        if not self.executableArea.startswith("/"):
            self.executableArea = os.path.join(self.sharedArea, self.executableArea)
        self.workArea = self.ceParameters["WorkArea"]
        if not self.workArea.startswith("/"):
            self.workArea = os.path.join(self.sharedArea, self.workArea)

        result = self._prepareHost()
        if not result["OK"]:
            self.log.error("Failed to initialize CE", self.ceName)
            return result

        self.removeOutput = True
        if "RemoveOutput" in self.ceParameters:
            if self.ceParameters["RemoveOutput"].lower() in ["no", "false", "0"]:
                self.removeOutput = False

        self.submitOptions = self.ceParameters.get("SubmitOptions", "")
        self.numberOfProcessors = self.ceParameters.get("NumberOfProcessors", 1)
        self.wholeNode = self.ceParameters.get("WholeNode", False)
        # numberOfNodes is treated as a string as it can contain values such as "2-4"
        # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
        self.numberOfNodes = self.ceParameters.get("NumberOfNodes", "1")
        self.numberOfGPUs = self.ceParameters.get("NumberOfGPUs")

        return S_OK()

    def _addCEConfigDefaults(self):
        """Method to make sure all necessary Configuration Parameters are defined"""
        # First assure that any global parameters are loaded
        ComputingElement._addCEConfigDefaults(self)
        # Now batch system specific ones
        if "SharedArea" not in self.ceParameters:
            defaultPath = os.environ.get("HOME", ".")
            self.ceParameters["SharedArea"] = defaultPath

        if "BatchOutput" not in self.ceParameters:
            self.ceParameters["BatchOutput"] = "data"

        if "BatchError" not in self.ceParameters:
            self.ceParameters["BatchError"] = "data"

        if "ExecutableArea" not in self.ceParameters:
            self.ceParameters["ExecutableArea"] = "data"

        if "InfoArea" not in self.ceParameters:
            self.ceParameters["InfoArea"] = "info"

        if "WorkArea" not in self.ceParameters:
            self.ceParameters["WorkArea"] = "work"

    def _prepareHost(self):
        """Prepare directories and copy control script"""

        # Make remote directories
        dirTuple = uniqueElements(
            [self.sharedArea, self.executableArea, self.infoArea, self.batchOutput, self.batchError, self.workArea]
        )
        cmdTuple = ["mkdir", "-p"] + dirTuple
        self.log.verbose("Creating working directories")
        result = systemCall(30, cmdTuple)
        if not result["OK"]:
            self.log.error("Failed creating working directories", f"({result['Message'][1]})")
            return result
        status, output, error = result["Value"]
        if status != 0:
            self.log.error("Failed to create directories", f"({error})")
            return S_ERROR(errno.EACCES, "Failed to create directories")

        return S_OK()

    def submitJob(self, executableFile, proxy=None, numberOfJobs=1):
        if not os.access(executableFile, 5):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

        jobStamps = []
        for _i in range(numberOfJobs):
            jobStamps.append(uuid.uuid4().hex)

        batchDict = {
            "Executable": executableFile,
            "NJobs": numberOfJobs,
            "OutputDir": self.batchOutput,
            "ErrorDir": self.batchError,
            "SubmitOptions": self.submitOptions,
            "ExecutionContext": self.execution,
            "JobStamps": jobStamps,
            "Queue": self.queue,
            "WholeNode": self.wholeNode,
            "NumberOfProcessors": self.numberOfProcessors,
            "NumberOfNodes": self.numberOfNodes,
            "NumberOfGPUs": self.numberOfGPUs,
        }
        resultSubmit = self.batchSystem.submitJob(**batchDict)

        if resultSubmit["Status"] == 0:
            self.submittedJobs += len(resultSubmit["Jobs"])
            # jobIDs = [ self.ceType.lower()+'://'+self.ceName+'/'+_id for _id in resultSubmit['Jobs'] ]
            # FIXME: It would be more proper to fix pilotCommands.__setFlavour where 'ssh' is hardcoded than
            # making this illogical fix, but there is no good way for pilotCommands to know its origin ceType.
            # So, the jobIDs here need to start with 'ssh', not ceType, to accomodate
            # them to those hardcoded in pilotCommands.__setFlavour
            batchSystemName = self.batchSystem.__class__.__name__.lower()
            jobIDs = ["ssh" + batchSystemName + "://" + self.ceName + "/" + _id for _id in resultSubmit["Jobs"]]
            result = S_OK(jobIDs)
            if "ExecutableToKeep" in resultSubmit:
                result["ExecutableToKeep"] = resultSubmit["ExecutableToKeep"]
        else:
            result = S_ERROR(resultSubmit["Message"])

        return result

    def killJob(self, jobIDList):
        """Kill a bunch of jobs"""

        batchDict = {"JobIDList": jobIDList, "Queue": self.queue}
        resultKill = self.batchSystem.killJob(**batchDict)
        if resultKill["Status"] == 0:
            return S_OK()
        return S_ERROR(resultKill["Message"])

    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = 0
        result["WaitingJobs"] = 0

        batchDict = {"User": self.userName, "Queue": self.queue}
        resultGet = self.batchSystem.getCEStatus(**batchDict)
        if resultGet["Status"] == 0:
            result["RunningJobs"] = resultGet.get("Running", 0)
            result["WaitingJobs"] = resultGet.get("Waiting", 0)
        else:
            result = S_ERROR(resultGet["Message"])

        self.log.verbose("Waiting Jobs: ", result["WaitingJobs"])
        self.log.verbose("Running Jobs: ", result["RunningJobs"])

        return result

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
        resultDict = {}
        jobDict = {}

        # Extract the batch job ID from the full DIRAC job ID
        for job in jobIDList:
            stamp = os.path.basename(urlparse(job).path)
            jobDict[stamp] = job
        stampList = list(jobDict)

        # Get the status for a given batch job ID
        batchDict = {"JobIDList": stampList, "User": self.userName, "Queue": self.queue}
        resultGet = self.batchSystem.getJobStatus(**batchDict)

        if resultGet["Status"] != 0:
            return S_ERROR(resultGet["Message"])

        # Construct the dictionary to return: resultDict[dirac job ID] = status
        for stamp, status in resultGet["Jobs"].items():
            resultDict[jobDict[stamp]] = status

        return S_OK(resultDict)

    def getJobOutput(self, jobID, localDir=None):
        """Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned
        as strings.
        """
        self.log.verbose("Getting output for jobID", jobID)
        result = self._getJobOutputFiles(jobID)
        if not result["OK"]:
            return result

        jobStamp, _host, outputFile, errorFile = result["Value"]

        if not localDir:
            tempDir = tempfile.mkdtemp()
        else:
            tempDir = localDir

        try:
            localOut = os.path.join(tempDir, f"{jobStamp}.out")
            localErr = os.path.join(tempDir, f"{jobStamp}.err")
            if os.path.exists(outputFile):
                shutil.copy(outputFile, localOut)
            if os.path.exists(errorFile):
                shutil.copy(errorFile, localErr)
        except Exception as x:
            return S_ERROR(f"Failed to get output files: {str(x)}")

        open(localOut, "a").close()
        open(localErr, "a").close()

        # The result is OK, we can remove the output
        if self.removeOutput and os.path.exists(outputFile):
            os.remove(outputFile)
        if self.removeOutput and os.path.exists(errorFile):
            os.remove(errorFile)

        if localDir:
            return S_OK((localOut, localErr))

        # Return the output as a string
        with open(localOut) as outputFile:
            output = outputFile.read()
        with open(localErr) as errorFile:
            error = errorFile.read()
        shutil.rmtree(tempDir)
        return S_OK((output, error))

    def _getJobOutputFiles(self, jobID):
        """Get output file names for the specific CE"""
        jobStamp = os.path.basename(urlparse(jobID).path)
        host = urlparse(jobID).hostname

        if hasattr(self.batchSystem, "getJobOutputFiles"):
            batchDict = {
                "JobIDList": [jobStamp],
                "OutputDir": self.batchOutput,
                "ErrorDir": self.batchError,
                "NumberOfNodes": self.numberOfNodes,
            }
            result = self.batchSystem.getJobOutputFiles(**batchDict)
            if result["Status"] != 0:
                return S_ERROR(f"Failed to get job output files: {result['Message']}")

            output = result["Jobs"][jobStamp]["Output"]
            error = result["Jobs"][jobStamp]["Error"]
        else:
            output = f"{self.batchOutput}/{jobStamp}.out"
            error = f"{self.batchError}/{jobStamp}.out"

        return S_OK((jobStamp, host, output, error))
