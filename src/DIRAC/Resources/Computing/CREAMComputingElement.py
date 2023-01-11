""" CREAM Computing Element
"""
import os
import re
import tempfile
import stat

from DIRAC import S_OK, S_ERROR

from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


CE_NAME = "CREAM"
MANDATORY_PARAMETERS = ["Queue"]
STATES_MAP = {
    "DONE-OK": PilotStatus.DONE,
    "DONE-FAILED": PilotStatus.FAILED,
    "REGISTERED": PilotStatus.WAITING,
    "PENDING": PilotStatus.WAITING,
    "IDLE": PilotStatus.WAITING,
    "ABORTED": PilotStatus.ABORTED,
    "CANCELLED": PilotStatus.ABORTED,
    "RUNNING": PilotStatus.RUNNING,
    "REALLY-RUNNING": PilotStatus.RUNNING,
    "N/A": PilotStatus.UNKNOWN,
}


class CREAMComputingElement(ComputingElement):

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.ceType = CE_NAME
        self.submittedJobs = 0
        self.mandatoryParameters = MANDATORY_PARAMETERS
        self.pilotProxy = ""
        self.queue = ""
        self.outputURL = "gsiftp://localhost"
        self.gridEnv = ""
        self.proxyRenewal = 0

    #############################################################################
    def _addCEConfigDefaults(self):
        """Method to make sure all necessary Configuration Parameters are defined"""
        # First assure that any global parameters are loaded
        ComputingElement._addCEConfigDefaults(self)

    def __writeJDL(self, executableFile, processors=1):
        """Create the JDL for submission"""

        workingDirectory = self.ceParameters["WorkingDirectory"]
        fd, name = tempfile.mkstemp(suffix=".jdl", prefix="CREAM_", dir=workingDirectory)
        diracStamp = os.path.basename(name).replace(".jdl", "").replace("CREAM_", "")

        extraJDLParameters = []
        if processors != 1:
            if processors <= 0:
                extraJDLParameters.append("HostNumber = 1")
                extraJDLParameters.append("WholeNodes = true")
            else:
                extraJDLParameters.append("SMPGranularity = %d" % processors)
                extraJDLParameters.append("CPUNumber = %d" % processors)
                extraJDLParameters.append("WholeNodes = false")

        extraParams = self.ceParameters.get("ExtraJDLParameters")
        if extraParams:
            extraJDLParameters += extraParams.strip().split(";")

        extraJDLParameterList = ";\n  ".join([item.strip() for item in extraJDLParameters])

        jdlFile = os.fdopen(fd, "w")

        jdl = """
[
  JobType = "Normal";
  Executable = "{executable}";
  StdOutput="{diracStamp}.out";
  StdError="{diracStamp}.err";
  InputSandbox={{"{executableFile}"}};
  OutputSandbox={{"{diracStamp}.out", "{diracStamp}.err"}};
  OutputSandboxBaseDestUri="{outputURL}";
  {extraJDLParameters}
]
    """.format(
            executableFile=executableFile,
            executable=os.path.basename(executableFile),
            outputURL=self.outputURL,
            diracStamp=diracStamp,
            extraJDLParameters=extraJDLParameterList,
        )

        jdlFile.write(jdl)
        jdlFile.close()
        return name, diracStamp

    def _reset(self):
        self.queue = self.ceParameters.get("CEQueueName", self.ceParameters["Queue"])
        self.outputURL = self.ceParameters.get("OutputURL", "gsiftp://localhost")
        self.gridEnv = self.ceParameters.get("GridEnv", self.gridEnv)
        return S_OK()

    #############################################################################
    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        """Method to submit job"""

        self.log.verbose("Executable file path: %s" % executableFile)
        if not os.access(executableFile, 5):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

        nProcessors = self.ceParameters.get("NumberOfProcessors", 1)

        batchIDList = []
        stampDict = {}
        if numberOfJobs == 1:
            jdlName, diracStamp = self.__writeJDL(executableFile, processors=nProcessors)
            cmd = ["glite-ce-job-submit", "-n", "-a", "-N", "-r", f"{self.ceName}/{self.queue}", "%s" % jdlName]

            result = executeGridCommand(self.proxy, cmd, self.gridEnv)
            os.unlink(jdlName)
            if result["OK"]:
                if result["Value"][0]:
                    # We have got a non-zero status code
                    errorString = "\n".join(result["Value"][1:]).strip()
                    return S_ERROR("Pilot submission failed with error: %s " % errorString)
                pilotJobReference = result["Value"][1].strip()
                if not pilotJobReference:
                    return S_ERROR("No pilot reference returned from the glite job submission command")
                if not pilotJobReference.startswith("https"):
                    return S_ERROR("Invalid pilot reference %s" % pilotJobReference)
                batchIDList.append(pilotJobReference)
                stampDict[pilotJobReference] = diracStamp
        else:
            delegationID = makeGuid()
            cmd = ["glite-ce-delegate-proxy", "-e", "%s" % self.ceName, "%s" % delegationID]
            result = executeGridCommand(self.proxy, cmd, self.gridEnv)
            if not result["OK"]:
                self.log.error("Failed to delegate proxy", result["Message"])
                return result
            for _i in range(numberOfJobs):
                jdlName, diracStamp = self.__writeJDL(executableFile, processors=nProcessors)
                cmd = [
                    "glite-ce-job-submit",
                    "-n",
                    "-N",
                    "-r",
                    f"{self.ceName}/{self.queue}",
                    "-D",
                    "%s" % delegationID,
                    "%s" % jdlName,
                ]
                result = executeGridCommand(self.proxy, cmd, self.gridEnv)
                os.unlink(jdlName)
                if not result["OK"]:
                    self.log.error("General error in execution of glite-ce-job-submit command")
                    break
                if result["Value"][0] != 0:
                    self.log.error("Error in glite-ce-job-submit command", result["Value"][1] + result["Value"][2])
                    break
                pilotJobReference = result["Value"][1].strip()
                if pilotJobReference and pilotJobReference.startswith("https"):
                    batchIDList.append(pilotJobReference)
                    stampDict[pilotJobReference] = diracStamp
                else:
                    break
        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No pilot references obtained from the glite job submission")
        return result

    def killJob(self, jobIDList):
        """Kill the specified jobs"""
        jobList = list(jobIDList)
        if isinstance(jobIDList, str):
            jobList = [jobIDList]

        cmd = ["glite-ce-job-cancel", "-n", "-N"] + jobList
        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
        if not result["OK"]:
            return result
        if result["Value"][0] != 0:
            errorString = "\n".join(result["Value"][1:]).strip()
            return S_ERROR("Failed kill job: %s" % errorString)

        return S_OK()

    #############################################################################
    def getCEStatus(self, jobIDList=None):
        """Method to return information on running and pending jobs.

        :param jobIDList: list of job IDs to be considered
        :type jobIDList: python:list
        """
        statusList = ["REGISTERED", "PENDING", "IDLE", "RUNNING", "REALLY-RUNNING"]
        cmd = ["glite-ce-job-status", "-n", "-a", "-e", "%s" % self.ceName, "-s", "%s" % ":".join(statusList)]
        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
        resultDict = {}
        if not result["OK"]:
            return result
        if result["Value"][0]:
            if result["Value"][0] == 11:
                return S_ERROR("Segmentation fault while calling glite-ce-job-status")
            elif result["Value"][2]:
                return S_ERROR(result["Value"][2])
            elif "Authorization error" in result["Value"][1]:
                return S_ERROR("Authorization error")
            elif "FaultString" in result["Value"][1]:
                res = re.search(r"FaultString=\[([\w\s]+)\]", result["Value"][1])
                fault = ""
                if res:
                    fault = res.group(1)
                detail = ""
                res = re.search(r"FaultDetail=\[([\w\s]+)\]", result["Value"][1])
                if res:
                    detail = res.group(1)
                    return S_ERROR(f"Error: {fault}:{detail}")
            else:
                return S_ERROR("Error while interrogating CE status")
        if result["Value"][1]:
            resultDict = self.__parseJobStatus(result["Value"][1])

        running = 0
        waiting = 0
        statusDict = {}
        for ref, status in resultDict.items():
            if jobIDList is not None and ref not in jobIDList:
                continue
            if status == "Scheduled":
                waiting += 1
            if status == "Running":
                running += 1
            statusDict[ref] = status

        result = S_OK()
        result["RunningJobs"] = running
        result["WaitingJobs"] = waiting
        result["SubmittedJobs"] = 0
        result["JobStatusDict"] = statusDict
        return result

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
        if self.proxyRenewal % 60 == 0:
            self.proxyRenewal += 1
            statusList = ["REGISTERED", "PENDING", "IDLE", "RUNNING", "REALLY-RUNNING"]
            cmd = [
                "glite-ce-job-status",
                "-L",
                "2",
                "--all",
                "-e",
                "%s" % self.ceName,
                "-s",
                "%s" % ":".join(statusList),
            ]
            result = executeGridCommand(self.proxy, cmd, self.gridEnv)
            if result["OK"]:
                delegationIDs = []
                for line in result["Value"][1].split("\n"):
                    if line.find("Deleg Proxy ID") != -1:
                        delegationID = line.split()[-1].replace("[", "").replace("]", "")
                        if delegationID not in delegationIDs:
                            delegationIDs.append(delegationID)
                if delegationIDs:
                    # Renew proxies in batches to avoid timeouts
                    chunkSize = 10
                    for i in range(0, len(delegationIDs), chunkSize):
                        chunk = delegationIDs[i : i + chunkSize]
                        cmd = ["glite-ce-proxy-renew", "-e", self.ceName]
                        cmd.extend(chunk)
                        self.log.info("Refreshing proxy for:", " ".join(chunk))
                        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
                        if result["OK"]:
                            status, output, error = result["Value"]
                            if status:
                                self.log.error(
                                    "Failed to renew proxy delegation", "Output:\n" + output + "\nError:\n" + error
                                )

        workingDirectory = self.ceParameters["WorkingDirectory"]
        fd, idFileName = tempfile.mkstemp(suffix=".ids", prefix="CREAM_", dir=workingDirectory)
        idFile = os.fdopen(fd, "w")
        idFile.write("##CREAMJOBS##")
        for id_ in jobIDList:
            if ":::" in id_:
                ref, _stamp = id_.split(":::")
            else:
                ref = id_
            idFile.write("\n" + ref)
        idFile.close()

        cmd = ["glite-ce-job-status", "-n", "-i", "%s" % idFileName]
        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
        os.unlink(idFileName)
        resultDict = {}
        if not result["OK"]:
            self.log.error("Failed to get job status", result["Message"])
            return result
        if result["Value"][0]:
            if result["Value"][2]:
                return S_ERROR(result["Value"][2])
            return S_ERROR("Error while interrogating job statuses")
        if result["Value"][1]:
            resultDict = self.__parseJobStatus(result["Value"][1])

        if not resultDict:
            return S_ERROR("No job statuses returned")

        # If CE does not know about a job, set the status to Unknown
        for job in jobIDList:
            if job not in resultDict:
                resultDict[job] = PilotStatus.UNKNOWN

        return S_OK(resultDict)

    def __parseJobStatus(self, output):
        """Parse the output of the glite-ce-job-status"""
        resultDict = {}
        ref = ""
        for line in output.split("\n"):
            if not line:
                continue
            match = re.search(r"JobID=\[(.*)\]", line)
            if match and len(match.groups()) == 1:
                ref = match.group(1)
            match = re.search(r"Status.*\[(.*)\]", line)
            if match and len(match.groups()) == 1:
                creamStatus = match.group(1)
                resultDict[ref] = STATES_MAP.get(creamStatus, PilotStatus.UNKNOWN)

        return resultDict

    def getJobLog(self, jobID):
        """Get pilot job logging info

        :param str jobID: pilot job identifier
        :return: string representing the logging info of a given pilot job
        """
        # pilotRef may integrate the pilot stamp
        # it has to be removed before being passed in parameter
        jobID = jobID.split(":::")[0]
        cmd = ["glite-ce-job-status", "-L", "2", "%s" % jobID]
        ret = executeGridCommand("", cmd, self.gridEnv)
        if not ret["OK"]:
            return ret

        status, output, error = ret["Value"]
        if status:
            return S_ERROR(error)

        return S_OK(output)

    def getJobOutput(self, jobID):
        """Get the specified job standard output and error files. The output is returned
        as strings.
        """
        if jobID.find(":::") != -1:
            pilotRef, stamp = jobID.split(":::")
        else:
            pilotRef = jobID
            stamp = ""
        if not stamp:
            return S_ERROR("Pilot stamp not defined for %s" % pilotRef)

        outURL = self.ceParameters.get("OutputURL", "gsiftp://localhost")
        if outURL == "gsiftp://localhost":
            result = self.__resolveOutputURL(pilotRef)
            if not result["OK"]:
                return result
            outURL = result["Value"]

        outputURL = os.path.join(outURL, "%s.out" % stamp)
        errorURL = os.path.join(outURL, "%s.err" % stamp)
        workingDirectory = self.ceParameters["WorkingDirectory"]
        outFileName = os.path.join(workingDirectory, os.path.basename(outputURL))
        errFileName = os.path.join(workingDirectory, os.path.basename(errorURL))

        cmd = ["globus-url-copy", "%s" % outputURL, "file://%s" % outFileName]
        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
        output = ""
        if result["OK"]:
            if not result["Value"][0]:
                outFile = open(outFileName)
                output = outFile.read()
                outFile.close()
                os.unlink(outFileName)
            elif result["Value"][0] == 1 and "No such file or directory" in result["Value"][2]:
                output = "Standard Output is not available on the CREAM service"
                if os.path.exists(outFileName):
                    os.unlink(outFileName)
                return S_ERROR(output)
            else:
                error = "\n".join(result["Value"][1:])
                return S_ERROR(error)
        else:
            return S_ERROR("Failed to retrieve output for %s" % jobID)

        cmd = ["globus-url-copy", "%s" % errorURL, "%s" % errFileName]
        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
        error = ""
        if result["OK"]:
            if not result["Value"][0]:
                errFile = open(errFileName)
                error = errFile.read()
                errFile.close()
                os.unlink(errFileName)
        elif result["Value"][0] == 1 and "No such file or directory" in result["Value"][2]:
            error = "Standard Error is not available on the CREAM service"
            if os.path.exists(errFileName):
                os.unlink(errFileName)
            return S_ERROR(error)
        else:
            return S_ERROR("Failed to retrieve error for %s" % jobID)

        return S_OK((output, error))

    def __resolveOutputURL(self, pilotRef):
        """Resolve the URL of the pilot output files"""

        cmd = ["glite-ce-job-status", "-L", "2", "%s" % pilotRef, "| grep -i osb"]
        result = executeGridCommand(self.proxy, cmd, self.gridEnv)
        url = ""
        if result["OK"]:
            if not result["Value"][0]:
                output = result["Value"][1]
                for line in output.split("\n"):
                    line = line.strip()
                    if line.find("OSB") != -1:
                        match = re.search(r"\[(.*)\]", line)
                        if match:
                            url = match.group(1)
            if url:
                return S_OK(url)
            return S_ERROR("output URL not found for %s" % pilotRef)
        else:
            return S_ERROR("Failed to retrieve long status for %s" % pilotRef)
