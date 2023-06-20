""" HTCondorCE Computing Element

Allows direct submission to HTCondorCE Computing Elements with a SiteDirector Agent

**Configuration Parameters**

Configuration for the HTCondorCE submission can be done via the configuration system.

DaysToKeepRemoteLogs:
   How long to keep the log files on the remote schedd until they are removed

DaysToKeepLogs:
   How long to keep the log files locally until they are removed

ExtraSubmitString:
   Additional options for the condor submit file, separate options with '\\n', for example::

     request_cpus = 8 \\n periodic_remove = ...

   CERN proposes additional features to the standard HTCondor implementation. Among these features, one can find
   an option to limit the allocation runtime (`+MaxRuntime`), that does not exist in the standard HTCondor version:
   no explicit way to define a runtime limit (`maxCPUTime` would act as the limit). On CERN-HTCondor CEs, one can use
   CERN-specific features via the `ExtraSubmitString` configuration parameter.

UseLocalSchedd:
   If False, directly submit to a remote condor schedule daemon,
   then one does not need to run condor daemons on the submit machine.
   If True requires the condor grid middleware (condor_submit, condor_history, condor_q, condor_rm)

WorkingDirectory:
   Location to store the pilot and condor log files locally. It should exist on the server and be accessible (both
   readable and writeable).  Also temporary files like condor submit files are kept here. This option is only read
   from the global Resources/Computing/HTCondorCE location.

**Proxy renewal or lifetime**

When not using a local condor_schedd, add ``delegate_job_GSI_credentials_lifetime = 0`` to the ``ExtraSubmitString``.

When using a local condor_schedd look at the HTCondor documentation for enabling the proxy refresh.

**Code Documentation**
"""
# Note: if you read this documentation in the source code and not via the sphinx
# created documentation, there should only be one slash when setting the option,
# but "\n" gets rendered as a linebreak in sphinx

import os
import tempfile

import subprocess
import datetime
import errno
import threading
import textwrap
import uuid

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import writeToTokenFile
from DIRAC.Core.Security.Locations import getCAsLocation
from DIRAC.Resources.Computing.BatchSystems.Condor import parseCondorStatus

MANDATORY_PARAMETERS = ["Queue"]
DEFAULT_WORKINGDIRECTORY = "/opt/dirac/pro/runit/WorkloadManagement/SiteDirectorHT"
DEFAULT_DAYSTOKEEPREMOTELOGS = 1
DEFAULT_DAYSTOKEEPLOGS = 15


def logDir(ceName, stamp):
    """Return path to log and output files for pilot.

    :param str ceName: Name of the CE
    :param str stamp: pilot stamp from/for jobRef
    """
    return os.path.join(ceName, stamp[0], stamp[1:3])


def condorIDAndPathToResultFromJobRef(jobRef):
    """Extract tuple of jobURL and jobID from the jobRef string.
    The condorID as well as the path leading to the job results are also extracted from the jobID.

    :param str jobRef: PilotJobReference of the following form: ``htcondorce://<ceName>/<condorID>:::<pilotStamp>``

    :return: tuple composed of the jobURL, the path to the job results and the condorID of the given jobRef
    """
    splits = jobRef.split(":::")
    jobURL = splits[0]
    stamp = splits[1] if len(splits) > 1 else ""
    _, _, ceName, condorID = jobURL.split("/")

    # Reconstruct the path leading to the result (log, output)
    # Construction of the path can be found in submitJob()
    pathToResult = logDir(ceName, stamp) if len(stamp) >= 3 else ""

    return jobURL, pathToResult, condorID


def findFile(workingDir, fileName, pathToResult):
    """Find a file in a file system.

    :param str workingDir: the name of the directory containing the given file to search for
    :param str fileName: the name of the file to find
    :param str pathToResult: the path to follow from workingDir to find the file

    :return: path leading to the file
    """
    path = os.path.join(workingDir, pathToResult, fileName)
    if os.path.exists(path):
        return S_OK(path)
    return S_ERROR(errno.ENOENT, f"Could not find {path}")


class HTCondorCEComputingElement(ComputingElement):
    """HTCondorCE computing element class
    implementing the functions jobSubmit, getJobOutput
    """

    # static variables to ensure single cleanup every minute
    _lastCleanupTime = datetime.datetime.utcnow()
    _cleanupLock = threading.Lock()

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)
        self.mandatoryParameters = MANDATORY_PARAMETERS

        self.port = "9619"
        self.audienceName = f"{self.ceName}:{self.port}"
        self.submittedJobs = 0
        self.pilotProxy = ""
        self.queue = ""
        self.outputURL = "gsiftp://localhost"
        self.gridEnv = ""
        self.proxyRenewal = 0
        self.daysToKeepLogs = DEFAULT_DAYSTOKEEPLOGS
        self.daysToKeepRemoteLogs = DEFAULT_DAYSTOKEEPREMOTELOGS
        self.extraSubmitString = ""
        # see note on getCondorLogFile, why we can only use the global setting
        # FIXME: just use gConfig.getValue("Resources/Computing/CEDefaults/HTCondorCE/WorkingDirectory", DEFAULT_WORKINGDIRECTORY) from v8.2
        self.workingDirectory = gConfig.getValue(
            "Resources/Computing/CEDefaults/HTCondorCE/WorkingDirectory",
            gConfig.getValue("Resources/Computing/HTCondorCE/WorkingDirectory", DEFAULT_WORKINGDIRECTORY),
        )
        self.useLocalSchedd = True
        self.remoteScheddOptions = ""
        self.tokenFile = None

    #############################################################################
    def __writeSub(self, executable, nJobs, location, processors, pilotStamps, tokenFile=None):
        """Create the Sub File for submission.

        :param str executable: name of the script to execute
        :param int nJobs: number of desired jobs
        :param str location: directory that should contain the result of the jobs
        :param int processors: number of CPU cores to allocate
        :param list pilotStamps: list of pilot stamps (strings)
        """

        self.log.debug("Working directory:", self.workingDirectory)
        mkDir(os.path.join(self.workingDirectory, location))

        self.log.debug("InitialDir:", os.path.join(self.workingDirectory, location))

        self.log.debug(f"ExtraSubmitString:\n### \n {self.extraSubmitString} \n###")

        fd, name = tempfile.mkstemp(suffix=".sub", prefix="HTCondorCE_", dir=self.workingDirectory)
        subFile = os.fdopen(fd, "w")

        executable = os.path.join(self.workingDirectory, executable)

        useCredentials = "use_x509userproxy = true"
        if tokenFile:
            useCredentials += textwrap.dedent(
                f"""
                use_scitokens = true
                scitokens_file = {tokenFile.name}
                """
            )

        # This is used to remove outputs from the remote schedd
        # Used in case a local schedd is not used
        periodicRemove = "periodic_remove = "
        periodicRemove += "(JobStatus == 4) && "
        periodicRemove += f"(time() - EnteredCurrentStatus) > ({self.daysToKeepRemoteLogs} * 24 * 3600)"

        localScheddOptions = (
            """
ShouldTransferFiles = YES
WhenToTransferOutput = ON_EXIT_OR_EVICT
"""
            if self.useLocalSchedd
            else periodicRemove
        )

        targetUniverse = "grid" if self.useLocalSchedd else "vanilla"

        sub = """
executable = %(executable)s
universe = %(targetUniverse)s
%(useCredentials)s
output = $(Cluster).$(Process).out
error = $(Cluster).$(Process).err
log = $(Cluster).$(Process).log
environment = "HTCONDOR_JOBID=$(Cluster).$(Process) DIRAC_PILOT_STAMP=$(stamp)"
initialdir = %(initialDir)s
grid_resource = condor %(ceName)s %(ceName)s:%(port)s
transfer_output_files = ""
request_cpus = %(processors)s
%(localScheddOptions)s

kill_sig=SIGTERM

%(extraString)s

Queue stamp in %(pilotStampList)s

""" % dict(
            executable=executable,
            nJobs=nJobs,
            processors=processors,
            ceName=self.ceName,
            port=self.port,
            extraString=self.extraSubmitString,
            initialDir=os.path.join(self.workingDirectory, location),
            localScheddOptions=localScheddOptions,
            targetUniverse=targetUniverse,
            pilotStampList=",".join(pilotStamps),
            useCredentials=useCredentials,
        )
        subFile.write(sub)
        subFile.close()
        return name

    def _reset(self):
        self.queue = self.ceParameters["Queue"]
        self.outputURL = self.ceParameters.get("OutputURL", "gsiftp://localhost")
        self.gridEnv = self.ceParameters.get("GridEnv")
        self.daysToKeepLogs = self.ceParameters.get("DaysToKeepLogs", DEFAULT_DAYSTOKEEPLOGS)
        self.extraSubmitString = str(self.ceParameters.get("ExtraSubmitString", "").encode().decode("unicode_escape"))
        self.daysToKeepRemoteLogs = self.ceParameters.get("DaysToKeepRemoteLogs", DEFAULT_DAYSTOKEEPREMOTELOGS)
        self.useLocalSchedd = self.ceParameters.get("UseLocalSchedd", self.useLocalSchedd)
        if isinstance(self.useLocalSchedd, str):
            if self.useLocalSchedd == "False":
                self.useLocalSchedd = False

        self.remoteScheddOptions = (
            "" if self.useLocalSchedd else f"-pool {self.ceName}:{self.port} -name {self.ceName} "
        )

        self.log.debug("Using local schedd:", self.useLocalSchedd)
        self.log.debug("Remote scheduler option:", self.remoteScheddOptions)
        return S_OK()

    def _executeCondorCommand(self, cmd, keepTokenFile=False):
        """Execute condor command in a specially prepared environment

        :param list cmd: list of the condor command elements
        :param bool keepTokenFile: flag to reuse or not the previously created token file
        :return: S_OK/S_ERROR - the result of the executeGridCommand() call
        """
        if not self.token and not self.proxy:
            return S_ERROR(f"Cannot execute the command, token and proxy not found: {cmd}")

        # Prepare proxy
        result = self._prepareProxy()
        if not result["OK"]:
            return result

        htcEnv = {
            "_CONDOR_SEC_CLIENT_AUTHENTICATION_METHODS": "GSI",
        }
        # If a token is present, then we use it (overriding htcEnv)
        if self.token:
            # Create a new token file if we do not keep it across several calls
            if not self.tokenFile or not keepTokenFile:
                self.tokenFile = tempfile.NamedTemporaryFile(
                    suffix=".token", prefix="HTCondorCE_", dir=self.workingDirectory
                )
                writeToTokenFile(self.token["access_token"], self.tokenFile.name)

            htcEnv = {
                "_CONDOR_SEC_CLIENT_AUTHENTICATION_METHODS": "SCITOKENS",
                "_CONDOR_SCITOKENS_FILE": self.tokenFile.name,
            }
            if cas := getCAsLocation():
                htcEnv["_CONDOR_AUTH_SSL_CLIENT_CADIR"] = cas

        result = executeGridCommand(
            cmd,
            gridEnvScript=self.gridEnv,
            gridEnvDict=htcEnv,
        )
        # Remove token file if we do not want to keep it
        self.tokenFile = self.tokenFile if keepTokenFile else None

        return result

    #############################################################################
    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        """Method to submit job"""

        self.log.verbose("Executable file path:", executableFile)
        if not os.access(executableFile, 5):
            os.chmod(executableFile, 0o755)

        # The submitted pilots are going to have a common part of the stamp to construct a path to retrieve results
        # Then they also have an individual part to make them unique
        jobStamps = []
        commonJobStampPart = uuid.uuid4().hex[:3]
        for _i in range(numberOfJobs):
            jobStamp = commonJobStampPart + uuid.uuid4().hex[:29]
            jobStamps.append(jobStamp)

        # We randomize the location of the pilot output and log, because there are just too many of them
        location = logDir(self.ceName, commonJobStampPart)
        nProcessors = self.ceParameters.get("NumberOfProcessors", 1)
        if self.token:
            self.tokenFile = tempfile.NamedTemporaryFile(
                suffix=".token", prefix="HTCondorCE_", dir=self.workingDirectory
            )
            writeToTokenFile(self.token["access_token"], self.tokenFile.name)
        subName = self.__writeSub(
            executableFile, numberOfJobs, location, nProcessors, jobStamps, tokenFile=self.tokenFile
        )

        cmd = ["condor_submit", "-terse", subName]
        # the options for submit to remote are different than the other remoteScheddOptions
        # -remote: submit to a remote condor_schedd and spool all the required inputs
        scheddOptions = [] if self.useLocalSchedd else ["-pool", f"{self.ceName}:{self.port}", "-remote", self.ceName]
        for op in scheddOptions:
            cmd.insert(-1, op)

        result = self._executeCondorCommand(cmd, keepTokenFile=True)
        self.log.verbose(result)
        os.remove(subName)
        self.tokenFile = None
        if not result["OK"]:
            self.log.error("Failed to submit jobs to htcondor", result["Message"])
            return result

        status, stdout, stderr = result["Value"]

        if status:
            # We have got a non-zero status code
            errorString = stderr if stderr else stdout
            return S_ERROR(f"Pilot submission failed with error: {errorString.strip()}")

        pilotJobReferences = self.__getPilotReferences(stdout.strip())
        if not pilotJobReferences["OK"]:
            return pilotJobReferences
        pilotJobReferences = pilotJobReferences["Value"]

        self.log.verbose("JobStamps:", jobStamps)
        self.log.verbose("pilotRefs:", pilotJobReferences)

        result = S_OK(pilotJobReferences)
        result["PilotStampDict"] = dict(zip(pilotJobReferences, jobStamps))
        if self.useLocalSchedd:
            # Executable is transferred afterward
            # Inform the caller that Condor cannot delete it before the end of the execution
            result["ExecutableToKeep"] = executableFile

        self.log.verbose("Result for submission:", result)
        return result

    def killJob(self, jobIDList):
        """Kill the specified jobs"""
        if not jobIDList:
            return S_OK()
        if isinstance(jobIDList, str):
            jobIDList = [jobIDList]

        self.log.verbose("KillJob jobIDList", jobIDList)

        self.tokenFile = None

        for jobRef in jobIDList:
            job, _, jobID = condorIDAndPathToResultFromJobRef(jobRef)
            self.log.verbose("Killing pilot", job)
            cmd = ["condor_rm"]
            cmd.extend(self.remoteScheddOptions.strip().split(" "))
            cmd.append(jobID)
            result = self._executeCondorCommand(cmd, keepTokenFile=True)
            if not result["OK"]:
                self.tokenFile = None
                return S_ERROR(f"condor_rm failed completely: {result['Message']}")
            status, stdout, stderr = result["Value"]
            if status != 0:
                self.log.warn("Failed to kill pilot", f"{job}: {stdout}, {stderr}")
                self.tokenFile = None
                return S_ERROR(f"Failed to kill pilot {job}: {stderr}")

        self.tokenFile = None

        return S_OK()

    #############################################################################
    def getCEStatus(self):
        """Method to return information on running and pending jobs.

        Warning: information currently returned depends on the PilotManager and not HTCondor.
        Results might be wrong if pilots or jobs are submitted manually via the CE.
        """
        result = S_OK()
        result["SubmittedJobs"] = 0
        result["RunningJobs"] = 0
        result["WaitingJobs"] = 0

        # getWaitingPilots
        condDict = {"DestinationSite": self.ceName, "Status": PilotStatus.PILOT_WAITING_STATES}
        res = PilotManagerClient().countPilots(condDict)
        if res["OK"]:
            result["WaitingJobs"] = int(res["Value"])
        else:
            self.log.warn(f"Failure getting pilot count for {self.ceName}: {res['Message']} ")

        # getRunningPilots
        condDict = {"DestinationSite": self.ceName, "Status": PilotStatus.RUNNING}
        res = PilotManagerClient().countPilots(condDict)
        if res["OK"]:
            result["RunningJobs"] = int(res["Value"])
        else:
            self.log.warn(f"Failure getting pilot count for {self.ceName}: {res['Message']} ")

        return result

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
        # If we use a local schedd, then we have to cleanup executables regularly
        if self.useLocalSchedd:
            self.__cleanup()

        self.log.verbose("Job ID List for status:", jobIDList)
        if isinstance(jobIDList, str):
            jobIDList = [jobIDList]

        resultDict = {}
        condorIDs = {}
        # Get all condorIDs so we can just call condor_q and condor_history once
        for jobRef in jobIDList:
            job, _, jobID = condorIDAndPathToResultFromJobRef(jobRef)
            condorIDs[job] = jobID

        self.tokenFile = None

        qList = []
        for _condorIDs in breakListIntoChunks(condorIDs.values(), 100):
            # This will return a list of 1245.75 3
            cmd = ["condor_q"]
            cmd.extend(self.remoteScheddOptions.strip().split(" "))
            cmd.extend(_condorIDs)
            cmd.extend(["-af:j", "JobStatus"])
            result = self._executeCondorCommand(cmd, keepTokenFile=True)
            if not result["OK"]:
                self.tokenFile = None
                return S_ERROR(f"condor_q failed completely: {result['Message']}")
            status, stdout, stderr = result["Value"]
            if status != 0:
                self.tokenFile = None
                return S_ERROR(stdout + stderr)
            _qList = stdout.strip().split("\n")
            qList.extend(_qList)

            # FIXME: condor_history does only support j for autoformat from 8.5.3,
            # format adds whitespace for each field This will return a list of 1245 75 3
            # needs to concatenate the first two with a dot
            condorHistCall = ["condor_history"]
            condorHistCall.extend(self.remoteScheddOptions.strip().split(" "))
            condorHistCall.extend(_condorIDs)
            condorHistCall.extend(["-af", "ClusterId", "ProcId", "JobStatus"])

            self._treatCondorHistory(condorHistCall, qList)

        for job, jobID in condorIDs.items():
            pilotStatus = parseCondorStatus(qList, jobID)
            if pilotStatus == "HELD":
                # make sure the pilot stays dead and gets taken out of the condor_q
                cmd = f"condor_rm {self.remoteScheddOptions} {jobID}".split()
                _result = self._executeCondorCommand(cmd, keepTokenFile=True)
                pilotStatus = PilotStatus.ABORTED

            resultDict[job] = pilotStatus

        self.tokenFile = None

        self.log.verbose(f"Pilot Statuses: {resultDict} ")
        return S_OK(resultDict)

    def _treatCondorHistory(self, condorHistCall, qList):
        """concatenate clusterID and processID to get the same output as condor_q
        until we can expect condor version 8.5.3 everywhere

        :param str condorHistCall: condor_history command to run
        :param list qList: list of jobID and status from condor_q output, will be modified in this function
        :returns: None
        """

        result = self._executeCondorCommand(condorHistCall)
        if not result["OK"]:
            return S_ERROR(f"condorHistCall failed completely: {result['Message']}")

        status_history, stdout_history, stderr_history = result["Value"]

        # Join the ClusterId and the ProcId and add to existing list of statuses
        if status_history == 0:
            for line in stdout_history.split("\n"):
                values = line.strip().split()
                if len(values) == 3:
                    qList.append("%s.%s %s" % tuple(values))

    def getJobLog(self, jobID):
        """Get pilot job logging info from HTCondor

        :param str jobID: pilot job identifier
        :return: string representing the logging info of a given pilot job
        """
        self.log.verbose("Getting logging info for jobID", jobID)
        result = self.__getJobOutput(jobID, ["logging"])
        if not result["OK"]:
            return result

        return S_OK(result["Value"]["logging"])

    def getJobOutput(self, jobID):
        """Get job outputs: output and error files from HTCondor

        :param str jobID: job identifier
        :return: tuple of strings containing output and error contents
        """
        self.log.verbose("Getting job output for jobID:", jobID)
        result = self.__getJobOutput(jobID, ["output", "error"])
        if not result["OK"]:
            return result

        return S_OK((result["Value"]["output"], result["Value"]["error"]))

    def __getJobOutput(self, jobID, outTypes):
        """Get job outputs: output, error and logging files from HTCondor

        :param str jobID: job identifier
        :param list outTypes: output types targeted (output, error and/or logging)
        """
        _job, pathToResult, condorID = condorIDAndPathToResultFromJobRef(jobID)
        iwd = os.path.join(self.workingDirectory, pathToResult)

        try:
            mkDir(iwd)
        except OSError as e:
            errorMessage = "Failed to create the pilot output directory"
            self.log.exception(errorMessage, iwd)
            return S_ERROR(e.errno, f"{errorMessage} ({iwd})")

        if not self.useLocalSchedd:
            cmd = ["condor_transfer_data", "-pool", f"{self.ceName}:{self.port}", "-name", self.ceName, condorID]
            result = self._executeCondorCommand(cmd)
            self.log.verbose(result)

            # Getting 'logging' without 'error' and 'output' is possible but will generate command errors
            # We do not check the command errors if we only want 'logging'
            if "error" in outTypes or "output" in outTypes:
                errorMessage = "Failed to get job output from htcondor"
                if not result["OK"]:
                    self.log.error(errorMessage, result["Message"])
                    return result
                # Even if result is OK, the actual exit code of cmd can still be an error
                status, stdout, stderr = result["Value"]
                if status != 0:
                    outMessage = stdout.strip()
                    errMessage = stderr.strip()
                    varMessage = outMessage + " " + errMessage
                    self.log.error(errorMessage, varMessage)
                    return S_ERROR(f"{errorMessage}: {varMessage}")

        outputsSuffix = {"output": "out", "error": "err", "logging": "log"}
        outputs = {}
        for output, suffix in outputsSuffix.items():
            resOut = findFile(self.workingDirectory, f"{condorID}.{suffix}", pathToResult)
            if not resOut["OK"]:
                # Return an error if the output type was targeted, else we continue
                if output in outTypes:
                    self.log.error("Failed to find", f"{output} for condor job {jobID}")
                    return resOut
                continue
            outputfilename = resOut["Value"]

            try:
                # If the output type is not targeted, then we directly remove the file
                if output in outTypes:
                    with open(outputfilename) as outputfile:
                        outputs[output] = outputfile.read()
                # If a local schedd is used, we cannot retrieve the outputs again if we delete them
                if not self.useLocalSchedd:
                    os.remove(outputfilename)
            except OSError as e:
                self.log.error("Failed to open", f"{output} file: {str(e)}")
                return S_ERROR(f"Failed to get pilot {output}")

        return S_OK(outputs)

    def __getPilotReferences(self, jobString):
        """Get the references from the condor_submit output.
        Cluster ids look like " 107.0 - 107.0 " or " 107.0 - 107.4 "

        :param str jobString: the output of condor_submit

        :return: job references such as htcondorce://<CE name>/<path to result>-<clusterID>.<i>
        """
        self.log.verbose("getPilotReferences:", jobString)
        clusterIDs = jobString.split("-")
        if len(clusterIDs) != 2:
            return S_ERROR(f"Something wrong with the condor_submit output: {jobString}")
        clusterIDs = [clu.strip() for clu in clusterIDs]
        self.log.verbose("Cluster IDs parsed:", clusterIDs)
        try:
            clusterID = clusterIDs[0].split(".")[0]
            numJobs = clusterIDs[1].split(".")[1]
        except IndexError:
            return S_ERROR(f"Something wrong with the condor_submit output: {jobString}")
        cePrefix = f"htcondorce://{self.ceName}/"
        jobReferences = [f"{cePrefix}{clusterID}.{i}" for i in range(int(numJobs) + 1)]
        return S_OK(jobReferences)

    def __cleanup(self):
        """Clean the working directory of old jobs"""
        if not HTCondorCEComputingElement._cleanupLock.acquire(False):
            return

        now = datetime.datetime.utcnow()
        if (now - HTCondorCEComputingElement._lastCleanupTime).total_seconds() < 60:
            HTCondorCEComputingElement._cleanupLock.release()
            return

        HTCondorCEComputingElement._lastCleanupTime = now

        self.log.debug("Cleaning working directory:", self.workingDirectory)

        # remove all files older than 120 minutes starting with DIRAC_ Condor will
        # push files on submission, but it takes at least a few seconds until this
        # happens so we can't directly unlink after condor_submit
        status, stdout = subprocess.getstatusoutput(
            f'find -O3 {self.workingDirectory} -maxdepth 1 -mmin +120 -name "DIRAC_*" -delete '
        )
        if status:
            self.log.error("Failure during HTCondorCE __cleanup", stdout)

        # remove all out/err/log files older than "DaysToKeepLogs" days in the working directory
        # not running this for each CE so we do global cleanup
        findPars = dict(workDir=self.workingDirectory, days=self.daysToKeepLogs)
        # remove all out/err/log files older than "DaysToKeepLogs" days
        status, stdout = subprocess.getstatusoutput(
            r'find %(workDir)s -mtime +%(days)s -type f \( -name "*.out" -o -name "*.err" -o -name "*.log" \) -delete '
            % findPars
        )
        if status:
            self.log.error("Failure during HTCondorCE __cleanup", stdout)
        self._cleanupLock.release()
