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

UseSSLSubmission:
   If 'True', use SSL via a DN configured at the given computing element to submit jobs.
   This is a bridge feature until everyone is capable to use Tokens to submit to computing elements.

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

import datetime
import errno
import json
import os
import subprocess
import tempfile
import textwrap
import threading
import uuid

from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.Core.Security.Locations import getCAsLocation
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import writeToTokenFile
from DIRAC.Resources.Computing.BatchSystems.Condor import (
    HOLD_REASON_SUBCODE,
    STATE_ATTRIBUTES,
    getCondorStatus,
    subTemplate,
)
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.WorkloadManagementSystem.Client import PilotStatus

MANDATORY_PARAMETERS = ["Queue"]
DEFAULT_WORKINGDIRECTORY = "/opt/dirac/pro/runit/WorkloadManagement/SiteDirectorHT"
DEFAULT_DAYSTOKEEPREMOTELOGS = 1
DEFAULT_DAYSTOKEEPLOGS = 15


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
        self.useSSLSubmission = False
        self.remoteScheddOptions = ""
        self.tokenFile = None

    #############################################################################

    def _jobReferenceToCondorID(self, jobReference):
        """Convert a job reference into a Condor jobID.
        Example: htcondorce://<ce>/1234.0 becomes 1234.0

        :param str: job reference, a condor jobID with additional details
        :return: Condor jobID
        """
        # Remove CE and protocol information from arc Job ID
        if "://" in jobReference:
            condorJobID = jobReference.split("/")[-1]
            return condorJobID
        return jobReference

    def _condorIDToJobReference(self, condorJobIDs):
        """Get the job references from the condor job IDs.
        Cluster ids look like " 107.0 - 107.0 " or " 107.0 - 107.4 "

        :param str condorJobIDs: the output of condor_submit

        :return: job references such as htcondorce://<ce>/<clusterID>.<i>
        """
        clusterIDs = condorJobIDs.split("-")
        if len(clusterIDs) != 2:
            return S_ERROR(f"Something wrong with the condor_submit output: {condorJobIDs}")
        clusterIDs = [clu.strip() for clu in clusterIDs]
        self.log.verbose("Cluster IDs parsed:", clusterIDs)
        try:
            clusterID = clusterIDs[0].split(".")[0]
            numJobs = clusterIDs[1].split(".")[1]
        except IndexError:
            return S_ERROR(f"Something wrong with the condor_submit output: {condorJobIDs}")

        cePrefix = f"htcondorce://{self.ceName}/"
        jobReferences = [f"{cePrefix}{clusterID}.{i}" for i in range(int(numJobs) + 1)]
        return S_OK(jobReferences)

    #############################################################################

    def __writeSub(self, executable, location, processors, pilotStamps, tokenFile=None):
        """Create the Sub File for submission.

        :param str executable: name of the script to execute
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

        # For now, we still need to include a proxy in the submit file
        # HTCondor extracts VOMS attribute from it for the sites
        useCredentials = "use_x509userproxy = true"
        # If tokenFile is present, then we transfer it to the worker node
        if tokenFile:
            useCredentials += textwrap.dedent(
                f"""
                use_scitokens = true
                scitokens_file = {tokenFile.name}
                """
            )

        # Remote schedd options by default
        targetUniverse = "vanilla"
        scheddOptions = ""
        if self.useLocalSchedd:
            targetUniverse = "grid"
            scheddOptions = f"grid_resource = condor {self.ceName} {self.ceName}:{self.port}"

        sub = subTemplate % dict(
            targetUniverse=targetUniverse,
            executable=executable,
            initialDir=os.path.join(self.workingDirectory, location),
            environment="HTCONDOR_JOBID=$(Cluster).$(Process)",
            useCredentials=useCredentials,
            holdReasonSubcode=HOLD_REASON_SUBCODE,
            processors=processors,
            daysToKeepRemoteLogs=self.daysToKeepRemoteLogs,
            scheddOptions=scheddOptions,
            extraString=self.extraSubmitString,
            pilotStampList=",".join(pilotStamps),
        )
        subFile.write(sub)
        subFile.close()
        return name

    def _reset(self):
        self.queue = self.ceParameters["Queue"]
        self.outputURL = self.ceParameters.get("OutputURL", "gsiftp://localhost")
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

        self.useSSLSubmission = self.ceParameters.get("UseSSLSubmission", self.useSSLSubmission)
        if self.useSSLSubmission == "True":
            self.useSSLSubmission = True

        self.log.debug("Using local schedd:", self.useLocalSchedd)
        self.log.debug("Remote scheduler option:", self.remoteScheddOptions)
        return S_OK()

    def _executeCondorCommand(self, cmd, keepTokenFile=False):
        """Execute condor command in a specially prepared environment

        :param list cmd: list of the condor command elements
        :param bool keepTokenFile: flag to reuse or not the previously created token file
        :return: S_OK/S_ERROR - the stdout parameter of the systemCall() call
        """
        if not self.token and not self.proxy:
            return S_ERROR(f"Cannot execute the command, token and proxy not found: {cmd}")

        # Prepare proxy
        result = self._prepareProxy()
        if not result["OK"]:
            return result

        htcEnv = {}
        if self.useSSLSubmission:
            # this is guaranteed by _prepareProxy above
            proxyFile = os.environ.get("X509_USER_PROXY")
            if not (caFiles := getCAsLocation()):
                return S_ERROR("You want to use SSL Submission, but no CA files are present")
            htcEnv = {
                "_condor_SEC_CLIENT_AUTHENTICATION_METHODS": "SSL",
                "_condor_AUTH_SSL_CLIENT_CERTFILE": proxyFile,
                "_condor_AUTH_SSL_CLIENT_KEYFILE": proxyFile,
                "_condor_AUTH_SSL_CLIENT_CADIR": caFiles,
                "_condor_AUTH_SSL_SERVER_CADIR": caFiles,
                "_condor_AUTH_SSL_USE_CLIENT_PROXY_ENV_VAR": "false",
                "_condor_AUTH_SSL_SERVER_CAFILE": "",
                "_condor_AUTH_SSL_CLIENT_CAFILE": "",
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
                # This options is needed because we are still passing the proxy in the JDL (see use_x509userproxy)
                # In condor v24.4, there is a bug preventing us from delegating the proxy, so we have to set
                # it to false: https://opensciencegrid.atlassian.net/browse/HTCONDOR-2904
                "_CONDOR_DELEGATE_JOB_GSI_CREDENTIALS": "false",
            }
            if cas := getCAsLocation():
                htcEnv["_CONDOR_AUTH_SSL_CLIENT_CADIR"] = cas

        # Execute the command
        currentEnv = dict(os.environ)
        currentEnv.update(htcEnv)
        result = systemCall(120, cmd, env=currentEnv)
        if not result["OK"]:
            self.tokenFile = None
            self.log.error("Command", f"{cmd} failed with: {result['Message']}")
            return result

        status, stdout, stderr = result["Value"]
        if status:
            self.tokenFile = None
            # We have got a non-zero status code
            errorString = stderr if stderr else stdout
            return S_ERROR(f"Command {cmd} failed with: {status} - {errorString.strip()}")

        # Remove token file if we do not want to keep it
        self.tokenFile = self.tokenFile if keepTokenFile else None

        return S_OK(stdout.strip())

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
        location = os.path.join(self.ceName, commonJobStampPart[0], commonJobStampPart[1:3])
        nProcessors = self.ceParameters.get("NumberOfProcessors", 1)
        if self.token:
            self.tokenFile = tempfile.NamedTemporaryFile(
                suffix=".token", prefix="HTCondorCE_", dir=self.workingDirectory
            )
            writeToTokenFile(self.token["access_token"], self.tokenFile.name)
        subName = self.__writeSub(executableFile, location, nProcessors, jobStamps, tokenFile=self.tokenFile)

        cmd = ["condor_submit", "-terse", subName]
        # the options for submit to remote are different than the other remoteScheddOptions
        # -remote: submit to a remote condor_schedd and spool all the required inputs
        scheddOptions = [] if self.useLocalSchedd else ["-pool", f"{self.ceName}:{self.port}", "-remote", self.ceName]
        for op in scheddOptions:
            cmd.insert(-1, op)

        result = self._executeCondorCommand(cmd, keepTokenFile=True)
        os.remove(subName)
        if not result["OK"]:
            self.log.error("Failed to submit jobs to htcondor", result["Message"])
            return result

        stdout = result["Value"]
        jobReferences = self._condorIDToJobReference(stdout)
        if not jobReferences["OK"]:
            return jobReferences
        jobReferences = jobReferences["Value"]

        self.log.verbose("JobStamps:", jobStamps)
        self.log.verbose("pilotRefs:", jobReferences)

        result = S_OK(jobReferences)
        result["PilotStampDict"] = dict(zip(jobReferences, jobStamps))
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

        for jobReference in jobIDList:
            condorJobID = self._jobReferenceToCondorID(jobReference.split(":::")[0])
            self.log.verbose("Killing pilot", jobReference)
            cmd = ["condor_rm"]
            cmd.extend(self.remoteScheddOptions.strip().split(" "))
            cmd.append(condorJobID)
            result = self._executeCondorCommand(cmd, keepTokenFile=True)
            if not result["OK"]:
                self.log.error("Failed to kill pilot", f"{jobReference}: {result['Message']}")
                return result

        self.tokenFile = None
        return S_OK()

    #############################################################################
    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        return S_ERROR(
            "getCEStatus() not supported for HTCondorCEComputingElement: HTCondor does not expose this information"
        )

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
        # If we use a local schedd, then we have to cleanup executables regularly
        if self.useLocalSchedd:
            self.__cleanup()

        self.log.verbose("Job ID List for status:", jobIDList)
        if isinstance(jobIDList, str):
            jobIDList = [jobIDList]

        self.tokenFile = None
        resultDict = {}
        condorIDs = {}
        # Get all condorIDs so we can just call condor_q and condor_history once
        for jobReference in jobIDList:
            jobReference = jobReference.split(":::")[0]
            condorIDs[self._jobReferenceToCondorID(jobReference)] = jobReference

        jobsMetadata = []
        for _condorIDs in breakListIntoChunks(condorIDs.keys(), 100):
            cmd = ["condor_q"]
            cmd.extend(self.remoteScheddOptions.strip().split(" "))
            cmd.extend(_condorIDs)
            cmd.extend(["-attributes", STATE_ATTRIBUTES])
            cmd.extend(["-json"])
            result = self._executeCondorCommand(cmd, keepTokenFile=True)
            if not result["OK"]:
                return result

            if result["Value"]:
                jobsMetadata.extend(json.loads(result["Value"]))

            condorHistCall = ["condor_history"]
            condorHistCall.extend(self.remoteScheddOptions.strip().split(" "))
            condorHistCall.extend(_condorIDs)
            condorHistCall.extend(["-attributes", STATE_ATTRIBUTES])
            condorHistCall.extend(["-json"])
            result = self._executeCondorCommand(cmd, keepTokenFile=True)
            if not result["OK"]:
                return result

            if result["Value"]:
                jobsMetadata.extend(json.loads(result["Value"]))

        foundJobIDs = set()
        for jobDict in jobsMetadata:
            jobStatus, reason = getCondorStatus(jobDict)
            condorId = f"{jobDict['ClusterId']}.{jobDict['ProcId']}"
            jobReference = condorIDs.get(condorId)

            if jobStatus == PilotStatus.ABORTED:
                self.log.verbose("Job", f"{jobReference} held: {reason}")

            resultDict[jobReference] = jobStatus
            foundJobIDs.add(jobReference)

        # Check if we have any jobs that were not found in the condor_q or condor_history
        for jobReference in condorIDs.values():
            if jobReference not in foundJobIDs:
                self.log.verbose("Job", f"{jobReference} not found in condor_q or condor_history")
                resultDict[jobReference] = PilotStatus.UNKNOWN

        self.tokenFile = None

        self.log.verbose(f"Pilot Statuses: {resultDict} ")
        return S_OK(resultDict)

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

    def _findFile(self, fileName, pathToResult):
        """Find a file in a file system.

        :param str workingDir: the name of the directory containing the given file to search for
        :param str fileName: the name of the file to find
        :param str pathToResult: the path to follow from workingDir to find the file

        :return: path leading to the file
        """
        path = os.path.join(self.workingDirectory, pathToResult, fileName)
        if os.path.exists(path):
            return S_OK(path)
        return S_ERROR(errno.ENOENT, f"Could not find {path}")

    def __getJobOutput(self, jobID, outTypes):
        """Get job outputs: output, error and logging files from HTCondor

        :param str jobID: job identifier
        :param list outTypes: output types targeted (output, error and/or logging)
        """
        # Extract stamp from the Job ID
        if ":::" in jobID:
            jobReference, stamp = jobID.split(":::")
        else:
            return S_ERROR(f"DIRAC stamp not defined for {jobID}")

        # Reconstruct the path leading to the result (log, output)
        # Construction of the path can be found in submitJob()
        if len(stamp) < 3:
            return S_ERROR(f"Stamp is not long enough: {stamp}")
        pathToResult = os.path.join(self.ceName, stamp[0], stamp[1:3])

        condorJobID = self._jobReferenceToCondorID(jobReference)
        iwd = os.path.join(self.workingDirectory, pathToResult)

        try:
            mkDir(iwd)
        except OSError as e:
            errorMessage = "Failed to create the pilot output directory"
            self.log.exception(errorMessage, iwd)
            return S_ERROR(e.errno, f"{errorMessage} ({iwd})")

        if not self.useLocalSchedd:
            cmd = ["condor_transfer_data", "-pool", f"{self.ceName}:{self.port}", "-name", self.ceName, condorJobID]
            result = self._executeCondorCommand(cmd)

            # Getting 'logging' without 'error' and 'output' is possible but will generate command errors
            # We do not check the command errors if we only want 'logging'
            if "error" in outTypes or "output" in outTypes:
                if not result["OK"]:
                    return result

        outputsSuffix = {"output": "out", "error": "err", "logging": "log"}
        outputs = {}
        for output, suffix in outputsSuffix.items():
            resOut = self._findFile(f"{condorJobID}.{suffix}", pathToResult)
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
