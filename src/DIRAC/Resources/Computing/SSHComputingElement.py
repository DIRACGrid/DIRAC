""" SSH (Virtual) Computing Element

For a given IP/host it will send jobs directly through ssh

**Configuration Parameters**

Configuration for the SSHComputingElement submission can be done via the configuration system.

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
   remotely accessible. By default, the SSHComputingElement submits directly on the host via the Host class.
   Available batch systems are defined in :mod:`~DIRAC.Resources.Computing.BatchSystems`.

ExecutableArea:
   Area where the executable files are stored if necessary.
   If not defined: SharedArea + '/data' is used.
   If not absolute: SharedArea + path is used.

SharedArea:
   Area used to store executable/output/error files if they are not aready defined via BatchOutput, BatchError,
   InfoArea, ExecutableArea and/or WorkArea. The path should be absolute.

SSHHost:
   SSH host name

SSHUser:
   SSH user login

SSHPassword:
   SSH password

SSHPort:
   Port number if not standard

SSHKey:
   Location of the ssh private key for no-password connection

SSHTunnel:
   String defining the use of intermediate SSH host. Example::

     ssh -i /private/key/location -l final_user final_host

Timeout:
    Timeout for the SSH commands. Default is 120 seconds.


**Code Documentation**
"""
import errno
import json
import os
import shutil
import stat
import uuid
from urllib.parse import quote, unquote, urlparse

from fabric import Connection
from invoke.exceptions import CommandTimedOut
from paramiko.ssh_exception import SSHException

import DIRAC
from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Utilities.List import breakListIntoChunks, uniqueElements
from DIRAC.Resources.Computing.BatchSystems.executeBatch import executeBatchContent
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.PilotBundle import bundleProxy, writeScript


class SSHComputingElement(ComputingElement):
    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.submittedJobs = 0

        # SSH connection
        self.hosts = []
        self.connection = None
        self.timeout = 120
        self.user = None

        # Submission parameters
        self.queue = None
        self.submitOptions = None
        self.preamble = None
        self.account = None
        self.execution = "SSHCE"

        # Directories
        self.sharedArea = "$HOME"
        self.batchOutput = "data"
        self.batchError = "data"
        self.infoArea = "data"
        self.executableArea = "info"
        self.workArea = "work"

        # Output and error templates
        self.outputTemplate = ""
        self.errorTemplate = ""

    #############################################################################

    def _run(self, connection: Connection, command: str):
        """Run the command on the remote host"""
        try:
            result = connection.run(command, warn=True, hide=True)
            if result.failed:
                return S_ERROR(f"[{connection.host}] Command returned an error: {result.stderr}")
            return S_OK(result.stdout)
        except CommandTimedOut as e:
            return S_ERROR(
                errno.ETIME, f"[{connection.host}] The command timed out. Consider increasing the timeout: {e}"
            )
        except SSHException as e:
            return S_ERROR(f"[{connection.host}] SSH error occurred: {str(e)}")

    def _put(self, connection: Connection, local: str, remote: str, preserveMode: bool = True):
        """Upload a file to the remote host"""
        try:
            connection.put(local, remote=remote, preserve_mode=preserveMode)
            return S_OK()
        except OSError as e:
            return S_ERROR(f"[{connection.host}] Failed uploading file: {str(e)}")
        except SSHException as e:
            return S_ERROR(f"[{connection.host}] SSH error occurred: {str(e)}")

    def _get(self, connection: Connection, remote: str, local: str, preserveMode: bool = True):
        """Upload a file to the remote host"""
        try:
            connection.get(local, remote=remote, preserve_mode=preserveMode)
            return S_OK()
        except OSError as e:
            return S_ERROR(f"[{connection.host}] Failed uploading file: {str(e)}")
        except SSHException as e:
            return S_ERROR(f"[{connection.host}] SSH error occurred: {str(e)}")

    #############################################################################

    def _getBatchSystem(self):
        """Load a Batch System instance from the CE Parameters"""
        batchSystemName = self.ceParameters.get("BatchSystem", "Host")
        if "BatchSystem" not in self.ceParameters:
            self.ceParameters["BatchSystem"] = batchSystemName
        result = self.loadBatchSystem(batchSystemName)
        if not result["OK"]:
            self.log.error("Failed to load the batch system plugin", batchSystemName)
        return result

    def _getBatchSystemDirectoryLocations(self):
        """Get names of the locations to store outputs, errors, info and executables."""
        self.sharedArea = self.ceParameters.get("SharedArea", self.sharedArea)

        def _get_dir(directory: str, defaultValue: str) -> str:
            value = self.ceParameters.get(directory, defaultValue)
            if value.startswith("/"):
                return value
            return os.path.join(self.sharedArea, value)

        self.batchOutput = _get_dir("BatchOutput", self.batchOutput)
        self.batchError = _get_dir("BatchError", self.batchError)
        self.infoArea = _get_dir("InfoArea", self.infoArea)
        self.executableArea = _get_dir("ExecutableArea", self.executableArea)
        self.workArea = _get_dir("WorkArea", self.workArea)

    def _getConnection(self, host: str, user: str, port: int, password: str, key: str, tunnel: str):
        """Get a Connection instance to the host"""
        connectionParams = {}
        if password:
            connectionParams["password"] = password
        if key:
            connectionParams["key_filename"] = key

        gateway = None
        if tunnel:
            gateway = Connection(tunnel, user=user, connect_kwargs=connectionParams)

        return Connection(
            host,
            user=user,
            port=port,
            gateway=gateway,
            connect_kwargs=connectionParams,
            connect_timeout=self.timeout,
        )

    def _reset(self):
        """Process CE parameters and make necessary adjustments"""
        # Get the Batch System instance
        result = self._getBatchSystem()
        if not result["OK"]:
            return result

        # Get the location of the remote directories
        self._getBatchSystemDirectoryLocations()

        # Get the SSH parameters
        self.host = self.ceParameters.get("SSHHost", self.host)
        self.timeout = self.ceParameters.get("Timeout", self.timeout)
        self.user = self.ceParameters.get("SSHUser", self.user)
        port = self.ceParameters.get("SSHPort", None)
        password = self.ceParameters.get("SSHPassword", None)
        key = self.ceParameters.get("SSHKey", None)
        tunnel = self.ceParameters.get("SSHTunnel", None)

        # Configure the SSH connection
        self.connection = self._getConnection(self.host, self.user, port, password, key, tunnel)

        # Get submission parameters
        self.submitOptions = self.ceParameters.get("SubmitOptions", self.submitOptions)
        self.preamble = self.ceParameters.get("Preamble", self.preamble)
        self.account = self.ceParameters.get("Account", self.account)
        self.queue = self.ceParameters["Queue"]
        self.log.info("Using queue: ", self.queue)

        # Get output and error templates
        self.outputTemplate = self.ceParameters.get("OutputTemplate", self.outputTemplate)
        self.errorTemplate = self.ceParameters.get("ErrorTemplate", self.errorTemplate)

        # Prepare the remote host
        result = self._prepareRemoteHost(self.connection)
        if not result["OK"]:
            return result

        return S_OK()

    def _prepareRemoteHost(self, connection: Connection):
        """Prepare remote directories and upload control script"""
        # Make remote directories
        self.log.verbose(f"Creating working directories on {self.host}")
        dirTuple = tuple(
            uniqueElements(
                [self.sharedArea, self.executableArea, self.infoArea, self.batchOutput, self.batchError, self.workArea]
            )
        )
        cmd = f"mkdir -p {' '.join(dirTuple)}"
        result = self._run(connection, cmd)
        if not result["OK"]:
            self.log.error("Failed creating working directories: ", result["Message"])
            return result

        # Upload the control script now
        self.log.verbose("Generating control script")
        result = self._generateControlScript()
        if not result["OK"]:
            self.log.error("Failed generating control script")
            return result
        localScript = result["Value"]
        os.chmod(localScript, 0o755)

        self.log.verbose(f"Uploading {self.batchSystem.__class__.__name__} script to {self.host}")
        remoteScript = f"{self.sharedArea}/execute_batch"

        result = self._put(connection, localScript, remote=remoteScript)
        if not result["OK"]:
            self.log.error(f"Failed uploading control script: {result['Message']}")
            return result

        # Delete the generated control script locally
        try:
            os.remove(localScript)
        except OSError:
            self.log.warn("Failed removing the generated control script locally")
            return S_ERROR("Failed removing the generated control script locally")

        return S_OK()

    def _generateControlScript(self):
        """Generates a control script from a BatchSystem and a script called executeBatch

        :return: a path containing the script generated
        """
        # Get the batch system module to use
        batchSystemDir = os.path.join(os.path.dirname(DIRAC.__file__), "Resources", "Computing", "BatchSystems")
        batchSystemScript = os.path.join(batchSystemDir, f"{self.batchSystem.__class__.__name__}.py")

        # Get the executeBatch.py content: an str variable composed of code content that has to be extracted
        # The control script is generated from the batch system module and this variable
        controlScript = os.path.join(batchSystemDir, "control_script.py")

        try:
            shutil.copyfile(batchSystemScript, controlScript)
            with open(controlScript, "a") as cs:
                cs.write(executeBatchContent)
        except OSError:
            return S_ERROR("IO Error trying to generate control script")

        return S_OK(f"{controlScript}")

    #############################################################################

    def __executeHostCommand(self, connection: Connection, command: str, options: dict[str]):
        """Execute a command on the remote host"""
        options["BatchSystem"] = self.batchSystem.__class__.__name__
        options["Method"] = command
        options["SharedDir"] = self.sharedArea
        options["OutputDir"] = self.batchOutput
        options["ErrorDir"] = self.batchError
        options["WorkDir"] = self.workArea
        options["InfoDir"] = self.infoArea
        options["ExecutionContext"] = self.execution
        options["User"] = self.user
        options["Queue"] = self.queue

        options = json.dumps(options)
        options = quote(options)

        cmd = (
            f"python {self.sharedArea}/execute_batch {options} || "
            f"python3 {self.sharedArea}/execute_batch {options} || "
            f"python2 {self.sharedArea}/execute_batch {options}"
        )

        self.log.verbose("Command:", f"[{connection.host}] {cmd}")

        result = self._run(connection, cmd)
        if not result["OK"]:
            return result

        # Examine results of the job submission
        output = result["Value"].strip()
        if not output:
            return S_ERROR("No output from remote command")

        try:
            index = output.index("============= Start output ===============")
            output = output[index + 42 :]
        except ValueError:
            return S_ERROR(f"Invalid output from remote command: {output}")

        try:
            output = unquote(output)
            result = json.loads(output)
            if isinstance(result, str) and result.startswith("Exception:"):
                return S_ERROR(result)
            return S_OK(result)
        except Exception:
            return S_ERROR("Invalid return structure from job submission")

    #############################################################################

    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        if not os.access(executableFile, 5):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

        # if no proxy is supplied, the executable can be submitted directly
        # otherwise a wrapper script is needed to get the proxy to the execution node
        # The wrapper script makes debugging more complicated and thus it is
        # recommended to transfer a proxy inside the executable if possible.
        if proxy:
            self.log.verbose("Setting up proxy for payload")
            wrapperContent = bundleProxy(executableFile, proxy)
            name = writeScript(wrapperContent, os.getcwd())
            submitFile = name
        else:  # no proxy
            submitFile = executableFile

        result = self._submitJobToHost(self.connection, submitFile, numberOfJobs)

        if proxy:
            os.remove(submitFile)

        if not result["OK"]:
            return result

        batchIDs, jobStamps = result["Value"]
        batchSystemName = self.batchSystem.__class__.__name__.lower()
        jobIDs = [f"{self.ceType.lower()}{batchSystemName}://{self.ceName}/{_id}" for _id in batchIDs]

        result = S_OK(jobIDs)
        stampDict = {}
        for iJob, jobID in enumerate(jobIDs):
            stampDict[jobID] = jobStamps[iJob]
        result["PilotStampDict"] = stampDict
        self.submittedJobs += len(batchIDs)

        return result

    def _submitJobToHost(self, connection: Connection, executableFile: str, numberOfJobs: int):
        """Submit prepared executable to the given host"""
        # Copy the executable
        self.log.verbose(f"Copying executable to {self.host}")
        submitFile = os.path.join(self.executableArea, os.path.basename(executableFile))
        os.chmod(executableFile, 0o755)

        result = self._put(connection, executableFile, submitFile)
        if not result["OK"]:
            return result

        jobStamps = []
        for _ in range(numberOfJobs):
            jobStamps.append(uuid.uuid4().hex)

        numberOfProcessors = self.ceParameters.get("NumberOfProcessors", 1)
        wholeNode = self.ceParameters.get("WholeNode", False)
        # numberOfNodes is treated as a string as it can contain values such as "2-4"
        # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
        numberOfNodes = self.ceParameters.get("NumberOfNodes", "1")
        self.numberOfGPUs = self.ceParameters.get("NumberOfGPUs")

        # Collect command options
        commandOptions = {
            "Executable": submitFile,
            "NJobs": numberOfJobs,
            "SubmitOptions": self.submitOptions,
            "JobStamps": jobStamps,
            "WholeNode": wholeNode,
            "NumberOfProcessors": numberOfProcessors,
            "NumberOfNodes": numberOfNodes,
            "Preamble": self.preamble,
            "NumberOfGPUs": self.numberOfGPUs,
            "Account": self.account,
        }

        resultCommand = self.__executeHostCommand(connection, "submitJob", commandOptions)
        if not resultCommand["OK"]:
            return resultCommand

        result = resultCommand["Value"]
        if result["Status"] != 0:
            return S_ERROR(f"Failed job submission: {result['Message']}")

        batchIDs = result["Jobs"]
        if not batchIDs:
            return S_ERROR("No jobs IDs returned")

        return S_OK((batchIDs, jobStamps))

    #############################################################################

    def killJob(self, jobIDList):
        """Kill a bunch of jobs"""
        if isinstance(jobIDList, str):
            jobIDList = [jobIDList]
        return self._killJobOnHost(self.connection, jobIDList)

    def _killJobOnHost(self, connection: Connection, jobIDList: list[str]):
        """Kill the jobs for the given list of job IDs"""
        batchSystemJobList = []
        for jobID in jobIDList:
            batchSystemJobList.append(os.path.basename(urlparse(jobID.split(":::")[0]).path))

        commandOptions = {"JobIDList": batchSystemJobList, "User": self.user}
        resultCommand = self.__executeHostCommand(connection, "killJob", commandOptions)
        if not resultCommand["OK"]:
            return resultCommand

        result = resultCommand["Value"]
        if result["Status"] != 0:
            return S_ERROR(f"Failed job kill: {result['Message']}")

        if result["Failed"]:
            return S_ERROR(f"{len(result['Failed'])} jobs failed killing")

        return S_OK(len(result["Successful"]))

    #############################################################################

    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = 0
        result["WaitingJobs"] = 0

        resultHost = self._getHostStatus(self.connection)
        if not resultHost["OK"]:
            return resultHost

        result["RunningJobs"] = resultHost["Value"].get("Running", 0)
        result["WaitingJobs"] = resultHost["Value"].get("Waiting", 0)
        if "AvailableCores" in resultHost["Value"]:
            result["AvailableCores"] = resultHost["Value"]["AvailableCores"]
        self.log.verbose("Waiting Jobs: ", result["WaitingJobs"])
        self.log.verbose("Running Jobs: ", result["RunningJobs"])

        return result

    def _getHostStatus(self, connection: Connection):
        """Get jobs running at a given host"""
        resultCommand = self.__executeHostCommand(connection, "getCEStatus", {})
        if not resultCommand["OK"]:
            return resultCommand

        result = resultCommand["Value"]
        if result["Status"] != 0:
            return S_ERROR(f"Failed to get CE status: {result['Message']}")

        return S_OK(result)

    #############################################################################

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
        return self._getJobStatusOnHost(self.connection, jobIDList)

    def _getJobStatusOnHost(self, connection: Connection, jobIDList: list[str]):
        """Get the status information for the given list of jobs"""
        resultDict = {}
        batchSystemJobDict = {}
        for jobID in jobIDList:
            batchSystemJobID = os.path.basename(urlparse(jobID.split(":::")[0]).path)
            batchSystemJobDict[batchSystemJobID] = jobID

        for jobList in breakListIntoChunks(list(batchSystemJobDict), 100):
            resultCommand = self.__executeHostCommand(connection, "getJobStatus", {"JobIDList": jobList})
            if not resultCommand["OK"]:
                return resultCommand

            result = resultCommand["Value"]
            if result["Status"] != 0:
                return S_ERROR(f"Failed to get job status: {result['Message']}")

            for batchSystemJobID in result["Jobs"]:
                resultDict[batchSystemJobDict[batchSystemJobID]] = result["Jobs"][batchSystemJobID]

        return S_OK(resultDict)

    #############################################################################

    def getJobOutput(self, jobID, localDir=None):
        """Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned
        as strings.
        """
        self.log.verbose("Getting output for jobID", jobID)
        return self._getJobOutputFilesOnHost(self.connection, jobID, localDir)

    def _getJobOutputFilesOnHost(self, connection: Connection, jobID: str, localDir: str | None = None):
        """Get output file names for the specific CE"""
        batchSystemJobID = os.path.basename(urlparse(jobID.split(":::")[0]).path)

        if self.outputTemplate:
            outputFile = self.outputTemplate % batchSystemJobID
            errorFile = self.errorTemplate % batchSystemJobID
        elif hasattr(self.batchSystem, "getJobOutputFiles"):
            # numberOfNodes is treated as a string as it can contain values such as "2-4"
            # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
            numberOfNodes = self.ceParameters.get("NumberOfNodes", "1")
            commandOptions = {
                "JobIDList": [batchSystemJobID],
                "OutputDir": self.batchOutput,
                "ErrorDir": self.batchError,
                "NumberOfNodes": numberOfNodes,
            }
            resultCommand = self.__executeHostCommand(connection, "getJobOutputFiles", commandOptions)
            if not resultCommand["OK"]:
                return resultCommand

            result = resultCommand["Value"]
            if result["Status"] != 0:
                return S_ERROR(f"Failed to get job output files: {result['Message']}")

            if "OutputTemplate" in result:
                self.outputTemplate = result["OutputTemplate"]
                self.errorTemplate = result["ErrorTemplate"]

            outputFile = result["Jobs"][batchSystemJobID]["Output"]
            errorFile = result["Jobs"][batchSystemJobID]["Error"]
        else:
            outputFile = f"{self.batchOutput}/{batchSystemJobID}.out"
            errorFile = f"{self.batchError}/{batchSystemJobID}.err"

        if localDir:
            localOutputFile = f"{localDir}/{batchSystemJobID}.out"
            localErrorFile = f"{localDir}/{batchSystemJobID}.err"
        else:
            localOutputFile = "Memory"
            localErrorFile = "Memory"

        resultStdout = self._get(connection, outputFile, localOutputFile, preserveMode=False)
        if not resultStdout["OK"]:
            return resultStdout

        resultStderr = self._get(connection, errorFile, localErrorFile, preserveMode=False)
        if not resultStderr["OK"]:
            return resultStderr

        if localDir:
            output = localOutputFile
            error = localErrorFile
        else:
            output = resultStdout["Value"][1]
            error = resultStderr["Value"][1]

        return S_OK((output, error))
