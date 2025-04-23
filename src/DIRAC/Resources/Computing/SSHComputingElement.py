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
   Port number if not standard, e.g. for the gsissh access

SSHKey:
   Location of the ssh private key for no-password connection

SSHOptions:
   Any other SSH options to be used. Example::

     SSHOptions = -o UserKnownHostsFile=/local/path/to/known_hosts

   Allows to have a local copy of the ``known_hosts`` file, independent of the HOME directory.

SSHTunnel:
   String defining the use of intermediate SSH host. Example::

     ssh -i /private/key/location -l final_user final_host

SSHType:
   SSH (default) or gsissh

**Code Documentation**
"""
import errno
import json
import os
import shutil
import stat
import uuid
from shlex import quote as shlex_quote
from urllib.parse import quote, unquote, urlparse

import pexpect

import DIRAC
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks, uniqueElements
from DIRAC.Resources.Computing.BatchSystems.executeBatch import executeBatchContent
from DIRAC.Resources.Computing.ComputingElement import ComputingElement


class SSH:
    """SSH class encapsulates passing commands and files through an SSH tunnel
    to a remote host. It can use either ssh or gsissh access. The final host
    where the commands will be executed and where the files will copied/retrieved
    can be reached through an intermediate host if SSHTunnel parameters is defined.

    SSH constructor parameters are defined in a SSH accessible Computing Element
    in the Configuration System:

    - SSHHost: SSH host name
    - SSHUser: SSH user login
    - SSHPassword: SSH password
    - SSHPort: port number if not standard, e.g. for the gsissh access
    - SSHKey: location of the ssh private key for no-password connection
    - SSHOptions: any other SSH options to be used
    - SSHTunnel: string defining the use of intermediate SSH host. Example:
                 'ssh -i /private/key/location -l final_user final_host'
    - SSHType: ssh ( default ) or gsissh

    The class public interface includes two methods:

    sshCall( timeout, command_sequence )
    scpCall( timeout, local_file, remote_file, upload = False/True )
    """

    def __init__(self, host=None, parameters=None):
        self.host = host
        if parameters is None:
            parameters = {}
        if not host:
            self.host = parameters.get("SSHHost", "")

        self.user = parameters.get("SSHUser", "")
        self.password = parameters.get("SSHPassword", "")
        self.port = parameters.get("SSHPort", "")
        self.key = parameters.get("SSHKey", "")
        self.options = parameters.get("SSHOptions", "")
        self.sshTunnel = parameters.get("SSHTunnel", "")
        self.sshType = parameters.get("SSHType", "ssh")

        if self.port:
            self.options += f" -p {self.port}"
        if self.key:
            self.options += f" -i {self.key}"
        self.options = self.options.strip()

        self.log = gLogger.getSubLogger("SSH")

    def __ssh_call(self, command, timeout):
        if not timeout:
            timeout = 999

        ssh_newkey = "Are you sure you want to continue connecting"
        try:
            child = pexpect.spawn(command, timeout=timeout, encoding="utf-8")
            i = child.expect([pexpect.TIMEOUT, ssh_newkey, pexpect.EOF, "assword: "])
            if i == 0:  # Timeout
                return S_OK((-1, child.before, "SSH login failed"))

            if i == 1:  # SSH does not have the public key. Just accept it.
                child.sendline("yes")
                child.expect("assword: ")
                i = child.expect([pexpect.TIMEOUT, "assword: "])
                if i == 0:  # Timeout
                    return S_OK((-1, str(child.before) + str(child.after), "SSH login failed"))
                if i == 1:
                    child.sendline(self.password)
                    child.expect(pexpect.EOF)
                    return S_OK((0, child.before, ""))

            if i == 2:
                # Passwordless login, get the output
                return S_OK((0, child.before, ""))

            if self.password:
                child.sendline(self.password)
                child.expect(pexpect.EOF)
                return S_OK((0, child.before, ""))

            return S_ERROR(f"Unknown error: {child.before}")
        except Exception as x:
            return S_ERROR(f"Encountered exception: {str(x)}")

    def sshCall(self, timeout, cmdSeq):
        """Execute remote command via a ssh remote call

        :param int timeout: timeout of the command
        :param cmdSeq: list of command components
        :type cmdSeq: python:list
        """

        command = cmdSeq
        if isinstance(cmdSeq, list):
            command = " ".join(cmdSeq)

        pattern = "__DIRAC__"

        if self.sshTunnel:
            command = command.replace("'", '\\\\\\"')
            command = command.replace("$", "\\\\\\$")
            command = '/bin/sh -c \' {} -q {} -l {} {} "{} \\"echo {}; {}\\" " \' '.format(
                self.sshType,
                self.options,
                self.user,
                self.host,
                self.sshTunnel,
                pattern,
                command,
            )
        else:
            # command = command.replace( '$', '\$' )
            command = '{} -q {} -l {} {} "echo {}; {}"'.format(
                self.sshType,
                self.options,
                self.user,
                self.host,
                pattern,
                command,
            )
        self.log.debug(f"SSH command: {command}")
        result = self.__ssh_call(command, timeout)
        self.log.debug(f"SSH command result {str(result)}")
        if not result["OK"]:
            return result

        # Take the output only after the predefined pattern
        ind = result["Value"][1].find("__DIRAC__")
        if ind == -1:
            return result

        status, output, error = result["Value"]
        output = output[ind + 9 :]
        if output.startswith("\r"):
            output = output[1:]
        if output.startswith("\n"):
            output = output[1:]

        result["Value"] = (status, output, error)
        return result

    def scpCall(self, timeout, localFile, remoteFile, postUploadCommand="", upload=True):
        """Perform file copy through an SSH magic.

        :param int timeout: timeout of the command
        :param str localFile: local file path, serves as source for uploading and destination for downloading.
                              Can take 'Memory' as value, in this case the downloaded contents is returned
                              as result['Value']
        :param str remoteFile: remote file full path
        :param str postUploadCommand: command executed on the remote side after file upload
        :param bool upload: upload if True, download otherwise
        """
        # shlex_quote aims to prevent any security issue or problems with filepath containing spaces
        # it returns a shell-escaped version of the filename
        localFile = shlex_quote(localFile)
        remoteFile = shlex_quote(remoteFile)
        if upload:
            if self.sshTunnel:
                remoteFile = remoteFile.replace("$", r"\\\\\$")
                postUploadCommand = postUploadCommand.replace("$", r"\\\\\$")
                command = '/bin/sh -c \'cat {} | {} -q {} {}@{} "{} \\"cat > {}; {}\\""\' '.format(
                    localFile,
                    self.sshType,
                    self.options,
                    self.user,
                    self.host,
                    self.sshTunnel,
                    remoteFile,
                    postUploadCommand,
                )
            else:
                command = "/bin/sh -c \"cat {} | {} -q {} {}@{} 'cat > {}; {}'\" ".format(
                    localFile,
                    self.sshType,
                    self.options,
                    self.user,
                    self.host,
                    remoteFile,
                    postUploadCommand,
                )
        else:
            finalCat = f"| cat > {localFile}"
            if localFile.lower() == "memory":
                finalCat = ""
            if self.sshTunnel:
                remoteFile = remoteFile.replace("$", "\\\\\\$")
                command = '/bin/sh -c \'{} -q {} -l {} {} "{} \\"cat {}\\"" {}\''.format(
                    self.sshType,
                    self.options,
                    self.user,
                    self.host,
                    self.sshTunnel,
                    remoteFile,
                    finalCat,
                )
            else:
                remoteFile = remoteFile.replace("$", r"\$")
                command = "/bin/sh -c '{} -q {} -l {} {} \"cat {}\" {}'".format(
                    self.sshType,
                    self.options,
                    self.user,
                    self.host,
                    remoteFile,
                    finalCat,
                )

        self.log.debug(f"SSH copy command: {command}")
        return self.__ssh_call(command, timeout)


class SSHComputingElement(ComputingElement):
    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.execution = "SSHCE"
        self.submittedJobs = 0
        self.outputTemplate = ""
        self.errorTemplate = ""

    ############################################################################
    def setProxy(self, proxy):
        """
        Set and prepare proxy to use

        :param str proxy: proxy to use
        :return:  S_OK/S_ERROR
        """
        ComputingElement.setProxy(self, proxy)
        if self.ceParameters.get("SSHType", "ssh") == "gsissh":
            result = self._prepareProxy()
            if not result["OK"]:
                gLogger.error("SSHComputingElement: failed to set up proxy", result["Message"])
                return result
        return S_OK()

    #############################################################################
    def _addCEConfigDefaults(self):
        """Method to make sure all necessary Configuration Parameters are defined"""
        # First assure that any global parameters are loaded
        ComputingElement._addCEConfigDefaults(self)
        # Now batch system specific ones
        if "SharedArea" not in self.ceParameters:
            # . isn't a good location, move to $HOME
            self.ceParameters["SharedArea"] = "$HOME"

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

    def _reset(self):
        """Process CE parameters and make necessary adjustments"""
        result = self._getBatchSystem()
        if not result["OK"]:
            return result
        self._getBatchSystemDirectoryLocations()

        self.user = self.ceParameters["SSHUser"]
        self.queue = self.ceParameters["Queue"]
        self.log.info("Using queue: ", self.queue)

        self.submitOptions = self.ceParameters.get("SubmitOptions", "")
        self.preamble = self.ceParameters.get("Preamble", "")

        self.account = self.ceParameters.get("Account", "")
        result = self._prepareRemoteHost()
        if not result["OK"]:
            return result

        return S_OK()

    def _prepareRemoteHost(self, host=None):
        """Prepare remote directories and upload control script"""

        ssh = SSH(host=host, parameters=self.ceParameters)

        # Make remote directories
        dirTuple = tuple(
            uniqueElements(
                [self.sharedArea, self.executableArea, self.infoArea, self.batchOutput, self.batchError, self.workArea]
            )
        )
        nDirs = len(dirTuple)
        cmd = "mkdir -p %s; " * nDirs % dirTuple
        cmd = f"bash -c '{cmd}'"
        self.log.verbose(f"Creating working directories on {self.ceParameters['SSHHost']}")
        result = ssh.sshCall(30, cmd)
        if not result["OK"]:
            self.log.error("Failed creating working directories", f"({result['Message']})")
            return result
        status, output, _error = result["Value"]
        if status == -1:
            self.log.error("Timeout while creating directories")
            return S_ERROR(errno.ETIME, "Timeout while creating directories")
        if "cannot" in output:
            self.log.error("Failed to create directories", f"({output})")
            return S_ERROR(errno.EACCES, "Failed to create directories")

        # Upload the control script now
        result = self._generateControlScript()
        if not result["OK"]:
            self.log.warn("Failed generating control script")
            return result
        localScript = result["Value"]
        self.log.verbose(f"Uploading {self.batchSystem.__class__.__name__} script to {self.ceParameters['SSHHost']}")
        remoteScript = f"{self.sharedArea}/execute_batch"
        result = ssh.scpCall(30, localScript, remoteScript, postUploadCommand=f"chmod +x {remoteScript}")
        if not result["OK"]:
            self.log.warn(f"Failed uploading control script: {result['Message']}")
            return result
        status, output, _error = result["Value"]
        if status != 0:
            if status == -1:
                self.log.warn("Timeout while uploading control script")
                return S_ERROR("Timeout while uploading control script")
            self.log.warn(f"Failed uploading control script: {output}")
            return S_ERROR("Failed uploading control script")

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

    def __executeHostCommand(self, command, options, ssh=None, host=None):
        if not ssh:
            ssh = SSH(host=host, parameters=self.ceParameters)

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
            "bash --login -c 'python3 %s/execute_batch %s || python %s/execute_batch %s || python2 %s/execute_batch %s'"
            % (self.sharedArea, options, self.sharedArea, options, self.sharedArea, options)
        )

        self.log.verbose(f"CE submission command: {cmd}")

        result = ssh.sshCall(120, cmd)
        if not result["OK"]:
            self.log.error(f"{self.ceType} CE job submission failed", result["Message"])
            return result

        sshStatus = result["Value"][0]
        sshStdout = result["Value"][1]
        sshStderr = result["Value"][2]

        # Examine results of the job submission
        if sshStatus == 0:
            output = sshStdout.strip().replace("\r", "").strip()
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
        else:
            return S_ERROR("\n".join([sshStdout, sshStderr]))

    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        #    self.log.verbose( "Executable file path: %s" % executableFile )
        if not os.access(executableFile, 5):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

        return self._submitJobToHost(executableFile, numberOfJobs)

    def _submitJobToHost(self, executableFile, numberOfJobs, host=None):
        """Submit prepared executable to the given host"""
        ssh = SSH(host=host, parameters=self.ceParameters)
        # Copy the executable
        submitFile = os.path.join(self.executableArea, os.path.basename(executableFile))
        result = ssh.scpCall(30, executableFile, submitFile, postUploadCommand=f"chmod +x {submitFile}")
        if not result["OK"]:
            return result

        jobStamps = []
        for _i in range(numberOfJobs):
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
        if host:
            commandOptions["SSHNodeHost"] = host

        resultCommand = self.__executeHostCommand("submitJob", commandOptions, ssh=ssh, host=host)
        if not resultCommand["OK"]:
            return resultCommand

        result = resultCommand["Value"]
        if not isinstance(result, dict) or "Status" not in result:
            return S_ERROR("Invalid result from job submission")
        if result["Status"] != 0:
            return S_ERROR(f"Failed job submission: {result['Message']}")
        else:
            batchIDs = result["Jobs"]
            if batchIDs:
                batchSystemName = self.batchSystem.__class__.__name__.lower()
                if host is None:
                    jobIDs = [f"{self.ceType.lower()}{batchSystemName}://{self.ceName}/{_id}" for _id in batchIDs]
                else:
                    jobIDs = [
                        f"{self.ceType.lower()}{batchSystemName}://{self.ceName}/{host}/{_id}" for _id in batchIDs
                    ]
            else:
                return S_ERROR("No jobs IDs returned")

        result = S_OK(jobIDs)
        stampDict = {}
        for iJob, jobID in enumerate(jobIDs):
            stampDict[jobID] = jobStamps[iJob]
        result["PilotStampDict"] = stampDict
        self.submittedJobs += len(batchIDs)

        return result

    def killJob(self, jobIDList):
        """Kill a bunch of jobs"""
        if isinstance(jobIDList, str):
            jobIDList = [jobIDList]
        return self._killJobOnHost(jobIDList)

    def _killJobOnHost(self, jobIDList, host=None):
        """Kill the jobs for the given list of job IDs"""
        batchSystemJobList = []
        for jobID in jobIDList:
            batchSystemJobList.append(os.path.basename(urlparse(jobID.split(":::")[0]).path))

        commandOptions = {"JobIDList": batchSystemJobList, "User": self.user}
        resultCommand = self.__executeHostCommand("killJob", commandOptions, host=host)
        if not resultCommand["OK"]:
            return resultCommand

        result = resultCommand["Value"]
        if not isinstance(result, dict) or "Status" not in result:
            return S_ERROR("Invalid result from job submission")
        if result["Status"] != 0:
            return S_ERROR(f"Failed job kill: {result['Message']}")

        if result["Failed"]:
            return S_ERROR(f"{len(result['Failed'])} jobs failed killing")

        return S_OK(len(result["Successful"]))

    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = 0
        result["WaitingJobs"] = 0

        resultHost = self._getHostStatus()
        if not resultHost["OK"]:
            return resultHost

        result["RunningJobs"] = resultHost["Value"].get("Running", 0)
        result["WaitingJobs"] = resultHost["Value"].get("Waiting", 0)
        if "AvailableCores" in resultHost["Value"]:
            result["AvailableCores"] = resultHost["Value"]["AvailableCores"]
        self.log.verbose("Waiting Jobs: ", result["WaitingJobs"])
        self.log.verbose("Running Jobs: ", result["RunningJobs"])

        return result

    def _getHostStatus(self, host=None):
        """Get jobs running at a given host"""
        resultCommand = self.__executeHostCommand("getCEStatus", {}, host=host)
        if not resultCommand["OK"]:
            return resultCommand

        result = resultCommand["Value"]
        if not isinstance(result, dict) or "Status" not in result:
            return S_ERROR("Invalid result from job submission")
        if result["Status"] != 0:
            return S_ERROR(f"Failed to get CE status: {result['Message']}")

        return S_OK(result)

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
        return self._getJobStatusOnHost(jobIDList)

    def _getJobStatusOnHost(self, jobIDList, host=None):
        """Get the status information for the given list of jobs"""
        resultDict = {}
        batchSystemJobDict = {}
        for jobID in jobIDList:
            batchSystemJobID = os.path.basename(urlparse(jobID.split(":::")[0]).path)
            batchSystemJobDict[batchSystemJobID] = jobID

        for jobList in breakListIntoChunks(list(batchSystemJobDict), 100):
            resultCommand = self.__executeHostCommand("getJobStatus", {"JobIDList": jobList}, host=host)
            if not resultCommand["OK"]:
                return resultCommand

            result = resultCommand["Value"]
            if not isinstance(result, dict) or "Status" not in result:
                return S_ERROR("Invalid result from job submission")
            if result["Status"] != 0:
                return S_ERROR(f"Failed to get job status: {result['Message']}")

            for batchSystemJobID in result["Jobs"]:
                resultDict[batchSystemJobDict[batchSystemJobID]] = result["Jobs"][batchSystemJobID]

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

        batchSystemJobID, host, outputFile, errorFile = result["Value"]

        if localDir:
            localOutputFile = f"{localDir}/{batchSystemJobID}.out"
            localErrorFile = f"{localDir}/{batchSystemJobID}.err"
        else:
            localOutputFile = "Memory"
            localErrorFile = "Memory"

        # Take into account the SSHBatch possible SSHHost syntax
        host = host.split("/")[0]

        ssh = SSH(host=host, parameters=self.ceParameters)
        resultStdout = ssh.scpCall(30, localOutputFile, outputFile, upload=False)
        if not resultStdout["OK"]:
            return resultStdout

        resultStderr = ssh.scpCall(30, localErrorFile, errorFile, upload=False)
        if not resultStderr["OK"]:
            return resultStderr

        if localDir:
            output = localOutputFile
            error = localErrorFile
        else:
            output = resultStdout["Value"][1]
            error = resultStderr["Value"][1]

        return S_OK((output, error))

    def _getJobOutputFiles(self, jobID):
        """Get output file names for the specific CE"""
        batchSystemJobID = os.path.basename(urlparse(jobID.split(":::")[0]).path)
        # host can be retrieved from the path of the jobID
        # it might not be present, in this case host is an empty string and will be defined by the CE parameters later
        host = os.path.dirname(urlparse(jobID).path).lstrip("/")

        if "OutputTemplate" in self.ceParameters:
            self.outputTemplate = self.ceParameters["OutputTemplate"]
            self.errorTemplate = self.ceParameters["ErrorTemplate"]

        if self.outputTemplate:
            output = self.outputTemplate % batchSystemJobID
            error = self.errorTemplate % batchSystemJobID
        elif "OutputTemplate" in self.ceParameters:
            self.outputTemplate = self.ceParameters["OutputTemplate"]
            self.errorTemplate = self.ceParameters["ErrorTemplate"]
            output = self.outputTemplate % batchSystemJobID
            error = self.errorTemplate % batchSystemJobID
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
            resultCommand = self.__executeHostCommand("getJobOutputFiles", commandOptions, host=host)
            if not resultCommand["OK"]:
                return resultCommand

            result = resultCommand["Value"]
            if not isinstance(result, dict) or "Status" not in result:
                return S_ERROR("Invalid result from job submission")
            if result["Status"] != 0:
                return S_ERROR(f"Failed to get job output files: {result['Message']}")

            if "OutputTemplate" in result:
                self.outputTemplate = result["OutputTemplate"]
                self.errorTemplate = result["ErrorTemplate"]

            output = result["Jobs"][batchSystemJobID]["Output"]
            error = result["Jobs"][batchSystemJobID]["Error"]
        else:
            output = f"{self.batchOutput}/{batchSystemJobID}.out"
            error = f"{self.batchError}/{batchSystemJobID}.err"

        return S_OK((batchSystemJobID, host, output, error))
