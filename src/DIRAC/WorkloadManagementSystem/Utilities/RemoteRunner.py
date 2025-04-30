""" RemoteRunner

RemoteRunner has been designed to send scripts/applications and input files on remote worker nodes having
no outbound connectivity (e.g. supercomputers)

Mostly called by workflow modules, RemoteRunner is generally the last component to get through before
the script/application execution on a remote machine.
"""
import hashlib
import os
import shlex
import time

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities import DErrno
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


class RemoteRunner:
    def __init__(self, siteName, ceName, queueName):
        self.log = gLogger.getSubLogger("RemoteRunner")
        self.executable = "workloadExec.sh"
        self.checkSumOutput = "md5Checksum.txt"

        self._workloadSite = siteName
        self._workloadCE = ceName
        self._workloadQueue = queueName

    def execute(self, command, workingDirectory=".", numberOfProcessors=1, cleanRemoteJob=True):
        """Execute the command remotely via a CE

        :param str command: command to execute remotely
        :param str workingDirectory: directory containing the inputs required by the command
        :param int numberOfProcessors: number of processors to allocate to the command
        :param str cleanRemoteJob: clean the files related to the command on the remote host if True
        :return: (status, output, error)
        """
        self.log.verbose("Command to submit:", command)

        # Check whether CE parameters are set
        if not (result := self._checkParameters())["OK"]:
            result["Errno"] = DErrno.ESECTION
            return result
        self.log.info(
            "Preparing and submitting the command to",
            f"site {self._workloadSite}, CE {self._workloadCE}, queue {self._workloadQueue}",
        )

        # The CE interface needs to drop the token section from the proxy file to interact with the CE
        # So we save the current proxy file location (which likely contains the DiracX token)
        # and we will restore it at the end of the job
        originalProxyLocation = os.environ.get("X509_USER_PROXY")

        # Set up Application Queue
        if not (result := self._setUpWorkloadCE(numberOfProcessors))["OK"]:
            result["Errno"] = DErrno.ERESUNA
            return result
        workloadCE = result["Value"]
        self.log.debug("The CE interface has been set up")

        # Add the command in an executable file
        self._wrapCommand(command, workingDirectory)
        self.log.debug("The command has been wrapped into an executable")

        # Get inputs from the current working directory
        inputs = os.listdir(workingDirectory)
        inputs.remove(os.path.basename(self.executable))
        # We need to remove the standard output/error files if present
        # as they might change during the execution of the application and fail the integrity check
        if "std.out" in inputs:
            inputs.remove("std.out")
        if "std.err" in inputs:
            inputs.remove("std.err")
        self.log.verbose("The executable will be sent along with the following inputs:", ",".join(inputs))
        # Request the whole directory as output
        outputs = ["/"]

        # Interactions with the CE might be unstable, we need to retry the operations
        maxRetries = 10
        timeBetweenRetries = 120

        # Submit the command as a job with retries
        for _ in range(maxRetries):
            result = workloadCE.submitJob(self.executable, workloadCE.proxy, inputs=inputs, outputs=outputs)
            if result["OK"]:
                break
            else:
                self.log.warn("Failed to submit job, retrying...")
            time.sleep(timeBetweenRetries)
        else:
            result["Errno"] = DErrno.EWMSSUBM
            # Restore the original proxy location
            os.environ["X509_USER_PROXY"] = originalProxyLocation
            return result

        jobID = result["Value"][0]
        stamp = result["PilotStampDict"][jobID]
        self.log.info("The command has been wrapped in a job and sent. Remote JobID: ", jobID)

        # Get status of the job
        self.log.info("Waiting for the end of the job...")
        jobStatus = PilotStatus.RUNNING
        while jobStatus not in PilotStatus.PILOT_FINAL_STATES:
            time.sleep(120)
            for _ in range(maxRetries):
                result = workloadCE.getJobStatus([jobID])
                if result["OK"]:
                    break
                else:
                    self.log.warn("Failed to get job status, retrying...")
                time.sleep(timeBetweenRetries)
            else:
                result["Errno"] = DErrno.EWMSSTATUS
                # Restore the original proxy location
                os.environ["X509_USER_PROXY"] = originalProxyLocation
                return result

            jobStatus = result["Value"][jobID]
        self.log.info("The final status of the application/script is: ", jobStatus)

        # Get job outputs
        self.log.info("Getting the outputs of the command...")
        for _ in range(maxRetries):
            result = workloadCE.getJobOutput(f"{jobID}:::{stamp}", os.path.abspath("."))
            if result["OK"]:
                break
            else:
                self.log.warn("Failed to get job output, retrying...")
            time.sleep(timeBetweenRetries)
        else:
            result["Errno"] = DErrno.EWMSJMAN
            # Restore the original proxy location
            os.environ["X509_USER_PROXY"] = originalProxyLocation
            return result

        output, error = result["Value"]

        # Make sure the output is correct
        self.log.info("Checking the integrity of the outputs...")
        if not (result := self._checkOutputIntegrity("."))["OK"]:
            result["Errno"] = DErrno.EWMSJMAN
            # Restore the original proxy location
            os.environ["X509_USER_PROXY"] = originalProxyLocation
            return result
        self.log.info("The output has been retrieved and declared complete")

        # Clean up the job (local files not needed anymore)
        os.remove(f"{stamp}.out")
        os.remove(f"{stamp}.err")
        os.remove(self.checkSumOutput)
        os.remove(self.executable)

        # Remove the job from the CE
        if cleanRemoteJob:
            if not (result := workloadCE.cleanJob(jobID))["OK"]:
                self.log.warn("Failed to clean the output remotely", result["Message"])
            self.log.info("The job has been remotely removed")

        # Restore the original proxy location
        os.environ["X509_USER_PROXY"] = originalProxyLocation

        commandStatus = {"Done": 0, "Failed": -1, "Killed": -2}
        return S_OK((commandStatus[jobStatus], output, error))

    def _checkParameters(self):
        """Initialize the remote runner using the parameters of the CS.
        :return: S_OK, S_ERROR
        """
        if not self._workloadSite:
            return S_ERROR("The remote site is not defined")
        if not self._workloadCE:
            return S_ERROR("The remote CE is not defined")
        if not self._workloadQueue:
            return S_ERROR("The remote queue is not defined")

        return S_OK()

    def _setUpWorkloadCE(self, numberOfProcessorsPayload=1):
        """Get application queue and configure it

        :return: a ComputingElement instance
        """
        # Get CE Parameters
        result = getQueue(self._workloadSite, self._workloadCE, self._workloadQueue)
        if not result["OK"]:
            return result
        ceType = result["Value"]["CEType"]
        ceParams = result["Value"]

        # Build CE
        ceFactory = ComputingElementFactory()
        result = ceFactory.getCE(ceName=self._workloadCE, ceType=ceType, ceParametersDict=ceParams)
        if not result["OK"]:
            return result
        workloadCE = result["Value"]

        # Set the number of processors available according to the need of the payload
        numberOfProcessorsCE = workloadCE.ceParameters.get("NumberOfProcessors", 1)
        if numberOfProcessorsCE < 1 or numberOfProcessorsPayload < 1:
            self.log.warn(
                "Inappropriate values:",
                "number of processors required for the payload %s - for the CE %s"
                % (numberOfProcessorsPayload, numberOfProcessorsCE),
            )
            return S_ERROR("Inappropriate NumberOfProcessors value")

        if numberOfProcessorsPayload > numberOfProcessorsCE:
            self.log.warn(
                "Not enough processors to execute the payload: ",
                "number of processors required for the payload %s < %s the WN capacity"
                % (numberOfProcessorsPayload, numberOfProcessorsCE),
            )
            return S_ERROR("Not enough processors to execute the command")

        workloadCE.ceParameters["NumberOfProcessors"] = numberOfProcessorsPayload

        # Add a proxy to the CE
        result = getProxyInfo()
        if not result["OK"]:
            return result
        proxy = result["Value"]["chain"]
        workloadCE.setProxy(proxy)

        return S_OK(workloadCE)

    def _wrapCommand(self, command, workingDirectory):
        """Wrap the command in a file

        :param str command: command line to write in the executable
        :param str workingDirectory: directory containing the inputs required by the command
        :return: path of the executable
        """
        # Check whether the command contains any absolute path: there would be no way to access them remotely
        # They need to be converted into relative path
        argumentsProcessed = []
        for argument in shlex.split(command):
            argPath = os.path.dirname(argument)
            # The argument does not contain any path, not concerned
            if not argPath:
                argumentsProcessed.append(argument)
                continue

            argPathAbsolutePath = os.path.abspath(argPath)
            workingDirAbsolutePath = os.path.abspath(workingDirectory)
            # The argument is not included in the workingDirectory, not concerned
            if not argPathAbsolutePath.startswith(workingDirAbsolutePath):
                argumentsProcessed.append(argument)
                continue

            # The argument is included in the workingDirectory and should be converted
            argumentsProcessed.append(os.path.join(".", os.path.basename(argument)))

        command = shlex.join(argumentsProcessed)
        with open(self.executable, "w") as f:
            f.write(command)
            # Post-processing: compute the checksum of the outputs
            f.write(f"\nmd5sum * > {self.checkSumOutput}")

    def _checkOutputIntegrity(self, workingDirectory):
        """Make sure that output files are not corrupted.

        :param str workingDirectory: path of the outputs
        """
        checkSumOutput = os.path.join(workingDirectory, self.checkSumOutput)
        if not os.path.exists(checkSumOutput):
            return S_ERROR(f"Cannot guarantee the integrity of the outputs: {checkSumOutput} unavailable")

        with open(checkSumOutput) as f:
            # for each output file, compute the md5 checksum
            for line in f:
                checkSum, remoteOutput = list(filter(None, line.strip("\n").split(" ")))

                hash = hashlib.md5()
                localOutput = os.path.join(workingDirectory, remoteOutput)
                if not os.path.exists(localOutput):
                    return S_ERROR(f"{localOutput} was expected but not found")

                with open(localOutput, "rb") as f:
                    while chunk := f.read(128 * hash.block_size):
                        hash.update(chunk)
                if checkSum != hash.hexdigest():
                    return S_ERROR(f"{localOutput} is corrupted")

        return S_OK()
