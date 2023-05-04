""" RemoteRunner

RemoteRunner has been designed to send scripts/applications and input files on remote worker nodes having
no outbound connectivity (e.g. supercomputers)

Mostly called by workflow modules, RemoteRunner is generally the last component to get through before
the script/application execution on a remote machine.
"""
import os
import shlex
from six.moves import shlex_quote
import time

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


class RemoteRunner(object):
    def __init__(self, siteName=None, ceName=None, queueName=None):
        self.log = gLogger.getSubLogger("RemoteRunner")
        self._workloadSite = siteName
        if not self._workloadSite:
            self.log.warn("You are expected to provide a siteName in parameters from v8.0")
            self.log.warn("Trying to get workloadSite from /LocalSite/Site...")
            self._workloadSite = gConfig.getValue("/LocalSite/Site")
        self._workloadCE = ceName
        if not self._workloadCE:
            self.log.warn("You are expected to provide a ceName in parameters from v8.0")
            self.log.warn("Trying to get workloadSite from /LocalSite/GridCE...")
            self._workloadCE = gConfig.getValue("/LocalSite/GridCE")
        self._workloadQueue = queueName
        if not self._workloadQueue:
            self.log.warn("You are expected to provide a queueName in parameters from v8.0")
            self.log.warn("Trying to get workloadSite from /LocalSite/CEQueue...")
            self._workloadQueue = gConfig.getValue("/LocalSite/CEQueue")

    @deprecated('Use gConfig.getValue("/LocalSite/RemoteExecution", False) instead.')
    def is_remote_execution(self):
        """Main method: decides whether the execution will be done locally or remotely via a CE.

        This method does not really make sense: if we use RemoteRunner, it means we want to perform a remote execution.
        Therefore, this should be checked before calling RemoteRunner by checking /LocalSite/RemoteExecution for instance.

        :return: bool
        """
        return gConfig.getValue("/LocalSite/RemoteExecution", False)

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
        result = self._checkParameters()
        if not result["OK"]:
            result["Errno"] = DErrno.ESECTION
            return result
        self.log.verbose(
            "The command will be sent to",
            "site %s, CE %s, queue %s" % (self._workloadSite, self._workloadCE, self._workloadQueue),
        )

        # Set up Application Queue
        result = self._setUpWorkloadCE(numberOfProcessors)
        if not result["OK"]:
            result["Errno"] = DErrno.ERESUNA
            return result
        workloadCE = result["Value"]
        self.log.debug("The CE interface has been set up")

        # Add the command in an executable file
        executable = "workloadExec.sh"
        self._wrapCommand(command, workingDirectory, executable)
        self.log.debug("The command has been wrapped into an executable")

        # Get inputs from the current working directory
        inputs = os.listdir(workingDirectory)
        inputs.remove(os.path.basename(executable))
        self.log.verbose("The executable will be sent along with the following inputs:", ",".join(inputs))
        # Request the whole directory as output
        outputs = ["/"]

        # Submit the command as a job
        result = workloadCE.submitJob(executable, workloadCE.proxy, inputs=inputs, outputs=outputs)
        if not result["OK"]:
            result["Errno"] = DErrno.EWMSSUBM
            return result
        jobID = result["Value"][0]
        stamp = result["PilotStampDict"][jobID]

        # Get status of the job
        jobStatus = PilotStatus.RUNNING
        while jobStatus not in PilotStatus.PILOT_FINAL_STATES:
            time.sleep(120)
            result = workloadCE.getJobStatus([jobID])
            if not result["OK"]:
                result["Errno"] = DErrno.EWMSSTATUS
                return result
            jobStatus = result["Value"][jobID]
        self.log.verbose("The final status of the application/script is: ", jobStatus)

        # Get job outputs
        result = workloadCE.getJobOutput("%s:::%s" % (jobID, stamp), os.path.abspath("."))
        if not result["OK"]:
            result["Errno"] = DErrno.EWMSJMAN
            return result
        output, error = result["Value"]

        # Clean job in the remote resource
        if cleanRemoteJob:
            result = workloadCE.cleanJob(jobID)
            if not result["OK"]:
                self.log.warn("Failed to clean the output remotely", result["Message"])

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
        result = proxy.getRemainingSecs()
        if not result["OK"]:
            return result
        lifetime_secs = result["Value"]
        workloadCE.setProxy(proxy, lifetime_secs)

        return S_OK(workloadCE)

    def _wrapCommand(self, command, workingDirectory, executable):
        """Wrap the command in a file

        :param str command: command line to write in the executable
        :param str workingDirectory: directory containing the inputs required by the command
        :param str executable: path of the executable that should contain the command to submit
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

        # Fro v8.0, use: shlex.join(argumentsProcessed)
        command = " ".join(shlex_quote(arg) for arg in argumentsProcessed)
        with open(executable, "w") as f:
            f.write(command)
