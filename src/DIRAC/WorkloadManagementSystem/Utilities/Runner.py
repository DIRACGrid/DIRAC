import os
import shlex
import time

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue


class Runner(object):
    def __init__(self):
        self.log = gLogger.getSubLogger("Runner")

    def run(self, command, environment=None, callbackFunction=None, bufferLimit=52428800):
        workloadExecutionLocation = os.environ.get("WORKLOADEXECLOCATION")

        # if workloadExecutionLocation is not empty, this means the workload should be executed
        # in a different remote location. This mainly happens when the remote Site has no
        # external connectivity and can only execute the workload itself.
        if workloadExecutionLocation:
            return self._remoteRun(remoteSite=workloadExecutionLocation, command=command)

        return self._localRun(
            command=command, environment=environment, callbackFunction=callbackFunction, bufferLimit=bufferLimit
        )

    def _localRun(self, command, environment=None, callbackFunction=None, bufferLimit=0):
        """Execute the command locally"""
        self.log.verbose("Local application execution")
        outputDict = systemCall(
            timeout=0,
            cmdSeq=shlex.split(command),
            env=environment,
            callbackFunction=callbackFunction,
            bufferLimit=bufferLimit,
        )
        return outputDict

    def _remoteRun(self, remoteSite, command):
        """Execute the command remotely via a CE

        :param str remoteSite: name of the remote queue under the following form: Site:CE:Queue
        :param str command: command to execute remotely
        """
        # Set up Application Queue
        self.log.verbose("Remote application execution on:", "%s" % remoteSite)
        result = self._setUpworkloadCE(remoteSite)
        if not result["OK"]:
            return result
        workloadCE = result["Value"]

        # Add the command in an executable file
        executable = self._wrapCommand(command)
        # get inputs file from the current working directory
        inputs = os.listdir(".")
        inputs.remove(os.path.basename(executable))
        self.log.verbose("The executable will be sent along with the following inputs:", "%s" % ",".join(inputs))
        # request the whole directory as output
        outputs = ["/"]

        # Submit the command as a job
        result = workloadCE.submitJob(executable, workloadCE.proxy, inputs=inputs, outputs=outputs)
        if not result["OK"]:
            return S_ERROR("Cannot submit the command")
        jobID = result["Value"][0]
        stamp = result["PilotStampDict"][jobID]

        # Get status of the job
        jobStatus = "Running"
        while jobStatus not in ["Done", "Failed", "Killed"]:
            result = workloadCE.getJobStatus([jobID])
            if not result["OK"]:
                return S_ERROR("Cannot get the status of the job")
            jobStatus = result["Value"][jobID]
            time.sleep(120)

        # Get job outputs
        result = workloadCE.getJobOutput("%s:::%s" % (jobID, stamp))
        self.log.warn("Output: -%s-\n" % result)
        if not result["OK"]:
            return S_ERROR("Cannot get job outputs")

        commandStatus = {"Done": 0, "Failed": -1, "Killed": -2}
        output, error = result["Value"]
        outputDict = {"OK": True, "Value": [commandStatus[jobStatus], output, error]}
        return outputDict

    def _setUpworkloadCE(self, remoteSite):
        """Get application queue and configure it

        :param str remoteSite: name of the remote queue under the following form: Site:CE:Queue
        :return: a ComputingElement instance
        """
        # Get CE parameters
        workloadSite, workloadCE, workloadQueue = remoteSite.split(":")
        result = getQueue(workloadSite, workloadCE, workloadQueue)
        if not result["OK"]:
            return result
        ceType = result["Value"]["CEType"]
        ceParams = result["Value"]

        # Build CE
        ceFactory = ComputingElementFactory()
        result = ceFactory.getCE(ceName=workloadCE, ceType=ceType, ceParametersDict=ceParams)
        if not result["OK"]:
            return S_ERROR("Cannot instantiate the workloadCE")
        workloadCE = result["Value"]

        # Add a proxy to the CE
        result = getProxyInfo()
        if not result["OK"] and not result["Value"]["chain"]:
            return S_ERROR("Cannot get proxy")
        proxy = result["Value"]["chain"]
        result = proxy.getRemainingSecs()
        if not result["OK"]:
            return result
        lifetime_secs = result["Value"]
        workloadCE.setProxy(proxy, lifetime_secs)

        return S_OK(workloadCE)

    def _wrapCommand(self, command):
        """Wrap the command in a file

        :param str command: command line to write in the executable
        :return: name of the executable file
        """
        executable = "workloadExec.sh"
        with open(executable, "w") as f:
            f.write(command)
        return executable
