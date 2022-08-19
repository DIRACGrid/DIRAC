""" RemoteRunner

Mostly called by workflow modules, RemoteRunner is generally the last component to get through before
the script/application execution on a remote machine.
Depending on an environment variable WORKLOADEXECLOCATION, it decides whether it should take care of the execution.
RemoteRunner has been designed to send script/application on remote worker nodes having no outbound connectivity
(e.g. supercomputers)
"""
import os
import shlex
import time

from DIRAC import gLogger, gConfig, S_OK
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


class RemoteRunner:
    def __init__(self):
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.remoteExecution = gConfig.getValue("/LocalSite/RemoteExecution", "false")

    def is_remote_execution(self):
        """Main method: decides whether the execution will be done locally or remotely via a CE.

        :return: bool
        """

        # if remoteExecution is true, this means the workload should be executed
        # in a different remote location. This mainly happens when the remote Site has no
        # external connectivity and can only execute the workload itself.
        return self.remoteExecution.lower() in ["true", "yes"]

    def execute(self, command):
        """Execute the command remotely via a CE

        :param str command: command to execute remotely
        """
        # Set up Application Queue
        self.log.verbose("Remote application execution on:", self.remoteExecution)
        result = self._setUpworkloadCE()
        if not result["OK"]:
            return result
        workloadCE = result["Value"]

        # Add the command in an executable file
        executable = self._wrapCommand(command)
        # get inputs file from the current working directory
        inputs = os.listdir(".")
        inputs.remove(os.path.basename(executable))
        self.log.verbose("The executable will be sent along with the following inputs:", ",".join(inputs))
        # request the whole directory as output
        outputs = ["/"]

        # Submit the command as a job
        result = workloadCE.submitJob(executable, workloadCE.proxy, inputs=inputs, outputs=outputs)
        if not result["OK"]:
            return result
        jobID = result["Value"][0]
        stamp = result["PilotStampDict"][jobID]

        # Get status of the job
        jobStatus = PilotStatus.RUNNING
        while jobStatus not in PilotStatus.PILOT_FINAL_STATES:
            time.sleep(120)
            result = workloadCE.getJobStatus([jobID])
            if not result["OK"]:
                return result
            jobStatus = result["Value"][jobID]
        self.log.verbose("The final status of the application/script is: ", jobStatus)

        # Get job outputs
        result = workloadCE.getJobOutput(f"{jobID}:::{stamp}", os.path.abspath("."))
        if not result["OK"]:
            return result

        commandStatus = {"Done": 0, "Failed": -1, "Killed": -2}
        output, error = result["Value"]
        outputDict = {"OK": True, "Value": [commandStatus[jobStatus], output, error]}
        return outputDict

    def _setUpworkloadCE(self):
        """Get application queue and configure it

        :return: a ComputingElement instance
        """
        # Get CE parameters
        workloadSite = gConfig.getValue("/LocalSite/Site")
        workloadCE = gConfig.getValue("/LocalSite/GridCE")
        workloadQueue = gConfig.getValue("/LocalSite/CEQueue")

        result = getQueue(workloadSite, workloadCE, workloadQueue)
        if not result["OK"]:
            return result
        ceType = result["Value"]["CEType"]
        ceParams = result["Value"]

        # Build CE
        ceFactory = ComputingElementFactory()
        result = ceFactory.getCE(ceName=workloadCE, ceType=ceType, ceParametersDict=ceParams)
        if not result["OK"]:
            return result
        workloadCE = result["Value"]

        # Add a proxy to the CE
        result = getProxyInfo()
        if not result["OK"] and not result["Value"]["chain"]:
            return result
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
