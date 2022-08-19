########################################################################
# File :   SSHComputingElement.py
# Author : Dumitru Laurentiu
########################################################################

""" SSH (Virtual) Computing Element: For a given list of ip/cores pair it will send jobs
    directly through ssh
"""
import os
import socket
import stat
from urllib.parse import urlparse

from DIRAC import S_OK, S_ERROR
from DIRAC import rootPath

from DIRAC.Resources.Computing.SSHComputingElement import SSHComputingElement
from DIRAC.Resources.Computing.PilotBundle import bundleProxy, writeScript
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


class SSHBatchComputingElement(SSHComputingElement):

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.ceType = "SSHBatch"
        self.sshHost = []
        self.execution = "SSHBATCH"

    def _reset(self):

        batchSystemName = self.ceParameters.get("BatchSystem", "Host")
        if "BatchSystem" not in self.ceParameters:
            self.ceParameters["BatchSystem"] = batchSystemName
        result = self.loadBatchSystem(batchSystemName)
        if not result["OK"]:
            self.log.error("Failed to load the batch system plugin", batchSystemName)
            return result

        self.user = self.ceParameters["SSHUser"]
        self.queue = self.ceParameters["Queue"]
        self.sshScript = os.path.join(rootPath, "DIRAC", "Resources", "Computing", "remote_scripts", "sshce")
        if "ExecQueue" not in self.ceParameters or not self.ceParameters["ExecQueue"]:
            self.ceParameters["ExecQueue"] = self.ceParameters.get("Queue", "")
        self.execQueue = self.ceParameters["ExecQueue"]
        self.log.info("Using queue: ", self.queue)
        self.hostname = socket.gethostname()
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

        # Prepare all the hosts
        for hPar in self.ceParameters["SSHHost"].strip().split(","):
            host = hPar.strip().split("/")[0]
            result = self._prepareRemoteHost(host=host)
            if result["OK"]:
                self.log.info("Host %s registered for usage" % host)
                self.sshHost.append(hPar.strip())
            else:
                self.log.error("Failed to initialize host", host)
                return result

        self.submitOptions = self.ceParameters.get("SubmitOptions", "")
        self.removeOutput = True
        if "RemoveOutput" in self.ceParameters:
            if self.ceParameters["RemoveOutput"].lower() in ["no", "false", "0"]:
                self.removeOutput = False
        self.preamble = self.ceParameters.get("Preamble", "")

        return S_OK()

    #############################################################################
    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        """Method to submit job"""

        # Choose eligible hosts, rank them by the number of available slots
        rankHosts = {}
        maxSlots = 0
        for host in self.sshHost:
            thost = host.split("/")
            hostName = thost[0]
            maxHostJobs = 1
            if len(thost) > 1:
                maxHostJobs = int(thost[1])

            result = self._getHostStatus(hostName)
            if not result["OK"]:
                continue
            slots = maxHostJobs - result["Value"]["Running"]
            if slots > 0:
                rankHosts.setdefault(slots, [])
                rankHosts[slots].append(hostName)
            if slots > maxSlots:
                maxSlots = slots

        if maxSlots == 0:
            return S_ERROR("No online node found on queue")
        # make it executable
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

        # Submit jobs now
        restJobs = numberOfJobs
        submittedJobs = []
        for slots in range(maxSlots, 0, -1):
            if slots not in rankHosts:
                continue
            for host in rankHosts[slots]:
                result = self._submitJobToHost(submitFile, min(slots, restJobs), host)
                if not result["OK"]:
                    continue
                else:
                    nJobs = len(result["Value"])
                    if nJobs > 0:
                        submittedJobs.extend(result["Value"])
                        restJobs = restJobs - nJobs
                        if restJobs <= 0:
                            break
            if restJobs <= 0:
                break

        if proxy:
            os.remove(submitFile)

        return S_OK(submittedJobs)

    def killJob(self, jobIDs):
        """Kill specified jobs"""
        jobIDList = list(jobIDs)
        if isinstance(jobIDs, str):
            jobIDList = [jobIDs]

        hostDict = {}
        for job in jobIDList:

            host = os.path.dirname(urlparse(job).path).lstrip("/")
            hostDict.setdefault(host, [])
            hostDict[host].append(job)

        failed = []
        for host, jobIDList in hostDict.items():
            result = self._killJobOnHost(jobIDList, host)
            if not result["OK"]:
                failed.extend(jobIDList)
                message = result["Message"]

        if failed:
            result = S_ERROR(message)
            result["Failed"] = failed
        else:
            result = S_OK()

        return result

    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = 0
        result["WaitingJobs"] = 0

        for host in self.sshHost:
            thost = host.split("/")
            resultHost = self._getHostStatus(thost[0])
            if resultHost["OK"]:
                result["RunningJobs"] += resultHost["Value"]["Running"]

        self.log.verbose("Waiting Jobs: ", 0)
        self.log.verbose("Running Jobs: ", result["RunningJobs"])

        return result

    def getJobStatus(self, jobIDList):
        """Get status of the jobs in the given list"""
        hostDict = {}
        for job in jobIDList:
            host = os.path.dirname(urlparse(job).path).lstrip("/")
            hostDict.setdefault(host, [])
            hostDict[host].append(job)

        resultDict = {}
        failed = []
        for host, jobIDList in hostDict.items():
            result = self._getJobStatusOnHost(jobIDList, host)
            if not result["OK"]:
                failed.extend(jobIDList)
                continue
            resultDict.update(result["Value"])

        for job in failed:
            if job not in resultDict:
                resultDict[job] = PilotStatus.UNKNOWN

        return S_OK(resultDict)
