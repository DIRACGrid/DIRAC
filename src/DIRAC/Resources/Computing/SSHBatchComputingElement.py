""" SSH (Virtual) Batch Computing Element: For a given list of ip/cores pair it will send jobs
    directly through ssh
"""

import os
import stat
from urllib.parse import urlparse

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Computing.PilotBundle import bundleProxy, writeScript
from DIRAC.Resources.Computing.SSHComputingElement import SSHComputingElement
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


class SSHBatchComputingElement(SSHComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.connections = {}
        self.execution = "SSHBATCH"

    def _reset(self):
        """Process CE parameters and make necessary adjustments"""
        # Get the Batch System instance
        result = self._getBatchSystem()
        if not result["OK"]:
            return result

        # Get the location of the remote directories
        self._getBatchSystemDirectoryLocations()

        # Get the SSH parameters
        self.timeout = self.ceParameters.get("Timeout", self.timeout)
        self.user = self.ceParameters.get("SSHUser", self.user)
        port = self.ceParameters.get("SSHPort", None)
        password = self.ceParameters.get("SSHPassword", None)
        key = self.ceParameters.get("SSHKey", None)
        tunnel = self.ceParameters.get("SSHTunnel", None)

        # Get submission parameters
        self.submitOptions = self.ceParameters.get("SubmitOptions", self.submitOptions)
        self.preamble = self.ceParameters.get("Preamble", self.preamble)
        self.account = self.ceParameters.get("Account", self.account)
        self.queue = self.ceParameters["Queue"]
        self.log.info("Using queue: ", self.queue)

        # Get output and error templates
        self.outputTemplate = self.ceParameters.get("OutputTemplate", self.outputTemplate)
        self.errorTemplate = self.ceParameters.get("ErrorTemplate", self.errorTemplate)

        # Prepare the remote hosts
        for host in self.ceParameters.get("SSHHost", "").strip().split(","):
            hostDetails = host.strip().split("/")
            if len(hostDetails) > 1:
                hostname = hostDetails[0]
                maxJobs = int(hostDetails[1])
            else:
                hostname = hostDetails[0]
                maxJobs = self.ceParameters.get("MaxTotalJobs", 0)

            connection = self._getConnection(hostname, self.user, port, password, key, tunnel)

            result = self._prepareRemoteHost(connection)
            if not result["OK"]:
                return result

            self.connections[hostname] = {"connection": connection, "maxJobs": maxJobs}
            self.log.info(f"Host {hostname} registered for usage")

        return S_OK()

    #############################################################################

    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        """Method to submit job"""
        # Choose eligible hosts, rank them by the number of available slots
        rankHosts = {}
        maxSlots = 0
        for _, details in self.connections.items():
            result = self._getHostStatus(details["connection"])
            if not result["OK"]:
                continue
            slots = details["maxJobs"] - result["Value"]["Running"]
            if slots > 0:
                rankHosts.setdefault(slots, [])
                rankHosts[slots].append(details["connection"])
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
        stampDict = {}
        batchSystemName = self.batchSystem.__class__.__name__.lower()

        for slots in range(maxSlots, 0, -1):
            if slots not in rankHosts:
                continue
            for connection in rankHosts[slots]:
                result = self._submitJobToHost(connection, submitFile, min(slots, restJobs))
                if not result["OK"]:
                    continue

                batchIDs, jobStamps = result["Value"]

                nJobs = len(batchIDs)
                if nJobs > 0:
                    jobIDs = [
                        f"{self.ceType.lower()}{batchSystemName}://{self.ceName}/{connection.host}/{_id}"
                        for _id in batchIDs
                    ]
                    submittedJobs.extend(jobIDs)
                    for iJob, jobID in enumerate(jobIDs):
                        stampDict[jobID] = jobStamps[iJob]

                    restJobs = restJobs - nJobs
                    if restJobs <= 0:
                        break
            if restJobs <= 0:
                break

        if proxy:
            os.remove(submitFile)

        result = S_OK(submittedJobs)
        result["PilotStampDict"] = stampDict
        return result

    #############################################################################

    def killJob(self, jobIDs):
        """Kill specified jobs"""
        jobIDList = list(jobIDs)
        if isinstance(jobIDs, str):
            jobIDList = [jobIDs]
        message = None

        hostDict = {}
        for job in jobIDList:
            host = os.path.dirname(urlparse(job).path).lstrip("/")
            hostDict.setdefault(host, [])
            hostDict[host].append(job)

        failed = []
        for host, jobIDList in hostDict.items():
            result = self._killJobOnHost(self.connections[host]["connection"], jobIDList)
            if not result["OK"]:
                failed.extend(jobIDList)
                message = result["Message"]

        if failed:
            result = S_ERROR(message)
            result["Failed"] = failed
        else:
            result = S_OK()

        return result

    #############################################################################

    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = 0
        result["WaitingJobs"] = 0

        for _, details in self.connections:
            resultHost = self._getHostStatus(details["connection"])
            if resultHost["OK"]:
                result["RunningJobs"] += resultHost["Value"]["Running"]

        self.log.verbose("Waiting Jobs: ", 0)
        self.log.verbose("Running Jobs: ", result["RunningJobs"])

        return result

    #############################################################################

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
            result = self._getJobStatusOnHost(self.connections[host]["connection"], jobIDList)
            if not result["OK"]:
                failed.extend(jobIDList)
                continue
            resultDict.update(result["Value"])

        for job in failed:
            if job not in resultDict:
                resultDict[job] = PilotStatus.UNKNOWN

        return S_OK(resultDict)

    #############################################################################

    def getJobOutput(self, jobID, localDir=None):
        """Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned
        as strings.
        """
        self.log.verbose("Getting output for jobID", jobID)

        # host can be retrieved from the path of the jobID
        host = os.path.dirname(urlparse(jobID).path).lstrip("/")
        return self._getJobOutputFilesOnHost(self.connections[host]["connection"], jobID, localDir)
