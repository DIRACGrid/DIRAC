""" SSH (Virtual) Computing Element: For a given list of ip/cores pair it will send jobs
    directly through ssh
"""

import os
import stat
from urllib.parse import urlparse

from DIRAC import S_ERROR, S_OK
from DIRAC.Resources.Computing.SSHComputingElement import SSHComputingElement
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

        # Prepare all the hosts
        for hPar in self.ceParameters["SSHHost"].strip().split(","):
            host = hPar.strip().split("/")[0]
            result = self._prepareRemoteHost(host=host)
            if result["OK"]:
                self.log.info(f"Host {host} registered for usage")
                self.sshHost.append(hPar.strip())
            else:
                self.log.error("Failed to initialize host", host)
                return result

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

        # Submit jobs now
        restJobs = numberOfJobs
        submittedJobs = []
        stampDict = {}
        for slots in range(maxSlots, 0, -1):
            if slots not in rankHosts:
                continue
            for host in rankHosts[slots]:
                result = self._submitJobToHost(executableFile, min(slots, restJobs), host)
                if not result["OK"]:
                    continue

                nJobs = len(result["Value"])
                if nJobs > 0:
                    submittedJobs.extend(result["Value"])
                    stampDict.update(result.get("PilotStampDict", {}))
                    restJobs = restJobs - nJobs
                    if restJobs <= 0:
                        break
            if restJobs <= 0:
                break

        result = S_OK(submittedJobs)
        result["PilotStampDict"] = stampDict
        return result

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
