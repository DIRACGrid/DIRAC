"""TransformationInfo class to be used by ILCTransformation System"""
from collections import OrderedDict, defaultdict
from itertools import zip_longest

from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.Proxy import UserProxy
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient


class TransformationInfo:
    """Hold information about a transformation."""

    def __init__(self, transformationID, transInfoDict, enabled, tClient, fcClient, jobMon):
        """Store clients etc."""
        self.log = gLogger.getSubLogger(__name__ + "[%s]" % transformationID)
        self.enabled = enabled
        self.tID = transformationID
        self.transName = transInfoDict["TransformationName"]
        self.tClient = tClient
        self.jobMon = jobMon
        self.fcClient = fcClient
        self.transType = transInfoDict["Type"]
        self.authorDN = transInfoDict["AuthorDN"]
        self.authorGroup = transInfoDict["AuthorGroup"]
        self.jobStateClient = JobStateUpdateClient()

    def checkTasksStatus(self):
        """Check the status for the task of given transformation and taskID"""

        res = self.tClient.getTransformationFiles(condDict={"TransformationID": self.tID})
        if not res["OK"]:
            raise RuntimeError("Failed to get transformation tasks: %s" % res["Message"])

        tasksDict = defaultdict(list)
        for task in res["Value"]:
            taskID = task["TaskID"]
            lfn = task["LFN"]
            status = task["Status"]
            fileID = task["FileID"]
            errorCount = task["ErrorCount"]
            tasksDict[taskID].append(dict(FileID=fileID, LFN=lfn, Status=status, ErrorCount=errorCount))

        return tasksDict

    def setJobDone(self, job):
        """set the taskID to Done"""
        if not self.enabled:
            return
        self.__setTaskStatus(job, "Done")
        if job.status != JobStatus.DONE:
            self.__updateJobStatus(job.jobID, JobStatus.DONE, "Job forced to Done")

    def setJobFailed(self, job):
        """set the taskID to Done"""
        if not self.enabled:
            return
        self.__setTaskStatus(job, "Failed")
        if job.status != JobStatus.FAILED:
            self.__updateJobStatus(job.jobID, JobStatus.FAILED, "Job forced to Failed")

    def setInputUnused(self, job):
        """Set the inputfiles to unused"""
        self.__setInputStatus(job, "Unused")

    def setInputMaxReset(self, job):
        """set the inputfile to MaxReset"""
        self.__setInputStatus(job, "MaxReset")

    def setInputProcessed(self, job):
        """set the inputfile to processed"""
        self.__setInputStatus(job, "Processed")

    def setInputDeleted(self, job):
        """set the inputfile to processed"""
        self.__setInputStatus(job, "Deleted")

    def __setInputStatus(self, job, status):
        """set the input file to status"""
        if self.enabled:
            result = self.tClient.setFileStatusForTransformation(self.tID, status, job.inputFiles, force=True)
            if not result["OK"]:
                gLogger.error("Failed updating status", result["Message"])
                raise RuntimeError("Failed updating file status")

    def __setTaskStatus(self, job, status):
        """update the task in the TransformationDB"""
        taskID = job.taskID
        res = self.tClient.setTaskStatus(self.transName, taskID, status)
        if not res["OK"]:
            raise RuntimeError("Failed updating task status: %s" % res["Message"])

    def __updateJobStatus(self, jobID, status, minorstatus=""):
        """Update the job status."""
        if self.enabled:
            source = "DataRecoveryAgent"
            result = self.jobStateClient.setJobStatus(jobID, status, minorstatus, source, None, True)
        else:
            return S_OK("DisabledMode")
        if not result["OK"]:
            self.log.error("Failed to update job status", result["Message"])
            raise RuntimeError("Failed to update job status")
        return result

    def __findAllDescendants(self, lfnList):
        """Find all descendants of a list of LFNs"""
        allDescendants = []
        result = self.fcClient.getFileDescendents(lfnList, list(range(1, 8)))
        if not result["OK"]:
            return allDescendants
        for dummy_lfn, descendants in result["Value"]["Successful"].items():
            allDescendants.extend(descendants)
        return allDescendants

    def cleanOutputs(self, jobInfo):
        """Remove all job outputs for job represented by jobInfo object.

        Including removal of descendents, if defined.
        """
        if len(jobInfo.outputFiles) == 0:
            return
        descendants = self.__findAllDescendants(jobInfo.outputFiles)
        existingOutputFiles = [
            lfn for lfn, status in zip_longest(jobInfo.outputFiles, jobInfo.outputFileStatus) if status == "Exists"
        ]
        filesToDelete = existingOutputFiles + descendants

        if not filesToDelete:
            return

        if not self.enabled:
            self.log.notice("Would have removed these files: \n +++ %s " % "\n +++ ".join(filesToDelete))
            return
        self.log.notice("Remove these files: \n +++ %s " % "\n +++ ".join(filesToDelete))

        errorReasons = defaultdict(list)
        successfullyRemoved = 0

        for lfnList in breakListIntoChunks(filesToDelete, 200):
            with UserProxy(proxyUserDN=self.authorDN, proxyUserGroup=self.authorGroup) as proxyResult:
                if not proxyResult["OK"]:
                    raise RuntimeError("Failed to get a proxy: %s" % proxyResult["Message"])
                result = DataManager().removeFile(lfnList)
                if not result["OK"]:
                    self.log.error("Failed to remove LFNs", result["Message"])
                    raise RuntimeError("Failed to remove LFNs: %s" % result["Message"])
                for lfn, err in result["Value"]["Failed"].items():
                    reason = str(err)
                    errorReasons[reason].append(lfn)
                successfullyRemoved += len(result["Value"]["Successful"])
        for reason, lfns in errorReasons.items():
            self.log.error("Failed to remove %d files with error: %s" % (len(lfns), reason))
        self.log.notice("Successfully removed %d files" % successfullyRemoved)

    def getJobs(self, statusList=None):
        """Get done and failed jobs.

        :param list statusList: optional list of status to find jobs
        :returns: 3-tuple of OrderedDict of JobInfo objects, keyed by jobID;
                  number of Done jobs; number of Failed jobs
        """
        done = S_OK([])
        failed = S_OK([])
        if statusList is None:
            statusList = [JobStatus.DONE, JobStatus.FAILED]
        if "Done" in statusList:
            self.log.notice("Getting 'Done' Jobs...")
            done = self.__getJobs([JobStatus.DONE])
        if "Failed" in statusList:
            self.log.notice("Getting 'Failed' Jobs...")
            failed = self.__getJobs([JobStatus.FAILED])
        done = done["Value"]
        failed = failed["Value"]

        jobsUnsorted = {}
        for job in done:
            jobsUnsorted[int(job)] = JobInfo(job, JobStatus.DONE, self.tID, self.transType)
        for job in failed:
            jobsUnsorted[int(job)] = JobInfo(job, JobStatus.FAILED, self.tID, self.transType)
        jobs = OrderedDict(sorted(jobsUnsorted.items(), key=lambda t: t[0]))

        self.log.notice("Found %d Done Jobs " % len(done))
        self.log.notice("Found %d Failed Jobs " % len(failed))
        return jobs, len(done), len(failed)

    def __getJobs(self, status):
        """Return list of jobs with given status.

        :param list status: list of status to find
        :returns: S_OK with result
        :raises: RuntimeError when failing to find jobs
        """
        attrDict = dict(Status=status, JobGroup="%08d" % int(self.tID))
        res = self.jobMon.getJobs(attrDict)
        if res["OK"]:
            self.log.debug("Found Trans jobs: %s" % res["Value"])
            return res
        else:
            self.log.error("Error finding jobs: ", res["Message"])
            raise RuntimeError("Failed to get jobs")
