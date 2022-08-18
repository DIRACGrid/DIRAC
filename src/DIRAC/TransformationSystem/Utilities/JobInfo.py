"""Job Information"""
from pprint import pformat
from itertools import zip_longest

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.List import fromChar

ASSIGNEDSTATES = ["Assigned", "Processed"]

LOG = gLogger.getSubLogger(__name__)


class TaskInfoException(Exception):
    """Exception when the task info is not recoverable"""

    def __init__(self, message):
        super().__init__(message)


class JobInfo:
    """hold information about jobs"""

    def __init__(self, jobID, status, tID, tType=None):
        self.tID = int(tID)
        self.tType = tType
        self.jobID = int(jobID)
        self.status = status

        # information related to input files
        self.inputFiles = []
        self.inputFileStatus = []  # from the filecatalog
        self.transFileStatus = []  # from the transformation
        self.inputFilesExist = []
        self.errorCounts = []
        self.otherTasks = []

        self.outputFiles = []
        self.outputFileStatus = []

        self.taskID = None
        self.pendingRequest = False

        self.maxResetCounter = Operations().getValue("Transformations/FilesMaxResetCounter", 10)

    def __str__(self):
        info = "%d: %s" % (self.jobID, self.status)
        if self.tID and self.taskID:
            info += " %s Transformation: %d -- %d " % (self.tType, self.tID, self.taskID)
        if self.otherTasks:
            info += " (Last task %s)" % self.otherTasks
        if self.inputFiles:
            ifInfo = [
                "<<< %s (%s, %s, Errors %s)" % _
                for _ in zip_longest(self.inputFiles, self.inputFilesExist, self.transFileStatus, self.errorCounts)
            ]
            info += "\n".join(ifInfo)
        if self.outputFiles:
            info += "\n::: OutputFiles: "
            efInfo = ["%s (%s)" % _ for _ in zip_longest(self.outputFiles, self.outputFileStatus)]
            info += ", ".join(efInfo)
        if self.pendingRequest:
            info += "\n PENDING REQUEST IGNORE THIS JOB!!!"
        else:
            info += "\n No Pending Requests"

        return info

    def allFilesExist(self):
        """Check if all files exists, if there are no output files, return False."""
        return bool(self.outputFileStatus) and all("Exists" in status for status in self.outputFileStatus)

    def allFilesMissing(self):
        """Check if all files are missing, if not output files, return False"""
        return bool(self.outputFileStatus) and all("Missing" in status for status in self.outputFileStatus)

    def someFilesMissing(self):
        """check if some files are missing and therefore some files exist"""
        return bool(self.outputFileStatus) and not (self.allFilesExist() or self.allFilesMissing())

    def allInputFilesExist(self):
        """check if all input files exists"""
        if not self.inputFileStatus:
            return False
        return all("Exists" in status for status in self.inputFileStatus)

    def allInputFilesMissing(self):
        """check if all input files are missing"""
        if not self.inputFileStatus:
            return False
        return all("Missing" in status for status in self.inputFileStatus)

    def someInputFilesMissing(self):
        """check if some input files are missing and therefore some files exist"""
        if not self.inputFileStatus:
            return False
        return not (self.allInputFilesExist() or self.allInputFilesMissing())

    def allFilesProcessed(self):
        """Check if all input files are processed."""
        return bool(self.transFileStatus) and all("Processed" in status for status in self.transFileStatus)

    def allFilesAssigned(self):
        """Check if all input files are assigned."""
        return bool(self.transFileStatus) and all(status in ASSIGNEDSTATES for status in self.transFileStatus)

    def allTransFilesDeleted(self):
        """Check if all input files are deleted in the Transformation System."""
        return bool(self.transFileStatus) and all(status == "Deleted" for status in self.transFileStatus)

    def checkErrorCount(self):
        """Check if any file is above Operations/Transformations/FilesMaxResetCounter error count."""
        return any(errorCount > self.maxResetCounter for errorCount in self.errorCounts)

    def getJobInformation(self, diracAPI, jobMon, jdlOnly=False):
        """Get all the information for the job.

        The InputData, TaskID, OutputData can either be taken from properly filled JDL or

        * inputData from jobMonitor getInputData
        * TaskID from the name of The job via jobMonitor getJobAttribute JobName
        * ProductionOutputData: from jobMonitor getJobParameter ProductionOutputData

        This would be faster if we could do bulk calls for all of these
        """
        if not jdlOnly:
            # this is actually slower than just getting the jdl, because getting the jdl is one service call
            # this is three service calls to three different DBs
            resInputData = jobMon.getInputData(self.jobID)
            if resInputData["OK"]:
                self.inputFiles = resInputData["Value"]
            resName = jobMon.getJobAttribute(self.jobID, "JobName")
            if resName["OK"] and "_" in resName["Value"]:
                self.__getTaskID(resName["Value"])
            resOutput = jobMon.getJobParameter(self.jobID, "ProductionOutputData")
            if resOutput["OK"]:
                self.outputFiles = fromChar(resOutput["Value"].get("ProductionOutputData", ""))
                if not self.outputFiles:
                    LOG.verbose("Did not find outputFiles for", str(self))

        if not (self.inputFiles and self.outputFiles and self.taskID):
            LOG.verbose("Have to check JDL")
            jdlParameters = self.__getJDL(diracAPI)
            # get taskID from JobName, get inputfile(s) from DownloadInputdata
            self.__getOutputFiles(jdlParameters)
            self.__getTaskID(jdlParameters)
            if not self.inputFiles:
                self.__getInputFile(jdlParameters)

    def getTaskInfo(self, tasksDict, lfnTaskDict, withInputTypes):
        """extract the task information from the taskDict"""

        if not self.inputFiles and self.tType in withInputTypes:
            raise TaskInfoException("InputFiles is empty: %s" % str(self))

        # taskID not in tasksDict means another job has been assigned to the files
        if self.taskID in tasksDict:
            for taskDict in tasksDict[self.taskID]:
                self.transFileStatus.append(taskDict["Status"])
                self.errorCounts.append(taskDict["ErrorCount"])
                if taskDict["LFN"] not in self.inputFiles:
                    raise TaskInfoException(
                        "InputFiles do not agree: {} not in InputFiles: \n {}".format(taskDict["LFN"], str(self))
                    )
            return

        # taskID not in tasksDict means another job has been assigned to the files
        for inputLFN in self.inputFiles:
            try:
                for taskDict in tasksDict[lfnTaskDict[inputLFN]]:
                    self.otherTasks.append(lfnTaskDict[inputLFN])
                    self.transFileStatus.append(taskDict["Status"])
                    self.errorCounts.append(taskDict["ErrorCount"])
            except KeyError as ke:
                LOG.error("ERROR for key:", str(ke))
                LOG.error("Failed to get taskDict", f"{self.taskID}, {self.inputFiles}: {pformat(lfnTaskDict)}")
                raise

    def checkFileExistence(self, success):
        """check if input and outputfile still exist"""
        for lfn in self.inputFiles:
            if lfn in success and success[lfn]:
                self.inputFilesExist.append(True)
                self.inputFileStatus.append("Exists")
            elif lfn in success:
                self.inputFileStatus.append("Missing")
                self.inputFilesExist.append(False)
            else:
                self.inputFileStatus.append("Unknown")
                self.inputFilesExist.append(False)

        for lfn in self.outputFiles:
            if lfn in success and success[lfn]:
                self.outputFileStatus.append("Exists")
            elif lfn in success:
                self.outputFileStatus.append("Missing")
            else:
                self.outputFileStatus.append("Unknown")

    def __getJDL(self, diracAPI):
        """return jdlParameterDictionary for this job"""
        res = diracAPI.getJobJDL(int(self.jobID))
        if not res["OK"]:
            raise RuntimeError("Failed to get jobJDL: %s" % res["Message"])
        jdlParameters = res["Value"]
        return jdlParameters

    def __getOutputFiles(self, jdlParameters):
        """get the Production Outputfiles for the given Job"""
        lfns = jdlParameters.get("ProductionOutputData", [])
        if isinstance(lfns, str):
            lfns = [lfns]
        self.outputFiles = lfns

    def __getInputFile(self, jdlParameters):
        """get the Inputdata for the given job"""
        lfn = jdlParameters.get("InputData", None)
        if isinstance(lfn, str):
            self.inputFiles.append(lfn)
            return
        if isinstance(lfn, list):
            self.inputFiles = lfn
            return

    def __getTaskID(self, jdlParameters):
        """Get the taskID."""
        if isinstance(jdlParameters, str):
            self.taskID = int(jdlParameters.split("_")[1])
        if "TaskID" not in jdlParameters:
            return
        try:
            self.taskID = int(jdlParameters.get("TaskID", None))
        except ValueError:
            LOG.warn("*" * 80)
            LOG.warn("TaskID broken?: %r" % jdlParameters.get("TaskID", None))
            LOG.warn(self)
            LOG.warn("*" * 80)
            raise

    def setJobDone(self, tInfo):
        """mark job as done in wms and transformationsystem"""
        tInfo.setJobDone(self)

    def setJobFailed(self, tInfo):
        """mark job as failed in  wms and transformationsystem"""
        tInfo.setJobFailed(self)

    def setInputProcessed(self, tInfo):
        """mark input file as Processed"""
        tInfo.setInputProcessed(self)

    def setInputMaxReset(self, tInfo):
        """mark input file as MaxReset"""
        tInfo.setInputMaxReset(self)

    def setInputDeleted(self, tInfo):
        """mark input file as Deleted"""
        tInfo.setInputDeleted(self)

    def setInputUnused(self, tInfo):
        """mark input file as Unused"""
        tInfo.setInputUnused(self)

    def cleanOutputs(self, tInfo):
        """remove all job outputs"""
        tInfo.cleanOutputs(self)
