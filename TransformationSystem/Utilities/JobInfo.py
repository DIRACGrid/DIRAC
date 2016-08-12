"""Job Information"""

from itertools import izip_longest
import re

__RCSID__ = "$Id$"


class TaskInfoException(Exception):
  """Exception when the task info is not recoverable"""

  def __init__(self, message):
    super(TaskInfoException, self).__init__(message)


class JobInfo(object):
  """ hold information about jobs"""

  def __init__(self, jobID, status, tID, tType=None):
    self.tID = int(tID)
    self.tType = tType
    self.jobID = int(jobID)
    self.status = status
    self.inputFile = None
    self.inputFileExists = False
    self.outputFiles = None
    self.outputFileStatus = []
    self.taskID = None
    self.fileStatus = None
    self.taskFileID = None
    self.pendingRequest = False
    self.otherTasks = None
    self.errorCount = 0

  def __str__(self):
    info = "%d: %s" % (self.jobID, self.status)
    if self.tID and self.taskID:
      info += " %s Transformation: %d -- %d " % (self.tType, self.tID, self.taskID)
    if self.fileStatus:
      info += "TaskStatus: %s " % self.fileStatus
      if self.otherTasks:
        info += "(Last task %d)" % self.otherTasks
      info += "ErrorCount: %d" % self.errorCount
    if self.inputFile:
      info += "\n---> inputFile: %s (%s)" % (self.inputFile, self.inputFileExists)
    if self.outputFiles:
      info += "\n---> OutputFiles: "
      efInfo = ["%s (%s)" % _ for _ in izip_longest(self.outputFiles, self.outputFileStatus)]
      info += ", ".join(efInfo)
    if self.pendingRequest:
      info += "\n PENDING REQUEST IGNORE THIS JOB!!!"
    else:
      info += "\n No Pending Requests"

    return info

  def allFilesExist(self):
    """check if all files exists"""
    return all("Exists" in status for status in self.outputFileStatus)

  def allFilesMissing(self):
    """check if all files are missing"""
    if not self.outputFileStatus:
      return False
    return all("Missing" in status for status in self.outputFileStatus)

  def someFilesMissing(self):
    """check if some files are missing and therefore some files exist """
    return not (self.allFilesExist() or self.allFilesMissing())

  def getJobInformation(self, dILC):
    """get all the information for the job"""
    jdlParameters = self.__getJDL(dILC)
    self.__getOutputFiles(jdlParameters)
    self.__getTaskID(jdlParameters)
    self.__getInputFile(jdlParameters)

  def getTaskInfo(self, tasksDict, lfnTaskDict):
    """extract the task information from the taskDict"""
    if self.taskID not in tasksDict:
      #print "taskID %d not in tasksDict" % self.taskID
      taskDict = tasksDict[lfnTaskDict[self.inputFile]]
      self.otherTasks = lfnTaskDict[self.inputFile]
    else:
      taskDict = tasksDict[self.taskID]

    #dict( FileID=fileID, LFN=lfn, Status=status )
    if self.inputFile != taskDict['LFN']:
      raise TaskInfoException(
          "InputFiles do not agree: %s vs . %s : \n %s" %
          (self.inputFile, taskDict['LFN'], str(self)))
    self.fileStatus = taskDict['Status']
    self.taskFileID = taskDict['FileID']
    self.errorCount = taskDict['ErrorCount']

  def checkFileExistance(self, fcClient):
    """check if input and outputfile still exist"""
    lfns = []
    if self.inputFile:
      lfns = [self.inputFile]
    lfns = lfns + self.outputFiles
    reps = fcClient.exists(lfns)
    if not reps['OK']:
      raise RuntimeError("Failed to check existance: %s" % reps['Message'])
    statuses = reps['Value']
    success = statuses['Successful']
    if self.inputFile:
      self.inputFileExists = True if (self.inputFile in success and success[self.inputFile]) else False
    for lfn in self.outputFiles:
      if lfn in success and success[lfn]:
        self.outputFileStatus.append("Exists")
      elif lfn in success:
        self.outputFileStatus.append("Missing")
      else:
        self.outputFileStatus.append("Unknown")

  def checkRequests(self, reqClient):
    """check if there are pending Requests"""
    result = reqClient.readRequestsForJobs([self.jobID])
    if not result['OK']:
      raise RuntimeError("Failed to check Requests: %s " % result['Message'])
    if self.jobID in result['Value']['Successful']:
      request = result['Value']['Successful'][self.jobID]
      self.pendingRequest = request.Status not in ("Done", "Canceled")

  def __getJDL(self, dILC):
    """return jdlParameterDictionary for this job"""
    res = dILC.getJobJDL(int(self.jobID))
    if not res['OK']:
      raise RuntimeError("Failed to get jobJDL: %s" % res['Message'])
    jdlParameters = res['Value']
    return jdlParameters

  def __getOutputFiles(self, jdlParameters):
    """get the Production Outputfiles for the given Job"""
    lfns = jdlParameters.get('ProductionOutputData', [])
    if isinstance(lfns, basestring):
      lfns = [lfns]
    self.outputFiles = lfns

  def __getInputFile(self, jdlParameters):
    """get the Inputdata for the given job"""
    lfn = jdlParameters.get('InputData', None)
    self.inputFile = lfn

  def __getTaskID(self, jdlParameters):
    """get the taskID """
    for val in jdlList:
      if 'TaskID' in val:
        try:
          self.taskID = int(val.strip(";").split("=")[1].strip(' "'))
        except ValueError:
          print "*" * 80
          print "ERROR"
          print val
          print self
          print "*" * 80
          raise
        break

  @staticmethod
  def __getSingleLFN(jdlList):
    """get the only productionOutputData LFN from the jdlString"""
    for val in jdlList:
      if 'ProductionOutputData' in val:
        lfn = re.search('".*"', val)
        lfn = lfn.group(0).strip('"')
        return [lfn]

  @staticmethod
  def __getMultiLFN(jdlList):
    """ get multiple outputfiles """
    lfns = []
    counter = 0
    getEm = False
    for val in jdlList:
      if 'ProductionOutputData' in val:
        counter += 1
        continue
      if counter == 1 and '{' in val:
        getEm = True
        continue
      if '}' in val and getEm:
        break
      if getEm:
        lfn = re.search('".*"', val)
        lfn = lfn.group(0).strip('"')
        lfns.append(lfn)
    return lfns

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
