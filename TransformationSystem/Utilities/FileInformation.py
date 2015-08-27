"""FileInformation class to be used by ILCTransformation System"""
__RCSID__ = "$Id$"

from itertools import izip_longest

from DIRAC import gLogger, S_OK

ENABLED = False


class FileInformation(object):
  """hold information about inputdata files"""

  def __init__(self, lfn, fileID, status, wmsID):
    self.lfn = lfn
    self.status = status
    self.wmsID = wmsID
    self.fileID = fileID
    self.jobID = 0
    self.jobStatus = None
    self.outputFiles = []
    self.outputStatus = []
    self.descendents = []

  def __str__(self):
    info = "LFN: %s, FileID %s FileStatus: %s WMSID: %s, Job: %d (%s)" % (self.lfn,
                                                                          self.fileID,
                                                                          self.status,
                                                                          self.wmsID,
                                                                          int(self.jobID),
                                                                          str(self.jobStatus))
    if self.outputFiles:
      efInfo = ["%s (%s)" % _ for _ in izip_longest(self.outputFiles, self.outputStatus)]
      info += "\n-->OutputFiles: "
      info += ", ".join(efInfo)
    if self.descendents:
      info += "\n-->Descendents: "
      info += ", ".join(self.descendents)
    return info

  def setJobDone(self, tInfo):
    """set the jobstatus to Done"""
    if ENABLED:
      res = tInfo.transClient.setTaskStatus(tInfo.transName, self.wmsID, "Done")
      if not res['OK']:
        raise RuntimeError("Failed updating task status")
      self.__updateJobStatus(tInfo, "Done", "Job Finished Successfully")

  def setInputUnused(self, tInfo):
    """set the inputfile to unused"""
    self.__setInputStatus(tInfo, "Unused")

  def setInputProcessed(self, tInfo):
    """set the inputfile to processed"""
    self.__setInputStatus(tInfo, "Processed")

  def __setInputStatus(self, tInfo, status):
    """set the input file to status"""
    if ENABLED:
      result = tInfo.transClient.setFileStatusForTransformation(tInfo.tID, status, [self.lfn], force=True)
      if not result['OK']:
        gLogger.error("Failed updating status", result['Message'])

  def __updateJobStatus(self, tInfo, status, minorstatus=None):
    """ This method updates the job status in the JobDB
    """
    tInfo.log.verbose("self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" % (self.jobID, status))

    if ENABLED:
      result = tInfo.jobDB.setJobAttribute(self.jobID, 'Status', status, update=True)
    else:
      return S_OK('DisabledMode')

    if result['OK']:
      if minorstatus:
        tInfo.log.verbose("self.jobDB.setJobAttribute(%s,'MinorStatus','%s',update=True)" % (self.jobID, minorstatus))
        result = tInfo.jobDB.setJobAttribute(self.jobID, 'MinorStatus', minorstatus, update=True)

    if not minorstatus:  # Retain last minor status for stalled jobs
      result = tInfo.jobDB.getJobAttributes(self.jobID, ['MinorStatus'])
      if result['OK']:
        minorstatus = result['Value']['MinorStatus']

    logStatus = status
    result = tInfo.logDB.addLoggingRecord(self.jobID, status=logStatus, minor=minorstatus, source='DataRecoveryAgent')
    if not result['OK']:
      tInfo.log.warn(result)

    return result
