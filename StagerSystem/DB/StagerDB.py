########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/DB/StagerDB.py,v 1.10 2008/04/04 11:31:45 paterson Exp $
########################################################################

""" StagerDB is a front end to the Stager Database.
    This maintains LFN,SURLs and SEs of files being staged.
    It also maintains timing information for the commands performed by the Stager Agent.
    A.Smith (17/05/07)
"""

__RCSID__ = "$Id: StagerDB.py,v 1.10 2008/04/04 11:31:45 paterson Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Time import toString
from DIRAC.Core.Base.DB import DB

import string

class StagerDB(DB):

  def __init__(self, systemInstance='Default', maxQueueSize=10 ):
    DB.__init__(self,'StagerDB','Stager/StagerDB',maxQueueSize)

  def setTiming(self,site,cmd,time,files):
    """
      Insert timing information for staging commands into the StagerDB SiteTiming table.
    """
    req = "INSERT INTO SiteTiming (Site,Command,CommTime,Files,Time) VALUES ('%s','%s',%f,%d,UTC_TIMESTAMP());" % (site,cmd,time,files)
    return self._update(req)

  def getAllJobs(self,source):
    """
      Selects the unique JobIDs from the SiteFiles table for given site and system source e.g. WMS, DMS.
    """
    req = "SELECT DISTINCT JobID FROM SiteFiles WHERE Source = '%s';" %(source)
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      jobIDs = []
      for jobid in result['Value']:
        jobIDs.append(jobid[0])
      result = S_OK()
      result['JobIDs'] = jobIDs
      return result

  def getJobFilesStatus(self,jobID):
    """Returns the surl, status and site information for a given jobID.
    """
    req = "SELECT LFN,SURL,Status,Site,Retry,SE from SiteFiles WHERE JobID = '%s';" % (jobID)
    result = self._query(req)
    if not result['OK']:
      return result
    lfnsDict = {}
    retryDict = {}
    siteName = ''
    seDict = {}
    for lfn,surl,status,site,retry,se in result['Value']:
      lfnsDict[lfn]={}
      lfnsDict[lfn].update({surl:status})
      siteName = site #always the same in the same job
      retryDict[lfn]=retry
      seDict[lfn]=se
    result = S_OK()
    result['Files'] = lfnsDict
    result['Site'] = siteName
    result['Retries'] = retryDict
    result['SE'] = seDict
    return result

  def getJobsForSystemAndState(self,state,source,limit):
    """Retrieves jobs with files in a given status for a particular system.
    """
    req = "SELECT JobID,SUM(Status!='%s'),SUM(Status='%s') FROM SiteFiles WHERE Source = '%s' GROUP BY JobID ORDER BY JobID LIMIT %d;" % (state,state,source,limit)
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      stateJobIDs = []
      for jobID,stateNo,stateYes in result['Value']:
        if stateNo == 0:
          stateJobIDs.append(jobID)
      result = S_OK()
      result['JobIDs'] = stateJobIDs
      return result

  def getJobsForRetry(self,retry,site):
    """
      Selects the unique JobIDs from the SiteFiles table where files have supplied retry count
    """
    req = "SELECT JobID,LFN FROM SiteFiles WHERE Site = '%s' AND Retry >= %d;" % (site,retry)
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      jobIDs = {}
      for jobid in result['Value']:
        jobID = jobid[0]
        lfn = jobid[1]
        if not jobIDs.has_key(jobID):
          jobIDs[jobID] = []
        jobIDs[jobID].append(lfn)
      result = S_OK()
      result['JobIDs'] = jobIDs
      return result

  def getLFNsForJob(self,jobid):
    """
      Selects all the LFNs associated with a given JobID from the SiteFiles table
    """
    req = "SELECT LFN FROM SiteFiles WHERE JobID = '%s';" % jobid
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      lfns = []
      for lfn in result['Value']:
        lfns.append(lfn[0])
      result = S_OK()
      result['LFNs'] = lfns
      return result

  def resetStageRequest(self,site,timeout):
    """
      Resets file status in the SiteFiles table for files in 'Submitted' state for timeout
    """
    req = "SELECT DATE_SUB(UTC_TIMESTAMP(), INTERVAL %d SECOND);" % timeout
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      if result['Value']:
        datetime = str(result['Value'][0][0])
        req = "UPDATE SiteFiles SET Status = 'New', Retry = Retry +1 WHERE Site = '%s' AND Status = 'Submitted' AND StageSubmit < '%s';" % (site,datetime)
        return self._update(req)
      else:
        errorStr = "resetStageRequest failed to obtain DATETIME"
        return S_ERROR(errorStr)

  def setJobsDone(self,jobids):
    """
      Deletes entries in the SiteFiles table for the supplied JobIDs.
    """
    str_jobids = []
    for jobid in jobids:
      str_jobids.append("'"+jobid+"'")
    str_jobid = string.join(str_jobids,",")
    req = "DELETE FROM SiteFiles WHERE JobID IN (%s);" % str_jobid
    return self._update(req)

  def getStageTimeForSystem(self,lfns,source):
    """
      This obtains the time taken to stage a file using the timestamps in the SiteFiles table.
    """
    str_lfns = []
    for lfn in lfns:
      str_lfns.append("'"+lfn+"'")
    str_lfn = string.join(str_lfns,",")
    req = "SELECT JobID,LFN,SEC_TO_TIME(StageComplete-StageSubmit) FROM SiteFiles WHERE Source = '%s' AND LFN IN (%s);" % (source,str_lfn)
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      timeDict = {}
      for jobid,lfn,stageTime in result['Value']:
        if not timeDict.has_key(jobid):
          timeDict[jobid] = {}
        timeDict[jobid][lfn] = toString(stageTime)
      result = S_OK()
      result['TimingDict'] = timeDict
      return result

  def getJobsForState(self,site,state,limit):
    """
      Gets the JobIDs where all the associated files are in the state supplied.
    """
    req = "SELECT JobID,SUM(Status!='%s'),SUM(Status='%s') FROM SiteFiles WHERE Site = '%s' GROUP BY JobID ORDER BY JobID LIMIT %d;" % (state,state,site,limit)
    result = self._query(req)
    if not result['OK']:
      return result
    else:
      stateJobIDs = []
      for jobID,stateNo,stateYes in result['Value']:
        if stateNo == 0:
          stateJobIDs.append(jobID)
      result = S_OK()
      result['JobIDs'] = stateJobIDs
      return result

  def setFilesState(self,lfns,site,state):
    """
      Updates the state of the supplied LFNs and where appropriate update timing information.
    """
    str_lfns = []
    for lfn in lfns:
      str_lfns.append("'"+lfn+"'")
    str_lfn = string.join(str_lfns,",")

    if state == 'Submitted':
      req = "UPDATE SiteFiles SET Status = '%s', StageSubmit = UTC_TIMESTAMP() WHERE Site = '%s' AND LFN IN (%s);" % (state,site,str_lfn)
      result = self._update(req)
    elif state == 'ToUpdate':
      req = "UPDATE SiteFiles SET Status = '%s', StageComplete = UTC_TIMESTAMP(), StageSubmit = UTC_TIMESTAMP() WHERE StageSubmit = '0000-00-00 00:00:00' AND Site = '%s' AND LFN IN (%s);" % (state,site,str_lfn)
      result = self._update(req)
      if not result['OK']:
        return result
      else:
        req = "UPDATE SiteFiles SET Status = '%s', StageComplete = UTC_TIMESTAMP() WHERE StageSubmit != '0000-00-00 00:00:00' AND Site = '%s' AND LFN IN (%s);" % (state,site,str_lfn)
        result = self._update(req)
    else:
      req = "UPDATE SiteFiles SET Status = '%s' WHERE Site = '%s' AND LFN IN (%s);" % (state,site,str_lfn)
      result = self._update(req)
    return result

  def getFilesForState(self,site,state,limit):
    """
      Gets the LFNs for a given state and returns them in order of their associated JobIDs.
    """
    if limit:
      req = "SELECT LFN,SURL,SE,JobID from SiteFiles WHERE Status = '%s' AND Site = '%s' ORDER BY JobID LIMIT %d;" % (state,site,limit)
    else:
      req = "SELECT LFN,SURL,SE,JobID from SiteFiles WHERE Status = '%s' AND Site = '%s' ORDER BY JobID;" % (state,site)
    result = self._query(req)
    if not result['OK']:
      return result
    lfnsDict = {}
    for lfn,surl,se,jobid in result['Value']:
      lfnsDict[lfn]={}
      lfnsDict[lfn].update({se:surl})
    result = S_OK()
    result['Files'] = lfnsDict
    return result

  def populateStageDB(self,jobid,files,source):
    """
       This method populates the SiteFiles table with file metadata for staging.
    """
    for site in files.keys():
      tuples = files[site]
      for lfn,surl,se in tuples:
        req = "SELECT * FROM SiteFiles WHERE LFN ='"+lfn+"' AND Site = '"+site+"' AND SURL = '"+surl+"' AND SE = '"+se+"' AND Source = '"+source+  "';"
        result = self._query(req)
        if result['OK']:
          existFlag = result['Value']
          if not existFlag:
            req = "INSERT INTO SiteFiles (LFN,Site,SURL,SE,JobID,Source) VALUES ('"+lfn+"','"+site+"','"+surl+"','"+se+"','"+jobid+"','"+source+"');"
            result = self._update(req)
            if not result['OK']:
              return result
    result = S_OK()
    return result
