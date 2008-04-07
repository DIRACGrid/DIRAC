########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Service/StagerHandler.py,v 1.7 2008/04/07 15:12:03 paterson Exp $
########################################################################

""" StagerHandler is the implementation of the StagerDB in the DISET framework
    A.Smith (17/05/07)
"""

__RCSID__ = "$Id: StagerHandler.py,v 1.7 2008/04/07 15:12:03 paterson Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.StagerSystem.DB.StagerDB import StagerDB

# This is a global instance of the StagerDB class
stagerDB = False

def initializeStagerHandler(serviceInfo):
  global stagerDB
  stagerDB = StagerDB()
  return S_OK()

class StagerHandler(RequestHandler):

  types_setTiming = [StringType,StringType,FloatType,IntType]
  def export_setTiming(self,site,cmd,time,files):
    """
       This method stuffs timing information into the DB
    """
    try:
      result = stagerDB.setTiming(site,cmd,time,files)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.setTiming failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getAllJobs = [StringType]
  def export_getAllJobs(self,source):
    """
       This method selects all the jobs for a given system source
    """
    try:
      result = stagerDB.getAllJobs(source)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getAllJobs failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getJobsForSystemAndState = [StringType,StringType,IntType]
  def export_getJobsForSystemAndState(self,state,source,limit):
    """Allows to retrieve a list of jobs for a particular system and status.
    """
    try:
      result = stagerDB.getJobsForSystemAndState(state,source,limit)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getJobsForSystemAndState failed"+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getJobFilesStatus = [StringType]
  def export_getJobFilesStatus(self,jobID):
    """Allows to retrieve the job file status.
    """
    try:
      result = stagerDB.getJobFilesStatus(jobID)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getJobFilesStatus failed"+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getJobsForRetry = [IntType,StringType]
  def export_getJobsForRetry(self,retry,site):
    """
       This method selects the jobs where one of the files has failed to stage after 'retry' attempts
    """
    try:
      result = stagerDB.getJobsForRetry(retry,site)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getJobsForRetry failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getLFNsForJob = [StringType]
  def export_getLFNsForJob(self,jobid):
    """
       This method selects the files associated to the given jobID at the site and returns a list of lfns
    """
    try:
      result = stagerDB.getLFNsForJob(jobid)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getLFNsForJob failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_resetStageRequest = [StringType,IntType]
  def export_resetStageRequest(self,site,timeout):
    """
       This method will set the status of files, in 'Submitted' state longer than 'timeout', back to state 'New'
    """
    try:
      result = stagerDB.resetStageRequest(site,timeout)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.resetStageRequest failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_setJobsDone = [ListType]
  def export_setJobsDone(self,jobids):
    """
       This method updates the state of the job in the JobFiles table.
       It assumes that all files are done and that the data will no longer be accessed.
    """
    try:
      result = stagerDB.setJobsDone(jobids)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.setJobsDone failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getStageTimeForSystem = [ListType,StringType]
  def export_getStageTimeForSystem(self,lfns,source):
    """
       This method returns the stage time for files. It is assumed the file is staged
    """
    try:
      result = stagerDB.getStageTimeForSystem(lfns,source)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getStageTimeForSystem failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getJobsForState = [StringType,StringType]
  def export_getJobsForState(self,site,state,limit):
    """
       This method with get the jobs for which all the files associated have the given state
    """
    try:
      result = stagerDB.getJobsForState(site,state,limit)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getJobsForState failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_setFilesState = [ListType,StringType,StringType]
  def export_setFilesState(self,lfns,site,state):
    """
       This method updates the status of the files given for site to state provided
    """
    try:
      result = stagerDB.setFilesState(lfns,site,state)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.setFilesState failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_getFilesForState = [StringType,StringType]
  def export_getFilesForState(self,site,state,limit):
    """
       This method obtains the files from SiteFiles table for the given site
    """
    try:
      result = stagerDB.getFilesForState(site,state,limit)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getFilesForState failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)

  types_stageFiles = [StringType,StringType,DictType,StringType]
  def export_stageFiles(self,jobid,site,replicas,source):
    """
       This method does the population of the files to the StagerDB
    """
    try:
      if not jobid or not site or not replicas or not source:
        err = 'Missing required parameters'
        print err
        return S_ERROR(err)

      files = {}
      files[site] = []
      for lfn,reps in replicas.items():
        for se,surl in reps.items():
          files[site].append((lfn,surl,se))

      result = stagerDB.populateStageDB(jobid,files,source)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.stageFiles failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)
      
  types_getPageSummary = [IntType, IntType, StringType]
  def export_getPageSummary(self, pageNumber, numberPerPage, site):
    """ Get the summary of the DB information for a given
        number of pages and items per page.
    """
    try:
      result = stagerDB.getPageSummary(pageNumber,numberPerPage,site)
      return result
    except Exception,x:
      errorStr = "StagerDBHandler.getPageSummary failed "+str(x)
      print errorStr
      return S_ERROR(errorStr)  
