########################################################################
# $Id: StagerClient.py,v 1.3 2008/03/31 16:15:31 paterson Exp $
########################################################################

"""Set of utilities and classes to handle Stager Database"""

from types import *
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

class StagerClient:

  def __init__(self,useCerts=False):
    """ Constructor of the StagerDBClient class
    """
    self.server = RPCClient('Stager/Stager',useCertificates=useCerts)

  def stageFiles(self,jobid,site,replicas,source):
    try:
      result = self.server.stageFiles(jobid,site,replicas,source)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.stageFiles failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def getFilesForState(self,site,state,limit=0):
    try:
      result = self.server.getFilesForState(site,state,limit)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getFilesForState failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def setFilesState(self,lfns,site,state):
    try:
      result = self.server.setFilesState(lfns,site,state)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.setFilesState failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def getJobsForState(self,site,state,limit=0):
    try:
      result = self.server.getJobsForState(site,state,limit)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getJobsForState failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def getStageTimeAtSite(self,lfns,site):
    try:
      result = self.server.getStageTimeAtSite(lfns,site)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getStageTimeAtSite failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def setJobsDone(self,jobIDs):
    try:
      result = self.server.setJobsDone(jobIDs)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.setJobsDone failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def resetStageRequest(self,site,timeout):
    try:
      result = self.server.resetStageRequest(site,timeout)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.resetStageRequest failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def getLFNsForJob(self,jobid):
    try:
      result = self.server.getLFNsForJob(jobid)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getLFNsForJob failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def getJobsForRetry(self,retry,site):
    try:
      result = self.server.getJobsForRetry(retry,site)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getJobsForRetry failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def getAllJobs(self,site):
    try:
      result = self.server.getAllJobs(site)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getAllJobs failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))

  def setTiming(self,site,cmd,time,files):
    try:
      result = self.server.setTiming(site,cmd,time,files)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.setTiming failed"
      gLogger.exception(errorStr,x)
      return S_ERROR(errorStr+": "+str(x))
