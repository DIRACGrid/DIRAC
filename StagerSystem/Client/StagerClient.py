########################################################################
# $Id: StagerClient.py,v 1.13 2008/11/15 22:49:19 acsmith Exp $
########################################################################

"""Set of utilities and classes to handle Stager Database"""

from types import *
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

class StagerClient:

  def __init__(self,useCerts=False):
    """ Constructor of the StagerDBClient class
    """
    self.useCerts = useCerts

  def stageFiles(self,jobid,site,replicas,source):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.stageFiles(jobid,site,replicas,source)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.stageFiles failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getJobFilesStatus(self,jobID):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getJobFilesStatus(jobID)
      return result
    except Exception,x:
      errorStr = "StagerDBClient.getJobFilesStatus failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getJobsForSystemAndState(self,state,source,limit=0):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getJobsForSystemAndState(state,source,limit)
      return result
    except Exception,x:
      errorStr = "StagerDBClient.getJobsForSystemAndState failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getFilesForState(self,site,state,limit=0):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getFilesForState(site,state,limit)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getFilesForState failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def setFilesState(self,lfns,site,state):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.setFilesState(lfns,site,state)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.setFilesState failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getJobsForState(self,site,state,limit=0):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getJobsForState(site,state,limit)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getJobsForState failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getStageTimeForSystem(self,lfns,source):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getStageTimeForSystem(lfns,source)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getStageTimeForSystem failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def setJobsDone(self,jobIDs):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.setJobsDone(jobIDs)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.setJobsDone failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def resetStageRequest(self,site,timeout):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.resetStageRequest(site,timeout)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.resetStageRequest failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getLFNsForJob(self,jobid):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getLFNsForJob(jobid)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getLFNsForJob failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getJobsForRetry(self,retry,site):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getJobsForRetry(retry,site)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getJobsForRetry failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getAllJobs(self,source):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getAllJobs(source)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getAllJobs failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def setTiming(self,site,cmd,time,files):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.setTiming(site,cmd,time,files)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.setTiming failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))

  def getStageSubmissionTiming(self,lfns,site):
    try:
      server = RPCClient('Stager/Stager',useCertificates=self.useCerts,timeout=120)
      result = server.getStageSubmissionTiming(lfns,site)
      return result
    except Exception, x:
      errorStr = "StagerDBClient.getStageSubmissionTiming failed"
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr+": "+str(x))
