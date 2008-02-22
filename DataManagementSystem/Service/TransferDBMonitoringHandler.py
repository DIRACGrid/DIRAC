""" TransferDBMonitoringHandler is the implementation of the TransferDB monitoring service in the DISET framework
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

# These are global instances of the DB classes
transferDB = False
#this should also select the SourceSite,DestinationSite
SUMMARY = ['Status','NumberOfFiles','PercentageComplete','TotalSize','SubmitTime','LastMonitor'] 

def initializeTransferDBMonitoringHandler(serviceInfo):

  global transferDB
  transferDB = TransferDB()
  return S_OK()

class TransferDBMonitoringHandler(RequestHandler):

  types_getSites = []
  def export_getSites(self):
    """ Get the details of the sites 
    """ 
    return transferDB.getSites()

  types_getFTSInfo = [IntType]
  def export_getFTSInfo(self,ftsReqID):
   """ Get the details of a particular FTS job
   """
   return transferDB.getFTSJobDetail(ftsReqID) 

  types_getFTSJobs = []
  def export_getFTSJobs(self):
    """ Get all the FTS jobs from the DB
    """
    return transferDB.getFTSJobs()

##############################################################################
  types_getReqPageSummary = [DictType, StringType, IntType, IntType]
  def export_getReqPageSummary(self, attrDict, orderAttribute, pageNumber, numberPerPage):
    """ Get the summary of the fts req information for a given page in the fts monitor
    """
    last_update = None  
    if attrDict.has_key('LastUpdate'):
      last_update = attrDict['LastUpdate']
      del attrDict['LastUpdate'] 
    res = transferDB.selectFTSReqs(attrDict, orderAttribute=orderAttribute, newer=last_update)
    if not res['OK']:
      return S_ERROR('Failed to select FTS requests: '+res['Message'])
    
    ftsReqList = res['Value']
    nFTSReqs = len(ftsReqList)
    if nFTSReqs == 0:
      resDict = {'TotalFTSReq':nFTSReqs}
      return S_OK(resDict)
    iniReq = pageNumber*numberPerPage
    lastReq = iniReq+numberPerPage
    if iniReq >= nFTSReqs:
      return S_ERROR('Page number out of range')
    if lastReq > nFTSReqs:
      lastReq = nFTSReqs

    summaryReqList = ftsReqList[iniReq:lastReq]
    res = transferDB.getAttributesForReqList(summaryReqList,SUMMARY)
    if not res['OK']:
      return S_ERROR('Failed to get request summary: '+res['Message'])
    summaryDict = res['Value']

    resDict = {}
    resDict['TotalFTSReq'] = nFTSReqs
    resDict['SummaryDict'] = summaryDict      
    return S_OK(resDict)

