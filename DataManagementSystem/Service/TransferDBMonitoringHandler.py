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


RequestsColumns = ['RequestID','RequestName','JobID','OwnerDN','DIRACInstance','Status','CreationTime','SubmissionTime']
SubRequestsColumns = ['RequestID','SubRequestID','RequestType','Status','Operation','SourceSE','TargetSE','Catalogue','SubmissionTime','LastUpdate']
FilesColumns = ['SubRequestID','FileID','LFN','Size','PFN','GUID','Md5','Addler','Attempt','Status']
DatasetColumns = ['SubRequestID','Dataset','Status']
 
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


######################################################################################
######################################################################################



  types_getRequestPageSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getRequestPageSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the request information for a given page in the
        request monitor in a generic format
    """
    resultDict = {}
    last_update = None
    if selectDict.has_key('LastUpdate'):
      last_update = selectDict['LastUpdate']
      del selectDict['LastUpdate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None

    result = transferDB.selectRequests(selectDict, orderAttribute=orderAttribute, newer=last_update)
    if not result['OK']:
      return S_ERROR('Failed to select jobs: '+result['Message'])

    requestList = result['Value']
    nRequests = len(requestList)
    resultDict['TotalRecords'] = nRequests
    if nRequests == 0:
      return S_OK(resultDict)

    iniRequest = startItem
    lastRequest = iniRequest + maxItems
    if iniRequest >= nRequests:
      return S_ERROR('Item number out of range')

    if lastRequest > nRequests:
      lastRequests = nRequests

    summaryRequestList = requestList[iniRequest:lastRequest]
    result = transferDB.getAttributesForRequestList(summaryRequestList,RequestsColumns)
    if not result['OK']:
      return S_ERROR('Failed to get request summary: '+result['Message'])

    summaryDict = result['Value']

    # prepare the standard structure now
    key = summaryDict.keys()[0]
    paramNames = summaryDict[key].keys()

    records = []
    for requestID, requestDict in summaryDict.items():
      rParList = []
      for pname in paramNames:
        rParList.append(requestDict[pname])
      records.append(rParList)

    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records
    return S_OK(resultDict)


    
  types_getSubRequestPageSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getSubRequestPageSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the request information for a given page in the
        request monitor in a generic format
    """
    resultDict = {}
    last_update = None
    if selectDict.has_key('LastUpdate'):
      last_update = selectDict['LastUpdate']
      del selectDict['LastUpdate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None
    
    result = transferDB.selectSubRequests(selectDict, orderAttribute=orderAttribute, newer=last_update)
    if not result['OK']:
      return S_ERROR('Failed to select jobs: '+result['Message'])

    requestList = result['Value']
    nRequests = len(requestList)
    resultDict['TotalRecords'] = nRequests
    if nRequests == 0:
      return S_OK(resultDict) 
        
    iniRequest = startItem
    lastRequest = iniRequest + maxItems
    if iniRequest >= nRequests:
      return S_ERROR('Item number out of range')

    if lastRequest > nRequests:
      lastRequests = nRequests

    summaryRequestList = requestList[iniRequest:lastRequest]
    result = transferDB.getAttributesForSubRequestList(summaryRequestList,SubRequestsColumns)
    if not result['OK']:
      return S_ERROR('Failed to get request summary: '+result['Message'])
    
    summaryDict = result['Value']

    # prepare the standard structure now
    key = summaryDict.keys()[0]
    paramNames = summaryDict[key].keys()

    records = []
    for requestID, requestDict in summaryDict.items():  
      rParList = []
      for pname in paramNames:
        rParList.append(requestDict[pname])
      records.append(rParList)

    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records
    return S_OK(resultDict)


  types_getFilesPageSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getFilesPageSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the request information for a given page in the
        request monitor in a generic format
    """
    resultDict = {}
    last_update = None
    if selectDict.has_key('LastUpdate'):
      last_update = selectDict['LastUpdate']
      del selectDict['LastUpdate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None
    
    result = transferDB.selectFiles(selectDict, orderAttribute=orderAttribute, newer=last_update)
    if not result['OK']:
      return S_ERROR('Failed to select jobs: '+result['Message'])

    requestList = result['Value']
    nRequests = len(requestList)
    resultDict['TotalRecords'] = nRequests
    if nRequests == 0:
      return S_OK(resultDict) 
        
    iniRequest = startItem
    lastRequest = iniRequest + maxItems
    if iniRequest >= nRequests:
      return S_ERROR('Item number out of range')

    if lastRequest > nRequests:
      lastRequests = nRequests
    summaryRequestList = requestList[iniRequest:lastRequest]
    result = transferDB.getAttributesForFilesList(summaryRequestList,FilesColumns)
    if not result['OK']:
      return S_ERROR('Failed to get request summary: '+result['Message'])
    
    summaryDict = result['Value']
    
    # prepare the standard structure now
    key = summaryDict.keys()[0]   
    paramNames = summaryDict[key].keys()
    
    records = []
    for requestID, requestDict in summaryDict.items():
      rParList = []
      for pname in paramNames:
        rParList.append(requestDict[pname])
      records.append(rParList)
      
    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records
    return S_OK(resultDict)

