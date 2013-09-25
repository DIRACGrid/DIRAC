########################################################################
# $HeadURL $
# File: TransferDBMonitoringHandler.py
########################################################################

""" :mod: TransferDBMonitoringHandler
    =================================

    .. module: TransferDBMonitoringHandler
    :synopsis: Implementation of the TransferDB monitoring service in the DISET framework.

    :deprecated:

"""

__RCSID__ = "$Id"

# # imports
from types import IntType, StringType, DictType, ListType
# # fro DIARC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

# These are global instances of the DB classes
gTransferDB = False

# this should also select the SourceSite,DestinationSite
SUMMARY = [ 'Status',
            'NumberOfFiles',
            'PercentageComplete',
            'TotalSize',
            'SubmitTime',
            'LastMonitor' ]

REQUEST_COLS = [ 'RequestID',
                 'RequestName',
                 'JobID',
                 'OwnerDN',
                 'DIRACInstance',
                 'Status',
                 'CreationTime',
                 'SubmissionTime' ]

SUBREQUEST_COLS = [ 'RequestID',
                    'SubRequestID',
                    'RequestType',
                    'Status',
                    'Operation',
                    'SourceSE',
                    'TargetSE',
                    'Catalogue',
                    'SubmissionTime',
                    'LastUpdate' ]

FILES_COLS = [ 'SubRequestID',
               'FileID',
               'LFN',
               'Size',
               'PFN',
               'GUID',
               'Md5',
               'Addler',
               'Attempt',
               'Status' ]

DATASET_COLS = [ 'SubRequestID',
                 'Dataset',
                 'Status' ]

def initializeTransferDBMonitoringHandler( serviceInfo ):
  """ handler initialization

  :param tuple serviceInfo: service info
  """
  global gTransferDB
  gTransferDB = TransferDB()
  return S_OK()

class TransferDBMonitoringHandler( RequestHandler ):
  """
  .. class:: TransferDBMonitoringHandler
  """

  types_getChannels = []
  @staticmethod
  def export_getChannels():
    """ Get the details of the channels """
    return gTransferDB.getChannels()

  types_increaseChannelFiles = [ IntType ]
  @staticmethod
  def export_increaseChannelFiles( channelID ):
    """ Increase the number of files on a channel """
    return gTransferDB.increaseChannelFiles( channelID )

  types_decreaseChannelFiles = [ IntType ]
  @staticmethod
  def export_decreaseChannelFiles( channelID ):
    """ Decrease the numner of files on a channel """
    return gTransferDB.decreaseChannelFiles( channelID )

  types_getSites = []
  @staticmethod
  def export_getSites():
    """ Get the details of the sites """
    return gTransferDB.getSites()

  types_getFTSInfo = [ IntType ]
  @staticmethod
  def export_getFTSInfo( ftsReqID ):
    """ Get the details of a particular FTS job """
    return gTransferDB.getFTSJobDetail( ftsReqID )

  types_getFTSJobs = []
  @staticmethod
  def export_getFTSJobs():
    """ Get all the FTS jobs from the DB """
    return gTransferDB.getFTSJobs()

  types_getChannelObservedThroughput = [ IntType ]
  @staticmethod
  def export_getChannelObservedThroughput( interval ):
    """ Get the observed throughput on the channels defined """
    return gTransferDB.getChannelObservedThroughput( interval )

  types_getChannelQueues = [ StringType]
  @staticmethod
  def export_getChannelQueues( status ):
    """ Get the channel queues """
    return gTransferDB.getChannelQueues( status )

  types_getCountFileToFTS = [ IntType, StringType ]
  @staticmethod
  def export_getCountFileToFTS( interval, status ):
    """ Get the count of distinct failed files in FileToFTS per channel """
    return gTransferDB.getCountFileToFTS( interval, status )

  ##############################################################################
  types_getReqPageSummary = [ DictType, StringType, IntType, IntType ]
  @staticmethod
  def export_getReqPageSummary( attrDict, orderAttribute, pageNumber, numberPerPage ):
    """ Get the summary of the fts req information for a given page in the fts monitor
    """
    last_update = None
    if attrDict.has_key( 'LastUpdate' ):
      last_update = attrDict['LastUpdate']
      del attrDict['LastUpdate']
    res = gTransferDB.selectFTSReqs( attrDict, orderAttribute = orderAttribute, newer = last_update )
    if not res['OK']:
      return S_ERROR( 'Failed to select FTS requests: ' + res['Message'] )

    ftsReqList = res['Value']
    nFTSReqs = len( ftsReqList )
    if nFTSReqs == 0:
      resDict = {'TotalFTSReq':nFTSReqs}
      return S_OK( resDict )
    iniReq = pageNumber * numberPerPage
    lastReq = iniReq + numberPerPage
    if iniReq >= nFTSReqs:
      return S_ERROR( 'Page number out of range' )
    if lastReq > nFTSReqs:
      lastReq = nFTSReqs

    summaryReqList = ftsReqList[iniReq:lastReq]
    res = gTransferDB.getAttributesForReqList( summaryReqList, SUMMARY )
    if not res['OK']:
      return S_ERROR( 'Failed to get request summary: ' + res['Message'] )
    summaryDict = res['Value']

    resDict = {}
    resDict['TotalFTSReq'] = nFTSReqs
    resDict['SummaryDict'] = summaryDict
    return S_OK( resDict )


######################################################################################
######################################################################################



  types_getRequestPageSummaryWeb = [DictType, ListType, IntType, IntType]
  @staticmethod
  def export_getRequestPageSummaryWeb( selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the request information for a given page in the
        request monitor in a generic format
    """
    resultDict = {}
    last_update = None
    if selectDict.has_key( 'LastUpdate' ):
      last_update = selectDict['LastUpdate']
      del selectDict['LastUpdate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    result = gTransferDB.selectRequests( selectDict, orderAttribute = orderAttribute, newer = last_update )
    if not result['OK']:
      return S_ERROR( 'Failed to select jobs: ' + result['Message'] )

    requestList = result['Value']
    nRequests = len( requestList )
    resultDict['TotalRecords'] = nRequests
    if nRequests == 0:
      return S_OK( resultDict )

    iniRequest = startItem
    lastRequest = iniRequest + maxItems
    if iniRequest >= nRequests:
      return S_ERROR( 'Item number out of range' )

    if lastRequest > nRequests:
      lastRequest = nRequests

    summaryRequestList = requestList[iniRequest:lastRequest]
    result = gTransferDB.getAttributesForRequestList( summaryRequestList, REQUEST_COLS )
    if not result['OK']:
      return S_ERROR( 'Failed to get request summary: ' + result['Message'] )

    summaryDict = result['Value']

    # prepare the standard structure now
    key = summaryDict.keys()[0]
    paramNames = summaryDict[key].keys()

    records = []
    for requestDict in summaryDict.values():
      rParList = []
      for pname in paramNames:
        rParList.append( requestDict[pname] )
      records.append( rParList )

    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records
    return S_OK( resultDict )



  types_getSubRequestPageSummaryWeb = [DictType, ListType, IntType, IntType]
  @staticmethod
  def export_getSubRequestPageSummaryWeb( selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the request information for a given page in the
        request monitor in a generic format
    """
    resultDict = {}
    last_update = None
    if selectDict.has_key( 'LastUpdate' ):
      last_update = selectDict['LastUpdate']
      del selectDict['LastUpdate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    result = gTransferDB.selectSubRequests( selectDict, orderAttribute = orderAttribute, newer = last_update )
    if not result['OK']:
      return S_ERROR( 'Failed to select jobs: ' + result['Message'] )

    requestList = result['Value']
    nRequests = len( requestList )
    resultDict['TotalRecords'] = nRequests
    if nRequests == 0:
      return S_OK( resultDict )

    iniRequest = startItem
    lastRequest = iniRequest + maxItems
    if iniRequest >= nRequests:
      return S_ERROR( 'Item number out of range' )

    if lastRequest > nRequests:
      lastRequest = nRequests

    summaryRequestList = requestList[iniRequest:lastRequest]
    result = gTransferDB.getAttributesForSubRequestList( summaryRequestList, SUBREQUEST_COLS )
    if not result['OK']:
      return S_ERROR( 'Failed to get request summary: ' + result['Message'] )

    summaryDict = result['Value']

    # prepare the standard structure now
    key = summaryDict.keys()[0]
    paramNames = summaryDict[key].keys()

    records = []
    for requestDict in summaryDict.values():
      rParList = []
      for pname in paramNames:
        rParList.append( requestDict[pname] )
      records.append( rParList )

    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records
    return S_OK( resultDict )


  types_getFilesPageSummaryWeb = [DictType, ListType, IntType, IntType]
  @staticmethod
  def export_getFilesPageSummaryWeb( selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the request information for a given page in the
        request monitor in a generic format
    """
    resultDict = {}
    last_update = None
    if selectDict.has_key( 'LastUpdate' ):
      last_update = selectDict['LastUpdate']
      del selectDict['LastUpdate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    result = gTransferDB.selectFiles( selectDict, orderAttribute = orderAttribute, newer = last_update )
    if not result['OK']:
      return S_ERROR( 'Failed to select jobs: ' + result['Message'] )

    requestList = result['Value']
    nRequests = len( requestList )
    resultDict['TotalRecords'] = nRequests
    if nRequests == 0:
      return S_OK( resultDict )

    iniRequest = startItem
    lastRequest = iniRequest + maxItems
    if iniRequest >= nRequests:
      return S_ERROR( 'Item number out of range' )

    if lastRequest > nRequests:
      lastRequest = nRequests
    summaryRequestList = requestList[iniRequest:lastRequest]
    result = gTransferDB.getAttributesForFilesList( summaryRequestList, FILES_COLS )
    if not result['OK']:
      return S_ERROR( 'Failed to get request summary: ' + result['Message'] )

    summaryDict = result['Value']

    # prepare the standard structure now
    key = summaryDict.keys()[0]
    paramNames = summaryDict[key].keys()

    records = []
    for requestDict in summaryDict.values():
      rParList = []
      for pname in paramNames:
        rParList.append( requestDict[pname] )
      records.append( rParList )

    resultDict['ParameterNames'] = paramNames
    resultDict['Records'] = records
    return S_OK( resultDict )

  ########################################################################
  #
  # Sub Request monitor methods
  #
  types_getSubRequestStatuses = []
  @staticmethod
  def export_getSubRequestStatuses():
    """ get distict sub-request's statuses """
    return gTransferDB.getDistinctSubRequestAttributes( 'Status' )

  types_getSubRequestTypes = []
  @staticmethod
  def export_getSubRequestTypes():
    """ get distinct sub-request's types"""
    return gTransferDB.getDistinctSubRequestAttributes( 'RequestType' )

  types_getSubRequestOperations = []
  @staticmethod
  def export_getSubRequestOperations():
    """ get distinct sub-request's operations """
    return gTransferDB.getDistinctSubRequestAttributes( 'Operation' )

  types_getSubRequestSourceSEs = []
  @staticmethod
  def export_getSubRequestSourceSEs():
    """ get distinct sub-request's source SE """
    return gTransferDB.getDistinctSubRequestAttributes( 'SourceSE' )

  types_getSubRequestTargetSEs = []
  @staticmethod
  def export_getSubRequestTargetSEs():
    """ get distinct sub-request's target SE """
    return gTransferDB.getDistinctSubRequestAttributes( 'TargetSE' )

  ########################################################################
  #
  # Request monitor methods
  #
  types_getRequestStatuses = []
  @staticmethod
  def export_getRequestStatuses():
    """ get disticnt request's statuses """
    return gTransferDB.getDistinctRequestAttributes( 'Status' )

  ########################################################################
  #
  # File monitor methods
  #
  types_getFilesStatuses = []
  @staticmethod
  def export_getFilesStatuses():
    """ get distinct file's statuses """
    return gTransferDB.getDistinctFilesAttributes( 'Status' )

  ########################################################################
  #
  # Channels monitor methods
  #
  types_getChannelSources = []
  @staticmethod
  def export_getChannelSources():
    """ get distinct channel's sources"""
    return gTransferDB.getDistinctChannelsAttributes( 'SourceSite' )

  types_getChannelDestinations = []
  @staticmethod
  def export_getChannelDestinations():
    """ get distinct channel's destinations """
    return gTransferDB.getDistinctChannelsAttributes( 'DestinationSite' )

  types_getChannelStatus = []
  @staticmethod
  def export_getChannelStatus():
    """ get distinct channel's statuses """
    return gTransferDB.getDistinctChannelsAttributes( 'Status' )

  types_getFilesForChannel = [IntType, IntType, StringType, StringType, StringType]
  @staticmethod
  def export_getFilesForChannel( channelID, nFiles, status, sourceSE, targetSE ):
    """ get files for a given channel and a given status"""
    return gTransferDB.getFilesForChannel( channelID, nFiles, status, sourceSE, targetSE )

  types_resetFileChannelStatus = [IntType, ListType]
  @staticmethod
  def export_resetFileChannelStatus( channelID, fileIDs ):
    """ reset files for a given channel """
    return gTransferDB.resetFileChannelStatus( channelID, fileIDs )
