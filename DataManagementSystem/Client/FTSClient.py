########################################################################
# File: FTSClient.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:29:43
########################################################################

"""
:mod: FTSClient

.. module: FTSClient

:synopsis: FTS client

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

FTS client
"""

# #
# @file FTSClient.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:29:47
# @brief Definition of FTSClient class.

# # imports
from DIRAC import gLogger, S_OK, S_ERROR

from DIRAC.Core.Base.Client               import Client
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob             import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile            import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView    import FTSHistoryView
from DIRAC.DataManagementSystem.private.FTSValidator      import FTSValidator
from DIRAC.DataManagementSystem.Client.DataManager        import DataManager
# # from Resources
from DIRAC.Resources.Storage.StorageFactory   import StorageFactory

import json

########################################################################
class FTSClient( Client ):
  """
  .. class:: FTSClient

  """

  def __init__( self, url = None, useCertificates = False, **kwargs ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    super( FTSClient, self ).__init__( **kwargs )
    self.log = gLogger.getSubLogger( "DataManagement/FTSClient" )
    self.serverURL = 'DataManagement/FTSManager' if not url else url

    # getting other clients
    self.ftsValidator = FTSValidator()
    self.dataManager = DataManager()
    self.storageFactory = StorageFactory()

  def getFTSFileList( self, statusList = None, limit = None ):
    """ get list of FTSFiles with status in statusList """
    statusList = statusList if statusList else [ "Waiting" ]
    limit = limit if limit else 1000
    getFTSFileList = self._getRPC().getFTSFileList( statusList, limit )
    if not getFTSFileList['OK']:
      self.log.error( "Failed getFTSFileList", "%s" % getFTSFileList['Message'] )
      return getFTSFileList
    getFTSFileList = getFTSFileList['Value']
    return S_OK( [ FTSFile( ftsFile ) for ftsFile in getFTSFileList ] )

  def getFTSJobList( self, statusList = None, limit = None ):
    """ get FTSJobs wit statues in :statusList: """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    limit = limit if limit else 500
    getFTSJobList = self._getRPC().getFTSJobList( statusList, limit )
    if not getFTSJobList['OK']:
      self.log.error( "Failed getFTSJobList", "%s" % getFTSJobList['Message'] )
      return getFTSJobList
    getFTSJobList = getFTSJobList['Value']
    return S_OK( [ FTSJob( ftsJobDict ) for ftsJobDict in getFTSJobList ] )

  def getFTSFilesForRequest( self, requestID, statusList = None ):
    """ read FTSFiles for a given :requestID:

    :param int requestID: ReqDB.Request.RequestID
    :param statusList: List of statuses (default: Waiting)
    :type statusList: python:list
    """
    ftsFiles = self._getRPC().getFTSFilesForRequest( requestID, statusList )
    if not ftsFiles['OK']:
      self.log.error( "Failed getFTSFilesForRequest", "%s" % ftsFiles['Message'] )
      return ftsFiles
    return S_OK( [ FTSFile( ftsFileDict ) for ftsFileDict in ftsFiles['Value'] ] )

  def getAllFTSFilesForRequest( self, requestID ):
    """ read FTSFiles for a given :requestID:

    :param int requestID: ReqDB.Request.RequestID
    """
    ftsFiles = self._getRPC().getAllFTSFilesForRequest( requestID )
    if not ftsFiles['OK']:
      self.log.error( "Failed getFTSFilesForRequest", "%s" % ftsFiles['Message'] )
      return ftsFiles
    return S_OK( [ FTSFile( ftsFileDict ) for ftsFileDict in ftsFiles['Value'] ] )

  def getFTSJobsForRequest( self, requestID, statusList = None ):
    """ get list of FTSJobs with statues in :statusList: given requestID

    :param int requestID: ReqDB.Request.RequestID
    :param statusList: list with FTSJob statuses
    :type statusList: python:list

    :return: [ FTSJob, FTSJob, ... ]
    """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    getJobs = self._getRPC().getFTSJobsForRequest( requestID, statusList )
    if not getJobs['OK']:
      self.log.error( "Failed getFTSJobsForRequest", "%s" % getJobs['Message'] )
      return getJobs
    return S_OK( [ FTSJob( ftsJobDict ) for ftsJobDict in getJobs['Value'] ] )

  def getFTSFile( self, ftsFileID = None ):
    """ get FTSFile

    :param int ftsFileID: FTSFileID
    """
    getFile = self._getRPC().getFTSFile( ftsFileID )
    if not getFile['OK']:
      self.log.error( 'Failed to get FTS file', getFile['Message'] )
    # # de-serialize
    if getFile['Value']:
      ftsFile = FTSFile( getFile['Value'] )
    return S_OK( ftsFile )

  def putFTSJob( self, ftsJob ):
    """ put FTSJob into FTSDB

    :param FTSJob.FTSJob ftsJob: FTSJob instance
    """
    ftsJobJSON = ftsJob.toJSON()
    if not ftsJobJSON['OK']:
      self.log.error( 'Failed to get JSON of an FTS job', ftsJobJSON['Message'] )
      return ftsJobJSON
    isValid = self.ftsValidator.validate( ftsJob )
    if not isValid['OK']:
      self.log.error( "Failed to validate FTS job", "%s %s" % ( isValid['Message'], str( ftsJobJSON['Value'] ) ) )
      return isValid
    return self._getRPC().putFTSJob( ftsJobJSON['Value'] )

  def getFTSJob( self, ftsJobID ):
    """ get FTS job, change its status to 'Assigned'

    :param int ftsJobID: FTSJobID
    """
    getJob = self._getRPC().getFTSJob( ftsJobID )
    if not getJob['OK']:
      self.log.error( 'Failed to get FTS job', getJob['Message'] )
      return getJob
    setStatus = self._getRPC().setFTSJobStatus( ftsJobID, 'Assigned' )
    if not setStatus['OK']:
      self.log.error( 'Failed to set status of FTS job', setStatus['Message'] )
    # # de-serialize
#    if getJob['Value']:
#      getJob = FTSJob( getJob['Value'] )
    return getJob

  def peekFTSJob( self, ftsJobID ):
    """ just peek FTSJob

    :param int ftsJobID: FTSJobID
    """
    getJob = self._getRPC().getFTSJob( ftsJobID )
    if not getJob['OK']:
      self.log.error( 'Failed to get FTS job', getJob['Message'] )
      return getJob
    return getJob

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob into FTSDB

    :param int ftsJob: FTSJobID
    """
    deleteJob = self._getRPC().deleteFTSJob( ftsJobID )
    if not deleteJob['OK']:
      self.log.error( 'Failed to delete FTS job', deleteJob['Message'] )
    return deleteJob

  def getFTSJobIDs( self, statusList = None ):
    """ get list of FTSJobIDs for a given status list """
    statusList = statusList if statusList else [ "Submitted", "Ready", "Active" ]
    ftsJobIDs = self._getRPC().getFTSJobIDs( statusList )
    if not ftsJobIDs['OK']:
      self.log.error( 'Failed to get FTS job IDs', ftsJobIDs['Message'] )
    return ftsJobIDs

  def getFTSFileIDs( self, statusList = None ):
    """ get list of FTSFileIDs for a given status list """
    statusList = statusList if statusList else [ "Waiting" ]
    ftsFileIDs = self._getRPC().getFTSFileIDs( statusList )
    if not ftsFileIDs['OK']:
      self.log.error( 'Failed to get FTS file IDs', ftsFileIDs['Message'] )
    return ftsFileIDs

  def getFTSHistory( self ):
    """ get FTS history snapshot """
    getFTSHistory = self._getRPC().getFTSHistory()
    if not getFTSHistory['OK']:
      self.log.error( 'Failed to get FTS history', getFTSHistory['Message'] )
      return getFTSHistory
    getFTSHistory = getFTSHistory['Value']
    return S_OK( [ FTSHistoryView( ftsHistory ) for ftsHistory in getFTSHistory ] )

  def getDBSummary( self ):
    """ get FTDB summary """
    dbSummary = self._getRPC().getDBSummary()
    if not dbSummary['OK']:
      self.log.error( "Failed getDBSummary", "%s" % dbSummary['Message'] )
    return dbSummary

  def setFTSFilesWaiting( self, operationID, sourceSE, opFileIDList = None ):
    """ update status for waiting FTSFiles from 'Waiting#SourceSE' to 'Waiting'

    :param int operationID: ReqDB.Operation.OperationID
    :param str sourceSE: source SE name
    :param opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    return self._getRPC().setFTSFilesWaiting( operationID, sourceSE, opFileIDList )

  def deleteFTSFiles( self, operationID, opFileIDList = None ):
    """ delete FTSFiles for rescheduling

    :param int operationID: ReqDB.Operation.OperationID
    :param opFileIDList: [ ReqDB.File.FileID, ... ]
    :type opFileIDList: python:list
    """
    return self._getRPC().deleteFTSFiles( operationID, opFileIDList )

  def ftsSchedule( self, requestID, operationID, opFileList ):
    """ schedule lfn for FTS job

    :param int requestID: RequestDB.Request.RequestID
    :param int operationID: RequestDB.Operation.OperationID
    :param opFileList: list of tuples ( File.toJSON()['Value'], sourcesList, targetList )
    :type opFileList: python:list
    """

    # Check whether there are duplicates
    fList = []
    for fileJSON, sourceSEs, targetSEs in opFileList:
      fTuple = ( json.loads( fileJSON ), sourceSEs, targetSEs )
      if fTuple not in fList:
        fList.append( fTuple )
      else:
        self.log.warn( 'File list for FTS scheduling has duplicates, fix it:\n', fTuple )
    fileIDs = [int( fileJSON.get( 'FileID', 0 ) ) for fileJSON, _sourceSEs, _targetSEs in fList ]
    res = self._getRPC().cleanUpFTSFiles( requestID, fileIDs )
    if not res['OK']:
      self.log.error( "Failed ftsSchedule", "%s" % res['Message'] )
      return S_ERROR( "ftsSchedule: %s" % res['Message'] )
    ftsFiles = []

    # # this will be returned on success
    result = { "Successful": [], "Failed": {} }

    for fileJSON, sourceSEs, targetSEs in fList:

      lfn = fileJSON.get( "LFN", "" )
      size = int( fileJSON.get( "Size", 0 ) )
      fileID = int( fileJSON.get( "FileID", 0 ) )
      opID = int( fileJSON.get( "OperationID", 0 ) )

      self.log.verbose( "ftsSchedule: LFN=%s FileID=%s OperationID=%s sources=%s targets=%s" % ( lfn, fileID, opID,
                                                                                                 sourceSEs,
                                                                                                 targetSEs ) )

      res = self.dataManager.getActiveReplicas( lfn )
      if not res['OK']:
        self.log.error( "Failed ftsSchedule", "%s" % res['Message'] )
        result["Failed"][fileID] = res['Message']
        continue
      replicaDict = res['Value']

      if lfn in replicaDict["Failed"] and lfn not in replicaDict["Successful"]:
        result["Failed"][fileID] = "no active replicas found"
        continue
      replicaDict = replicaDict["Successful"].get( lfn, {} )
      # # use valid replicas only
      validReplicasDict = dict( [ ( se, pfn ) for se, pfn in replicaDict.items() if se in sourceSEs ] )

      if not validReplicasDict:
        self.log.warn( "No active replicas found in sources" )
        result["Failed"][fileID] = "no active replicas found in sources"
        continue

      tree = self._getRPC().getReplicationTree( sourceSEs, targetSEs, size )
      if not tree['OK']:
        self.log.error( "Failed ftsSchedule", "%s cannot be scheduled: %s" % ( lfn, tree['Message'] ) )
        result["Failed"][fileID] = tree['Message']
        continue
      tree = tree['Value']

      self.log.verbose( "LFN=%s tree=%s" % ( lfn, tree ) )

      treeBranches = []
      printed = False
      for repDict in tree.values():
        if repDict in treeBranches:
          if not printed:
            self.log.warn( 'Duplicate tree branch', str( tree ) )
            printed = True
        else:
          treeBranches.append( repDict )

      for repDict in treeBranches:
        self.log.verbose( "Strategy=%s Ancestor=%s SourceSE=%s TargetSE=%s" % ( repDict["Strategy"],
                                                                                repDict["Ancestor"],
                                                                                repDict["SourceSE"],
                                                                                repDict["TargetSE"] ) )
        transferSURLs = self._getTransferURLs( lfn, repDict, sourceSEs, validReplicasDict )
        if not transferSURLs['OK']:
          result["Failed"][fileID] = transferSURLs['Message']
          continue

        sourceSURL, targetSURL, fileStatus = transferSURLs['Value']
        if sourceSURL == targetSURL:
          result["Failed"][fileID] = "sourceSURL equals to targetSURL for %s" % lfn
          continue

        self.log.verbose( "sourceURL=%s targetURL=%s FTSFile.Status=%s" % ( sourceSURL, targetSURL, fileStatus ) )

        ftsFile = FTSFile()
        for key in ( "LFN", "FileID", "OperationID", "Checksum", "ChecksumType", "Size" ):
          if fileJSON.get( key ):
            setattr( ftsFile, key, fileJSON.get( key ) )
        ftsFile.RequestID = requestID
        ftsFile.OperationID = operationID
        ftsFile.SourceSURL = sourceSURL
        ftsFile.TargetSURL = targetSURL
        ftsFile.SourceSE = repDict["SourceSE"]
        ftsFile.TargetSE = repDict["TargetSE"]
        ftsFile.Status = fileStatus
        ftsFiles.append( ftsFile )

    if not ftsFiles:
      self.log.info( "ftsSchedule: no FTSFiles to put for request %d" % requestID )
      return S_OK( result )

    ftsFilesJSONList = [ftsFile.toJSON()['Value'] for ftsFile in ftsFiles]
    res = self._getRPC().putFTSFileList( ftsFilesJSONList )
    if not res['OK']:
      self.log.error( "Failed ftsSchedule", "%s" % res['Message'] )
      return S_ERROR( "ftsSchedule: %s" % res['Message'] )

    result['Successful'] += [ fileID for fileID in fileIDs if fileID not in result['Failed']]

    # # if we land here some files have been properly scheduled
    return S_OK( result )

  ################################################################################################################
  # Some utilities function

  def _getSurlForLFN( self, targetSE, lfn ):
    """ Get the targetSURL for the storage and LFN supplied.

    :param self: self reference
    :param str targetSE: target SE
    :param str lfn: LFN
    """
    res = StorageFactory().getStorages( targetSE, pluginList = ["SRM2", "GFAL2_SRM2"] )
    if not res['OK']:
      errStr = "_getSurlForLFN: Failed to create SRM2 storage for %s: %s" % ( targetSE, res['Message'] )
      self.log.error( "_getSurlForLFN: Failed to create SRM2 storage",
                      "%s: %s" % ( targetSE, res['Message'] ) )
      return S_ERROR( errStr )
    storageObjects = res['Value']["StorageObjects"]
    for storageObject in storageObjects:
      res = storageObject.getCurrentURL( lfn )
      if res['OK']:
        return res
    self.log.error( "_getSurlForLFN: Failed to get SRM compliant storage.", targetSE )
    return S_ERROR( "_getSurlForLFN: Failed to get SRM compliant storage." )

  def _getTransferURLs( self, lfn, repDict, replicas, replicaDict ):
    """ prepare TURLs for given LFN and replication tree

    :param self: self reference
    :param str lfn: LFN
    :param dict repDict: replication dictionary
    :param dict replicas: LFN replicas
    """
    hopSourceSE = repDict["SourceSE"]
    hopTargetSE = repDict["TargetSE"]
    hopAncestor = repDict["Ancestor"]

    # # get targetSURL
    res = self._getSurlForLFN( hopTargetSE, lfn )
    if not res['OK']:
      self.log.error( "Failed _getTransferURLs", "%s" % res['Message'] )
      return res
    targetSURL = res['Value']

    status = "Waiting"

    # # get the sourceSURL
    if hopAncestor:
      status = "Waiting#%s" % ( hopAncestor )
    res = self._getSurlForLFN( hopSourceSE, lfn )
    sourceSURL = res.get( 'Value', replicaDict.get( hopSourceSE, None ) )
    if not sourceSURL:
      self.log.error( "Failed _getTransferURLs", "%s" % res['Message'] )
      return res

    return S_OK( ( sourceSURL, targetSURL, status ) )

