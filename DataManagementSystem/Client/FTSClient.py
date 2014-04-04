########################################################################
# File: FTSClient.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:29:43
########################################################################

""" :mod: FTSClient
    ===============

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

from DIRAC.Core.DISET.RPCClient           import RPCClient
from DIRAC.ConfigurationSystem.Client     import PathFinder
from DIRAC.Core.Base.Client               import Client
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob             import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile            import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView    import FTSHistoryView
from DIRAC.DataManagementSystem.private.FTSValidator      import FTSValidator
from DIRAC.DataManagementSystem.Client.DataManager     import DataManager
# # from Resources
from DIRAC.Resources.Storage.StorageFactory   import StorageFactory

########################################################################
class FTSClient( Client ):
  """
  .. class:: FTSClient

  """

  def __init__( self, useCertificates = False ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self )
    self.log = gLogger.getSubLogger( "DataManagement/FTSClient" )
    self.setServer( "DataManagement/FTSManager" )

    # getting other clients
    self.ftsValidator = FTSValidator()
    self.dataManager = DataManager()
    self.storageFactory = StorageFactory()

    url = PathFinder.getServiceURL( "DataManagement/FTSManager" )
    if not url:
      raise RuntimeError( "CS option DataManagement/FTSManager URL is not set!" )
    self.ftsManager = RPCClient( url )

  def getFTSFileList( self, statusList = None, limit = None ):
    """ get list of FTSFiles with status in statusList """
    statusList = statusList if statusList else [ "Waiting" ]
    limit = limit if limit else 1000
    getFTSFileList = self.ftsManager.getFTSFileList( statusList, limit )
    if not getFTSFileList['OK']:
      self.log.error( "getFTSFileList: %s" % getFTSFileList['Message'] )
      return getFTSFileList
    getFTSFileList = getFTSFileList['Value']
    return S_OK( [ FTSFile( ftsFile ) for ftsFile in getFTSFileList ] )

  def getFTSJobList( self, statusList = None, limit = None ):
    """ get FTSJobs wit statues in :statusList: """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    limit = limit if limit else 500
    getFTSJobList = self.ftsManager.getFTSJobList( statusList, limit )
    if not getFTSJobList['OK']:
      self.log.error( "getFTSJobList: %s" % getFTSJobList['Message'] )
      return getFTSJobList
    getFTSJobList = getFTSJobList['Value']
    return S_OK( [ FTSJob( ftsJobDict ) for ftsJobDict in getFTSJobList ] )

  def getFTSFilesForRequest( self, requestID, operationID = None ):
    """ read FTSFiles for a given :requestID:

    :param int requestID: ReqDB.Request.RequestID
    :param int operationID: ReqDB.Operation.OperationID
    """
    ftsFiles = self.ftsManager.getFTSFilesForRequest( requestID, operationID )
    if not ftsFiles['OK']:
      self.log.error( "getFTSFilesForRequest: %s" % ftsFiles['Message'] )
      return ftsFiles
    return S_OK( [ FTSFile( ftsFileDict ) for ftsFileDict in ftsFiles['Value'] ] )

  def getFTSJobsForRequest( self, requestID, statusList = None ):
    """ get list of FTSJobs with statues in :statusList: given requestID

    :param int requestID: ReqDB.Request.RequestID
    :param list statusList: list with FTSJob statuses

    :return: [ FTSJob, FTSJob, ... ]
    """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    getJobs = self.ftsManager.getFTSJobsForRequest( requestID, statusList )
    if not getJobs['OK']:
      self.log.error( "getFTSJobsForRequest: %s" % getJobs['Message'] )
      return getJobs
    return S_OK( [ FTSJob( ftsJobDict ) for ftsJobDict in getJobs['Value'] ] )

  def getFTSFile( self, ftsFileID = None ):
    """ get FTSFile

    :param int ftsFileID: FTSFileID
    """
    getFile = self.ftsManager.getFTSFile( ftsFileID )
    if not getFile['OK']:
      self.log.error( getFile['Message'] )
    # # de-serialize
    if getFile['Value']:
      ftsFile = FTSFile( getFile['Value'] )
    return S_OK( ftsFile )

  def putFTSJob( self, ftsJob ):
    """ put FTSJob into FTSDB

    :param FTSJob ftsJob: FTSJob instance
    """
    isValid = self.ftsValidator.validate( ftsJob )
    if not isValid['OK']:
      self.log.error( isValid['Message'] )
      return isValid
    ftsJobJSON = ftsJob.toJSON()
    if not ftsJobJSON['OK']:
      self.log.error( ftsJobJSON['Message'] )
      return ftsJobJSON
    return self.ftsManager.putFTSJob( ftsJobJSON['Value'] )

  def getFTSJob( self, ftsJobID ):
    """ get FTS job, change its status to 'Assigned'

    :param int ftsJobID: FTSJobID
    """
    getJob = self.ftsManager.getFTSJob( ftsJobID )
    if not getJob['OK']:
      self.log.error( getJob['Message'] )
      return getJob
    setStatus = self.ftsManager.setFTSJobStatus( ftsJobID, 'Assigned' )
    if not setStatus['OK']:
      self.log.error( setStatus['Message'] )
    # # de-serialize
#    if getJob['Value']:
#      getJob = FTSJob( getJob['Value'] )
    return getJob

  def peekFTSJob( self, ftsJobID ):
    """ just peek FTSJob

    :param int ftsJobID: FTSJobID
    """
    getJob = self.ftsManager.getFTSJob( ftsJobID )
    if not getJob['OK']:
      self.log.error( getJob['Message'] )
      return getJob
    return getJob

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob into FTSDB

    :param int ftsJob: FTSJobID
    """
    deleteJob = self.ftsManager.deleteFTSJob( ftsJobID )
    if not deleteJob['OK']:
      self.log.error( deleteJob['Message'] )
    return deleteJob

  def getFTSJobIDs( self, statusList = None ):
    """ get list of FTSJobIDs for a given status list """
    statusList = statusList if statusList else [ "Submitted", "Ready", "Active" ]
    ftsJobIDs = self.ftsManager.getFTSJobIDs( statusList )
    if not ftsJobIDs['OK']:
      self.log.error( ftsJobIDs['Message'] )
    return ftsJobIDs

  def getFTSFileIDs( self, statusList = None ):
    """ get list of FTSFileIDs for a given status list """
    statusList = statusList if statusList else [ "Waiting" ]
    ftsFileIDs = self.ftsManager.getFTSFileIDs( statusList )
    if not ftsFileIDs['OK']:
      self.log.error( ftsFileIDs['Message'] )
    return ftsFileIDs

  def getFTSHistory( self ):
    """ get FTS history snapshot """
    getFTSHistory = self.ftsManager.getFTSHistory()
    if not getFTSHistory['OK']:
      self.log.error( getFTSHistory['Message'] )
      return getFTSHistory
    getFTSHistory = getFTSHistory['Value']
    return S_OK( [ FTSHistoryView( ftsHistory ) for ftsHistory in getFTSHistory ] )

  def getDBSummary( self ):
    """ get FTDB summary """
    dbSummary = self.ftsManager.getDBSummary()
    if not dbSummary['OK']:
      self.log.error( "getDBSummary: %s" % dbSummary['Message'] )
    return dbSummary

  def setFTSFilesWaiting( self, operationID, sourceSE, opFileIDList = None ):
    """ update status for waiting FTSFiles from 'Waiting#SourceSE' to 'Waiting'

    :param int operationID: ReqDB.Operation.OperationID
    :param str sourceSE: source SE name
    :param opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    return self.ftsManager.setFTSFilesWaiting( operationID, sourceSE, opFileIDList )

  def deleteFTSFiles( self, operationID, opFileIDList = None ):
    """ delete FTSFiles for rescheduling

    :param int operationID: ReqDB.Operation.OperationID
    :param list opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    return self.ftsManager.deleteFTSFiles( operationID, opFileIDList )

  def ftsSchedule( self, requestID, operationID, opFileList ):
    """ schedule lfn for FTS job

    :param int requestID: RequestDB.Request.RequestID
    :param int operationID: RequestDB.Operation.OperationID
    :param list opFileList: list of tuples ( File.toJSON()['Value'], sourcesList, targetList )
    """

    fileIDs = [int( fileJSON.get( 'FileID', 0 ) ) for fileJSON, _sourceSEs, _targetSEs in opFileList ]
    res = self.ftsManager.cleanUpFTSFiles( requestID, fileIDs )
    if not res['OK']:
      self.log.error( "ftsSchedule: %s" % res['Message'] )
      return S_ERROR( "ftsSchedule: %s" % res['Message'] )

    ftsFiles = []

    # # this will be returned on success
    ret = { "Successful": [], "Failed": {} }

    for fileJSON, sourceSEs, targetSEs in opFileList:

      lfn = fileJSON.get( "LFN", "" )
      size = int( fileJSON.get( "Size", 0 ) )
      fileID = int( fileJSON.get( "FileID", 0 ) )
      opID = int( fileJSON.get( "OperationID", 0 ) )

      self.log.verbose( "ftsSchedule: LFN=%s FileID=%s OperationID=%s sources=%s targets=%s" % ( lfn, fileID, opID,
                                                                                                 sourceSEs,
                                                                                                 targetSEs ) )

      res = self.dataManager.getActiveReplicas( lfn )
      if not res['OK']:
        self.log.error( "ftsSchedule: %s" % res['Message'] )
        ret["Failed"][fileID] = res['Message']
        continue
      replicaDict = res['Value']

      if lfn in replicaDict["Failed"] and lfn not in replicaDict["Successful"]:
        ret["Failed"][fileID] = "no active replicas found"
        continue
      replicaDict = replicaDict["Successful"].get( lfn, {} )
      # # use valid replicas only
      validReplicasDict = dict( [ ( se, pfn ) for se, pfn in replicaDict.items() if se in sourceSEs ] )

      if not validReplicasDict:
        self.log.warn( "No active replicas found in sources" )
        ret["Failed"][fileID] = "no active replicas found in sources"
        continue

      tree = self.ftsManager.getReplicationTree( sourceSEs, targetSEs, size )
      if not tree['OK']:
        self.log.error( "ftsSchedule: %s cannot be scheduled: %s" % ( lfn, tree['Message'] ) )
        ret["Failed"][fileID] = tree['Message']
        continue
      tree = tree['Value']

      self.log.verbose( "LFN=%s tree=%s" % ( lfn, tree ) )

      for repDict in tree.values():
        self.log.verbose( "Strategy=%s Ancestor=%s SourceSE=%s TargetSE=%s" % ( repDict["Strategy"],
                                                                                repDict["Ancestor"],
                                                                                repDict["SourceSE"],
                                                                                repDict["TargetSE"] ) )
        transferSURLs = self._getTransferURLs( lfn, repDict, sourceSEs, validReplicasDict )
        if not transferSURLs['OK']:
          ret["Failed"][fileID] = transferSURLs['Message']
          continue

        sourceSURL, targetSURL, fileStatus = transferSURLs['Value']
        if sourceSURL == targetSURL:
          ret["Failed"][fileID] = "sourceSURL equals to targetSURL for %s" % lfn
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
      return S_OK( ret )

    ftsFilesJSONList = [ftsFile.toJSON()['Value'] for ftsFile in ftsFiles]
    res = self.ftsManager.putFTSFileList( ftsFilesJSONList )
    if not res['OK']:
      self.log.error( "ftsSchedule: %s" % res['Message'] )
      return S_ERROR( "ftsSchedule: %s" % res['Message'] )

    ret['Successful'] += [ fileID for fileID in fileIDs if fileID not in ret['Failed']]

    # # if we land here some files have been properly scheduled
    return S_OK( ret )

  ################################################################################################################
  # Some utilities function

  def _getSurlForLFN( self, targetSE, lfn ):
    """ Get the targetSURL for the storage and LFN supplied.

    :param self: self reference
    :param str targetSE: target SE
    :param str lfn: LFN
    """
    res = self.storageFactory.getStorages( targetSE, protocolList = ["SRM2"] )
    if not res['OK']:
      errStr = "_getSurlForLFN: Failed to create SRM2 storage for %s: %s" % ( targetSE, res['Message'] )
      self.log.error( errStr )
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
      self.log.error( "_getTransferURLs: %s" % res['Message'] )
      return res
    targetSURL = res['Value']

    status = "Waiting"

    # # get the sourceSURL
    if hopAncestor:
      status = "Waiting#%s" % ( hopAncestor )
    res = self._getSurlForLFN( hopSourceSE, lfn )
    sourceSURL = res.get( 'Value', replicaDict.get( hopSourceSE, None ) )
    if not sourceSURL:
      self.log.error( "_getTransferURLs: %s" % res['Message'] )
      return res

    return S_OK( ( sourceSURL, targetSURL, status ) )

