########################################################################
# File: FTSManagerHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:24:08
########################################################################
""" :mod: FTSManagerHandler
    =======================

    .. module: FTSManagerHandler
    :synopsis: handler for FTSDB using DISET
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    service handler for FTSDB using DISET
"""

# #
# @file FTSManagerHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:24:30
# @brief Definition of FTSManagerHandler class.

# # imports
from types import DictType, LongType, ListType, IntType, StringTypes, NoneType
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from DIRAC.ConfigurationSystem.Client.PathFinder        import getServiceSection
from DIRAC.Core.Utilities.ThreadScheduler               import gThreadScheduler
# # from Resources
from DIRAC.Resources.Storage.StorageFactory             import StorageFactory
# # from DMS
from DIRAC.DataManagementSystem.Client.ReplicaManager   import ReplicaManager
from DIRAC.DataManagementSystem.Client.FTSJob           import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile          import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView  import FTSHistoryView
# # for FTS scheduling
from DIRAC.DataManagementSystem.private.FTSStrategy     import FTSStrategy
# # for FTS objects validation
from DIRAC.DataManagementSystem.private.FTSValidator    import FTSValidator
# # FTS DB
from DIRAC.DataManagementSystem.DB.FTSDB                import FTSDB
# # for proxy
from DIRAC.Core.Utilities.Shifter                       import setupShifterProxyInEnv

########################################################################
class FTSManagerHandler( RequestHandler ):
  """
  .. class:: FTSManagerHandler

  """

  @classmethod
  def initializeHandler( self, serviceInfoDict ):
    """ initialize handler """

    try:
      self.ftsDB = FTSDB()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )

    self.ftsValidator = FTSValidator()
    self.storageFactory = StorageFactory()
    self.replicaManager = ReplicaManager()

    # # create tables for empty db
    getTables = self.ftsDB.getTables()
    if not getTables["OK"]:
      gLogger.error( getTables['Message'] )
      return getTables
    getTables = getTables['Value']
    toCreate = [ tab for tab in self.ftsDB.getTableMeta().keys() if tab not in getTables ]
    if toCreate:
      createTables = self.ftsDB.createTables( toCreate )
      if not createTables["OK"]:
        gLogger.error( createTables['Message'] )
        return createTables
    # # always re-create views
    createViews = self.ftsDB.createViews( True )
    if not createViews["OK"]:
      return createViews

    # # connect
    connect = self.ftsDB._connect()
    if not connect["OK"]:
      gLogger.error( connect['Message'] )
      return connect

    # # get FTSStrategy
    self.ftsStrategy = self.getFtsStrategy()
    # # put DataManager proxy to env
    dmProxy = self.refreshProxy()
    if not dmProxy["OK"]:
      return dmProxy

    # # every 10 minutes update RW access in FTSGraph
    gThreadScheduler.addPeriodicTask( 600, self.updateRWAccess )
    # # every hour replace FTSGraph
    gThreadScheduler.addPeriodicTask( FTSHistoryView.INTERVAL , self.updateFTSStrategy )
    # # every 6 hours refresh DataManager proxy
    gThreadScheduler.addPeriodicTask( 21600, self.refreshProxy )

    return S_OK()

  @classmethod
  def getFtsStrategy( self ):
    """ fts strategy getter """
    csPath = getServiceSection( "DataManagement/FTSManager" )
    csPath = "%s/%s" % ( csPath, "FTSStrategy" )

    ftsHistory = self.ftsDB.getFTSHistory()
    if not ftsHistory["OK"]:
      gLogger.warn( "unable to get FTSHistory for FTSStrategy: %s" % ftsHistory['Message'] )
      ftsHistory['Value'] = []
    ftsHistory = ftsHistory['Value']

    return FTSStrategy( csPath, None, ftsHistory )

  @classmethod
  def refreshProxy( self ):
    """ setup DataManager shifter proxy in env """
    gLogger.info( "refreshProxy: getting proxy for DataManager..." )
    proxy = setupShifterProxyInEnv( "DataManager" )
    if not proxy["OK"]:
      gLogger.error( "refreshProxy: %s" % proxy['Message'] )
    return proxy

  @classmethod
  def updateFTSStrategy( self ):
    """ update FTS graph in the FTSStrategy """
    ftsHistory = self.ftsDB.getFTSHistory()
    if not ftsHistory["OK"]:
      return S_ERROR( "unable to get FTSHistory for FTSStrategy: %s" % ftsHistory['Message'] )
    self.ftsStrategy.resetGraph( ftsHistory['Value'] )
    return S_OK()

  @classmethod
  def updateRWAccess( self ):
    """ update RW access for SEs """
    return self.ftsStrategy.updateRWAccess()


  types_setFTSFilesWaiting = [ ( IntType, LongType ), StringTypes, ListType ]
  def export_setFTSFilesWaiting( self, operationID, sourceSE, opFileIDList ):
    """ update states for waiting replications """
    try:
      update = self.ftsDB.setFTSFilesWaiting( operationID, sourceSE, opFileIDList )
      if not update["OK"]:
        gLogger.error( "setFTSFilesWaiting: %s" % update['Message'] )
      return update
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_deleteFTSFiles = [ ( IntType, LongType ), ( NoneType, ListType ) ]
  def export_deleteFTSFiles( self, operationID, opFileIDList = None ):
    """ cleanup FTSFiles for rescheduling """
    opFileIDList = opFileIDList if opFileIDList else []
    try:
      self.log.warn( "Removing %s FTSFiles for OperationID = %s" % ( ( 'All' if not opFileIDList else opFileIDList ),
                                                                     operationID ) )
      delete = self.ftsDB.deleteFTSFiles( operationID, opFileIDList )
      if not delete["OK"]:
        gLogger.error( "deleteFTSFiles: %s" % delete['Message'] )
      return delete
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_ftsSchedule = [ ( IntType, LongType ), ( IntType, LongType ), ListType ]
  def export_ftsSchedule( self, requestID, operationID, fileJSONList ):
    """ call FTS scheduler

    :param int requestID: ReqDB.Request.RequestID
    :param int operationID: ReqDB.Operation.OperationID
    :param list fileJSONList: [ (fileJSON, [sourceSE, ...], [ targetSE, ...] ) ]
    """
    # # this will be returned on success
    ret = { "Successful": [], "Failed": {} }

    requestID = int( requestID )
    operationID = int( operationID )

    fileIDs = []
    for fileJSON, sourceSEs, targetSEs in fileJSONList:
      fileID = int( fileJSON.get( "FileID" ) )
      fileIDs.append( fileID )
    cleanUpFTSFiles = self.ftsDB.cleanUpFTSFiles( requestID, fileIDs )
    if not cleanUpFTSFiles["OK"]:
      self.log.error( "ftsSchedule: %s" % cleanUpFTSFiles['Message'] )
      return S_ERROR( cleanUpFTSFiles['Message'] )

    ftsFiles = []

    for fileJSON, sourceSEs, targetSEs in fileJSONList:

      lfn = fileJSON.get( "LFN", "" )
      size = int( fileJSON.get( "Size", 0 ) )
      fileID = int( fileJSON.get( "FileID", 0 ) )
      opID = int( fileJSON.get( "OperationID", 0 ) )

      gLogger.info( "ftsSchedule: LFN=%s FileID=%s OperationID=%s sources=%s targets=%s" % ( lfn, fileID, opID,
                                                                                             sourceSEs, targetSEs ) )

      replicaDict = self.replicaManager.getActiveReplicas( lfn )
      if not replicaDict["OK"]:
        gLogger.error( "ftsSchedule: %s" % replicaDict['Message'] )
        ret["Failed"][fileID] = replicaDict['Message']
        continue
      replicaDict = replicaDict['Value']

      if lfn in replicaDict["Failed"] and lfn not in replicaDict["Successful"]:
        ret["Failed"][fileID] = "no active replicas found"
        continue
      replicaDict = replicaDict["Successful"][lfn] if lfn in replicaDict["Successful"] else {}
      # # use valid replicas only
      replicaDict = dict( [ ( se, pfn ) for se, pfn in replicaDict.items() if se in sourceSEs ] )

      if not replicaDict:
        ret["Failed"][fileID] = "no active replicas found in sources"
        continue

      tree = self.ftsStrategy.replicationTree( sourceSEs, targetSEs, size )
      if not tree["OK"]:
        gLogger.error( "ftsSchedule: %s cannot be scheduled: %s" % ( lfn, tree['Message'] ) )
        ret["Failed"][fileID] = tree['Message']
        continue
      tree = tree['Value']

      gLogger.info( "LFN=%s tree=%s" % ( lfn, tree ) )

      for repDict in tree.values():
        gLogger.info( "Strategy=%s Ancestor=%s SourceSE=%s TargetSE=%s" % ( repDict["Strategy"], repDict["Ancestor"],
                                                                            repDict["SourceSE"], repDict["TargetSE"] ) )
        transferSURLs = self._getTransferURLs( lfn, repDict, sourceSEs, replicaDict )
        if not transferSURLs["OK"]:
          ret["Failed"][fileID] = transferSURLs['Message']
          continue

        sourceSURL, targetSURL, fileStatus = transferSURLs['Value']
        if sourceSURL == targetSURL:
          ret["Failed"][fileID] = "sourceSURL equals to targetSURL for %s" % lfn
          continue

        gLogger.info( "sourceURL=%s targetURL=%s FTSFile.Status=%s" % ( sourceSURL, targetSURL, fileStatus ) )

        ftsFile = FTSFile()
        for key in ( "LFN", "FileID", "OperationID", "Checksum", "ChecksumType", "Size" ):
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
      return S_ERROR( "ftsSchedule: no FTSFiles to put" )

    put = self.ftsDB.putFTSFileList( ftsFiles )
    if not put["OK"]:
      gLogger.error( "ftsSchedule: %s" % put['Message'] )
      return put

    for fileJSON, _sources, _targets in fileJSONList:
      lfn = fileJSON.get( "LFN", "" )
      fileID = fileJSON.get( "FileID", 0 )
      if fileID not in ret["Failed"]:
        ret["Successful"].append( int( fileID ) )

    # # if we land here some files have been properly scheduled
    return S_OK( ret )


  types_getFTSFile = [ [IntType, LongType] ]
  @classmethod
  def export_getFTSFile( self, ftsFileID ):
    """ get FTSFile from FTSDB """
    try:
      getFile = self.ftsDB.getFTSFile( ftsFileID )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

    if not getFile["OK"]:
      gLogger.error( "getFTSFile: %s" % getFile['Message'] )
      return getFile
    # # serialize
    if getFile['Value']:
      getFile = getFile['Value'].toJSON()
      if not getFile["OK"]:
        gLogger.error( getFile['Message'] )
    return getFile

  types_peekFTSFile = [ [IntType, LongType] ]
  @classmethod
  def export_peekFTSFile( self, ftsFileID ):
    """ peek FTSFile given FTSFileID """
    try:
      peekFile = self.ftsDB.peekFTSFile( ftsFileID )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

    if not peekFile["OK"]:
      gLogger.error( "peekFTSFile: %s" % peekFile['Message'] )
      return peekFile
    # # serialize
    if peekFile['Value']:
      peekFile = peekFile['Value'].toJSON()
      if not peekFile["OK"]:
        gLogger.error( peekFile['Message'] )
    return peekFile

  types_putFTSJob = [ DictType ]
  @classmethod
  def export_putFTSJob( self, ftsJobJSON ):
    """ put FTSJob (serialized in JSON into FTSDB """

    ftsFiles = []

    if "FTSFiles" in ftsJobJSON:
      ftsFiles = ftsJobJSON.get( "FTSFiles", [] )
      del ftsJobJSON["FTSFiles"]

    try:
      ftsJob = FTSJob( ftsJobJSON )
      for ftsFile in ftsFiles:
        ftsJob.addFile( FTSFile( ftsFile ) )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

    isValid = self.ftsValidator.validate( ftsJob )
    if not isValid["OK"]:
      gLogger.error( isValid['Message'] )
      return isValid
    try:
      put = self.ftsDB.putFTSJob( ftsJob )
      if not put["OK"]:
        return S_ERROR( put['Message'] )
      return S_OK()
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSJob = [ [IntType, LongType] ]
  @classmethod
  def export_getFTSJob( self, ftsJobID ):
    """ read FTSJob for processing given FTSJobID """
    try:
      getFTSJob = self.ftsDB.getFTSJob( ftsJobID )
      if not getFTSJob["OK"]:
        gLogger.error( getFTSJob['Message'] )
        return getFTSJob
      getFTSJob = getFTSJob['Value']
      if not getFTSJob:
        return S_OK()
      toJSON = getFTSJob.toJSON()
      if not toJSON["OK"]:
        gLogger.error( toJSON['Message'] )
      return toJSON
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_setFTSJobStatus = [[IntType, LongType], StringTypes]
  @classmethod
  def export_setFTSJobStatus( self, ftsJobID, status ):
    """ set FTSJob status
    """
    return self.ftsDB.setFTSJobStatus( ftsJobID, status )

  types_deleteFTSJob = [ [IntType, LongType] ]
  @classmethod
  def export_deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob given FTSJobID """
    try:
      deleteFTSJob = self.ftsDB.deleteFTSJob( ftsJobID )
      if not deleteFTSJob["OK"]:
        gLogger.error( deleteFTSJob['Message'] )
      return deleteFTSJob
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSJobIDs = [ ListType ]
  @classmethod
  def export_getFTSJobIDs( self, statusList = None ):
    """ get FTSJobIDs for a given status list """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    try:
      getFTSJobIDs = self.ftsDB.getFTSJobIDs( statusList )
      if not getFTSJobIDs["OK"]:
        gLogger.error( getFTSJobIDs['Message'] )
      return getFTSJobIDs
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSFileIDs = [ ListType ]
  @classmethod
  def export_getFTSFileIDs( self, statusList = None ):
    """ get FTSFilesIDs for a given status list """
    statusList = statusList if statusList else [ "Waiting" ]
    try:
      getFTSFileIDs = self.ftsDB.getFTSFileIDs( statusList )
      if not getFTSFileIDs["OK"]:
        gLogger.error( getFTSFileIDs['Message'] )
      return getFTSFileIDs
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSFileList = [ ListType, IntType ]
  @classmethod
  def export_getFTSFileList( self, statusList = None, limit = None ):
    """ get FTSFiles with status in :statusList: """
    statusList = statusList if statusList else [ "Waiting" ]
    limit = limit if limit else 1000
    try:
      getFTSFileList = self.ftsDB.getFTSFileList( statusList, limit )
      if not getFTSFileList["OK"]:
        gLogger.error( getFTSFileList[ 'Message' ] )
        return getFTSFileList
      fileList = []
      for ftsFile in getFTSFileList['Value']:
        fileJSON = ftsFile.toJSON()
        if not fileJSON["OK"]:
          gLogger.error( "getFTSFileList: %s" % fileJSON['Message'] )
          return fileJSON
        fileList.append( fileJSON['Value'] )
      return S_OK( fileList )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSJobList = [ ListType, IntType ]
  @classmethod
  def export_getFTSJobList( self, statusList = None, limit = 500 ):
    """ get FTSJobs with statuses in :statusList: """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    try:
      ftsJobs = self.ftsDB.getFTSJobList( statusList, limit )
      if not ftsJobs["OK"]:
        gLogger.error( "getFTSJobList: %s" % ftsJobs['Message'] )
        return ftsJobs
      ftsJobsJSON = []
      for ftsJob in ftsJobs['Value']:
        ftsJobJSON = ftsJob.toJSON()
        if not ftsJobJSON["OK"]:
          gLogger.error( "getFTSJobList: %s" % ftsJobJSON['Message'] )
          return ftsJobJSON
        ftsJobsJSON.append( ftsJobJSON['Value'] )
      return S_OK( ftsJobsJSON )
    except Exception, error:
      gLogger.exception( str( error ) )
      return S_ERROR( str( error ) )


  types_getFTSJobsForRequest = [ ( IntType, LongType ), ListType ]
  @classmethod
  def export_getFTSJobsForRequest( self, requestID, statusList = None ):
    """ get list of FTSJobs for request given its :requestID: and statues in :statusList:

    :param int requestID: ReqDB.Request.RequestID
    :param list statusList: FTSJobs status list
    """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    try:
      requestID = int( requestID )
      ftsJobs = self.ftsDB.getFTSJobsForRequest( requestID, statusList )
      if not ftsJobs["OK"]:
        gLogger.error( "getFTSJobsForRequest: %s" % ftsJobs['Message'] )
        return ftsJobs
      ftsJobsList = []
      for ftsJob in ftsJobs['Value']:
        ftsJobJSON = ftsJob.toJSON()
        if not ftsJobJSON["OK"]:
          gLogger.error( "getFTSJobsForRequest: %s" % ftsJobJSON['Message'] )
          return ftsJobJSON
        ftsJobsList.append( ftsJobJSON['Value'] )
      return S_OK( ftsJobsList )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_getFTSFilesForRequest = [ ( IntType, LongType ), ListType ]
  @classmethod
  def export_getFTSFilesForRequest( self, requestID, statusList = None ):
    """ get list of FTSFiles with statuses in :statusList: given :requestID: """
    statusList = statusList if statusList else [ "Waiting" ]
    try:
      requestID = int( requestID )
      ftsFiles = self.ftsDB.getFTSFilesForRequest( requestID, statusList )
      if not ftsFiles["OK"]:
        gLogger.error( "getFTSFilesForRequest: %s" % ftsFiles['Message'] )
        return ftsFiles
      ftsFilesList = []
      for ftsFile in ftsFiles['Value']:
        ftsFileJSON = ftsFile.toJSON()
        if not ftsFileJSON["OK"]:
          gLogger.error( "getFTSFilesForRequest: %s" % ftsFileJSON['Message'] )
          return ftsFileJSON
        ftsFilesList.append( ftsFileJSON['Value'] )
      return S_OK( ftsFilesList )
    except Exception, error:
      gLogger.exception( str( error ) )
      return S_ERROR( str( error ) )

  types_getFTSHistory = []
  @classmethod
  def export_getFTSHistory( self ):
    """ get last hour FTS history snapshot """
    try:
      ftsHistory = self.ftsDB.getFTSHistory()
      if not ftsHistory["OK"]:
        gLogger.error( ftsHistory['Message'] )
        return ftsHistory
      ftsHistory = ftsHistory['Value']
      history = []
      for ftsHistory in ftsHistory:
        ftsHistoryJSON = ftsHistory.toJSON()
        if not ftsHistoryJSON["OK"]:
          return ftsHistoryJSON
        history.append( ftsHistoryJSON['Value'] )
      return S_OK( history )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getDBSummary = []
  @classmethod
  def export_getDBSummary( self ):
    """ get FTSDB summary """
    try:
      dbSummary = self.ftsDB.getDBSummary()
      if not dbSummary["OK"]:
        gLogger.error( "getDBSummary: %s" % dbSummary['Message'] )
      return dbSummary
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  @staticmethod
  def _ancestorSortKeys( tree, aKey = "Ancestor" ):
    """ sorting keys of replicationTree by its hopAncestor value

    replicationTree is a dict ( channelID : { ... }, (...) }

    :param self: self reference
    :param dict tree: replication tree  to sort
    :param str aKey: a key in value dict used to sort
    """
    if False in [ bool( aKey in v ) for v in tree.values() ]:
      return S_ERROR( "ancestorSortKeys: %s key in not present in all values" % aKey )
    # # put parents of all parents
    sortedKeys = [ k for k in tree if aKey in tree[k] and not tree[k][aKey] ]
    # # get children
    pairs = dict( [ ( k, v[aKey] ) for k, v in tree.items() if v[aKey] ] )
    while pairs:
      for key, ancestor in dict( pairs ).items():
        if key not in sortedKeys and ancestor in sortedKeys:
          sortedKeys.insert( sortedKeys.index( ancestor ), key )
          del pairs[key]
    # # need to reverse this one, as we're inserting child before its parent
    sortedKeys.reverse()
    if sorted( sortedKeys ) != sorted( tree.keys() ):
      return S_ERROR( "ancestorSortKeys: cannot sort, some keys are missing!" )
    return S_OK( sortedKeys )

  def _getSurlForLFN( self, targetSE, lfn ):
    """ Get the targetSURL for the storage and LFN supplied.

    :param self: self reference
    :param str targetSE: target SE
    :param str lfn: LFN
    """
    res = self.storageFactory.getStorages( targetSE, protocolList = ["SRM2"] )
    if not res["OK"]:
      errStr = "_getSurlForLFN: Failed to create SRM2 storage for %s: %s" % ( targetSE, res['Message'] )
      gLogger.error( errStr )
      return S_ERROR( errStr )
    storageObjects = res['Value']["StorageObjects"]
    for storageObject in storageObjects:
      res = storageObject.getCurrentURL( lfn )
      if res["OK"]:
        return res
    gLogger.error( "_getSurlForLFN: Failed to get SRM compliant storage.", targetSE )
    return S_ERROR( "_getSurlForLFN: Failed to get SRM compliant storage." )

  def _getSurlForPFN( self, sourceSE, pfn ):
    """Creates the targetSURL for the storage and PFN supplied.

    :param self: self reference
    :param str sourceSE: source storage element
    :param str pfn: physical file name
    """
    res = self.replicaManager.getPfnForProtocol( [pfn], sourceSE )
    if not res["OK"]:
      return res
    if pfn in res['Value']["Failed"]:
      return S_ERROR( res['Value']["Failed"][pfn] )
    return S_OK( res['Value']["Successful"][pfn] )

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
    if not res["OK"]:
      self.log.error( "_getTransferURLs: %s" % res['Message'] )
      return res
    targetSURL = res['Value']

    status = "Waiting"

    # # get the sourceSURL
    if hopAncestor:
      status = "Waiting#%s" % ( hopAncestor )
      res = self._getSurlForLFN( hopSourceSE, lfn )
      if not res["OK"]:
        self.log.error( "_getTransferURLs: %s" % res['Message'] )
        return res
      sourceSURL = res['Value']
    else:
      res = self._getSurlForPFN( hopSourceSE, replicaDict[hopSourceSE] )
      sourceSURL = res['Value'] if res["OK"] else replicaDict[hopSourceSE]

    return S_OK( ( sourceSURL, targetSURL, status ) )

