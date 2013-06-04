########################################################################
# $HeadURL $
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
__RCSID__ = "$Id $"
# #
# @file FTSManagerHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:24:30
# @brief Definition of FTSManagerHandler class.

# # imports
from types import DictType, LongType, ListType, IntType, StringTypes, BooleanType
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
# # from Resources
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
# # from DMS
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
# # for FTS scheduling
from DIRAC.DataManagementSystem.private.FTSStrategy import FTSStrategy
# # for FTS objects validation
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator


########################################################################
class FTSManagerHandler( RequestHandler ):
  """
  .. class:: FTSManagerHandler

  """
  # # fts validator
  __ftsValidator = None
  # # storage factory
  __storageFactory = None
  # # replica manager
  __replicaManager = None
  # # FTSDB
  __ftsDB = None
  # # fts strategy
  __ftsStrategy = None
  # # fts graph
  __ftsGraph = None

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """ initialize handler """

    try:
      from DIRAC.DataManagementSystem.DB.FTSDB import FTSDB
      cls.__ftsDB = FTSDB()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )

    # # create tables for empty db
    getTables = cls.__ftsDB.getTables()
    if not getTables["OK"]:
      gLogger.error( getTables["Message"] )
      return getTables
    getTables = getTables["Value"]
    toCreate = [ tab for tab in cls.__ftsDB.getTableMeta().keys() if tab not in getTables ]
    if toCreate:
      createTables = cls.__ftsDB.createTables( toCreate )
      if not createTables["OK"]:
        gLogger.error( createTables["Message"] )
        return createTables
    # # always re-create views
    createViews = cls.__ftsDB.createViews( True )
    if not createViews["OK"]:
      return createViews

    # # connect
    connect = cls.__ftsDB._connect()
    if not connect["OK"]:
      gLogger.error( connect["Message"] )
      return connect

    cls.ftsMode = cls.srv_getCSOption( "FTSMode", False )

    gLogger.always( "FTS is %s" % { True: "enabled", False: "disabled"}[cls.ftsMode] )

    if cls.ftsMode:
      # # get FTSStrategy
      cls.ftsStrategy()
      # # every 10 minutes update RW access in FTSGraph
      gThreadScheduler.addPeriodicTask( 600, cls.updateRWAccess )
      # # every hour replace FTSGraph
      gThreadScheduler.addPeriodicTask( FTSHistoryView.INTERVAL , cls.updateFTSStrategy )

    return S_OK()

  @classmethod
  def updateFTSStrategy( cls ):
    """ update FTS graph in the FTSStrategy """
    ftsSites = cls.__ftsDB.getFTSSitesList()
    if not ftsSites["OK"]:
      gLogger.warn( "unable to read FTSSites: %s" % ftsSites["Message"] )
      return ftsSites
    ftsHistory = cls.__ftsDB.getFTSHistory()
    if not ftsHistory["OK"]:
      return S_ERROR( "unable to get FTSHistory for FTSStrategy: %s" % ftsHistory["Message"] )
    cls.ftsStrategy().resetGraph( ftsSites["Value"], ftsHistory["Value"] )
    return S_OK()

  @classmethod
  def updateRWAccess( cls ):
    """ update RW access for SEs """
    return cls.ftsStrategy().updateRWAccess()

  @classmethod
  def ftsStrategy( cls ):
    """ fts strategy getter """
    if not cls.__ftsStrategy:
      csPath = getServiceSection( "DataManagement/FTSManager" )
      csPath = "%s/%s" % ( csPath, "FTSStrategy" )

      ftsSites = cls.__ftsDB.getFTSSitesList()
      if not ftsSites["OK"]:
        gLogger.warn( "unable to read FTSSites: %s" % ftsSites["Message"] )
        ftsSites["Value"] = []
      ftsSites = ftsSites["Value"]

      ftsHistory = cls.__ftsDB.getFTSHistory()
      if not ftsHistory["OK"]:
        gLogger.warn( "unable to get FTSHistory for FTSStrategy: %s" % ftsHistory["Message"] )
        ftsHistory["Value"] = []
      ftsHistory = ftsHistory["Value"]

      cls.__ftsStrategy = FTSStrategy( csPath, ftsSites, ftsHistory )

    return cls.__ftsStrategy

  @classmethod
  def ftsValidator( cls ):
    """ FTSValidator instance getter """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  @classmethod
  def storageFactory( cls ):
    """ StorageFactory instance getter """
    if not cls.__storageFactory:
      cls.__storageFactory = StorageFactory()
    return cls.__storageFactory

  @classmethod
  def replicaManager( cls ):
    """ ReplicaManager instance getter """
    if not cls.__replicaManager:
      cls.__replicaManager = ReplicaManager()
    return cls.__replicaManager

  types_setFTSFilesWaiting = [ ( IntType, LongType ), StringTypes, ListType ]
  def export_setFTSFilesWaiting( self, operationID, sourceSE, opFileIDList ):
    """ update states for waiting replications """
    try:
      update = self.__ftsDB.setFTSFilesWaiting( operationID, sourceSE, opFileIDList )
      if not update["OK"]:
        gLogger.error( "setFTSFilesWaiting: %s" % update["Message"] )
      return update
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_deleteFTSFiles = [ ( IntType, LongType ), ListType ]
  def export_deleteFTSFiles( self, operationID, opFileIDList = None ):
    """ cleanup FTSFiles for rescheduling """
    opFileIDList = opFileIDList if opFileIDList else []
    try:
      delete = self.__ftsDB.deleteFTSFiles( operationID, opFileIDList )
      if not delete["OK"]:
        gLogger.error( "deleteFTSFiles: %s" % delete["Message"] )
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
    cleanUpFTSFiles = self.__ftsDB.cleanUpFTSFiles( requestID, fileIDs )
    if not cleanUpFTSFiles["OK"]:
      self.log.error( "ftsSchedule: %s" % cleanUpFTSFiles["Message"] )
      return S_ERROR( cleanUpFTSFiles["Message"] )

    ftsFiles = []

    for fileJSON, sourceSEs, targetSEs in fileJSONList:

      lfn = fileJSON.get( "LFN", "" )
      size = int( fileJSON.get( "Size", 0 ) )
      fileID = int( fileJSON.get( "FileID", 0 ) )
      opID = int( fileJSON.get( "OperationID", 0 ) )

      gLogger.info( "ftsSchedule: LFN=%s FileID=%s OperationID=%s sources=%s targets=%s" % ( lfn, fileID, opID,
                                                                                             sourceSEs, targetSEs ) )

      replicaDict = self.replicaManager().getActiveReplicas( lfn )
      if not replicaDict["OK"]:
        gLogger.error( "ftsSchedule: %s" % replicaDict["Message"] )
        ret["Failed"][lfn] = replicaDict["Message"]
        continue
      replicaDict = replicaDict["Value"]

      if lfn in replicaDict["Failed"] and lfn not in replicaDict["Successful"]:
        ret["Failed"][lfn] = "no active replicas found"
        continue
      replicaDict = replicaDict["Successful"][lfn] if lfn in replicaDict["Successful"] else {}
      # # use valid replicas only
      replicaDict = dict( [ ( se, pfn ) for se, pfn in replicaDict.items() if se in sourceSEs ] )

      if not replicaDict:
        ret["Failed"][lfn] = "no active replicas found in sources"
        continue

      tree = self.ftsStrategy().replicationTree( sourceSEs, targetSEs, size )
      if not tree["OK"]:
        gLogger.error( "ftsSchedule: %s cannot be scheduled: %s" % ( lfn, tree["Message"] ) )
        ret["Failed"][lfn] = tree["Message"]
        continue
      tree = tree["Value"]

      gLogger.info( "LFN=%s tree=%s" % ( lfn, tree ) )

      for repDict in tree.values():
        gLogger.info( "Strategy=%s Ancestor=%s SourceSE=%s TargetSE=%s" % ( repDict["Strategy"], repDict["Ancestor"],
                                                                            repDict["SourceSE"], repDict["TargetSE"] ) )
        transferSURLs = self._getTransferURLs( lfn, repDict, sourceSEs, replicaDict )
        if not transferSURLs["OK"]:
          ret["Failed"][lfn] = transferSURLs["Message"]
          continue

        sourceSURL, targetSURL, fileStatus = transferSURLs["Value"]
        if sourceSURL == targetSURL:
          ret["Failed"][lfn] = "sourceSURL equals to targetSURL for %s" % lfn
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

    put = self.__ftsDB.putFTSFileList( ftsFiles )
    if not put["OK"]:
      gLogger.error( "ftsSchedule: %s" % put["Message"] )
      return put

    for fileJSON, sources, targets in fileJSONList:
      lfn = fileJSON.get( "LFN", "" )
      fileID = fileJSON.get( "FileID", 0 )
      if lfn not in ret["Failed"]:
        ret["Successful"].append( int( fileID ) )

    # # if we land here some files have been properly scheduled
    return S_OK( ret )

  types_putFTSSite = [ DictType ]
  @classmethod
  def export_putFTSSite( cls, ftsSiteJSON ):
    """ put FTSSite """
    try:
      ftsSite = FTSSite( ftsSiteJSON )
      put = cls.__ftsDB.putFTSSite( ftsSite )
      if not put["OK"]:
        gLogger.error( "putFTSSite: %s" % put["Message"] )
      return put
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSSite = [ LongType ]
  @classmethod
  def export_getFTSSite( cls, ftsSiteID ):
    """ get FTSSite given its id """
    try:
      getSite = cls.__ftsDB.getFTSSite( ftsSiteID )
      if not getSite["OK"]:
        gLogger.error( "getFTSSite: %s" % getSite["Message"] )
        return getSite
      getSite = getSite["Value"] if getSite["Value"] else None
      if not getSite:
        return S_OK()
      getSite = getSite.toJSON()
      if not getSite["OK"]:
        gLogger.error( "getFTSSite: %s" % getSite["Message"] )
      return getSite
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSSitesList = []
  @classmethod
  def export_getFTSSitesList( cls ):
    """ get list of FTS sites """
    try:
      sitesList = cls.__ftsDB.getFTSSitesList()
      if not sitesList["OK"]:
        gLogger.error( "getFTSSitesList: %s" % sitesList["Message"] )
        return sitesList
      sitesList = sitesList["Value"]
      sitesJSON = []
      for site in sitesList:
        siteJSON = site.toJSON()
        if not siteJSON["OK"]:
          gLogger.error( "getFTSSitesList: %s" % siteJSON["Message"] )
          return siteJSON
        sitesJSON.append( siteJSON["Value"] )
      return S_OK( sitesJSON )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSFile = [ LongType ]
  @classmethod
  def export_getFTSFile( cls, ftsFileID ):
    """ get FTSFile from FTSDB """
    try:
      getFile = cls.__ftsDB.getFTSFile( ftsFileID )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

    if not getFile["OK"]:
      gLogger.error( "getFTSFile: %s" % getFile["Message"] )
      return getFile
    # # serialize
    if getFile["Value"]:
      getFile = getFile["Value"].toXML( True )
      if not getFile["OK"]:
        gLogger.error( getFile["Message"] )
    return getFile

  types_peekFTSFile = [ LongType ]
  @classmethod
  def export_peekFTSFile( cls, ftsFileID ):
    """ peek FTSFile given FTSFileID """
    try:
      peekFile = cls.__ftsDB.peekFTSFile( ftsFileID )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

    if not peekFile["OK"]:
      gLogger.error( "peekFTSFile: %s" % peekFile["Message"] )
      return peekFile
    # # serialize
    if peekFile["Value"]:
      peekFile = peekFile["Value"].toXML( True )
      if not peekFile["OK"]:
        gLogger.error( peekFile["Message"] )
    return peekFile

  types_putFTSFile = [ DictType ]
  @classmethod
  def export_putFTSFile( cls, ftsFileJSON ):
    """ put FTSFile into FTSDB """
    try:
      ftsFile = FTSFile( ftsFileJSON )
      isValid = cls.ftsValidator().validate( ftsFile )
      if not isValid["OK"]:
        gLogger.error( isValid["Message"] )
        return isValid
      return cls.__ftsDB.putFTSFile( ftsFile )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_deleteFTSFile = [ LongType ]
  @classmethod
  def export_deleteFTSFile( cls, ftsFileID ):
    """ delete FTSFile record given FTSFileID """
    try:
      deleteFTSFile = cls.__ftsDB.deleteFTSFile( ftsFileID )
      if not deleteFTSFile["OK"]:
        gLogger.error( deleteFTSFile["Message"] )
      return deleteFTSFile
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_putFTSJob = [ DictType ]
  @classmethod
  def export_putFTSJob( cls, ftsJobJSON ):
    """ put FTSLfn into FTSDB """

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

    isValid = cls.ftsValidator().validate( ftsJob )
    if not isValid["OK"]:
      gLogger.error( isValid["Message"] )
      return isValid
    try:
      put = cls.__ftsDB.putFTSJob( ftsJob )
      if not put["OK"]:
        return S_ERROR( put["Message"] )
      return S_OK()
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSJob = [ LongType ]
  @classmethod
  def export_getFTSJob( cls, ftsJobID ):
    """ read FTSJob for processing given FTSJobID """
    try:
      getFTSJob = cls.__ftsDB.getFTSJob( ftsJobID )
      if not getFTSJob["OK"]:
        gLogger.error( getFTSJob["Error"] )
        return getFTSJob
      getFTSJob = getFTSJob["Value"]
      if not getFTSJob:
        return S_OK()
      toJSON = getFTSJob.toJSON()
      if not toJSON["OK"]:
        gLogger.error( toJSON["Message"] )
      return toJSON
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_peekFTSJob = [ LongType ]
  @classmethod
  def export_peekFTSJob( cls, ftsJobID ):
    """ peek FTSJob given ftsJobID """
    try:
      peekFTSJob = cls.__ftsDB.peekFTSJob( ftsJobID )
      if not peekFTSJob["OK"]:
        gLogger.error( peekFTSJob["Error"] )
        return peekFTSJob
      peekFTSJob = peekFTSJob["Value"]
      if not peekFTSJob:
        return S_OK()
      toJSON = peekFTSJob.toJSON()
      if not toJSON["OK"]:
        gLogger.error( toJSON["Message"] )
      return toJSON
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_deleteFTSJob = [ LongType ]
  @classmethod
  def export_deleteFTSJob( cls, ftsJobID ):
    """ delete FTSJob given FTSJobID """
    try:
      deleteFTSJob = cls.__ftsDB.deleteFTSJob( ftsJobID )
      if not deleteFTSJob["OK"]:
        gLogger.error( deleteFTSJob["Message"] )
      return deleteFTSJob
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSJobIDs = [ ListType ]
  @classmethod
  def export_getFTSJobIDs( cls, statusList = None ):
    """ get FTSJobIDs for a given status list """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    try:
      getFTSJobIDs = cls.__ftsDB.getFTSJobIDs( statusList )
      if not getFTSJobIDs["OK"]:
        gLogger.error( getFTSJobIDs["Message"] )
      return getFTSJobIDs
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSFilesIDs = [ ListType ]
  @classmethod
  def export_getFTSFileIDs( cls, statusList = None ):
    """ get FTSFilesIDs for a given status list """
    statusList = statusList if statusList else [ "Waiting" ]
    try:
      getFTSFileIDs = cls.__ftsDB.getFTSFileIDs( statusList )
      if not getFTSFileIDs["OK"]:
        gLogger.error( getFTSFileIDs["Message"] )
      return getFTSFileIDs
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSFileList = [ ListType, IntType ]
  @classmethod
  def export_getFTSFileList( cls, statusList = None, limit = None ):
    """ get FTSFiles with status in :statusList: """
    statusList = statusList if statusList else [ "Waiting" ]
    limit = limit if limit else 1000
    try:
      getFTSFileList = cls.__ftsDB.getFTSFileList( statusList, limit )
      if not getFTSFileList["OK"]:
        gLogger.error( getFTSFileList[ "Message" ] )
        return getFTSFileList
      fileList = []
      for ftsFile in getFTSFileList["Value"]:
        fileJSON = ftsFile.toJSON()
        if not fileJSON["OK"]:
          gLogger.error( "getFTSFileList: %s" % fileJSON["Message"] )
          return fileJSON
        fileList.append( fileJSON["Value"] )
      return S_OK( fileList )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

  types_getFTSJobList = [ ListType, IntType ]
  @classmethod
  def export_getFTSJobList( cls, statusList = None, limit = 500 ):
    """ get FTSJobs with statuses in :statusList: """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    try:
      ftsJobs = cls.__ftsDB.getFTSJobList( statusList, limit )
      if not ftsJobs["OK"]:
        gLogger.error( "getFTSJobList: %s" % ftsJobs["Message"] )
        return ftsJobs
      ftsJobsJSON = []
      for ftsJob in ftsJobs["Value"]:
        ftsJobJSON = ftsJob.toJSON()
        if not ftsJobJSON["OK"]:
          gLogger.error( "getFTSJobList: %s" % ftsJobJSON["Message"] )
          return ftsJobJSON
        ftsJobsJSON.append( ftsJobJSON["Value"] )
      return S_OK( ftsJobsJSON )
    except Exception, error:
      gLogger.exception( str( error ) )
      return S_ERROR( str( error ) )


  types_getFTSJobsForRequest = [ ( IntType, LongType ), ListType ]
  @classmethod
  def export_getFTSJobsForRequest( cls, requestID, statusList = None ):
    """ get list of FTSJobs for request given its :requestID: and statues in :statusList:

    :param int requestID: ReqDB.Request.RequestID
    :param list statusList: FTSJobs status list
    """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    try:
      requestID = int( requestID )
      ftsJobs = cls.__ftsDB.getFTSJobsForRequest( requestID, statusList )
      if not ftsJobs["OK"]:
        gLogger.error( "getFTSJobsForRequest: %s" % ftsJobs["Message"] )
        return ftsJobs
      ftsJobsList = []
      for ftsJob in ftsJobs["Value"]:
        ftsJobJSON = ftsJob.toJSON()
        if not ftsJobJSON["OK"]:
          gLogger.error( "getFTSJobsForRequest: %s" % ftsJobJSON["Message"] )
          return ftsJobJSON
        ftsJobsList.append( ftsJobJSON["Value"] )
      return S_OK( ftsJobsList )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( str( error ) )

  types_getFTSHistory = []
  @classmethod
  def export_getFTSHistory( cls ):
    """ get last hour FTS history snapshot """
    try:
      ftsHistory = cls.__ftsDB.getFTSHistory()
      if not ftsHistory["OK"]:
        gLogger.error( ftsHistory["Message"] )
        return ftsHistory
      ftsHistory = ftsHistory["Value"]
      history = []
      for ftsHistory in ftsHistory:
        ftsHistoryJSON = ftsHistory.toJSON()
        if not ftsHistoryJSON["OK"]:
          return ftsHistoryJSON
        history.append( ftsHistoryJSON["Value"] )
      return S_OK( history )
    except Exception, error:
      gLogger.exception( error )
      return S_ERROR( error )

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
    res = self.storageFactory().getStorages( targetSE, protocolList = ["SRM2"] )
    if not res["OK"]:
      errStr = "_getSurlForLFN: Failed to create SRM2 storage for %s: %s" % ( targetSE, res["Message"] )
      gLogger.error( errStr )
      return S_ERROR( errStr )
    storageObjects = res["Value"]["StorageObjects"]
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
    res = self.replicaManager().getPfnForProtocol( [pfn], sourceSE )
    if not res["OK"]:
      return res
    if pfn in res["Value"]["Failed"]:
      return S_ERROR( res["Value"]["Failed"][pfn] )
    return S_OK( res["Value"]["Successful"][pfn] )

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
      self.log.error( "_getTransferURLs: %s" % res["Message"] )
      return res
    targetSURL = res["Value"]

    status = "Waiting"

    # # get the sourceSURL
    if hopAncestor:
      status = "Waiting#%s" % ( hopAncestor )
      res = self._getSurlForLFN( hopSourceSE, lfn )
      if not res["OK"]:
        self.log.error( "_getTransferURLs: %s" % res["Message"] )
        return res
      sourceSURL = res["Value"]
    else:
      res = self._getSurlForPFN( hopSourceSE, replicaDict[hopSourceSE] )
      sourceSURL = res["Value"] if res["OK"] else replicaDict[hopSourceSE]

    return S_OK( ( sourceSURL, targetSURL, status ) )

