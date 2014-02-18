########################################################################
# $HeadURL $
# File: TransferAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/05/30 09:40:33
########################################################################
""" :mod: TransferAgent
    ===================

    TransferAgent executes 'transfer' requests read from the RequestClient.

    This agent has two modes of operation:
    - standalone, when all Requests are handled using ProcessPool and TransferTask
    - scheduling for FTS with fallback TransferTask functionality

    The fallback mechanism is fired in case that:

    - FTS channels between SourceSE and TargetSE is not defined
    - there is a trouble to define correct replication tree
    - request's owner is different from DataManager

    :deprecated:
"""

# # imports
import time
import re
# # from DIRAC (globals and Core)
from DIRAC import gMonitor, S_OK, S_ERROR
# # base class
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
# # replica manager
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
# # startegy handler
from DIRAC.DataManagementSystem.private.StrategyHandler import StrategyHandler, SHGraphCreationError
# # task to be executed
from DIRAC.DataManagementSystem.private.TransferTask import TransferTask
# # from RMS
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
# # from RSS
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Adler import compareAdler
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Utilities           import Utils
# # agent name
AGENT_NAME = "DataManagement/TransferAgent"

class TransferAgentError( Exception ):
  """
  .. class:: TransferAgentError

  Exception raised when neither scheduling nor task execution is enabled in CS.
  """
  def __init__( self, msg ):
    """ c'tor

    :param self: self reference
    :param str msg: description
    """
    Exception.__init__( self )
    self.msg = msg
  def __str__( self ):
    """ str() operator

    :param self: self reference
    """
    return str( self.msg )

###################################################################################
# AGENT
###################################################################################
class TransferAgent( RequestAgentBase ):
  """
  .. class:: TransferAgent

  This class is dealing with 'transfer' DIRAC requests.

  A request could be processed in a two different modes: request's files could be scheduled for FTS processing
  or alternatively subrequests could be processed by TransferTask in a standalone subprocesses.
  If FTS scheduling fails for any reason OR request itself cannot be processed using FTS machinery (i.e. its owner
  is not a DataManager, FTS channel for at least on file doesn't exist), the processing will go through
  TransferTasks and ProcessPool.

  By default FTS scheduling is disabled and all requests are processed using tasks.

  Config options
  --------------

  * maximal number of request to be executed in one cycle
    RequestsPerCycle = 10
  * minimal number of sub-processes working togehter
    MinProcess = 1
  * maximal number of sub-processes working togehter
    MaxProcess = 4
  * results queue size
    ProcessPoolQueueSize = 10
  * pool timeout
    ProcessPoolTimeout = 300
  * task timeout
    ProcessTaskTimeout = 300
  * request type
    RequestType = transfer
  * proxy
    shifterProxy = DataManager
  * task executing
    TaskMode = True
  * FTS scheduling
    FTSMode = True
  * time interval for throughput (FTS scheduling) in seconds
    ThroughputTimescale = 3600
  * for StrategyHandler
     - hop time
      HopSigma = 0.0
     - files/time or througput/time
      SchedulingType = File
     - list of active strategies
      ActiveStrategies = MinimiseTotalWait
     - acceptable failure rate
      AcceptableFailureRate = 75
  """

  # # placeholder for rss client
  __rssClient = None
  # # placeholder for StorageFactory instance
  __storageFactory = None
  # # placeholder for StrategyHandler instance
  __strategyHandler = None
  # # placeholder for TransferDB instance (for FTS mode)
  __transferDB = None
  # # time scale for throughput
  __throughputTimescale = 3600
  # # exectuon modes
  __executionMode = { "Tasks" : True, "FTS" : False }

  def __init__( self, *args, **kwargs ):
    """ c'tor

    :param self: self reference
    :param str agentName: agent name
    :param str loadName: module name
    :param str baseAgentName: base agent name
    :param dict properties: whatever else properties
    """
    self.setRequestType( "transfer" )
    self.setRequestTask( TransferTask )
    RequestAgentBase.__init__( self, *args, **kwargs )
    agentName = args[0]


    self.fc = FileCatalog()

    # # gMonitor stuff
    self.monitor.registerActivity( "Replicate and register", "Replicate and register operations",
                                   "TransferAgent", "Attempts/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Replicate", "Replicate operations", "TransferAgent",
                                   "Attempts/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Put and register", "Put and register operations",
                                   "TransferAgent", "Attempts/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Put", "Put operations",
                                   "TransferAgent", "Attempts/min", gMonitor.OP_SUM )

    self.monitor.registerActivity( "Replication successful", "Successful replications",
                                   "TransferAgent", "Successful/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Put successful", "Successful puts",
                                   "TransferAgent", "Successful/min", gMonitor.OP_SUM )

    self.monitor.registerActivity( "Replication failed", "Failed replications",
                                   "TransferAgent", "Failed/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Put failed", "Failed puts",
                                   "TransferAgent", "Failed/min", gMonitor.OP_SUM )

    self.monitor.registerActivity( "Replica registration successful", "Successful replica registrations",
                                   "TransferAgent", "Successful/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "File registration successful", "Successful file registrations",
                                   "TransferAgent", "Successful/min", gMonitor.OP_SUM )

    self.monitor.registerActivity( "Replica registration failed", "Failed replica registrations",
                                   "TransferAgent", "Failed/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "File registration failed", "Failed file registrations",
                                   "TransferAgent", "Failed/min", gMonitor.OP_SUM )

    # # tasks mode enabled by default
    self.__executionMode["Tasks"] = self.am_getOption( "TaskMode", True )
    self.log.info( "Tasks execution mode is %s." % { True : "enabled",
                                                     False : "disabled" }[ self.__executionMode["Tasks"] ] )
    # # but FTS only if requested
    self.__executionMode["FTS"] = self.am_getOption( "FTSMode", False )
    self.log.info( "FTS execution mode is %s." % { True : "enabled",
                                                   False : "disabled" }[ self.__executionMode["FTS"] ] )

    # # get TransferDB instance
    if self.__executionMode["FTS"]:
      transferDB = None
      try:
        transferDB = self.transferDB()
      except Exception, error:
        self.log.exception( error )
      if not transferDB:
        self.log.warn( "Can't create TransferDB instance, disabling FTS execution mode." )
        self.__executionMode["FTS"] = False
      else:
        # # throughptu time scale for monitoring in StrategyHandler
        self.__throughputTimescale = self.am_getOption( 'ThroughputTimescale', self.__throughputTimescale )
        self.log.info( "ThroughputTimescale = %s s" % str( self.__throughputTimescale ) )
        # # OwnerGroups not allowed to execute FTS transfers
        self.__ftsDisabledOwnerGroups = self.am_getOption( "FTSDisabledOwnerGroups", [ "lhcb_user" ] )
        self.log.info( "FTSDisabledOwnerGroups = %s" % self.__ftsDisabledOwnerGroups )
        # # cache for SEs
        self.seCache = {}

    # # is there any mode enabled?
    if True not in self.__executionMode.values():
      self.log.error( "TransferAgent misconfiguration, neither FTS nor Tasks execution mode is enabled." )
      raise TransferAgentError( "TransferAgent misconfiguration, neither FTS nor Tasks execution mode is enabled." )

    self.log.info( "%s has been constructed" % agentName )

  def finalize( self ):
    """ agent finalisation

    :param self: self reference
    """
    if self.hasProcessPool():
      self.processPool().finalize( timeout = self.poolTimeout() )
    self.resetAllRequests()
    return S_OK()

  ###################################################################################
  # facades for various DIRAC tools
  ###################################################################################


  def rssClient( self ):
    """ rss client getter """
    if not self.__rssClient:
      self.__rssClient = ResourceStatus()
    return self.__rssClient

  def storageFactory( self ):
    """ StorageFactory instance getter

    :param self: self reference
    """
    if not self.__storageFactory:
      self.__storageFactory = StorageFactory()
    return self.__storageFactory

  def transferDB( self ):
    """ TransferDB instance getter

    :warning: Need to put import over here, ONLINE hasn't got MySQLdb module.

    :param self: self reference
    """
    if not self.__transferDB:
      try:
        from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
        self.__transferDB = TransferDB()
      except Exception, error:
        self.log.error( "transferDB: unable to create TransferDB instance: %s" % str( error ) )
    return self.__transferDB

  ###################################################################################
  # strategy handler helper functions
  ###################################################################################
  def strategyHandler( self ):
    """ facade for StrategyHandler.

    :param self: self reference
    """
    if not self.__strategyHandler:
      try:
        self.__strategyHandler = StrategyHandler( self.configPath() )
      except SHGraphCreationError, error:
        self.log.exception( "strategyHandler: %s" % str( error ) )
    return self.__strategyHandler

  def setupStrategyHandler( self ):
    """Obtain information of current state of channel queues and throughput
    from TransferDB and setup StrategyHandler.

    :param self: self reference
    """
    res = self.transferDB().getChannelQueues()
    if not res["OK"]:
      errStr = "setupStrategyHandler: Failed to get channel queues from TransferDB."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    channels = res["Value"] or {}
    res = self.transferDB().getChannelObservedThroughput( self.__throughputTimescale )
    if not res["OK"]:
      errStr = "setupStrategyHandler: Failed to get observed throughput from TransferDB."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    bandwidths = res["Value"] or {}
    res = self.transferDB().getCountFileToFTS( self.__throughputTimescale, "Failed" )
    if not res["OK"]:
      errStr = "setupStrategyHandler: Failed to get Failed files counters from TransferDB."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    failedFiles = res["Value"] or {}
    # # neither channels nor bandwidths
    if not ( channels and bandwidths ):
      return S_ERROR( "setupStrategyHandler: No active channels found for replication" )
    self.strategyHandler().setup( channels, bandwidths, failedFiles )
    self.strategyHandler().reset()
    return S_OK()

  def checkSourceSE( self, sourceSE, lfn, catalogMetadata ):
    """ filter out SourceSEs where PFN is not existing or got wrong checksum

    :param self: self reference
    :param str lfn: logical file name
    :param dict catalogMetadata: catalog metadata
    """
    seRead = self.seRSSStatus( sourceSE, "ReadAccess" )
    if not seRead["OK"]:
      self.log.error( "checkSourceSE: %s" % seRead["Message"] )
      return S_ERROR( "%s in banned for reading right now" % sourceSE )
    else:
      if not seRead["Value"]:
        self.log.error( "checkSourceSE: StorageElement '%s' is banned for reading" % ( sourceSE ) )
        return S_ERROR( "%s in banned for reading right now" % sourceSE )
      se = self.seCache.get( sourceSE, None )
      if not se:
        se = StorageElement( sourceSE, "SRM2" )
        self.seCache[sourceSE] = se
      pfn = Utils.executeSingleFileOrDirWrapper( se.getPfnForLfn( lfn ) )
      if not pfn["OK"]:
        self.log.warn( "checkSourceSE: unable to create pfn for %s lfn: %s" % ( lfn, pfn["Message"] ) )
        return pfn
      pfn = pfn["Value"]
      seMetadata = se.getFileMetadata( pfn, singleFile = True )
      if not seMetadata["OK"]:
        self.log.warn( "checkSourceSE: %s" % seMetadata["Message"] )
        return S_ERROR( "checkSourceSE: failed to get metadata" )
      seMetadata = seMetadata["Value"]
      if not compareAdler( catalogMetadata["Checksum"], seMetadata["Checksum"] ):
        self.log.warn( "checkSourceSE: %s checksum mismatch catalogue:%s %s:%s" % ( lfn,
                                                                                    catalogMetadata["Checksum"],
                                                                                    sourceSE,
                                                                                    seMetadata["Checksum"] ) )
        return S_ERROR( "checkSourceSE: checksum mismatch" )
      # # if we're here everything is OK
      return S_OK()

  def seRSSStatus( self, se, status ):
    """ get se :status: from RSS for SE :se:

    :param str se: SE name
    :param str status: RSS status name
    """
    #rssStatus = self.rssClient().getStorageStatus( se, status )
    rssStatus = self.rssClient().isUsableStorage( se, status )
    return S_OK( rssStatus )
#    if not rssStatus["OK"]:
#      return S_ERROR( "unknown SE: %s" % se )
#    if rssStatus["Value"][se][status] == "Banned":
#      return S_OK( False )
#    return S_OK( True )

  @staticmethod
  def ancestorSortKeys( aDict, aKey = "Ancestor" ):
    """ sorting keys of replicationTree by its hopAncestor value

    replicationTree is a dict ( channelID : { ... }, (...) }

    :param self: self reference
    :param dict aDict: replication tree  to sort
    :param str aKey: a key in value dict used to sort
    """
    if False in [ bool( aKey in v ) for v in aDict.values() ]:
      return S_ERROR( "ancestorSortKeys: %s key in not present in all values" % aKey )
    # # put parents of all parents
    sortedKeys = [ k for k in aDict if aKey in aDict[k] and not aDict[k][aKey] ]
    # # get children
    pairs = dict( [ ( k, v[aKey] ) for k, v in aDict.items() if v[aKey] ] )
    while pairs:
      for key, ancestor in dict( pairs ).items():
        if key not in sortedKeys and ancestor in sortedKeys:
          sortedKeys.insert( sortedKeys.index( ancestor ), key )
          del pairs[key]
    # # need to revese this one, as we're instering child before its parent
    sortedKeys.reverse()
    if sorted( sortedKeys ) != sorted( aDict.keys() ):
      return S_ERROR( "ancestorSortKeys: cannot sort, some keys are missing!" )
    return S_OK( sortedKeys )

  ###################################################################################
  # SURL manipulation helpers
  ###################################################################################
  def getTransferURLs( self, lfn, repDict, replicas, ancestorSwap = None ):
    """ prepare TURLs for given LFN and replication tree

    :param self: self reference
    :param str lfn: LFN
    :param dict repDict: replication dictionary
    :param dict replicas: LFN replicas
    """

    hopSourceSE = repDict["SourceSE"]
    hopDestSE = repDict["DestSE"]
    hopAncestor = repDict["Ancestor"]

    if ancestorSwap and str( hopAncestor ) in ancestorSwap:
      self.log.debug( "getTransferURLs: swapping Ancestor %s with %s" % ( hopAncestor,
                                                                         ancestorSwap[str( hopAncestor )] ) )
      hopAncestor = ancestorSwap[ str( hopAncestor ) ]

    # # get targetSURL
    res = self.getSurlForLFN( hopDestSE, lfn )
    if not res["OK"]:
      errStr = res["Message"]
      self.log.error( errStr )
      return S_ERROR( errStr )
    targetSURL = res["Value"]

    # get the sourceSURL
    if hopAncestor:
      status = "Waiting%s" % ( hopAncestor )
      res = self.getSurlForLFN( hopSourceSE, lfn )
      if not res["OK"]:
        errStr = res["Message"]
        self.log.error( errStr )
        return S_ERROR( errStr )
      sourceSURL = res["Value"]
    else:
      status = "Waiting"
      res = self.getSurlForPFN( hopSourceSE, replicas[hopSourceSE] )
      if not res["OK"]:
        sourceSURL = replicas[hopSourceSE]
      else:
        sourceSURL = res["Value"]

    # # new status - Done or Done%d for TargetSURL = SourceSURL
    if targetSURL == sourceSURL:
      status = "Done"
      if hopAncestor:
        status = "Done%s" % hopAncestor

    return S_OK( ( sourceSURL, targetSURL, status ) )

  def getSurlForLFN( self, targetSE, lfn ):
    """ Get the targetSURL for the storage and LFN supplied.

    :param self: self reference
    :param str targetSURL: target SURL
    :param str lfn: LFN
    """
    res = self.storageFactory().getStorages( targetSE, protocolList = ["SRM2"] )
    if not res["OK"]:
      errStr = "getSurlForLFN: Failed to create SRM2 storage for %s: %s" % ( targetSE, res["Message"] )
      self.log.error( errStr )
      return S_ERROR( errStr )
    storageObjects = res["Value"]["StorageObjects"]
    for storageObject in storageObjects:
      res = storageObject.getCurrentURL( lfn )
      if res["OK"]:
        return res
    self.log.error( "getSurlForLFN: Failed to get SRM compliant storage.", targetSE )
    return S_ERROR( "getSurlForLFN: Failed to get SRM compliant storage." )

  def getSurlForPFN( self, sourceSE, pfn ):
    """Creates the targetSURL for the storage and PFN supplied.

    :param self: self reference
    :param str sourceSE: source storage element
    :param str pfn: phisical file name
    """
    res = StorageElement( sourceSE ).getPfnForProtocol( [pfn] )
    if not res["OK"]:
      return res
    if pfn in res["Value"]["Failed"]:
      return S_ERROR( res["Value"]["Failed"][pfn] )
    return S_OK( res["Value"]["Successful"][pfn] )

  ###################################################################################
  # FTS mode helpers
  ###################################################################################
  def collectFiles( self, requestObj, iSubRequest, status = 'Waiting' ):
    """ Get SubRequest files with status :status:, collect their replicas and metadata information from
    DataManager.

    :param self: self reference
    :param RequestContainer requestObj: request being processed
    :param int iSubRequest: SubRequest index
    :return: S_OK( (waitingFilesDict, replicas, metadata) ) or S_ERROR( errMsg )
    """

    waitingFiles = {}
    replicas = {}
    metadata = {}

    subRequestFiles = requestObj.getSubRequestFiles( iSubRequest, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.debug( "collectFiles: subrequest %s found with %d files." % ( iSubRequest,
                                                                           len( subRequestFiles ) ) )
    for subRequestFile in subRequestFiles:
      fileStatus = subRequestFile["Status"]
      fileLFN = subRequestFile["LFN"]
      if fileStatus != status:
        self.log.debug( "collectFiles: skipping %s file, status is '%s'" % ( fileLFN, fileStatus ) )
        continue
      else:
        waitingFiles.setdefault( fileLFN, subRequestFile["FileID"] )

    if waitingFiles:
      replicas = self.dm.getActiveReplicas( waitingFiles.keys() )
      if not replicas["OK"]:
        self.log.error( "collectFiles: failed to get replica information", replicas["Message"] )
        return replicas
      for lfn, failure in replicas["Value"]["Failed"].items():
        self.log.error( "collectFiles: Failed to get replicas %s: %s" % ( lfn, failure ) )
      replicas = replicas["Value"]["Successful"]

      if replicas:
        metadata = self.fc.getFileMetadata( replicas )
        if not metadata["OK"]:
          self.log.error( "collectFiles: failed to get file size information", metadata["Message"] )
          return metadata
        for lfn, failure in metadata["Value"]["Failed"].items():
          self.log.error( "collectFiles: failed to get file size %s: %s" % ( lfn, failure ) )
        metadata = metadata["Value"]["Successful"]

    self.log.debug( "collectFiles: waitingFiles=%d replicas=%d metadata=%d" % ( len( waitingFiles ),
                                                                                len( replicas ),
                                                                                len( metadata ) ) )

    toRemove = []
    for waitingFile in waitingFiles:
      validSourceSEs = {}
      downTime = False
      for replicaSE, sURL in replicas.get( waitingFile, {} ).items():
        checkSourceSE = self.checkSourceSE( replicaSE, waitingFile, metadata.get( waitingFile, {} ) )
        if checkSourceSE["OK"]:
          validSourceSEs[replicaSE] = sURL
        elif "banned for reading" in checkSourceSE["Message"]:
          downTime = True
      # # no valid replicas
      if not validSourceSEs:
        # # but there is a downtime at some of sourceSEs, skip this file rigth now
        if downTime:
          self.log.warn( "collectFiles: cannot find valid sources for %s at the moment" % waitingFile )
          continue
        # # no downtime on SEs, no valid sourceSEs at all, mark transfer of this file as failed
        self.log.error( "collectFiles: valid sources not found for %s, marking file as 'Failed'" % waitingFile )
        requestObj.setSubRequestFileAttributeValue( iSubRequest, "transfer", waitingFile,
                                                    "Status", "Failed" )
        requestObj.setSubRequestFileAttributeValue( iSubRequest, "transfer", waitingFile,
                                                    "Error", "valid source not found" )
        toRemove.append( waitingFile )
        if waitingFile in replicas:
          del replicas[waitingFile]
        if waitingFile in metadata:
          del metadata[waitingFile]
      else:
        replicas[waitingFile] = validSourceSEs

    # # fileter out not valid files
    waitingFiles = dict( [ ( k, v ) for k, v in waitingFiles.items() if k not in toRemove ] )

    return S_OK( ( waitingFiles, replicas, metadata ) )








  def initialize( self ):
    """ agent's initialization """

    # # data manager
    self.dm = DataManager()

    return S_OK()







  ###################################################################################
  # agent execution
  ###################################################################################
  def execute( self ):
    """ agent execution in one cycle

    :param self: self reference
    """
    requestCounter = self.requestsPerCycle()
    failback = False

    strategyHandlerSetupError = False
    if self.__executionMode["FTS"]:
      self.log.info( "execute: will setup StrategyHandler for FTS scheduling..." )
      self.__throughputTimescale = self.am_getOption( 'ThroughputTimescale', self.__throughputTimescale )
      self.log.debug( "execute: ThroughputTimescale = %s s" % str( self.__throughputTimescale ) )
      # # setup strategy handler
      setupStrategyHandler = self.setupStrategyHandler()
      if not setupStrategyHandler["OK"]:
        self.log.error( setupStrategyHandler["Message"] )
        self.log.error( "execute: disabling FTS scheduling in this cycle..." )
        strategyHandlerSetupError = True

    # # loop over requests
    while requestCounter:
      failback = strategyHandlerSetupError if strategyHandlerSetupError else False
      requestDict = self.getRequest( "transfer" )
      if not requestDict["OK"]:
        self.log.error( "execute: error when getteing 'transfer' request: %s" % requestDict["Message"] )
        return requestDict
      if not requestDict["Value"]:
        self.log.info( "execute: no more 'Waiting' requests found in RequestDB" )
        return S_OK()
      requestDict = requestDict["Value"]

      self.log.info( "execute: processing request (%d) %s" % ( self.requestsPerCycle() - requestCounter + 1,
                                                               requestDict["requestName"] ) )

      # # FTS scheduling
      if self.__executionMode["FTS"] and not failback:
        self.log.debug( "execute: using FTS" )
        executeFTS = self.executeFTS( requestDict )
        if not executeFTS["OK"]:
          self.log.error( executeFTS["Message"] )
          failback = True
        elif executeFTS["OK"]:
          if executeFTS["Value"]:
            self.log.debug( "execute: request %s has been processed in FTS" % requestDict["requestName"] )
            requestCounter = requestCounter - 1
            self.deleteRequest( requestDict["requestName"] )
            continue
          else:
            failback = True

      # # failback
      if failback and not self.__executionMode["Tasks"]:
        self.log.error( "execute: not able to process %s request" % requestDict["requestName"] )
        self.log.error( "execute: FTS scheduling has failed and Task mode is disabled" )
        # # put request back to RequestClient
        res = self.requestClient().updateRequest( requestDict["requestName"], requestDict["requestString"] )
        if not res["OK"]:
          self.log.error( "execute: failed to update request %s: %s" % ( requestDict["requestName"],
                                                                         res["Message"] ) )
        # # delete it from requestHolder
        self.deleteRequest( requestDict["requestName"] )
        # # decrease request counter
        requestCounter = requestCounter - 1
        continue

      # # Task execution
      if self.__executionMode["Tasks"]:
        self.log.debug( "execute: using TransferTask" )
        res = self.executeTask( requestDict )
        if res["OK"]:
          requestCounter = requestCounter - 1
          continue

    return S_OK()

  def executeFTS( self, requestDict ):
    """ execute Request in FTS mode

    :return: S_ERROR on error in scheduling, S_OK( False ) for failover tasks and S_OK( True )
    if scheduling was OK

    :param self: self reference
    :param dict requestDict: request dictionary
    """
    requestObj = RequestContainer( requestDict["requestString"] )
    requestDict["requestObj"] = requestObj

    # # check request owner
    ownerGroup = requestObj.getAttribute( "OwnerGroup" )
    if ownerGroup["OK"] and ownerGroup["Value"] in self.__ftsDisabledOwnerGroups:
      self.log.info( "excuteFTS: request %s OwnerGroup=%s is banned from FTS" % ( requestDict["requestName"],
                                                                                 ownerGroup["Value"] ) )
      return S_OK( False )

    # # check operation
    res = requestObj.getNumSubRequests( "transfer" )
    if not res["OK"]:
      self.log.error( "executeFTS: failed to get number of 'transfer' subrequests", res["Message"] )
      return S_OK( False )
    numberRequests = res["Value"]
    for iSubRequest in range( numberRequests ):
      subAttrs = requestObj.getSubRequestAttributes( iSubRequest, "transfer" )["Value"]
      status = subAttrs["Status"]
      operation = subAttrs["Operation"]
      if status == "Waiting" and operation != "replicateAndRegister":
        self.log.error( "executeFTS: operation %s for subrequest %s is not supported in FTS mode" % ( operation,
                                                                                                     iSubRequest ) )
        return S_OK( False )

    schedule = self.schedule( requestDict )
    if schedule["OK"]:
      self.log.debug( "executeFTS: request %s has been processed" % requestDict["requestName"] )
    else:
      self.log.error( schedule["Message"] )
      return schedule

    return S_OK( True )

  def executeTask( self, requestDict ):
    """ create and queue task into the processPool

    :param self: self reference
    :param dict requestDict: requestDict
    """
    # # add confing path
    requestDict["configPath"] = self.configPath()
    # # remove requestObj
    if "requestObj" in requestDict:
      del requestDict["requestObj"]

    taskID = requestDict["requestName"]
    while True:
      if not self.processPool().getFreeSlots():
        self.log.info( "executeTask: no free slots available in pool, will wait 2 seconds to proceed..." )
        time.sleep( 2 )
      else:
        self.log.info( "executeTask: spawning task %s for request %s" % ( taskID, taskID ) )
        enqueue = self.processPool().createAndQueueTask( TransferTask,
                                                         kwargs = requestDict,
                                                         taskID = taskID,
                                                         blocking = True,
                                                         usePoolCallbacks = True,
                                                         timeOut = self.taskTimeout() )
        if not enqueue["OK"]:
          self.log.error( enqueue["Message"] )
        else:
          self.log.info( "executeTask: successfully enqueued request %s" % taskID )
          # # task created, a little time kick to proceed
          time.sleep( 0.2 )
          break

    return S_OK()

  ###################################################################################
  # FTS scheduling
  ###################################################################################
  def schedule( self, requestDict ):
    """ scheduling files for FTS

    here requestDict
    requestDict = { "requestString" : str,
                    "requestName" : str,
                    "sourceServer" : str,
                    "executionOrder" : int,
                    "jobID" : int,
                    "requestObj" : RequestContainer }

    :param self: self reference
    :param dict requestDict: request dictionary
    """

    requestObj = requestDict["requestObj"]
    requestName = requestDict["requestName"]

    res = requestObj.getNumSubRequests( "transfer" )
    if not res["OK"]:
      self.log.error( "schedule: Failed to get number of 'transfer' subrequests", res["Message"] )
      return S_ERROR( "schedule: Failed to get number of 'transfer' subrequests" )
    numberRequests = res["Value"]
    self.log.debug( "schedule: request '%s' has got %s 'transfer' subrequest(s)" % ( requestName,
                                                                                     numberRequests ) )
    for iSubRequest in range( numberRequests ):
      self.log.info( "schedule: treating subrequest %s from '%s'" % ( iSubRequest,
                                                                      requestName ) )
      subAttrs = requestObj.getSubRequestAttributes( iSubRequest, "transfer" )["Value"]

      subRequestStatus = subAttrs["Status"]

      execOrder = int( subAttrs["ExecutionOrder"] ) if "ExecutionOrder" in subAttrs else 0
      if execOrder != requestDict["executionOrder"]:
        strTup = ( iSubRequest, execOrder, requestDict["executionOrder"] )
        self.log.warn( "schedule: skipping (%s) subrequest, exeOrder (%s) != request's exeOrder (%s)" % strTup )
        continue

      if subRequestStatus != "Waiting" :
        # # sub-request is already in terminal state
        self.log.info( "schedule: subrequest %s status is '%s', it won't be executed" % ( iSubRequest,
                                                                                          subRequestStatus ) )
        continue

      # # check already replicated files
      checkReadyReplicas = self.checkReadyReplicas( requestObj, iSubRequest, subAttrs )
      if not checkReadyReplicas["OK"]:
        self.log.error( "schedule: %s" % checkReadyReplicas["Message"] )
        continue
      requestObj = checkReadyReplicas["Value"]

      # # failover registration (file has been transfered but registration failed)
      registerFiles = self.registerFiles( requestObj, iSubRequest )
      if not registerFiles["OK"]:
        self.log.error( "schedule: %s" % registerFiles["Message"] )
        continue
      # # get modified request obj
      requestObj = registerFiles["Value"]

      # # get subrequest files, filer not-Done
      subRequestFiles = requestObj.getSubRequestFiles( iSubRequest, "transfer" )
      if not subRequestFiles["OK"]:
        return subRequestFiles
      subRequestFiles = subRequestFiles["Value"]
      # # collect not done LFNs
      notDoneLFNs = []
      for subRequestFile in subRequestFiles:
        status = subRequestFile["Status"]
        if status != "Done":
          notDoneLFNs.append( subRequestFile["LFN"] )

      subRequestEmpty = requestObj.isSubRequestEmpty( iSubRequest, "transfer" )
      subRequestEmpty = subRequestEmpty["Value"] if "Value" in subRequestEmpty else False

      # # schedule files, some are still in Waiting State
      if not subRequestEmpty:
        scheduleFiles = self.scheduleFiles( requestObj, iSubRequest, subAttrs )
        if not scheduleFiles["OK"]:
          self.log.error( "schedule: %s" % scheduleFiles["Message"] )
          continue
        # # get modified request obj
        requestObj = scheduleFiles["Value"]
      elif notDoneLFNs:
        # # maybe some are not Done yet?
        self.log.info( "schedule: not-Done files found in subrequest" )
      else:
        # # nope, all Done or no Waiting found
        self.log.debug( "schedule: subrequest %d is empty" % iSubRequest )
        self.log.debug( "schedule: setting subrequest %d status to 'Done'" % iSubRequest )
        requestObj.setSubRequestStatus( iSubRequest, "transfer", "Done" )

      # # check if all files are in 'Done' status
      subRequestDone = requestObj.isSubRequestDone( iSubRequest, "transfer" )
      subRequestDone = subRequestDone["Value"] if "Value" in subRequestDone else False
      # # all files Done, make this subrequest Done too
      if subRequestDone:
        self.log.info( "schedule: subrequest %s is done" % iSubRequest )
        self.log.debug( "schedule: setting subrequest %d status to 'Done'" % iSubRequest )
        requestObj.setSubRequestStatus( iSubRequest, "transfer", "Done" )

    # # update Request in DB after operation
    # # if all subRequests are statuses = Done,
    # # this will also set the Request status to Done
    requestString = requestObj.toXML()["Value"]
    res = self.requestClient().updateRequest( requestName, requestString )
    if not res["OK"]:
      self.log.error( "schedule: failed to update request", "%s %s" % ( requestName, res["Message"] ) )
      return res

    # # finalisation only if jobID is set
    if requestDict["jobID"]:
      requestStatus = self.requestClient().getRequestStatus( requestName )
      if not requestStatus["OK"]:
        self.log.error( "schedule: failed to get request status", "%s %s" % ( requestName, requestStatus["Message"] ) )
        return requestStatus
      requestStatus = requestStatus["Value"]
      self.log.debug( "schedule: requestStatus is %s" % requestStatus )
      # # ...and request status == 'Done' (or subrequests statuses not in Waiting or Assigned
      if ( requestStatus["SubRequestStatus"] not in ( "Waiting", "Assigned" ) ) and \
            ( requestStatus["RequestStatus"] == "Done" ):
        self.log.info( "schedule: will finalize request: %s" % requestName )
        finalize = self.requestClient().finalizeRequest( requestName, requestDict["jobID"] )
        if not finalize["OK"]:
          self.log.error( "schedule: error in request finalization: %s" % finalize["Message"] )
          return finalize

    return S_OK()

  def scheduleFiles( self, requestObj, index, subAttrs ):
    """ schedule files for subrequest :index:

    :param self: self reference
    :param index: subrequest index
    :param RequestContainer requestObj: request being processed
    :param dict subAttrs: subrequest's attributes
    """
    self.log.debug( "scheduleFiles: FTS scheduling, processing subrequest %s" % index )
    # # get target SEs, no matter what's a type we need a list
    targetSEs = [ targetSE.strip() for targetSE in subAttrs["TargetSE"].split( "," ) if targetSE.strip() ]
    # # get replication strategy
    operation = subAttrs["Operation"]
    strategy = { False : None,
                 True: operation }[ operation in self.strategyHandler().getSupportedStrategies() ]

    # # get subrequest files
    subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.debug( "scheduleFiles: found %s files" % len( subRequestFiles ) )
    # # collect not done LFNS
    notDoneLFNs = []
    for subRequestFile in subRequestFiles:
      status = subRequestFile["Status"]
      if status != "Done":
        notDoneLFNs.append( subRequestFile["LFN"] )

    # # get subrequest files
    self.log.debug( "scheduleFiles: obtaining 'Waiting' files for %d subrequest" % index )
    files = self.collectFiles( requestObj, index, status = "Waiting" )
    if not files["OK"]:
      self.log.debug( "scheduleFiles: failed to get 'Waiting' files from subrequest", files["Message"] )
      return files

    waitingFiles, replicas, metadata = files["Value"]

    if not waitingFiles:
      self.log.debug( "scheduleFiles: not 'Waiting' files found in this subrequest" )
      return S_OK( requestObj )

    if not replicas or not metadata:
      return S_ERROR( "replica or metadata info is missing" )

    # # loop over waiting files, get replication tree
    for waitingFileLFN, waitingFileID in sorted( waitingFiles.items() ):

      self.log.info( "scheduleFiles: processing file FileID=%s LFN=%s" % ( waitingFileID, waitingFileLFN ) )

      waitingFileReplicas = [] if waitingFileLFN not in replicas else replicas[waitingFileLFN]
      if not waitingFileReplicas:
        self.log.warn( "scheduleFiles: no replica information available for LFN %s" % waitingFileLFN )
        continue
      waitingFileMetadata = None if waitingFileLFN not in metadata else metadata[waitingFileLFN]
      if not waitingFileMetadata:
        self.log.warn( "scheduleFiles: no metadata information available for LFN %s" % waitingFileLFN )
        continue
      waitingFileSize = None if not waitingFileMetadata else waitingFileMetadata["Size"]

      # # set target SEs for this file
      waitingFileTargets = [ targetSE for targetSE in targetSEs if targetSE not in waitingFileReplicas ]
      if not waitingFileTargets:
        self.log.info( "scheduleFiles: %s is replicated, setting its status to 'Done'" % waitingFileLFN )
        requestObj.setSubRequestFileAttributeValue( index, "transfer", waitingFileLFN, "Status", "Done" )
        continue

      self.log.info( "scheduleFiles: file %s size=%s replicas=%d targetSEs=%s" % ( waitingFileLFN,
                                                                                   waitingFileSize,
                                                                                   len( waitingFileReplicas ),
                                                                                   str( waitingFileTargets ) ) )
      # # get the replication tree at least
      tree = self.strategyHandler().replicationTree( waitingFileReplicas.keys(),
                                                     waitingFileTargets,
                                                     waitingFileSize,
                                                     strategy )
      if not tree["OK"]:
        self.log.warn( "scheduleFiles: file %s cannot be scheduled: %s" % ( waitingFileLFN, tree["Message"] ) )
        continue

      tree = tree["Value"]
      self.log.debug( "scheduleFiles: replicationTree: %s" % tree )

      # # sorting keys by hopAncestor
      sortedKeys = self.ancestorSortKeys( tree, "Ancestor" )
      if not sortedKeys["OK"]:
        self.log.warn( "scheduleFiles: unable to sort replication tree by Ancestor: %s" % sortedKeys["Message"] )
        sortedKeys = tree.keys()
      else:
        sortedKeys = sortedKeys["Value"]
      # # dict holding swap parent with child for same SURLs
      ancestorSwap = {}
      for channelID in sortedKeys:
        repDict = tree[channelID]
        self.log.debug( "scheduleFiles: Strategy=%s Ancestor=%s SrcSE=%s DesSE=%s" % ( repDict["Strategy"],
                                                                                       repDict["Ancestor"],
                                                                                       repDict["SourceSE"],
                                                                                       repDict["DestSE"] ) )
        transferURLs = self.getTransferURLs( waitingFileLFN, repDict, waitingFileReplicas )
        if not transferURLs["OK"]:
          return transferURLs
        sourceSURL, targetSURL, waitingFileStatus = transferURLs["Value"]

        # # save ancestor to swap
        if sourceSURL == targetSURL and waitingFileStatus.startswith( "Done" ):
          oldAncestor = str( channelID )
          newAncestor = waitingFileStatus[5:]
          ancestorSwap[ oldAncestor ] = newAncestor

        # # add file to channel
        res = self.transferDB().addFileToChannel( channelID,
                                                  waitingFileID,
                                                  repDict["SourceSE"],
                                                  sourceSURL,
                                                  repDict["DestSE"],
                                                  targetSURL,
                                                  waitingFileSize,
                                                  waitingFileStatus )
        if not res["OK"]:
          self.log.error( "scheduleFiles: failed to add file to channel" , "%s %s" % ( str( waitingFileID ),
                                                                                       str( channelID ) ) )
          return res
        # # add file registration
        res = self.transferDB().addFileRegistration( channelID,
                                                     waitingFileID,
                                                     waitingFileLFN,
                                                     targetSURL,
                                                     repDict["DestSE"] )
        if not res["OK"]:
          errStr = res["Message"]
          self.log.error( "scheduleFiles: failed to add File registration", "%s %s" % ( waitingFileID,
                                                                                        channelID ) )
          result = self.transferDB().removeFileFromChannel( channelID, waitingFileID )
          if not result["OK"]:
            errStr += result["Message"]
            self.log.error( "scheduleFiles: failed to remove file from channel" , "%s %s" % ( waitingFileID,
                                                                                              channelID ) )
            return S_ERROR( errStr )
        # # add replication tree
        res = self.transferDB().addReplicationTree( waitingFileID, tree )
        if not res["OK"]:
          self.log.error( "schedule: error adding replication tree for file %s: %s" % ( waitingFileLFN,
                                                                                       res["Message"] ) )
          continue

      # # update File status to 'Scheduled'
      requestObj.setSubRequestFileAttributeValue( index, "transfer",
                                                  waitingFileLFN, "Status", "Scheduled" )
      self.log.info( "scheduleFiles: %s has been scheduled for FTS" % waitingFileLFN )

    # # return modified requestObj
    return S_OK( requestObj )

  def checkReadyReplicas( self, requestObj, index, subAttrs ):
    """ check if Files are already replicated, mark thiose as Done

    :param self: self reference
    :param RequestContainer requestObj: request being processed
    :param index: subrequest index
    :param dict subAttrs: subrequest attributes
    """
    self.log.debug( "checkReadyReplicas: obtaining all files in %d subrequest" % index )
    # # get targetSEs
    targetSEs = [ targetSE.strip() for targetSE in subAttrs["TargetSE"].split( "," ) if targetSE.strip() ]
    # # get subrequest files
    subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.debug( "checkReadyReplicas: found %s files" % len( subRequestFiles ) )

    fileLFNs = []
    for subRequestFile in subRequestFiles:
      status = subRequestFile["Status"]
      if status not in ( "Done", "Failed" ):
        fileLFNs.append( subRequestFile["LFN"] )

    replicas = None
    if fileLFNs:
      self.log.debug( "checkReadyReplicas: got %s not-done files" % str( len( fileLFNs ) ) )
      replicas = self.fc.getReplicas( fileLFNs )
      if not replicas["OK"]:
        return replicas
      for lfn, failure in replicas["Value"]["Failed"].items():
        self.log.warn( "checkReadyReplicas: unable to get replicas for %s: %s" % ( lfn, str( failure ) ) )
        if re.search( "no such file or directory", str( failure ).lower() ) and status == "Scheduled":
          self.log.info( "checkReadyReplicas: %s cannot be transferred" % lfn )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", str( failure ) )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Status", "Failed" )
      replicas = replicas["Value"]["Successful"]

    # # are there any replicas?
    if replicas:
      for fileLFN in fileLFNs:
        self.log.debug( "checkReadyReplicas: processing file %s" % fileLFN )
        fileReplicas = [] if fileLFN not in replicas else replicas[fileLFN]
        fileTargets = [ targetSE for targetSE in targetSEs if targetSE not in fileReplicas ]
        if not fileTargets:
          self.log.info( "checkReadyReplicas: all transfers of %s are done" % fileLFN )
          requestObj.setSubRequestFileAttributeValue( index, "transfer", fileLFN, "Status", "Done" )
          continue
        else:
          self.log.debug( "checkReadyReplicas: file %s still needs to be replicated at %s" % ( fileLFN, fileTargets ) )

    return S_OK( requestObj )

  def registerFiles( self, requestObj, index ):
    """ failover registration for files in subrequest :index:

    :param self: self reference
    :param index: subrequest index
    :param RequestContainer requestObj: request being processed
    :param dict subAttrs: subrequest's attributes
    """
    self.log.debug( "registerFiles: failover registration, processing %s subrequest" % index )
    subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.debug( "registerFiles: found %s files" % len( subRequestFiles ) )
    for subRequestFile in subRequestFiles:
      status = subRequestFile["Status"]
      lfn = subRequestFile["LFN"]
      fileID = subRequestFile["FileID"]
      self.log.debug( "registerFiles: processing file FileID=%s LFN=%s Status=%s" % ( fileID, lfn, status ) )
      if status in ( "Waiting", "Scheduled" ):
        # # get failed to register [ ( PFN, SE, ChannelID ), ... ]
        toRegister = self.transferDB().getRegisterFailover( fileID )
        if not toRegister["OK"]:
          self.log.error( "registerFiles: %s" % toRegister["Message"] )
          return toRegister
        if not toRegister["Value"] or len( toRegister["Value"] ) == 0:
          self.log.debug( "registerFiles: no waiting registrations found for %s file" % lfn )
          continue
        # # loop and try to register
        toRegister = toRegister["Value"]
        for pfn, se, channelID in toRegister:
          self.log.info( "registerFiles: failover registration of %s to %s" % ( lfn, se ) )
          # # register replica now
          registerReplica = self.dm.registerReplica( ( lfn, pfn, se ) )
          if ( ( not registerReplica["OK"] )
               or ( not registerReplica["Value"] )
               or ( lfn in registerReplica["Value"]["Failed"] ) ):
            error = registerReplica["Message"] if "Message" in registerReplica else None
            if "Value" in registerReplica:
              if not registerReplica["Value"]:
                error = "RM call returned empty value"
              else:
                error = registerReplica["Value"]["Failed"][lfn]
            self.log.error( "registerFiles: unable to register %s at %s: %s" % ( lfn, se, error ) )
            return S_ERROR( error )
          elif lfn in registerReplica["Value"]["Successful"]:
            # # no other option, it must be in successfull
            register = self.transferDB().setRegistrationDone( channelID, [ fileID ] )
            if not register["OK"]:
              self.log.error( "registerFiles: set status error %s fileID=%s channelID=%s: %s" % ( lfn,
                                                                                                 fileID,
                                                                                                 channelID,
                                                                                                 register["Message"] ) )
              return register

    return S_OK( requestObj )
