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
    - scheduling for FTS with failback TransferTask functionality
   
    The failback mechanism is fired in case that:

    - FTS channels between SourceSE and TargetSE is not defined 
    - there is a trouble to define correct replication tree 
    - request's owner is different from DataManager   
"""

__RCSID__ = "$Id$"

## imports
import time
import re
import random
## from DIRAC (globals and Core)
from DIRAC import gLogger, gMonitor, S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
## from DMS
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
## task to be executed
from DIRAC.DataManagementSystem.private.TransferTask import TransferTask
## from RMS
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
## from Resources
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
## from RSS
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

## agent name
AGENT_NAME = "DataManagement/TransferAgent"

class StrategyHandlerLocalFound( Exception ):
  """ 
  .. class:: StrategyHandlerLocalFound

  Exception trown to exit nested loops if local transfer has been found.
  """
  def __init__(self, localSource ):
    """c'tor

    :param self: self reference
    :param tuple localSource: local source tuple
    """
    Exception.__init__( self )
    self.localSource = localSource
  
  def __str__( self ):
    """str operator

    :param self: self reference
    """
    return "local source %s found" % str( self.localSource )
    
class StrategyHandlerChannelNotDefined( Exception ):
  """
  .. class:: StrategyHandlerChannelNotDefined
  
  Exception thrown when FTS channel between two sites is not defined.
  """
  def __init__( self, channelName ):
    """c'tor

    :param self: self reference
    :param str channelName: name of undefined channel 
    """
    Exception.__init__(self)
    self.channelName = channelName

  def __str__( self ):
    """ str operator 
    
    :param self: self reference
    """
    return "Failed to determine replication tree, channel %s is not defined" % self.channelName

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
  * request type
    RequestType = transfer
  * proxy
    shifterProxy = DataManager
  * task executing
    Taskmode = True
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
  ## placeholder for ReplicaManager instance
  __replicaManager = None
  ## placeholder for StorageFactory instance
  __storageFactory = None
  ## placeholder for StrategyHandler instance
  __strategyHandler = None
  ## placeholder for TransferDB instance (for FTS mode)
  __transferDB = None
  ## time scale for throughput
  __throughputTimescale = 3600
  ## exectuon modes
  __executionMode = { "Tasks" : True, "FTS" : False }

  def __init__( self, agentName, baseAgentName=False, properties=dict() ):
    """ c'tor
     
    :param self: self reference
    :param str agentName: agent name
    :param str baseAgentName: base agent name
    :param dict properties: whatever else properties
    """
    RequestAgentBase.__init__( self, agentName, baseAgentName, properties )

    ## gMonitor stuff
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

    ## tasks mode enabled by default
    self.__executionMode["Tasks"] = self.am_getOption( "TaskMode", True )
    self.log.info( "Tasks execution mode is %s." % { True : "enabled", 
                                                     False : "disabled" }[ self.__executionMode["Tasks"] ] )
    ## but FTS only if requested
    self.__executionMode["FTS"] = self.am_getOption( "FTSMode", False )
    self.log.info( "FTS execution mode is %s." % { True : "enabled", 
                                                   False : "disabled" }[ self.__executionMode["FTS"] ] )

    ## get TransferDB instance 
    if self.__executionMode["FTS"]:
      transferDB = None
      try:
        transferDB = self.transferDB()
      except Exception, error:
        self.log.exception( error )
      if not transferDB:
        self.log.warn("Can't create TransferDB instance, disabling FTS execution mode.")
        self.__executionMode["FTS"] = False
      else:
        ## throughptu time scale for monitoring in StrategyHandler
        self.__throughputTimescale = self.am_getOption( 'ThroughputTimescale', self.__throughputTimescale )
        self.log.info( "ThroughputTimescale = %s s" % str( self.__throughputTimescale ) )
        ## OwnerGroups not allowed to execute FTS transfers
        self.__ftsDisabledOwnerGroups = self.am_getOption( "FTSDisabledOwnerGroups", [ "lhcb_user" ] )
        self.log.info("FTSDisabledOwnerGroups = %s" %  self.__ftsDisabledOwnerGroups )

    ## is there any mode enabled?
    if True not in self.__executionMode.values():
      self.log.error("TransferAgent misconfiguration, neither FTS nor Tasks execution mode is enabled.")
      raise TransferAgentError("TransferAgent misconfiguration, neither FTS nor Tasks execution mode is enabled.")

    self.log.info("%s has been constructed" % agentName )

  ###################################################################################
  # facades for various DIRAC tools
  ###################################################################################
  def replicaManager( self ):
    """ ReplicaManager instance getter 

    :param self: self reference
    """
    if not self.__replicaManager:
      self.__replicaManager = ReplicaManager()
    return self.__replicaManager

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
        self.log.error( "transferDB: unable to create TransferDB instance: %s" % str(error) )        
    return self.__transferDB

  ###################################################################################
  # strategy handler helper functions 
  ###################################################################################
  def strategyHandler( self ):
    """ facade for StrategyHandler. 

    :param self: self reference
    """
    if not self.__strategyHandler:
      self.__strategyHandler = StrategyHandler( self.configPath() )
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
    channels = res["Value"] or False

    res = self.transferDB().getChannelObservedThroughput( self.__throughputTimescale )
    if not res["OK"]:
      errStr = "setupStrategyHandler: Failed to get observed throughput from TransferDB."
      self.log.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    bandwidths = res["Value"] or False

    ## neither channels nor bandwidths 
    if not ( channels and bandwidths ):
      return S_ERROR( "setupStrategyHandler: No active channels found for replication" )
    
    self.strategyHandler().setBandwiths( bandwidths )
    self.strategyHandler().setChannels( channels )
    self.strategyHandler().reset()

    return S_OK()

  def ancestorSortKeys( self, aDict, aKey="Ancestor" ):
    """ sorting keys of replicationTree by its hopAncestor value 
    
    replicationTree is a dict ( channelID : { ... }, (...) }
    
    :param self: self reference
    :param dict aDict: replication tree  to sort
    :param str aKey: a key in value dict used to sort 
    """
    if False in [ bool(aKey in v) for v in aDict.values() ]:
      return S_ERROR( "ancestorSortKeys: %s key in not present in all values" % aKey )
    ## put parents of all parents
    sortedKeys = [ k for k in aDict if aKey in aDict[k] and not aDict[k][aKey] ]
    ## get children
    pairs = dict( [ (  k, v[aKey] ) for k, v in aDict.items() if v[aKey] ] )
    while pairs:
      for key, ancestor in dict(pairs).items():
        if key not in sortedKeys and ancestor in sortedKeys:
          sortedKeys.insert( sortedKeys.index(ancestor), key )
          del pairs[key]
    ## need to revese this one, as we're instering child before its parent 
    sortedKeys.reverse()
    if sorted( sortedKeys ) != sorted( aDict.keys() ):
      return S_ERROR( "ancestorSortKeys: cannot sort, some keys are missing!")
    return S_OK( sortedKeys )

  ###################################################################################
  # SURL manipulation helpers 
  ###################################################################################
  def getTransferURLs( self, lfn, repDict, replicas, ancestorSwap=None ):
    """ prepare TURLs for given LFN and replication tree

    :param self: self reference
    :param str lfn: LFN
    :param dict repDict: replication dictionary
    :param dict replicas: LFN replicas 
    """

    hopSourceSE = repDict["SourceSE"]
    hopDestSE = repDict["DestSE"]
    hopAncestor = repDict["Ancestor"]

    if ancestorSwap and str(hopAncestor) in ancestorSwap:
      self.log.debug("getTransferURLs: swapping Ancestor %s with %s" % ( hopAncestor, 
                                                                         ancestorSwap[str(hopAncestor)] ) )
      hopAncestor = ancestorSwap[ str(hopAncestor) ]
    
    ## get targetSURL
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

    ## new status - Done or Done%d for TargetSURL = SourceSURL
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
    res = self.replicaManager().getPfnForProtocol( [pfn], sourceSE )
    if not res["OK"]:
      return res
    if pfn in res["Value"]["Failed"]:
      return S_ERROR( res["Value"]["Failed"][pfn] )
    return S_OK( res["Value"]["Successful"][pfn] )


  ###################################################################################
  # FTS mode helpers
  ###################################################################################
  def collectFiles( self, requestObj, iSubRequest, status='Waiting' ):
    """ Get SubRequest files with status :status:, collect their replicas and metadata information from 
    ReplicaManager.

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
    self.log.info( "collectFiles: subrequest %s found with %d files." % ( iSubRequest, len( subRequestFiles ) ) )
    
    for subRequestFile in subRequestFiles:
      fileStatus = subRequestFile["Status"]
      fileLFN = subRequestFile["LFN"]
      if fileStatus != status:
        self.log.debug("collectFiles: skipping %s file, status is '%s'" % ( fileLFN, fileStatus ) )
        continue
      else:
        waitingFiles.setdefault( fileLFN, subRequestFile["FileID"] )

    if waitingFiles:     
      replicas = self.replicaManager().getCatalogReplicas( waitingFiles.keys() )
      if not replicas["OK"]:
        self.log.error( "collectFiles: failed to get replica information", replicas["Message"] )
        return replicas
      for lfn, failure in replicas["Value"]["Failed"].items():
        self.log.error( "collectFiles: Failed to get replicas %s: %s" % ( lfn, failure ) )    
      replicas = replicas["Value"]["Successful"]

      if replicas:
        metadata = self.replicaManager().getCatalogFileMetadata( replicas.keys() )
        if not metadata["OK"]:
          self.log.error( "collectFiles: failed to get file size information", metadata["Message"] )
          return metadata
        for lfn, failure in metadata["Value"]["Failed"].items():
          self.log.error( "collectFiles: failed to get file size %s: %s" % ( lfn, failure ) )
        metadata = metadata["Value"]["Successful"] 
   
    self.log.info( "collectFiles: waitingFiles=%d replicas=%d metadata=%d" % ( len(waitingFiles), 
                                                                               len(replicas), 
                                                                               len(metadata) ) )
    
    if not ( len( waitingFiles ) == len( replicas ) == len( metadata ) ):
      self.log.warn( "collectFiles: Not all requested information available!" )
          
    return S_OK( ( waitingFiles, replicas, metadata ) )

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
      self.log.info( "execute: will setup StrategyHandler for FTS scheduling...")
      self.__throughputTimescale = self.am_getOption( 'ThroughputTimescale', self.__throughputTimescale )
      self.log.debug( "execute: ThroughputTimescale = %s s" % str( self.__throughputTimescale ) )
      ## setup strategy handler
      setupStrategyHandler = self.setupStrategyHandler()
      if not setupStrategyHandler["OK"]:
        self.log.error( setupStrategyHandler["Message"] )
        self.log.error( "execute: disabling FTS scheduling in this cycle...")
        strategyHandlerSetupError = True 

    ## loop over requests
    while requestCounter:
      failback = strategyHandlerSetupError if strategyHandlerSetupError else False
      requestDict = self.getRequest( "transfer" )
      if not requestDict["OK"]:
        self.log.error("execute: error when getteing 'transfer' request: %s" % requestDict["Message"] )
        return requestDict 
      if not requestDict["Value"]:
        self.log.info("execute: no more 'Waiting' requests found in RequestDB")
        return S_OK()
      requestDict = requestDict["Value"]

      self.log.info("execute: processing request (%d) %s" %  ( self.requestsPerCycle() - requestCounter + 1, 
                                                               requestDict["requestName"] ) )

      ## FTS scheduling
      if self.__executionMode["FTS"] and not failback:
        self.log.info("execute: about to process request using FTS")
        executeFTS = self.executeFTS( requestDict )
        if not executeFTS["OK"]:
          self.log.error( executeFTS["Message"] )
          failback = True
        elif executeFTS["OK"]:
          if executeFTS["Value"]:
            self.log.info("execute: request %s has been processed in FTS" % requestDict["requestName"] )
            requestCounter = requestCounter - 1
            self.deleteRequest( requestDict["requestName"] )
            continue 
          else:
            failback = True

      ## failback 
      if failback and not self.__executionMode["Tasks"]:
        self.log.error("execute: not able to process %s request" % requestDict["requestName"] )
        self.log.error("execute: FTS scheduling has failed and Task mode is disabled" ) 
        ## put request back to RequestClient
        res = self.requestClient().updateRequest( requestDict["requestName"], 
                                                  requestDict["requestString"], 
                                                  requestDict["sourceServer"] )
        if not res["OK"]:
          self.log.error( "execute: failed to update request %s: %s" % ( requestDict["requestName"], res["Message"] ) )
        ## delete it from requestHolder
        self.deleteRequest( requestDict["requestName"] )
        ## decrease request counter 
        requestCounter = requestCounter - 1
        continue

      ## Task execution
      if self.__executionMode["Tasks"]: 
        self.log.info("execute: about to process request using TransferTask")
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

    ## check request owner
    ownerGroup = requestObj.getAttribute( "OwnerGroup" )
    if ownerGroup["OK"] and ownerGroup["Value"] in self.__ftsDisabledOwnerGroups:
      self.log.info("excuteFTS: request %s OwnerGroup=%s is not allowed to execute FTS transfer" % ( requestDict["requestName"], 
                                                                                                     ownerGroup["Value"] ) )
      return S_OK( False )

    ## check request owner
    #ownerDN = requestObj.getAttribute( "OwnerDN" ) 
    #if ownerDN["OK"] and ownerDN["Value"]:
    #  self.log.info("excuteFTS: request %s has its owner %s, can't use FTS" % ( requestDict["requestName"], 
    #                                                                            ownerDN["Value"] ) )
    #  return S_OK( False )

    ## check operation
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
        self.log.error("executeFTS: operation %s for subrequest %s is not supported in FTS mode" % ( operation, 
                                                                                                     iSubRequest ) )
        return S_OK( False )
    
    try:
      schedule = self.schedule( requestDict )
      if schedule["OK"]:
        self.log.info("executeFTS: request %s has been processed" % requestDict["requestName"] )
      else:
        self.log.error( schedule["Message"] )
        return schedule 
    except StrategyHandlerChannelNotDefined, error:
      self.log.info( str(error) )
      return S_OK( False )
    
    return S_OK( True )
          
  def executeTask( self, requestDict ):
    """ create and queue task into the processPool

    :param self: self reference
    :param dict requestDict: requestDict
    """
    ## add confing path
    requestDict["configPath"] = self.configPath()
    ## remove requestObj
    if "requestObj" in requestDict:
      del requestDict["requestObj"]

    taskID = requestDict["requestName"]
    while True:
      if not self.processPool().getFreeSlots():
        self.log.info("executeTask: no free slots available in pool, will wait 2 seconds to proceed...")
        time.sleep( 2 )
      else:
        self.log.info("executeTask: spawning task %s for request %s" % ( taskID, taskID ) )
        enqueue = self.processPool().createAndQueueTask( TransferTask,
                                                         kwargs = requestDict,
                                                         taskID = taskID,
                                                         blocking = True,
                                                         usePoolCallbacks = True,
                                                         timeOut = 600 )
        if not enqueue["OK"]:
          self.log.error( enqueue["Message"] )
        else:
          self.log.info("executeTask: successfully enqueued request %s" % taskID )
          ## task created, a little time kick to proceed
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
                    "executionOrder" : list,
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
    self.log.info( "schedule: '%s' found with %s 'transfer' subrequest(s)" % ( requestName, 
                                                                               numberRequests ) )
    for iSubRequest in range( numberRequests ):
      self.log.info( "schedule: treating subrequest %s from '%s'." % ( iSubRequest, 
                                                                       requestName ) )
      subAttrs = requestObj.getSubRequestAttributes( iSubRequest, "transfer" )["Value"]
      subRequestStatus = subAttrs["Status"]

      if subRequestStatus != "Waiting" :
        ## sub-request is already in terminal state
        self.log.info( "schedule: subrequest %s status is '%s', it won't be executed" % ( iSubRequest, 
                                                                                          subRequestStatus ) )
        continue

      ## check already replicated files
      checkReadyReplicas = self.checkReadyReplicas( requestObj, iSubRequest, subAttrs )
      if not checkReadyReplicas["OK"]:
        self.log.error("schedule: %s" % checkReadyReplicas["Message"] )
        continue
      requestObj = checkReadyReplicas["Value"]

      ## failover registration (file has been transfered but registration failed)
      registerFiles = self.registerFiles( requestObj, iSubRequest )
      if not registerFiles["OK"]:
        self.log.error("schedule: %s" % registerFiles["Message"] )
        continue
      ## get modified request obj
      requestObj = registerFiles["Value"]

      ## get subrequest files, filer not-Done
      subRequestFiles = requestObj.getSubRequestFiles( iSubRequest, "transfer" )
      if not subRequestFiles["OK"]:
        return subRequestFiles
      subRequestFiles = subRequestFiles["Value"]
      ## collect not done LFNs
      notDoneLFNs = []
      for subRequestFile in subRequestFiles:
        status = subRequestFile["Status"]
        if status != "Done":
          notDoneLFNs.append( subRequestFile["LFN"] )

      subRequestEmpty = requestObj.isSubRequestEmpty( iSubRequest, "transfer" )
      subRequestEmpty = subRequestEmpty["Value"] if "Value" in subRequestEmpty else False

      ## schedule files, some are still in Waiting State
      if not subRequestEmpty:
        scheduleFiles = self.scheduleFiles( requestObj, iSubRequest, subAttrs )
        if not scheduleFiles["OK"]:
          self.log.error("schedule: %s" % scheduleFiles["Message"] )
          continue
        ## get modified request obj
        requestObj = scheduleFiles["Value"]
      elif notDoneLFNs:
        ## maybe some are not Done yet?
        self.log.info("schedule: not-Done files found in subrequest")
      else:
        ## nope, all Done or no Waiting found
        self.log.info("schedule: subrequest %d is empty" % iSubRequest )
        self.log.info("schedule: setting subrequest %d status to 'Done'" % iSubRequest )
        requestObj.setSubRequestStatus( iSubRequest, "transfer", "Done" )

      ## check if all files are in 'Done' status
      subRequestDone = requestObj.isSubRequestDone( iSubRequest, "transfer" )
      subRequestDone = subRequestDone["Value"] if "Value" in subRequestDone else False
      ## all files Done, make this subrequest Done too 
      if subRequestDone:
        self.log.info("schedule: subrequest %s is done" % iSubRequest )
        self.log.info("schedule: setting subrequest %d status to 'Done'" % iSubRequest )
        requestObj.setSubRequestStatus( iSubRequest, "transfer", "Done" )

    ## update Request in DB after operation 
    ## if all subRequests are statuses = Done, 
    ## this will also set the Request status to Done
    requestString = requestObj.toXML()["Value"]
    res = self.requestClient().updateRequest( requestName, requestString, requestDict["sourceServer"] )
    if not res["OK"]:
      self.log.error( "schedule: failed to update request", "%s %s" % ( requestName, res["Message"] ) )

    ## finalisation
    requestDone = requestObj.isRequestDone()
    requestDone = requestDone["Value"] if "Value" in requestDone else False
    ## fianlize request, it's status is Done
    if requestDict["jobID"] and requestDone:
      res = self.requestClient().finalizeRequest( requestName,  requestDict["jobID"], requestDict["sourceServer"] )
      if not res["OK"]:
        self.log.error( "schedule: unable to finalize request %s: %s " % ( requestName, res["Message"] ) )

    return S_OK()

  def scheduleFiles( self, requestObj, index, subAttrs ):
    """ schedule files for subrequest :index:

    :param self: self reference
    :param index: subrequest index
    :param RequestContainer requestObj: request being processed
    :param dict subAttrs: subrequest's attributes
    """
    self.log.info( "scheduleFiles: *** FTS scheduling ***")
    self.log.info( "scheduleFiles: processing subrequest %s" % index )
    ## get source SE
    sourceSE = subAttrs["SourceSE"] if subAttrs["SourceSE"] not in ( None, "None", "" ) else None
    ## get target SEs, no matter what's a type we need a list
    targetSEs = [ targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",") if targetSE.strip() ]
    ## get replication strategy
    operation = subAttrs["Operation"]
    strategy = { False : None, 
                 True: operation }[ operation in self.strategyHandler().getSupportedStrategies() ]

    ## get subrequest files
    subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.info( "scheduleFiles: found %s files" % len( subRequestFiles ) ) 
    ## collect not done LFNS
    notDoneLFNs = []
    for subRequestFile in subRequestFiles:
      status = subRequestFile["Status"]
      if status != "Done":
        notDoneLFNs.append( subRequestFile["LFN"] )

    ## get subrequest files  
    self.log.info( "scheduleFiles: obtaining 'Waiting' files for %d subrequest" % index )
    files = self.collectFiles( requestObj, index, status = "Waiting" )
    if not files["OK"]:
      self.log.debug("scheduleFiles: failed to get 'Waiting' files from subrequest", files["Message"] )
      return files

    waitingFiles, replicas, metadata = files["Value"]

    if not waitingFiles:
      self.log.info("scheduleFiles: not 'Waiting' files found in this subrequest" )
      return S_OK( requestObj )

    if not replicas or not metadata:
      return S_ERROR( "replica or metadata info is missing" )

    ## loop over waiting files, get replication tree 
    for waitingFileLFN, waitingFileID in sorted( waitingFiles.items() ):
        
      self.log.info("scheduleFiles: processing file FileID=%s LFN=%s" % ( waitingFileID, waitingFileLFN ) )

      waitingFileReplicas = [] if waitingFileLFN not in replicas else replicas[waitingFileLFN]
      if not waitingFileReplicas:
        self.log.warn("scheduleFiles: no replica information available for LFN %s" % waitingFileLFN )
        continue 
      waitingFileMetadata = None if waitingFileLFN not in metadata else metadata[waitingFileLFN]
      if not waitingFileMetadata:
        self.log.warn("scheduleFiles: no metadata information available for LFN %s" % waitingFileLFN )
        continue 
      waitingFileSize = None if not waitingFileMetadata else waitingFileMetadata["Size"]
        
      ## set target SEs for this file
      waitingFileTargets = [ targetSE for targetSE in targetSEs if targetSE not in waitingFileReplicas ]
      if not waitingFileTargets:
        self.log.info( "scheduleFiles: %s is present at all targets, setting its status to 'Done'" % waitingFileLFN )
        requestObj.setSubRequestFileAttributeValue( index, "transfer", waitingFileLFN, "Status", "Done" )
        continue
        
      self.log.info( "scheduleFiles: file %s size=%s replicas=%d targetSEs=%s" % ( waitingFileLFN, 
                                                                                   waitingFileSize, 
                                                                                   len(waitingFileReplicas), 
                                                                                   str(waitingFileTargets) ) ) 
      ## get the replication tree at least
      tree = self.strategyHandler().determineReplicationTree( sourceSE, 
                                                              waitingFileTargets, 
                                                              waitingFileReplicas,  
                                                              waitingFileSize, 
                                                              strategy )

      if not tree["OK"] or not tree["Value"]:
        self.log.error( "scheduleFiles: failed to determine replication tree", tree["Message"] if "Message" in tree else "replication tree is empty")
        raise StrategyHandlerChannelNotDefined( "FTS" )
      tree = tree["Value"]
      self.log.debug( "scheduleFiles: replicationTree: %s" % tree )
 
      ## sorting keys by hopAncestor
      sortedKeys = self.ancestorSortKeys( tree, "Ancestor" )
      if not sortedKeys["OK"]:
        self.log.warn( "scheduleFiles: unable to sort replication tree by Ancestor: %s"% sortedKeys["Message"] )
        sortedKeys = tree.keys()
      else:
        sortedKeys = sortedKeys["Value"]
      ## dict holding swap parent with child for same SURLs
      ancestorSwap = {} 
      for channelID in sortedKeys:
        repDict = tree[channelID]
        self.log.info( "scheduleFiles: processing channel %d %s" % ( channelID, str( repDict ) ) )
        transferURLs = self.getTransferURLs( waitingFileLFN, repDict, waitingFileReplicas )
        if not transferURLs["OK"]:
          return transferURLs
        sourceSURL, targetSURL, waitingFileStatus = transferURLs["Value"]

        ## save ancestor to swap
        if sourceSURL == targetSURL and waitingFileStatus.startswith( "Done" ):
          oldAncestor = str(channelID)            
          newAncestor = waitingFileStatus[5:]
          ancestorSwap[ oldAncestor ] = newAncestor

        ## add file to channel
        res = self.transferDB().addFileToChannel( channelID, 
                                                  waitingFileID, 
                                                  repDict["SourceSE"], 
                                                  sourceSURL, 
                                                  repDict["DestSE"], 
                                                  targetSURL, 
                                                  waitingFileSize, 
                                                  waitingFileStatus )
        if not res["OK"]:
          self.log.error( "scheduleFiles: failed to add file to channel" , "%s %s" % ( str(waitingFileID), 
                                                                                       str(channelID) ) )
          return res
        ## add file registration 
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
        ## add replication tree
        res = self.transferDB().addReplicationTree( waitingFileID, tree )
        if not res["OK"]:
          self.log.error("schedule: error adding replication tree for file %s: %s" % ( waitingFileLFN, 
                                                                                       res["Message"]) )
          continue

        ## update File status to 'Scheduled'
        requestObj.setSubRequestFileAttributeValue( index, "transfer", 
                                                    waitingFileLFN, "Status", "Scheduled" )
        self.log.info( "scheduleFiles: status of %s file set to 'Scheduled'" % waitingFileLFN )
  
    ## return modified requestObj 
    return S_OK( requestObj )


  def checkReadyReplicas( self, requestObj, index, subAttrs ):
    """ check if Files are already replicated, mark thiose as Done

    :param self: self reference
    :param RequestContainer requestObj: request being processed
    :param index: subrequest index
    :param dict subAttrs: subrequest attributes
    """
    self.log.info( "checkReadyReplicas: *** check done replications ***" )
    self.log.info( "checkReadyReplicas: obtaining all files in %d subrequest" % index )
    
    ## get targetSEs
    targetSEs = [ targetSE.strip() for targetSE in subAttrs["TargetSE"].split(",") if targetSE.strip() ]

    ## get subrequest files
    subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.info( "checkReadyReplicas: found %s files" % len( subRequestFiles ) ) 

    fileLFNs = []
    for subRequestFile in subRequestFiles:
      status = subRequestFile["Status"]
      if status != "Done":
        fileLFNs.append( subRequestFile["LFN"] )
  
    replicas = None
    if fileLFNs:      
      self.log.debug( "checkReadyReplicas: got %s not-done files" % str(len(fileLFNs) ) )
      replicas = self.replicaManager().getCatalogReplicas( fileLFNs )
      if not replicas["OK"]:
        return replicas
      for lfn, failure in replicas["Value"]["Failed"].items():
        self.log.warn( "checkReadyReplicas: unable to get replicas for %s: %s" % ( lfn, str(failure) ) )
        if re.search( "no such file or directory", str(failure).lower()):
          requestObj.setSubRequestFileAttributeValue( index, "transfer", lfn, "Error", str(failure) )
      replicas = replicas["Value"]["Successful"]

    ## are there any replicas?
    if replicas:
      for fileLFN in fileLFNs:
        self.log.debug( "checkReadyReplicas: processing file %s" % fileLFN )
        fileReplicas = [] if fileLFN not in replicas else replicas[fileLFN]
        fileTargets = [ targetSE for targetSE in targetSEs if targetSE not in fileReplicas ]
        if not fileTargets:
          self.log.info( "checkReadyReplicas: %s is present at all targets, setting its status to 'Done'" % fileLFN )
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
    self.log.info( "registerFiles: *** failover registration *** ")
    self.log.info( "registerFiles: obtaining all files in %d subrequest" % index )
    subRequestFiles = requestObj.getSubRequestFiles( index, "transfer" )
    if not subRequestFiles["OK"]:
      return subRequestFiles
    subRequestFiles = subRequestFiles["Value"]
    self.log.info( "registerFiles: found %s files" % len( subRequestFiles ) ) 
    for subRequestFile in subRequestFiles:
      status = subRequestFile["Status"]
      lfn = subRequestFile["LFN"]
      fileID = subRequestFile["FileID"]
      self.log.debug("registerFiles: processing file FileID=%s LFN=%s Status=%s" % ( fileID, lfn, status ) )
      if status in ( "Waiting", "Scheduled" ):
        ## get failed to register [ ( PFN, SE, ChannelID ), ... ] 
        toRegister = self.transferDB().getRegisterFailover( fileID )
        if not toRegister["OK"]:
          self.log.error( "registerFiles: %s" % toRegister["Message"] )
          return toRegister
        if not toRegister["Value"] or len(toRegister["Value"]) == 0:
          self.log.debug("registerFiles: no waiting registrations found for %s file" % lfn )
          continue
        ## loop and try to register
        toRegister = toRegister["Value"]
        for pfn, se, channelID in toRegister:
          self.log.info("registerFiles: failover registration of %s to %s" % ( lfn, se ) )
          ## register replica now
          registerReplica = self.replicaManager().registerReplica( ( lfn, pfn, se ) )
          if ( ( not registerReplica["OK"] ) 
               or ( not registerReplica["Value"] ) 
               or ( lfn in registerReplica["Value"]["Failed"] ) ):
            error = registerReplica["Message"] if "Message" in registerReplica else None 
            if "Value" in registerReplica:
              if not registerReplica["Value"]:
                error = "RM call returned empty value"
              else:
                error = registerReplica["Value"]["Failed"][lfn]
            self.log.error( "registerFiles: unable to register %s at %s: %s" %  ( lfn, se, error ) )
            return S_ERROR( error )
          elif lfn in registerReplica["Value"]["Successfull"]:
            ## no other option, it must be in successfull
            register = self.transferDB().setRegistrationDone( channelID, fileID )
            if not register["OK"]:
              self.log.error("registerFiles: set status error %s fileID=%s channelID=%s: %s" % ( lfn,
                                                                                                 fileID,
                                                                                                 channelID,
                                                                                                 register["Message"] ) )
              return register

    return S_OK( requestObj )

###################################################################################
# StrategyHandler helper class
###################################################################################
class StrategyHandler( object ):
  """
  .. class:: StrategyHandler

  StrategyHandler is a helper class for determining optimal replication tree for given
  source files, their replicas and target storage elements.
  """

  def __init__( self, configSection, bandwidths=None, channels=None ):
    """c'tor

    :param self: self reference
    :param str configSection: path on CS to ReplicationScheduler agent
    :param bandwithds: observed throughput on active channels
    :param channels: active channels
    """
    ## save config section
    self.configSection = configSection + "/" + self.__class__.__name__
    ## sublogger
    self.log = gLogger.getSubLogger( "StrategyHandler", child=True )
    self.log.setLevel( gConfig.getValue( self.configSection + "/LogLevel", "DEBUG"  ) )
  
    self.supportedStrategies = [ 'Simple', 'DynamicThroughput', 'Swarm', 'MinimiseTotalWait' ]
    self.log.debug( "Supported strategies = %s" % ", ".join( self.supportedStrategies ) )
  
    self.sigma = gConfig.getValue( self.configSection + '/HopSigma', 0.0 )
    self.log.debug( "HopSigma = %s" % self.sigma )
    self.schedulingType = gConfig.getValue( self.configSection + '/SchedulingType', 'File' )
    self.log.debug( "SchedulingType = %s" % self.schedulingType )
    self.activeStrategies = gConfig.getValue( self.configSection + '/ActiveStrategies', ['MinimiseTotalWait'] )
    self.log.debug( "ActiveStrategies = %s" % ", ".join( self.activeStrategies ) )
    self.numberOfStrategies = len( self.activeStrategies )
    self.log.debug( "Number of active strategies = %s" % self.numberOfStrategies )
    self.acceptableFailureRate = gConfig.getValue( self.configSection + '/AcceptableFailureRate', 75 )
    self.log.debug( "AcceptableFailureRate = %s" % self.acceptableFailureRate )
            
    self.bandwidths = bandwidths
    self.channels = channels
    self.chosenStrategy = 0

    # dispatcher
    self.strategyDispatcher = { re.compile("MinimiseTotalWait") : self.__minimiseTotalWait, 
                                re.compile("DynamicThroughput") : self.__dynamicThroughput,
                                re.compile("Simple") : self.__simple, 
                                re.compile("Swarm") : self.__swarm }

    self.resourceStatus = ResourceStatus()

    self.log.debug( "strategyDispatcher entries:" )
    for key, value in self.strategyDispatcher.items():
      self.log.debug( "%s : %s" % ( key.pattern, value.__name__ ) )

    self.log.debug("%s has been constructed" % self.__class__.__name__ )

  def reset( self ):
    """ reset :chosenStrategy: 

    :param self: self reference
    """
    self.chosenStrategy = 0

  def setBandwiths( self, bandwidths ):
    """ set the bandwidths 

    :param self: self reference
    :param bandwithds: observed througput of active FTS channels
    """
    self.bandwidths = bandwidths

  def setChannels( self, channels ):
    """ set the channels
    
    :param self: self reference
    :param channels: active channels queues
    """
    self.channels = channels 

  def getSupportedStrategies( self ):
    """ Get supported strategies.

    :param self: self reference
    """    
    return self.supportedStrategies

  def determineReplicationTree( self, sourceSE, targetSEs, replicas, size, strategy = None, sigma = None ):
    """ resolve and find replication tree given source and target storage elements, active replicas, 
    and file size.

    :param self: self reference
    :param str sourceSE: source storage element name
    :param list targetSEs: list of target storage elements
    :param dict replicas: active replicas
    :param int size: fiel size
    :param str strategy: strategy to use
    :param float sigma: hop sigma
    """
    if not strategy:
      strategy = self.__selectStrategy()
    self.log.debug( "determineReplicationTree: will use %s strategy"  % strategy )

    if sigma:
      self.log.debug( "determineReplicationTree: sigma = %s"  % sigma )
      self.sigma = sigma

    # For each strategy implemented an 'if' must be placed here 
    tree = {}
    for reStrategy in self.strategyDispatcher:
      self.log.debug( reStrategy.pattern )
      if reStrategy.search( strategy ):
        if "_" in strategy:
          try:
            self.sigma = float(strategy.split("_")[1])
            self.log.debug("determineReplicationTree: new sigma %s" % self.sigma )
          except ValueError:
            self.log.warn("determineRepliactionTree: can't set new sigma value from '%s'" % strategy )
        if reStrategy.pattern in [ "MinimiseTotalWait", "DynamicThroughput" ]:
          replicasToUse = replicas.keys() if sourceSE == None else [ sourceSE ]
          tree = self.strategyDispatcher[ reStrategy ].__call__( replicasToUse, targetSEs  )
        elif reStrategy.pattern == "Simple":
          if not sourceSE in replicas.keys():
            return S_ERROR( "File does not exist at specified source site" )
          tree = self.__simple( sourceSE, targetSEs )
        elif reStrategy.pattern == "Swarm":
          tree = self.__swarm( targetSEs[0], replicas.keys() )
      
    # Now update the queues to reflect the chosen strategies
    for channelID in tree:
      self.channels[channelID]["Files"] += 1
      self.channels[channelID]["Size"] += size

    return S_OK( tree )

  def __selectStrategy( self ):
    """ If more than one active strategy use one after the other.

    :param self: self reference
    """
    chosenStrategy = self.activeStrategies[self.chosenStrategy]
    self.chosenStrategy += 1
    if self.chosenStrategy == self.numberOfStrategies:
      self.chosenStrategy = 0
    return chosenStrategy

  def __simple( self, sourceSE, destSEs ):
    """ This just does a simple replication from the source to all the targets.

    :param self: self reference
    :param str sourceSE: source storage element name
    :param list destSEs: destination storage elements  
    """
    tree = {}
    if not self.__getActiveSEs( [ sourceSE ] ):
      return tree
    sourceSites = self.__getChannelSitesForSE( sourceSE )
    for destSE in destSEs:
      destSites = self.__getChannelSitesForSE( destSE )
      for channelID, channelDict in self.channels.items():
        if channelID in tree: 
          continue
        if channelDict["Source"] in sourceSites and channelDict["Destination"] in destSites:
          tree[channelID] = { "Ancestor" : False, 
                              "SourceSE" : sourceSE, 
                              "DestSE" : destSE,
                              "Strategy" : "Simple" }
    return tree

  def __swarm( self, destSE, replicas ):
    """ This strategy is to be used to the data the the target site as quickly as possible from any source.

    :param self: self reference
    :param str destSE: destination storage element
    :param list replicas: replicas dictionary keys
    """
    tree = {}
    res = self.__getTimeToStart()
    if not res["OK"]:
      self.log.error( res["Message"] )
      return tree
    channelInfo = res["Value"]
    minTimeToStart = float( "inf" )

    sourceSEs = self.__getActiveSEs( replicas )
    destSites = self.__getChannelSitesForSE( destSE )

    selectedChannelID = None
    selectedSourceSE = None
    selectedDestSE = None

    for destSite in destSites:
      for sourceSE in sourceSEs:
        for sourceSite in self.__getChannelSitesForSE( sourceSE ):
          channelName = "%s-%s" % ( sourceSite, destSite )
          if channelName not in channelInfo:
            errStr = "__swarm: Channel not defined"
            self.log.warn( errStr, channelName )
            continue
          channelTimeToStart = channelInfo[channelName]["TimeToStart"]
          if channelTimeToStart <= minTimeToStart:
            minTimeToStart = channelTimeToStart
            selectedSourceSE = sourceSE
            selectedDestSE = destSE
            selectedChannelID = channelInfo[channelName]["ChannelID"]
         
    if selectedChannelID and selectedSourceSE and selectedDestSE:
      tree[selectedChannelID] = { "Ancestor" : False,
                                  "SourceSE" : selectedSourceSE,
                                  "DestSE" : selectedDestSE,
                                  "Strategy" : "Swarm" }
    return tree

  def __dynamicThroughput( self, sourceSEs, destSEs ):
    """ This creates a replication tree based on observed throughput on the channels.

    :param self: self reference
    :param list sourceSEs: source storage elements names
    :param list destSEs: destination storage elements names
    """
    tree = {}
    res = self.__getTimeToStart()
    if not res["OK"]:
      self.log.error( res["Message"] )
      return tree
    channelInfo = res["Value"]

    timeToSite = {}   # Maintains time to site including previous hops
    siteAncestor = {} # Maintains the ancestor channel for a site

    while len( destSEs ) > 0:
      try:
        minTotalTimeToStart = float( "inf" )
        candidateChannels = []
        sourceActiveSEs = self.__getActiveSEs( sourceSEs )
        for destSE in destSEs:
          destSites = self.__getChannelSitesForSE( destSE )
          for destSite in destSites:
            for sourceSE in sourceActiveSEs:
              sourceSites = self.__getChannelSitesForSE( sourceSE )
              for sourceSite in sourceSites:
                channelName = "%s-%s" % ( sourceSite, destSite )
                if channelName not in channelInfo:
                  self.log.warn( "dynamicThroughput: bailing out! channel %s not defined " % channelName )
                  raise StrategyHandlerChannelNotDefined( channelName )

                channelID = channelInfo[channelName]["ChannelID"]
                if channelID in tree:
                  continue
                channelTimeToStart = channelInfo[channelName]["TimeToStart"]

                totalTimeToStart = channelTimeToStart
                if sourceSE in timeToSite:
                  totalTimeToStart += timeToSite[sourceSE] + self.sigma
                  
                if ( sourceSite == destSite ) :
                  selectedPathTimeToStart = totalTimeToStart
                  candidateChannels = [ ( sourceSE, destSE, channelID ) ]
                  raise StrategyHandlerLocalFound( candidateChannels )

                if totalTimeToStart < minTotalTimeToStart:
                  minTotalTimeToStart = totalTimeToStart
                  selectedPathTimeToStart = totalTimeToStart
                  candidateChannels = [ ( sourceSE, destSE, channelID ) ]
                elif totalTimeToStart == minTotalTimeToStart and totalTimeToStart < float("inf"):
                  minTotalTimeToStart = totalTimeToStart
                  selectedPathTimeToStart = totalTimeToStart
                  candidateChannels.append( ( sourceSE, destSE, channelID ) )
               
      except StrategyHandlerLocalFound:
        pass

      random.shuffle( candidateChannels )
      selectedSourceSE, selectedDestSE, selectedChannelID = candidateChannels[0]
      timeToSite[selectedDestSE] = selectedPathTimeToStart
      siteAncestor[selectedDestSE] = selectedChannelID
      
      waitingChannel = False if selectedSourceSE not in siteAncestor else siteAncestor[selectedSourceSE]
    
      tree[selectedChannelID] = { "Ancestor" : waitingChannel,
                                  "SourceSE" : selectedSourceSE,
                                  "DestSE" : selectedDestSE,
                                  "Strategy" : "DynamicThroughput" }
      sourceSEs.append( selectedDestSE )
      destSEs.remove( selectedDestSE )
    return tree

  def __minimiseTotalWait( self, sourceSEs, destSEs ):
    """ This creates a replication tree based on observed throughput on the channels.

    :param self: self reference
    :param list sourceSEs: source storage elements names
    :param list destSEs: destination storage elements names
    """

    self.log.debug( "sourceSEs = %s" % sourceSEs )
    self.log.debug( "destSEs = %s" % destSEs )
    
    tree = {}
    res = self.__getTimeToStart()
    if not res["OK"]:
      self.log.error( res["Message"] )
      return tree
    channelInfo = res["Value"]

    timeToSite = {}                # Maintains time to site including previous hops
    siteAncestor = {}              # Maintains the ancestor channel for a site
    primarySources = sourceSEs

    

    while destSEs:
      try:
        minTotalTimeToStart = float( "inf" )
        candidateChannels = []
        sourceActiveSEs = self.__getActiveSEs( sourceSEs )
        for destSE in destSEs:
          destSites = self.__getChannelSitesForSE( destSE )
          for destSite in destSites:
            for sourceSE in sourceActiveSEs:
              sourceSites = self.__getChannelSitesForSE( sourceSE )
              for sourceSite in sourceSites:
                channelName = "%s-%s" % ( sourceSite, destSite )

                if channelName not in channelInfo:
                  continue
                
                channelID = channelInfo[channelName]["ChannelID"]
                # If this channel is already used, look for another sourceSE
                if channelID in tree:
                  continue
                channelTimeToStart = channelInfo[channelName]["TimeToStart"]
                if not sourceSE in primarySources:
                  channelTimeToStart += self.sigma
                ## local transfer found
                if sourceSite == destSite:
                  selectedPathTimeToStart = channelTimeToStart
                  candidateChannels = [ ( sourceSE, destSE, channelID ) ]
                  ## bail out to save rainforests
                  raise StrategyHandlerLocalFound( candidateChannels )
                if channelTimeToStart < minTotalTimeToStart:
                  minTotalTimeToStart = channelTimeToStart
                  selectedPathTimeToStart = channelTimeToStart
                  candidateChannels = [ ( sourceSE, destSE, channelID ) ]
                elif channelTimeToStart == minTotalTimeToStart and channelTimeToStart != float("inf"):
                  minTotalTimeToStart = channelTimeToStart
                  selectedPathTimeToStart = channelTimeToStart
                  candidateChannels.append( ( sourceSE, destSE, channelID ) )

      except StrategyHandlerLocalFound:
        pass

      if not candidateChannels:
        return tree
      
      ## shuffle candidates and pick the 1st one
      random.shuffle( candidateChannels )
      selectedSourceSE, selectedDestSE, selectedChannelID = candidateChannels[0]
      timeToSite[selectedDestSE] = selectedPathTimeToStart
      siteAncestor[selectedDestSE] = selectedChannelID
      waitingChannel = False if selectedSourceSE not in siteAncestor else siteAncestor[selectedSourceSE]

      tree[selectedChannelID] = { "Ancestor" : waitingChannel,
                                  "SourceSE" : selectedSourceSE,
                                  "DestSE" : selectedDestSE,
                                  "Strategy" : "MinimiseTotalWait" }
      sourceSEs.append( selectedDestSE )
      destSEs.remove( selectedDestSE )
      
    return tree

  def __getTimeToStart( self ):
    """ Generate the dictionary of times to start based on task queue contents and observed throughput.

    :param self: self reference
    """

    if self.schedulingType not in ( "File", "Throughput" ):
      errStr = "__getTimeToStart: CS SchedulingType entry must be either 'File' or 'Throughput'"
      self.log.error( errStr )
      return S_ERROR( errStr )

    channelInfo = {}
    for channelID, bandwidth in self.bandwidths.items():

      channelDict = self.channels[channelID] 
      channelName = channelDict["ChannelName"]

      # initial equal 0.0
      timeToStart = 0.0

      channelStatus = channelDict["Status"]

      ## channel is active?
      if channelStatus == "Active":
        
        channelFileSuccess = bandwidth["SuccessfulFiles"]
        channelFileFailed = bandwidth["FailedFiles"]
        attempted = channelFileSuccess + channelFileFailed

        successRate = 100.0
        if attempted != 0:
          successRate = 100.0 * ( channelFileSuccess / float( attempted ) )
        
        ## success rate too low?, make channel unattractive
        if successRate < self.acceptableFailureRate:
          timeToStart = float( "inf" ) 
        else:

          ## scheduling type == Throughput
          transferSpeed = bandwidth["Throughput"] 
          waitingTransfers = channelDict["Size"]

          ## scheduling type == File, overwrite transferSpeed and waitingTransfer
          if self.schedulingType == "File":
            transferSpeed = bandwidth["Fileput"] 
            waitingTransfers = channelDict["Files"]
            
          if transferSpeed > 0:
            timeToStart = waitingTransfers / float( transferSpeed )
            
      else:
        ## channel not active, make it unattractive
        timeToStart = float( "inf" ) 

      channelInfo.setdefault( channelName, { "ChannelID" : channelID, 
                                             "TimeToStart" : timeToStart } )

    return S_OK( channelInfo )

  def __getActiveSEs( self, seList, access = "Read" ):
    """Get active storage elements.

    :param self: self reference
    :param list seList: stogare element list
    :param str access: storage element accesss, could be 'Read' (default) or 'Write' 
    """
    res = self.resourceStatus.getStorageElementStatus( seList, statusType = access, default = 'Unknown' )
    if not res["OK"]:
      return []
    return [ k for k, v in res["Value"].items() if access in v and v[access] in ( "Active", "Bad" ) ]
   
  def __getChannelSitesForSE( self, storageElement ):
    """Get sites for given storage element.
    
    :param self: self reference
    :param str storageElement: storage element name
    """
    res = getSitesForSE( storageElement )
    if not res["OK"]:
      return []
    sites = []
    for site in res["Value"]:
      siteName = site.split( "." )
      if len( siteName ) > 1:
        if not siteName[1] in sites:
          sites.append( siteName[1] )
    return sites

