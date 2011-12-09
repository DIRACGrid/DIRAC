########################################################################
# $HeadURL $
# File: TransferAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/05/30 09:40:33
########################################################################
""" :mod: TransferAgent 
    ===================

    TransferAgent executes 'transfer' requests read from the RequestDB.
    
    This agent has two modes of operation:
    - standalone, when all Requests are handled using ProcessPool and TransferTask
    - scheduling for FTS with failback TransferTask functionality
   
    The failback mechanism is fired in case that:

    - FTS channels between SourceSE and TargetSE is not defined 
    - there is a trouble to define correct replication tree 
    - request's owner is different than DataManager
   
"""

__RCSID__ = "$Id$"

## imports
import time
import re
import random

## from DIRAC
from DIRAC import gLogger, gMonitor, S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client import PathFinder

## base classes
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.Agent.TransferTask import TransferTask

## DIRAC tools
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE

## agent name
AGENT_NAME = 'DataManagement/TransferAgent'

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
    MinProcess = 2
  * maximal number of sub-processes working togehter
    MaxProcess = 8
  * results queue size
    ProcessPoolQueueSize = 10
  * request type
    RequestType = transfer
  * proxy
    shifterProxy = DataManager
  * task executing
    TaskExecuting = True
  * FTS scheduling 
    FTSScheduling = True
  * time interval for throughput (FTS scheduling)
    ThroughputTimescale = 3600
  * for StrategyHandler
     - acceptable time to start shift
      HopSigma = 0.0
     - ???
      SchedulingType = File
     - list of active strategies
      ActiveStrategies = MinimiseTotalWait 
     - acceptable failure rate
      AcceptableFailureRate = 75  
  
  """
  ## placeholder for ReplicaManager instance
  __replicaManager = None
  ## placeholder for RequestDBMySQL instance
  __requestDBMySQL = None
  ## placeholder for StorageFactory instance
  __storageFactory = None
  ## placeholder for StrategyHandler instance
  __strategyHandler = None
  ## placeholder for  TransferDB instance (for FTS mode)
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


    self.__executionMode["Tasks"] = self.am_getOption( "TasksExecuting", True )
    self.log.info( "Tasks execution mode is %s." % { True : "enabled", 
                                                     False : "disabled" }[ self.__executionMode["Tasks"] ] )
    
    self.__executionMode["FTS"] = self.am_getOption( "FTSScheduling", False )
    self.log.info( "FTS execution mode is %s." % { True : "enabled", 
                                                   False : "disabled" }[ self.__executionMode["FTS"] ] )

    ## get TransferDB instance 
    if self.__executionMode["FTS"]:
      try:
        self.__transferDB = TransferDB()
      except Exception, error:
        self.log.exception( error )
      if not isinstance( self.__transferDB, TransferDB ):
        self.log.warn("Can't create TransferDB instance, disabling FTS execution mode.")
        self.__executionMode["FTS"] = False
      else:
        self.__throughputTimescale = self.am_getOption( 'ThroughputTimescale', 3600 )
        self.log.debug( "ThroughputTimescale = %s s" % str( self.__throughputTimescale ) )

    ## is there any mode enabled?
    if True not in self.__executionMode.values():
      return S_ERROR("Agent misconfiguration, neither FTS nor Tasks execution mode is enabled.")

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

  def requestDBMySQL( self ):
    """ RequestDBMySQL instance getter 

    :param self: self reference
    """
    if not self.__requestDBMySQL:
      self.__requestDBMySQL = RequestDBMySQL()
    return self.__requestDBMySQL

  def storageFactory( self ):
    """ StorageFactory instance getter 

    :param self: self reference
    """
    if not self.__storageFactory:
      self.__storageFactory = StorageFactory()
    return self.__storageFactory

  def transferDB( self ):
    """ TransferDB instance getter

    :param self: self reference
    """
    if not self.__transferDB:
      self.__transferDB = TransferDB()
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

  def getTransferURLs( self, lfn, repDict, replicas ):
    """ prepare TURLs for given LFN and replication tree

    :param self: self reference
    :param str lfn: LFN
    :param dict repDict: replication dictionary
    :param dict replicas: LFN replicas 
    """
    hopSourceSE = repDict["SourceSE"]
    hopDestSE = repDict["DestSE"]
    hopAncestor = repDict["Ancestor"]

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
        
    # get the targetSURL
    res = self.getSurlForLFN( hopDestSE, lfn )
    if not res["OK"]:
      errStr = res["Message"]
      self.log.error( errStr )
      return S_ERROR( errStr )
    targetSURL = res["Value"]

    return S_OK( (sourceSURL, targetSURL, status ) )


  def collectFiles( self, requestObj, iSubRequest ):
    """ Get SubRequest files with 'Waiting' status, collect their replicas and metadata information from 
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
    self.log.info( "collectFiles: SubRequest %s found with %d files." % ( iSubRequest, len( subRequestFiles ) ) )
    
    for subRequestFile in subRequestFiles:
      fileStatus = subRequestFile["Status"]
      fileLFN = subRequestFile["LFN"]
      if fileStatus != "Waiting":
        self.log.debug("collectFiles: File %s won't be processed as it is in '%s' status." % ( fileLFN, 
                                                                                               fileStatus ) )
        continue
      else:
        waitingFiles.setdefault( fileLFN, subRequestFile["FileID"] )

    if waitingFiles:     
      replicas = self.replicaManager().getCatalogReplicas( waitingFiles.keys() )
      if not replicas["OK"]:
        self.log.error( "collectFiles: Failed to get replica information.", replicas["Message"] )
        return replicas
      for lfn, failure in replicas["Value"]["Failed"].items():
        self.log.error( "collectFiles: Failed to get replicas.", "%s: %s" % ( lfn, failure ) )    
      replicas = replicas["Value"]["Successful"]

      if replicas:
        metadata = self.replicaManager().getCatalogFileMetadata( replicas.keys() )
        if not metadata["OK"]:
          self.log.error( "collectFiles: Failed to get file size information.", metadata["Message"] )
          return metadata
        for lfn, failure in metadata["Value"]["Failed"].items():
          self.log.error( "collectFiles: Failed to get file size.", "%s: %s" % ( lfn, failure ) )
        metadata = metadata["Value"]["Successful"] 
   
    self.log.info( "collectFiles: waitingFiles=%d replicas=%d metadata=%d" % ( len(waitingFiles), 
                                                                               len(replicas), 
                                                                               len(metadata) ) )
    
    if not ( len( waitingFiles ) == len( replicas ) == len( metadata ) ):
      self.log.warn( "collectFiles: Not all requested information available!" )
    
    return S_OK( ( waitingFiles, replicas, metadata ) )


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
    if pfn in res["Value"]["Failed"].keys():
      return S_ERROR( res["Value"]["Failed"][pfn] )
    return S_OK( res["Value"]["Successful"][pfn] )

  def execute( self ):
    """ request execution goes here

    :param self: self reference
    """
    requestCounter = self.__requestsPerCycle
    failback = False

    if self.__executionMode["FTS"]:
      self.log.info( "Will setup StrategyHandler for FTS scheduling...")
      self.__throughputTimescale = self.am_getOption( 'ThroughputTimescale', 3600 )
      self.log.debug( "ThroughputTimescale = %s s" % str( self.__throughputTimescale ) )

      ## setup strategy handler
      setupStrategyHandler = self.setupStrategyHandler()
      if not setupStrategyHandler["OK"]:
        self.log.error( setupStrategyHandler["Message"] )
        self.log.error( "Disabling FTS scheduling in this cycle...")
        failback = True 

    ## loop over requests
    while requestCounter:

      requestDict = self.getRequest( "transfer" )
      if not requestDict["OK"]:
        self.log.error("Error when getteing 'transfer' requests out of RequestDB: %s" % requestDict["Message"] )
        return requestDict 
      if not requestDict["Value"]:
        self.log.info("No more 'Waiting' requests found in RequestDB")
        return S_OK()
      requestDict = requestDict["Value"]
      self.log.info("Processing request (%d) %s" %  ( self.requestsPerCycle() - requestCounter + 1, 
                                                      requestDict["requestName"] ) )

      requestObj = RequestContainer( requestDict["requestString"] )
      ownerDN = requestObj.getAttribute( "OwnerDN" )
      
     
      if ownerDN["OK"] and ownerDN["Value"]:
        self.log.info("Request %s has its owner %s, FTS scheduling is disabled" % ( requestDict["requestName"], ownerDN["Value"] ) )
        failback = True
      
      ## if ownerDN is NOT present and FTS scheduling is enabled we can proceed with it 
      if not failback and self.__executionMode["FTS"]:
        self.log.info("About to schedule files for FTS")
        try:
          schedule = self.schedule( requestDict )
          if schedule["OK"]:
            self.log.info("Request %s has been scheduled for FTS" % requestDict["requestName"] )
            requestCounter = requestCounter - 1
            failback = False
            continue
          else:
            self.log.error( schedule["Message"] )
            failback = True
        except StrategyHandlerChannelNotDefined, error:
          self.log.info( str(error) )
          failback = True

      ## failback 
      if failback:
        if self.__executionMode["Tasks"]:
          self.log.info( "Will process request in TransferTask, as FTS scheduling has failed" )
        else:
          self.log.error("Not able to process this request, FTS scheduling has failed but task mode is disabled.")
          continue

      ## if we land here anyway this request has to be processed by tasks
      requestDict["configPath"] = self.configPath()
      self.log.info("About to process request using TransferTask")
      
      ## TransferTask main loop 
      while True:
        if self.processPool().getFreeSlots():
          self.log.info("spawning task %d" % ( self.requestsPerCycle() - requestCounter + 1 ) ) 
          enqueue = self.processPool().createAndQueueTask( TransferTask, 
                                                           kwargs = requestDict, 
                                                           callback =  self.requestCallback,
                                                           exceptionCallback = self.exceptionCallback,
                                                           blocking = True )
          if not enqueue["OK"]:
            self.log.error( enqueue["Message"] )
            continue
          ## update request counter
          requestCounter = requestCounter - 1
          ## task created, a little time kick to proceed 
          time.sleep( 0.1 )
          break
        else:
          self.log.info("No free slots available in processPool, will wait a second to proceed...")
          time.sleep( 1 )

    return S_OK()
          

  def finalize( self ):
    """ finalisation of one agent cycle

    :param self: self refernce
    """
    if self.__processPool:
      self.__processPool.processAllResults()
    return S_OK()
    
  def schedule( self, requestDict ):
    """ scheduling files for FTS
  
    :param self: self reference
    :param dict requestDict: request dictionary 
    """

    requestObj = requestDict["requestObj"]
    requestName = requestDict["requestName"]

    ## get request ID
    requestID = requestObj.getAttribute("RequestID")
    if not requestID["OK"]:
      self.log.error( "schedule: Failed to get RequestID.", requestID["Message"] )
      return S_ERROR( "schedule: Failed to get RequestID." )
    requestID = requestID["Value"]

    res = requestObj.getNumSubRequests( "transfer" )
    if not res["OK"]:
      self.log.error( "schedule: Failed to get number of SubRequests.", res["Message"] )
      return S_ERROR( "schedule: Failed to get number of SubRequests." )
    numberRequests = res["Value"]
    self.log.info( "schedule: '%s' found with %s 'transfer' SubRequests." % ( requestDict["requestName"], 
                                                                              numberRequests ) )

    for iSubRequest in range( numberRequests ):
      self.log.info( "schedule: Treating SubRequest %s from '%s'." % ( iSubRequest, 
                                                                       requestDict["requestName"] ) )
      subAttrs = requestObj.getSubRequestAttributes( iSubRequest, "transfer" )["Value"]
      subRequestStatus = subAttrs["Status"]
      if subRequestStatus != "Waiting":
        #  If the sub-request is already in terminal state
        self.log.info( "schedule: SubRequest %s status is '%s', it won't be executed." % ( iSubRequest, 
                                                                                           subRequestStatus ) )
        continue

      ## get source SE
      sourceSE = subAttrs["SourceSE"]
      ## get target SEs, no matter what's a type we need a list
      targetSEs = [ target.strip() for target in subAttrs["TargetSE"].split(",") ]
      ## get replication strategy
      operation = subAttrs["Operation"]
      strategy = { False : None, 
                   True: operation }[ operation in self.strategyHandler().getSupportedStrategies() ]

      ## get subrequest files  
      self.log.info( "schedule: Obtaining 'Waiting' files for %d SubRequest." % iSubRequest )
      files = self.collectFiles( requestObj, iSubRequest )
      if not files["OK"]:
        self.log.debug("schedule: Failed to get Waiting files from SubRequest.", files["Message"] )
      waitingFiles, replicas, metadata = files["Value"]

      if not waitingFiles or not replicas or not metadata:
        return S_ERROR( "waiting files, replica or metadata info is missing" )

      ## loop over waiting files, get replication tree 
      for waitingFileLFN, waitingFileID in sorted( waitingFiles.items() ):
        
        waitingFileReplicas = [] if waitingFileLFN not in replicas else replicas[waitingFileLFN]
        if not waitingFileReplicas:
          self.log.warn("schedule: no replica information available for LFN %s" % waitingFileLFN )
          continue 
        waitingFileMetadata = None if waitingFileLFN not in metadata else metadata[waitingFileLFN]
        if not waitingFileMetadata:
          self.log.warn("schedule: no metadata information available for LFN %s" % waitingFileLFN )
          continue 
        waitingFileSize = None if not waitingFileMetadata else waitingFileMetadata["Size"]
        
        ## set target SEs for this file
        waitingFileTargets = [ targetSE for targetSE in targetSEs if targetSE not in waitingFileReplicas ]
        if not waitingFileTargets:
          self.log.info( "schedule: %s present at all targets." % waitingFileLFN  )
          requestObj.setSubRequestFileAttributeValue( iSubRequest, "transfer", waitingFileLFN, "Status", "Done" )
          continue
        
        self.log.info( "Processing %s size=%s replicas=%d targetSEs=%s" % ( waitingFileLFN, 
                                                                            waitingFileSize, 
                                                                            len(waitingFileReplicas), 
                                                                            str(waitingFileTargets) ) ) 


        ## get the tree at least
        tree = self.strategyHandler().determineReplicationTree( sourceSE, 
                                                                waitingFileTargets, 
                                                                waitingFileReplicas,  
                                                                waitingFileSize, 
                                                                strategy )

        if not tree["OK"]:
          self.log.error( "schedule: failed to determine replication tree.", tree["Message"] )
          continue
        tree = tree["Value"]
        if not tree:
          self.log.error("schedule: unable to schedule %s file, replication tree is empty" % waitingFileLFN )
          continue
        else:
          self.log.debug( "replicationTree: %s" % tree )

        
        for channelID, repDict in tree.items():
          self.log.info( "schedule: processing for channel %d %s" % ( channelID, str( repDict ) ) )
          transferURLs = self.getTransferURLs( waitingFileLFN, repDict, waitingFileReplicas )
          if not transferURLs["OK"]:
            return transferURLs
          sourceSURL, targetSURL, waitingFileStatus = transferURLs["Value"]

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
            self.log.error( "schedule: Failed to add file to channel." , "%s %s" % ( str(waitingFileID), 
                                                                                           str(channelID) ) )
            return res

          res = self.transferDB().addFileRegistration( channelID, 
                                                       waitingFileID, 
                                                       waitingFileLFN, 
                                                       targetSURL, 
                                                       repDict["DestSE"] )
          if not res["OK"]:
            errStr = res["Message"]
            self.log.error( "schedule: Failed to add File registration.", "%s %s" % ( waitingFileID, 
                                                                                     channelID ) )
            result = self.transferDB().removeFileFromChannel( channelID, waitingFileID )
            if not result["OK"]:
              errStr += result["Message"]
              self.log.error( "schedule: Failed to remove file from channel." , "%s %s" % ( waitingFileID, 
                                                                                            channelID ) )
              return S_ERROR( errStr )
        
            requestObj.setSubRequestFileAttributeValue( iSubRequest, "transfer", 
                                                        waitingFileLFN, "Status", "Scheduled" )
            res = self.transferDB().addReplicationTree( waitingFileID, tree )

      if requestObj.isSubRequestEmpty( iSubRequest, "transfer" )["Value"]:
        requestObj.setSubRequestStatus( iSubRequest, "transfer", "Scheduled" )

    ## update Request in DB after operation
    requestString = requestObj.toXML()["Value"]
    res = self.requestDBMySQL().updateRequest( requestName, requestString )
    if not res["OK"]:
      self.log.error( "schedule: Failed to update request", "%s %s" % ( requestName, res["Message"] ) )
      
    return S_OK()

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

    self.log.debug( "strategyDispatcher entries:" )
    for key, value in self.strategyDispatcher.items():
      self.log.debug( "%s : %s" % ( key.pattern, value.__name__ ) )

    self.log.debug("%s has been constructed" % self.__class__.__name__ )


  def reset( self ):
    """Reset chosenStrategy."""
    self.chosenStrategy = 0

  def setBandwiths( self, bandwidths ):
    """Set the bandwidths. 

    :param self: self reference
    :param bandwithds: observed througput of active FTS channels
    """
    self.bandwidths = bandwidths
    #self.log.debug( "bandwiths set to %s" % bandwidths )

  def setChannels( self, channels ):
    """Set the channels.
    
    :param self: self reference
    :param channels: active channels queues
    """
    self.channels = channels 
    #self.log.debug( "channels set to %s" % channels )

  def getSupportedStrategies( self ):
    """ Get supported strategies.

    :param self: self reference
    """    
    return self.supportedStrategies

  def determineReplicationTree( self, sourceSE, targetSEs, replicas, size, strategy = None, sigma = None ):
    """Resolve and find replication tree given source and target storage elements, active replicas, 
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
    self.log.debug( "Will use %s strategy"  % strategy )

    if sigma:
      self.log.debug( "Sigme set to %s"  % sigma )
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
            return S_ERROR( "File does not exist at specified source site." )
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
                  self.log.error( "minimiseTotalWait: bailing out! channel '%s' not defined" % channelName )
                  raise StrategyHandlerChannelNotDefined( channelName )

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
    activeSE = []
    for se in seList:
      res = gConfig.getOption( "/Resources/StorageElements/%s/%sAccess" % ( se, access ), "Unknown" )
      if res["OK"] and res["Value"] == "Active":
        activeSE.append( se )
    return activeSE

  def __getChannelSitesForSE( self, storageElement ):
    """Get sites for given storage element.
    
    :param self: self reference
    :param str storageElement: storage element name
    """
    res = getSitesForSE( storageElement )
    sites = []
    if res["OK"]:
      for site in res["Value"]:
        siteName = site.split( "." )
        if len( siteName ) > 1:
          if not siteName[1] in sites:
            sites.append( siteName[1] )
    return sites
