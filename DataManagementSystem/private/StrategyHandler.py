########################################################################
# $HeadURL $
# File: StrategyHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/05/25 07:49:30
########################################################################

""" :mod: StrategyHandler 
    =======================
 
    .. module: StrategyHandler
    :synopsis: implementation of helper class for FST scheduling
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    implementation of helper class for FST scheduling
"""

__RCSID__ = "$Id $"

##
# @file StrategyHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/05/25 07:50:12
# @brief Definition of StrategyHandler class.

## imports 
import random
import re
## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE

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

########################################################################
class StrategyHandler( object ):
  """
  .. class:: StrategyHandler

  StrategyHandler is a helper class for determining optimal replication tree for given
  source files, their replicas and target storage elements.
  """

  def __init__( self, configSection, bandwidths=None, channels=None, failedFiles=None ):
    """c'tor

    :param self: self reference
    :param str configSection: path on CS to ReplicationScheduler agent
    :param bandwithds: observed throughput on active channels
    :param channels: active channels
    :param int failedFiles: max number of distinct failed files to allow scheduling
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
    self.acceptableFailedFiles = gConfig.getValue( self.configSection + "/AcceptableFailedFiles", 5 )
    self.log.debug( "AcceptableFailedFiles = %s" % self.acceptableFailedFiles )

    self.bandwidths = bandwidths if bandwidths else {}
    self.channels = channels if channels else {}
    self.failedFiles = failedFiles if failedFiles else {}
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

  def setFailedFiles( self, failedFiles ):
    """ set the failed FTS files counters

    :param self: self reference
    :param failedFiles: observed distinct failed files
    """
    self.failedFiles = failedFiles if failedFiles else {}

  def setBandwiths( self, bandwidths ):
    """ set the bandwidths 

    :param self: self reference
    :param bandwithds: observed througput of active FTS channels
    """
  
    self.bandwidths = bandwidths if bandwidths else {}

  def setChannels( self, channels ):
    """ set the channels
    
    :param self: self reference
    :param channels: active channels queues
    """
    self.channels = channels if channels else {}

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
            self.log.warn("determineReplicationTree: can't set new sigma value from '%s'" % strategy )
        if reStrategy.pattern in [ "MinimiseTotalWait", "DynamicThroughput" ]:
          replicasToUse = replicas.keys() if sourceSE == None else [ sourceSE ]
          tree = self.strategyDispatcher[ reStrategy ].__call__( replicasToUse, targetSEs  )
        elif reStrategy.pattern == "Simple":
          if not sourceSE in replicas.keys():
            return S_ERROR( "File does not exist at specified source site" )
          tree = self.__simple( sourceSE, targetSEs )
        elif reStrategy.pattern == "Swarm":
          tree = self.__swarm( targetSEs[0], replicas.keys() )
   
    ## sanity check for tree, just checking if all targetSEs are in
    missing = set( targetSEs ) - set( [ rep["DestSE"] for rep in tree.values() ] )
    if missing:
      msg = "wrong replication tree, missing %s targetSEs" % ",".join( [ tSE for tSE in missing ] )
      self.log.error( "determineReplicationTree: %s" % msg )
      return S_ERROR( msg )
   
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
    
        ## get distinct failed files counter
        distinctFailedFiles = self.failedFiles.get( channelID, 0 )      
    
        ## success rate too low and more than acceptable distinct files are affected?, make channel unattractive
        if ( successRate < self.acceptableFailureRate ) and ( distinctFailedFiles > self.acceptableFailedFiles ):
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


