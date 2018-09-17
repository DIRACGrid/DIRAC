########################################################################
# $HeadURL $
# File: FTSStrategy.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/12 13:12:07
########################################################################
""" :mod: FTSStrategy

    =================

    .. module: FTSStrategy

    :synopsis: replication strategy for FTS transfers

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    replication strategy for all FTS transfers
"""
__RCSID__ = "$Id: $"
# #
# @file FTSStrategy.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/12 13:12:20
# @brief Definition of FTSStrategy class.

# # imports
import random
# # from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.Core.Utilities.LockRing import LockRing
# from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
from DIRAC.DataManagementSystem.private.FTS2.FTS2Graph import FTS2Graph

########################################################################
class FTS2Strategy( object ):
  """
  .. class:: FTSStrategy

  helper class to create replication forest for a given file and it's replicas using
  several different strategies
  """
  # # make it singleton
  __metaclass__ = DIRACSingleton
  # # list of supported strategies
  __supportedStrategies = [ 'Simple', 'DynamicThroughput', 'Swarm', 'MinimiseTotalWait' ]
  # # FTS graph
  ftsGraph = None
  # # lock
  __graphLock = None
  # # resources
  __resources = None
  # # rss client
  __rssClient = None
  # # acceptable failure rate
  acceptableFailureRate = 75
  # # acceptable failed files
  acceptableFailedFiles = 5
  # # scheduling type
  schedulingType = "Files"

  def __init__( self, csPath = None, ftsSites = None, ftsHistoryViews = None ):
    """c'tor

    :param self: self reference
    :param str csPath: CS path
    :param ftsSites: list of FTSSites
    :type ftsSites: python:list
    :param ftsHistoryViews: list of FTSHistoryViews
    :type ftsHistoryViews: python:list

    """
    # ## config path
    self.csPath = csPath
    # # fts sites
    ftsSites = ftsSites if ftsSites else []
    # # history views
    ftsHistoryViews = ftsHistoryViews if ftsHistoryViews else []
    # # own sub logger
    self.log = gLogger.getSubLogger( "FTSStrategy", child = True )
    self.log.setLevel( gConfig.getValue( self.csPath + "/LogLevel", "DEBUG" ) )
    for ftsSite in ftsSites:
      self.log.info( "FTSSite: %-16s FTSServer=%s" % ( ftsSite.Name, ftsSite.FTSServer ) )

    # # CS options
    self.log.info( "Supported strategies = %s" % ", ".join( self.supportedStrategies ) )
    self.activeStrategies = gConfig.getValue( "%s/%s" % ( self.csPath, "ActiveStrategies" ), ["MinimiseTotalWait"] )
    self.log.info( "ActiveStrategies = %s" % ", ".join( self.activeStrategies ) )
    self.numberOfStrategies = len( self.activeStrategies )
    self.log.info( "Number of active strategies = %s" % self.numberOfStrategies )
    self.sigma = gConfig.getValue( "%s/%s" % ( self.csPath, "HopSigma" ), 5 )
    self.log.info( "HopSigma = %s" % self.sigma )
    self.schedulingType = gConfig.getValue( "%s/%s" % ( self.csPath, "SchedulingType" ), "Files" )
    self.log.info( "SchedulingType = %s" % self.schedulingType )
    self.acceptableFailureRate = gConfig.getValue( "%s/%s" % ( self.csPath, "AcceptableFailureRate" ), 75 )
    self.log.info( "AcceptableFailureRate = %s" % self.acceptableFailureRate )
    self.acceptableFailedFiles = gConfig.getValue( "%s/%s" % ( self.csPath, "AcceptableFailedFiles" ), 5 )
    self.log.info( "AcceptableFailedFiles = %s" % self.acceptableFailedFiles )
    # # chosen strategy
    self.chosenStrategy = 0
    # dispatcher
    self.strategyDispatcher = { "MinimiseTotalWait" : self.minimiseTotalWait,
                                "DynamicThroughput" : self.dynamicThroughput,
                                "Simple" : self.simple,
                                "Swarm" : self.swarm }

    self.ftsGraph = FTS2Graph( "FTSGraph",
                              ftsHistoryViews,
                              self.acceptableFailureRate,
                              self.acceptableFailedFiles,
                              self.schedulingType )

    # for node in self.ftsGraph.nodes():
    #  self.log.debug( node )
    # for edge in self.ftsGraph.edges():
    #  self.log.debug( edge )

    # # if we land here everything is OK
    self.log.info( "%s has been constructed" % self.__class__.__name__ )


  @classmethod
  def graphLock( cls ):
    """ get graph lock """
    if not cls.__graphLock:
      cls.__graphLock = LockRing().getLock( "FTSGraphLock" )
    return cls.__graphLock

  @classmethod
  def resetGraph( cls, ftsHistoryViews ):
    """ reset graph

    :param ftsHistoryViews: list of FTSHistoryViews
    :type ftsHistoryViews: python:list
    """
    ftsGraph = None
    try:
      cls.graphLock().acquire()
      ftsGraph = FTS2Graph( "FTSGraph",
                           ftsHistoryViews,
                           cls.acceptableFailureRate,
                           cls.acceptableFailedFiles,
                           cls.schedulingType )
      if ftsGraph:
        cls.ftsGraph = ftsGraph
    finally:
      cls.graphLock().release()
    return S_OK()

  def updateRWAccess( self ):
    """ update RW access in FTS graph """
    updateRWAccess = S_OK()
    try:
      self.graphLock().acquire()
      updateRWAccess = self.ftsGraph.updateRWAccess()
      if not updateRWAccess["OK"]:
        self.log.error( updateRWAccess["Message"] )
    finally:
      self.graphLock().release()
    return updateRWAccess

  def addTreeToGraph( self, replicationTree = None, size = 0.0 ):
    """ update rw access for nodes (sites) and size anf files for edges (channels) """
    replicationTree = replicationTree if replicationTree else {}
    size = size if size else 0.0
    if replicationTree:
      try:
        self.graphLock().acquire()
        for route in self.ftsGraph.edges():
          if route.routeName in replicationTree:
            route.WaitingSize += size
            route.WaitingFiles += 1
      finally:
        self.graphLock().release()
    return S_OK()

  def simple( self, sourceSEs, targetSEs ):
    """ simple strategy - one source, many targets

    :param sourceSEs: list with only one sourceSE name
    :type sourceSEs: python:list
    :param targetSEs: list with target SE names
    :type targetSEs: python:list
    :param str lfn: logical file name
    """
    # # make targetSEs list unique
    if len( sourceSEs ) != 1:
      return S_ERROR( "simple: wrong argument supplied for sourceSEs, only one sourceSE allowed" )
    sourceSE = sourceSEs[0]
    tree = {}
    for targetSE in targetSEs:
      route = self.ftsGraph.findRoute( sourceSE, targetSE )
      if not route["OK"]:
        return S_ERROR( route["Message"] )
      route = route["Value"]
      if not route.fromNode.SEs[sourceSE]["read"]:
        return S_ERROR( "simple: sourceSE '%s' in banned for reading right now" % sourceSE )
      if not route.toNode.SEs[targetSE]["write"]:
        return S_ERROR( "simple: targetSE '%s' is banned for writing right now" % targetSE )
      if route.name in tree:
        return S_ERROR( "simple: unable to create replication tree, route '%s' cannot be used twice" % \
                          route.name )
      tree[route.name] = { "Ancestor" : False, "SourceSE" : sourceSE,
                           "TargetSE" : targetSE, "Strategy" : "Simple" }

    return S_OK( tree )

  def swarm( self, sourceSEs, targetSEs ):
    """ swarm strategy - one target, many sources, pick up the fastest

    :param sourceSEs: list of source SE
    :type sourceSEs: python:list
    :param str targetSEs: one element list with name of target SE
    :param str lfn: logical file name
    """
    tree = {}
    routes = []
    if len( targetSEs ) > 1:
      return S_ERROR( "swarm: wrong argument supplied for targetSEs, only one targetSE allowed" )
    targetSE = targetSEs[0]
    # # find channels
    for sourceSE in sourceSEs:
      route = self.ftsGraph.findRoute( sourceSE, targetSE )
      if not route["OK"]:
        self.log.warn( "swarm: %s" % route["Message"] )
        continue
      routes.append( ( sourceSE, route["Value"] ) )
    # # exit - no channels
    if not routes:
      return S_ERROR( "swarm: unable to find FTS routes between '%s' and '%s'" % ( ",".join( sourceSEs ), targetSE ) )
    # # filter out non active channels
    routes = [ ( sourceSE, route ) for sourceSE, route in routes
                 if route.fromNode.SEs[sourceSE]["read"] and route.toNode.SEs[targetSE]["write"] and
                 route.timeToStart < float( "inf" ) ]
    # # exit - no active channels
    if not routes:
      return S_ERROR( "swarm: no active routes found between %s and %s" % ( sourceSEs, targetSE ) )

    # # find min timeToStart
    minTimeToStart = float( "inf" )
    selSourceSE = selRoute = None
    for sourceSE, route in routes:
      if route.timeToStart < minTimeToStart:
        minTimeToStart = route.timeToStart
        selSourceSE = sourceSE
        selRoute = route

    if not selSourceSE:
      return S_ERROR( "swarm: no active routes found between %s and %s" % ( sourceSEs, targetSE ) )

    tree[selRoute.name] = { "Ancestor" : False, "SourceSE" : selSourceSE,
                            "TargetSE" : targetSE, "Strategy" : "Swarm" }
    return S_OK( tree )

  def minimiseTotalWait( self, sourceSEs, targetSEs ):
    """ find dag minimizing start time

    :param sourceSEs: list of available source SEs
    :type sourceSEs: python:list
    :param targetSEs: list of target SEs
    :type targetSEs: python:list
    :param str lfn: logical file name
    """

    tree = {}
    primarySources = sourceSEs
    while targetSEs:
      minTimeToStart = float( "inf" )
      channels = []
      self.log.verbose( "minimiseTotalWait: searching routes between %s and %s" % ( ",".join( sourceSEs ),
                                                                                 ",".join( targetSEs ) ) )
      for targetSE in targetSEs:
        for sourceSE in sourceSEs:
          ftsChannel = self.ftsGraph.findRoute( sourceSE, targetSE )
          if not ftsChannel["OK"]:
            self.log.warn( "minimiseTotalWait: %s" % ftsChannel["Message"] )
            continue
          ftsChannel = ftsChannel["Value"]
          channels.append( ( ftsChannel, sourceSE, targetSE ) )

      if not channels:
        msg = "minimiseTotalWait: FTS route between these SEs are not defined"
        self.log.error( msg )
        return S_ERROR( msg )

      self.log.verbose( "minimiseTotalWait: found %s candidate routes, checking RSS status" % len( channels ) )

      for ch, s, t in channels:
        self.log.verbose( "%s %s %s" % ( ch.routeName, ch.fromNode.SEs[s]["read"], ch.toNode.SEs[t]["write"] ) )



      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.fromNode.SEs[sourceSE]["read"] and channel.toNode.SEs[targetSE]["write"]
                   and channel.timeToStart < float( "inf" ) ]

      if not channels:
        self.log.error( "minimiseTotalWait: no active FTS routes found" )
        return S_ERROR( "minimiseTotalWait: no active FTS routes found" )

      candidates = []
      for channel, sourceSE, targetSE in channels:
        timeToStart = channel.timeToStart
        if sourceSE not in primarySources:
          timeToStart += self.sigma
        # # local found
        if channel.fromNode == channel.toNode:
          self.log.debug( "minimiseTotalWait: found local route '%s'" % channel.routeName )
          candidates = [ ( channel, sourceSE, targetSE ) ]
          break
        if timeToStart <= minTimeToStart:
          minTimeToStart = timeToStart
          candidates = [ ( channel, sourceSE, targetSE ) ]
        elif timeToStart == minTimeToStart:
          candidates.append( ( channel, sourceSE, targetSE ) )

      if not candidates:
        return S_ERROR( "minimiseTotalWait: unable to find candidate FTS routes minimizing total wait time" )

      random.shuffle( candidates )
      selChannel, selSourceSE, selTargetSE = candidates[0]
      ancestor = False
      for routeName, treeItem in tree.items():
        if selSourceSE in treeItem["TargetSE"]:
          ancestor = treeItem["TargetSE"]
      tree[selChannel.routeName] = { "Ancestor" : ancestor, "SourceSE" : selSourceSE,
                                     "TargetSE" : selTargetSE, "Strategy" : "MinimiseTotalWait" }
      sourceSEs.append( selTargetSE )
      targetSEs.remove( selTargetSE )

    return S_OK( tree )

  def dynamicThroughput( self, sourceSEs, targetSEs ):
    """ dynamic throughput - many sources, many targets - find dag minimizing overall throughput

    :param sourceSEs: list of available source SE names
    :type sourceSEs: python:list
    :param targetSE: list of target SE names
    :type targetSEs: python:list
    :param str lfn: logical file name
    """
    tree = {}
    primarySources = sourceSEs
    timeToSite = {}
    while targetSEs:
      minTimeToStart = float( "inf" )
      channels = []
      for targetSE in targetSEs:
        for sourceSE in sourceSEs:
          ftsChannel = self.ftsGraph.findRoute( sourceSE, targetSE )
          if not ftsChannel["OK"]:
            self.log.warn( "dynamicThroughput: %s" % ftsChannel["Message"] )
            continue
          ftsChannel = ftsChannel["Value"]
          channels.append( ( ftsChannel, sourceSE, targetSE ) )
      # # no candidate channels found
      if not channels:
        msg = "dynamicThroughput: FTS routes between %s and %s are not defined" % ( ",".join( sourceSEs ),
                                                                                    ",".join( targetSEs ) )
        self.log.error( msg )
        return S_ERROR( msg )
      # # filter out already used channels
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.routeName not in tree ]
      if not channels:
        msg = "dynamicThroughput: all FTS routes between %s and %s are already used in tree" % ( ",".join( sourceSEs ),
                                                                                                 ",".join( targetSEs ) )
        self.log.error( msg )
        return S_ERROR( msg )
      # # filter out non-active channels
      self.log.debug( "dynamicThroughput: found %s candidate routes, checking activity" % len( channels ) )
      channels = [ ( channel, sourceSE, targetSE ) for channel, sourceSE, targetSE in channels
                   if channel.fromNode.SEs[sourceSE]["read"] and channel.toNode.SEs[targetSE]["write"]
                  and channel.timeToStart < float( "inf" ) ]
      if not channels:
        self.log.warn( "dynamicThroughput: active candidate routes not found" )
        return S_ERROR( "dynamicThroughput: no active candidate FTS routes found" )

      candidates = []
      selTimeToStart = None
      for channel, sourceSE, targetSE in channels:
        timeToStart = channel.timeToStart
        if sourceSE not in primarySources:
          timeToStart += self.sigma
        if sourceSE in timeToSite:
          timeToStart += timeToSite[sourceSE]
        # # local found
        if channel.fromNode == channel.toNode:
          self.log.debug( "dynamicThroughput: found local route '%s'" % channel.channelName )
          candidates = [ ( channel, sourceSE, targetSE ) ]
          selTimeToStart = timeToStart
          break
        if timeToStart <= minTimeToStart:
          selTimeToStart = timeToStart
          minTimeToStart = timeToStart
          candidates = [ ( channel, sourceSE, targetSE ) ]
        elif timeToStart == minTimeToStart:
          candidates.append( ( channel, sourceSE, targetSE ) )

      if not candidates:
        return S_ERROR( "dynamicThroughput: unable to find candidate FTS routes" )

      random.shuffle( candidates )
      selChannel, selSourceSE, selTargetSE = candidates[0]
      ancestor = False
      for routeName, treeItem in tree.items():
        if selSourceSE in treeItem["TargetSE"]:
          ancestor = treeItem["TargetSE"]
      tree[selChannel.name] = { "Ancestor": ancestor, "SourceSE": selSourceSE,
                                "TargetSE": selTargetSE, "Strategy": "DynamicThroughput" }

      timeToSite[selTargetSE] = selTimeToStart
      sourceSEs.append( selTargetSE )
      targetSEs.remove( selTargetSE )

    return S_OK( tree )

  def reset( self ):
    """ reset :chosenStrategy:

    :param self: self reference
    """
    self.chosenStrategy = 0

  @property
  def supportedStrategies( self ):
    """ Get supported strategies.

    :param self: self reference
    """
    return self.__supportedStrategies

  def replicationTree( self, sourceSEs, targetSEs, size, strategy = None ):
    """ get replication tree

    :param str lfn: LFN
    :param sourceSEs: list of sources SE names to use
    :type sourceSEs: python:list
    :param targetSEs: list of target SE names to use
    :type targetSEs: python:list
    :param long size: file size
    :param str strategy: strategy name
    """
    # # get strategy
    strategy = strategy if strategy else self.__selectStrategy()
    if strategy not in self.activeStrategies:
      return S_ERROR( "replicationTree: inactive or unsupported strategy '%s'" % strategy )

    self.log.verbose( "replicationTree: strategy=%s sourceSEs=%s targetSEs=%s size=%s" % \
                     ( strategy, sourceSEs, targetSEs, size ) )
    # # fire action from dispatcher
    tree = self.strategyDispatcher[strategy]( sourceSEs, targetSEs )
    if not tree["OK"]:
      self.log.error( "replicationTree: %s" % tree["Message"] )
      return tree
    # # update graph edges
    self.log.verbose( "replicationTree: %s" % tree["Value"] )
    update = self.addTreeToGraph( replicationTree = tree["Value"], size = size )
    if not update["OK"]:
      self.log.error( "replicationTree: unable to update FTS graph: %s" % update["Message"] )
      return update
    return tree

  def __selectStrategy( self ):
    """ If more than one active strategy use one after the other.

    :param self: self reference
    """
    chosenStrategy = self.activeStrategies[self.chosenStrategy]
    self.chosenStrategy += 1
    if self.chosenStrategy == self.numberOfStrategies:
      self.chosenStrategy = 0
    return chosenStrategy
