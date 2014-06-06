########################################################################
# $HeadURL $
# File: FTSGraph.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/10 20:02:32
########################################################################
""" :mod: FTSGraph
    ==============

    .. module: FTSGraph
    :synopsis: FTS graph
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    nodes are FTS sites sites and edges are routes between them
"""
__RCSID__ = "$Id: $"
# #
# @file FTSGraph.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/10 20:03:00
# @brief Definition of FTSGraph class.

# # imports
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Graph import Graph, Node, Edge
# # from RSS
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
# from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getStorageElementSiteMapping, getSites, getFTSServersForSites
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView

class Site( Node ):
  """
  .. class:: Site

  not too much here, inherited to change the name
  """
  def __init__( self, name, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Node.__init__( self, name, rwAttrs, roAttrs )

  def __contains__( self, se ):
    """ check if SE is hosted at this site """
    return se in self.SEs

  def __str__( self ):
    """ str() op """
    return "<site name='%s' SEs='%s' />" % ( self.name, ",".join( self.SEs.keys() ) )

class Route( Edge ):
  """
  .. class:: Route

  class representing transfers between sites
  """
  def __init__( self, fromNode, toNode, rwAttrs = None, roAttrs = None ):
    """ c'tor """
    Edge.__init__( self, fromNode, toNode, rwAttrs, roAttrs )

  @property
  def isActive( self ):
    """ check activity of this channel """
    successRate = 100.0
    attempted = self.SuccessfulFiles + self.FailedFiles
    if attempted:
      successRate *= self.SuccessfulFiles / attempted
    return bool( successRate > self.AcceptableFailureRate )

  @property
  def timeToStart( self ):
    """ get time to start for this channel """
    if not self.isActive:
      return float( "inf" )
    transferSpeed = { "File": self.FilePut,
                      "Throughput": self.ThroughPut }[self.SchedulingType]
    waitingTransfers = { "File" : self.WaitingFiles,
                         "Throughput": self.WaitingSize }[self.SchedulingType]
    if transferSpeed:
      return waitingTransfers / float( transferSpeed )
    return 0.0

class FTSGraph( Graph ):
  """
  .. class:: FTSGraph

  graph holding FTS transfers (edges) and sites (nodes)
  """
  # # rss client
  __rssClient = None
  # # resources
  __resources = None

  def __init__( self,
                name,
                ftsHistoryViews = None,
                accFailureRate = 0.75,
                accFailedFiles = 5,
                schedulingType = "Files" ):
    """ c'tor

    :param str name: graph name
    :param list ftsHistoryViews: list with FTSHistoryViews
    :param float accFailureRate: acceptable failure rate
    :param int accFailedFiles: acceptable failed files
    :param str schedulingType: scheduling type
    """
    Graph.__init__( self, name )
    self.log = gLogger.getSubLogger( name, True )
    self.accFailureRate = accFailureRate
    self.accFailedFiles = accFailedFiles
    self.schedulingType = schedulingType
    self.initialize( ftsHistoryViews )

  def initialize( self, ftsHistoryViews = None ):
    """ initialize FTSGraph  given FTSSites and FTSHistoryViews

    :param list ftsSites: list with FTSSites instances
    :param list ftsHistoryViews: list with FTSHistoryViews instances
    """
    self.log.debug( "initializing FTS graph..." )

    ftsSites = self.ftsSites()
    if ftsSites["OK"]:
      ftsSites = ftsSites["Value"]
    else:
      ftsSites = []
    ftsHistoryViews = ftsHistoryViews if ftsHistoryViews else []

    sitesDict = getStorageElementSiteMapping()  # [ ftsSite.Name for ftsSite in ftsSites ] )
    if not sitesDict["OK"]:
      self.log.error( sitesDict["Message"] )
      # raise Exception( sitesDict["Message"] )
    sitesDict = sitesDict["Value"] if "Value" in sitesDict else {}

    # # revert to resources helper
    # sitesDict = self.resources().getEligibleResources( "Storage" )
    # if not sitesDict["OK"]:
    #  return sitesDict
    # sitesDict = sitesDict["Value"]

    # # create nodes
    for ftsSite in ftsSites:

      rwSEsDict = dict.fromkeys( sitesDict.get( ftsSite.Name, [] ), {} )
      for se in rwSEsDict:
        rwSEsDict[se] = { "read": False, "write": False }

      rwAttrs = { "SEs": rwSEsDict }
      roAttrs = { "FTSServer": ftsSite.FTSServer,
                  "MaxActiveJobs": ftsSite.MaxActiveJobs }
      site = Site( ftsSite.Name, rwAttrs, roAttrs )

      self.log.debug( "adding site %s using FTSServer %s" % ( ftsSite.Name, ftsSite.FTSServer ) )
      self.addNode( site )

    for sourceSite in self.nodes():
      for destSite in self.nodes():

        rwAttrs = { "WaitingFiles": 0, "WaitingSize": 0,
                    "SuccessfulFiles": 0, "SuccessfulSize": 0,
                    "FailedFiles": 0, "FailedSize": 0,
                    "FilePut": 0.0, "ThroughPut": 0.0,
                    "ActiveJobs": 0, "FinishedJobs": 0 }

        roAttrs = { "routeName": "%s#%s" % ( sourceSite.name, destSite.name ),
                    "AcceptableFailureRate": self.accFailureRate,
                    "AcceptableFailedFiles": self.accFailedFiles,
                    "SchedulingType": self.schedulingType }

        route = Route( sourceSite, destSite, rwAttrs, roAttrs )
        self.log.debug( "adding route between %s and %s" % ( route.fromNode.name, route.toNode.name ) )
        self.addEdge( route )

    for ftsHistory in ftsHistoryViews:

      route = self.findRoute( ftsHistory.SourceSE, ftsHistory.TargetSE )
      if not route["OK"]:
        self.log.warn( "route between %s and %s not found" % ( ftsHistory.SourceSE, ftsHistory.TargetSE ) )
        continue
      route = route["Value"]

      if ftsHistory.Status in FTSJob.INITSTATES:
        route.ActiveJobs += ftsHistory.FTSJobs
        route.WaitingFiles += ftsHistory.Files
        route.WaitingSize += ftsHistory.Size
      elif ftsHistory.Status in FTSJob.TRANSSTATES:
        route.ActiveJobs += ftsHistory.FTSJobs
        route.WaitingSize += ftsHistory.Completeness * ftsHistory.Size / 100.0
        route.WaitingFiles += int( ftsHistory.Completeness * ftsHistory.Files / 100.0 )
      elif ftsHistory.Status in FTSJob.FAILEDSTATES:
        route.FinishedJobs += ftsHistory.FTSJobs
        route.FailedFiles += ftsHistory.FailedFiles
        route.FailedSize += ftsHistory.FailedSize
      else:  # # FINISHEDSTATES
        route.FinishedJobs += ftsHistory.FTSJobs
        route.SuccessfulFiles += ( ftsHistory.Files - ftsHistory.FailedFiles )
        route.SuccessfulSize += ( ftsHistory.Size - ftsHistory.FailedSize )

      route.FilePut = float( route.SuccessfulFiles - route.FailedFiles ) / FTSHistoryView.INTERVAL
      route.ThroughPut = float( route.SuccessfulSize - route.FailedSize ) / FTSHistoryView.INTERVAL

    self.updateRWAccess()
    self.log.debug( "init done!" )

  def rssClient( self ):
    """ RSS client getter """
    if not self.__rssClient:
      self.__rssClient = ResourceStatus()
    return self.__rssClient

  # def resources( self ):
  #  """ resource helper getter """
  #  if not self.__resources:
  #    self.__resources = Resources()
  #  return self.__resources

  def updateRWAccess( self ):
    """ get RSS R/W for :seList:

    :param list seList: SE list
    """
    self.log.debug( "updateRWAccess: updating RW access..." )
    for site in self.nodes():
      seList = site.SEs.keys()
      rwDict = dict.fromkeys( seList )
      for se in rwDict:
        rwDict[se] = { "read": False, "write": False  }
      
      for se in seList:
               
        rwDict[se]["read"]  = self.rssClient().isUsableStorage( se, 'ReadAccess' )
        rwDict[se]["write"] = self.rssClient().isUsableStorage( se, 'WriteAccess' )
        
        self.log.debug( "Site '%s' SE '%s' read %s write %s " % ( site.name, se,
                                                                  rwDict[se]["read"], rwDict[se]["write"] ) )
      site.SEs = rwDict
    return S_OK()

  def findSiteForSE( self, se ):
    """ return FTSSite for a given SE """
    for node in self.nodes():
      if se in node:
        return S_OK( node )
    return S_ERROR( "StorageElement %s not found" % se )

  def findRoute( self, fromSE, toSE ):
    """ find route between :fromSE: and :toSE: """
    for edge in self.edges():
      if fromSE in edge.fromNode.SEs and toSE in edge.toNode.SEs:
        return S_OK( edge )
    return S_ERROR( "FTSGraph: unable to find route between '%s' and '%s'" % ( fromSE, toSE ) )

  def ftsSites( self ):
    """ get fts site list """
    sites = getSites()
    if not sites["OK"]:
      return sites
    sites = sites["Value"]
    ftsServers = getFTSServersForSites( sites )
    if not ftsServers["OK"]:
      return ftsServers
    ftsServers = ftsServers["Value"]
    ftsSites = []
    for site, ftsServerURL in ftsServers.items():
      ftsSite = FTSSite()
      ftsSite.Name = site
      ftsSite.FTSServer = ftsServerURL
      # # should be read from CS as well
      ftsSite.MaxActiveJobs = 50
      ftsSites.append( ftsSite )
    return S_OK( ftsSites )

