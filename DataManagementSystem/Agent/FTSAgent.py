########################################################################
# $HeadURL $
# File: FTSAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/31 10:00:13
########################################################################
""" :mod: FTSAgent
    ==============

    .. module: FTSAgent
    :synopsis: agent propagating scheduled RMS request in FTS
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    agent propagating scheduled RMS request in FTS
"""
__RCSID__ = "$Id: $"
# #
# @file FTSAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/31 10:00:51
# @brief Definition of FTSAgent class.
# # imports
import time
import datetime
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
# # from Core
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.List import getChunk
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.private.FTSGraph import FTSGraph
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
# # from RMS
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
# # from RSS
# #from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement
# # from Accounting
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation

# # agent base name
AGENT_NAME = "DataManagement/FTSAgent"

########################################################################
class FTSAgent( AgentModule ):
  """
  .. class:: FTSAgent

  """
  # # fts graph refresh in seconds
  FTSGRAPH_REFRESH = FTSHistoryView.INTERVAL / 2
  # # SE R/W access refresh in seconds
  RW_REFRESH = 600
  # # placeholder for max job per channel
  MAX_ACTIVE_JOBS = 50
  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10
  # # files per job
  MAX_FILES_PER_JOB = 100
  # # MAX FTS transfer per FTSFile
  MAX_ATTEMPT = 256
  # # placeholder fot FTS client
  __ftsClient = None
  # # placeholder for request client
  __requestClient = None
  # # placeholder for resources helper
  __resources = None
  # # placeholder for RSS client
  __rssClient = None
  # # placeholder for FTSGraph
  __ftsGraph = None
  # # graph regeneration time delta
  __ftsGraphValidStamp = None
  # # r/w access valid stamp
  __rwAccessValidStamp = None
  # # placeholder for threadPool
  __threadPool = None
  # # update lock
  __updateLock = None
  # # se cache
  __seCache = dict()

  def updateLock( self ):
    """ update lock """
    if not self.__updateLock:
      self.__updateLock = LockRing().getLock( "SubmitFTSAgentLock" )
    return self.__updateLock

  def requestClient( self ):
    """ request client getter """
    if not self.__requestClient:
      self.__requestClient = ReqClient()
    return self.__requestClient

  def ftsClient( self ):
    """ FTS client """
    if not self.__ftsClient:
      self.__ftsClient = FTSClient()
    return self.__ftsClient

  def rssClient( self ):
    """ RSS client getter """
    if not self.__rssClient:
      self.__rssClient = ResourceStatus()
    return self.__rssClient

  @classmethod
  def getSE( cls, seName ):
    """ keep se in cache"""
    if seName not in cls.__seCache:
      cls.__seCache[seName] = StorageElement( seName )
    return cls.__seCache[seName]

#  def resources( self ):
#    """ resource helper getter """
#    if not self.__resources:
#      self.__resources = Resources()
#    return self.__resources

  def threadPool( self ):
    """ thread pool getter """
    if not self.__threadPool:
      self.__threadPool = ThreadPool( self.MIN_THREADS, self.MAX_THREADS )
      self.__threadPool.daemonize()
    return self.__threadPool

  def resetFTSGraph( self ):
    """ create fts graph """
    log = gLogger.getSubLogger( "ftsGraph" )

    ftsSites = self.ftsClient().getFTSSitesList()
    if not ftsSites["OK"]:
      log.error( "unable to get FTS sites list: %s" % ftsSites["Message"] )
      return ftsSites
    ftsSites = ftsSites["Value"]
    if not ftsSites:
      log.error( "FTSSites list is empty, no records in FTSDB.FTSSite table?" )
      return S_ERROR( "no FTSSites found" )

    ftsHistory = self.ftsClient().getFTSHistory()
    if not ftsHistory["OK"]:
      log.error( "unable to get FTS history: %s" % ftsHistory["Message"] )
      return ftsHistory
    ftsHistory = ftsHistory["Value"]

    try:
      self.updateLock().acquire()
      self.__ftsGraph = FTSGraph( "FTSGraph", ftsSites, ftsHistory )
    finally:
      self.updateLock().release()

    log.info( "FTSSites:" )
    for i, site in enumerate( self.__ftsGraph.nodes() ):
      log.info( " [%02d] FTSSite: %-25s FTSServer: %s" % ( i, site.name, site.FTSServer ) )
    log.info( "FTSRoutes:" )
    for i, route in enumerate( self.__ftsGraph.edges() ):
      log.info( " [%02d] FTSRoute: %-25s Active FTSJobs (Max) = %s (%s)" % ( i,
                                                                             route.routeName,
                                                                             route.ActiveJobs,
                                                                             route.toNode.MaxActiveJobs ) )
    # # save graph stamp
    self.__ftsGraphValidStamp = datetime.datetime.now() + datetime.timedelta( seconds = self.FTSGRAPH_REFRESH )

    # # refresh SE R/W access
    try:
      self.updateLock().acquire()
      self.__ftsGraph.updateRWAccess()
    finally:
      self.updateLock().release()
    self.__rwAccessValidStamp = datetime.datetime.now() + datetime.timedelta( seconds = self.RW_REFRESH )

    return S_OK()


  def initialize( self ):
    """ agent's initialization """

    log = self.log.getSubLogger( "initialize" )

    self.FTSGRAPH_REFRESH = self.am_getOption( "FTSGraphValidityPeriod", self.FTSGRAPH_REFRESH )
    log.info( "FTSGraph validity period       = %s s" % self.FTSGRAPH_REFRESH )
    self.RW_REFRESH = self.am_getOption( "RWAccessValidityPeriod", self.RW_REFRESH )
    log.info( "SEs R/W access validity period = %s s" % self.RW_REFRESH )

    self.MAX_ACTIVE_JOBS = self.am_getOption( "MaxActiveJobsPerChannel", self.MAX_ACTIVE_JOBS )
    log.info( "Max active FTSJobs/route       = %s" % self.MAX_ACTIVE_JOBS )
    self.MAX_FILES_PER_JOB = self.am_getOption( "MaxFilesPerJob", self.MAX_FILES_PER_JOB )
    log.info( "Max FTSFiles/FTSJob            = %d" % self.MAX_FILES_PER_JOB )

    self.MAX_ATTEMPT = self.am_getOption( "MaxTransferAttempts", self.MAX_ATTEMPT )
    self.log.info( "Max transfer attempts  = %s" % self.MAX_ATTEMPT )


    # # thread pool
    self.MIN_THREADS = self.am_getOption( "MinThreads", self.MIN_THREADS )
    self.MAX_THREADS = self.am_getOption( "MaxThreads", self.MAX_THREADS )
    minmax = ( abs( self.MIN_THREADS ), abs( self.MAX_THREADS ) )
    self.MIN_THREADS, self.MAX_THREADS = min( minmax ), max( minmax )
    log.info( "ThreadPool min threads         = %s" % self.MIN_THREADS )
    log.info( "ThreadPool max threads         = %s" % self.MAX_THREADS )

    log.info( "initialize: creation of FTSGraph..." )
    createGraph = self.resetFTSGraph()
    if not createGraph["OK"]:
      log.error( "initialize: %s" % createGraph["Message"] )
      return createGraph

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    log.info( "will use DataManager proxy" )

    # # gMonitor stuff here
    gMonitor.registerActivity( "RequestsExecuted", "Requests executed",
                               "FTSAgent", "Requests/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "FTSJobsSubAtt", "FTSJob created",
                               "FTSAgent", "Created FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsSubOK", "FTSJobs submitted",
                               "FTSAgent", "Successful FTSJobs submissions/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsSubFail", "FTSJobs submissions failed",
                               "FTSAgent", "Failed FTSJobs submissions/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "FTSJobsMonAtt", "FTSJob created",
                               "FTSAgent", "Created FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsMonOK", "FTSJobs submitted",
                               "FTSAgent", "Submitted FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsMonFail", "FTSJobs submissions failed",
                               "FTSAgent", "Failed FTSJobs/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "FtSJobsPerRequest", "Average FTSJobs per request",
                               "FTSAgent", "FTSJobs/Request", gMonitor.OP_MEAN )

    gMonitor.registerActivity( "FTSFilesPerJob", "FTSFiles per FTSJob",
                               "FTSAgent", "Number of FTSFiles per FTSJob", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "FTSSizePerJob", "Average FTSFiles size per FTSJob",
                               "FTSAgent", "Average submitted size per FTSJob", gMonitor.OP_MEAN )
    return S_OK()

  def getRequest( self, requestName ):
    """ read request from cache or reqClient """

    pass

  def execute( self ):
    """ one cycle execution """
    log = gLogger.getSubLogger( "execute" )
    # # reset FTSGraph if expired
    now = datetime.datetime.now()
    if now > self.__ftsGraphValidStamp:
      log.info( "resetting expired FTS graph " )
      resetFTSGraph = self.resetFTSGraph()
      if not resetFTSGraph["OK"]:
        log.error( "FTSGraph recreation error: %s" % resetFTSGraph["Message"] )
        return resetFTSGraph
      self.__ftsGraphValidStamp = now
    # # update R/W access in FTSGraph if expired
    if now > self.__rwAccessValidStamp:
      log.info( "updating expired R/W access for SEs" )
      try:
        self.updateLock().acquire()
        self.__ftsGraph.updateRWAccess()
      finally:
        self.updateLock().release()
        self.__rwAccessValidStamp = now
    requestNames = self.requestClient().getRequestNamesList( [ "Scheduled" ] )

    if not requestNames["OK"]:
      log.error( "unable to read scheduled request names: %s" % requestNames["Message"] )
      return requestNames
    requestNames = requestNames["Value"]
    if not requestNames:
      log.info( "no more request to process" )
      return S_OK()
    log.info( "found %s requests to process" % len( requestNames ) )

    for requestName in requestNames:
      request = self.requestClient().getRequest( requestName )
      if not request["OK"]:
        log.error( request["Message"] )
        continue
      request = request["Value"]
      sTJId = request.RequestName
      while True:
        queue = self.threadPool().generateJobAndQueueIt( self.prcessRequest,
                                                          args = ( request, ),
                                                          sTJId = sTJId )
        if queue["OK"]:
          log.info( "'%s' enqueued for execution" % sTJId )
          gMonitor.addMark( "FTSJobsAtt", 1 )
          break
        time.sleep( 1 )

    # # process all results
    self.threadPool().processAllResults()
    return S_OK()

  def processRequest( self, request ):
    """ process one request

    :param Request request: scheduled Request obj instance
    """
    log = self.log.getSubLogger( request.RequestName )

    operation = request.getWaiting()
    if not operation:
      log.error( "unable to find 'Scheduled' ReplicateAndRegister operation in request" )

    activeJobs = self.ftsClient().getFTSJobsForRequest( request.RequestID )
    if not activeJobs["OK"]:
      log.error( activeJobs["Message"] )
      return activeJobs
    activeJobs = activeJobs["Value"]

    if not activeJobs:
      log.info( "no active FTS jobs found" )

    ftsFiles = self.ftsClient().getFTSFilesForRequest( request.RequestID )

    ftsFilesDict = dict( [ ( k, list() ) for k in ( "toRegister", "toRetry", "toFail", "toReschedule" ) ] )

    for ftsJob in activeJobs:
      monitorJob = self.monitorJob( request, ftsJob )

  def submitJobs( self, request ):
    pass

  def monitorJob( self, request, ftsJob ):
    pass

  def finalizeJob( self, request, ftsJob ):
    pass



