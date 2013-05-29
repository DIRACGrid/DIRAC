########################################################################
# $HeadURL$
########################################################################
""" :mod: SubmitFTSAgent
    ====================

    .. module: SubmitFTSAgent
    :synopsis: agent submitting FTS jobs to the external FTS services
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS Submit Agent takes files from the FTSDB and submits them to the FTS using
    FTSJob helper class.
"""
# # imports
import time
import datetime
import uuid
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
# # from RSS
# #from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

__RCSID__ = "$Id$"

AGENT_NAME = "DataManagement/SubmitFTSAgent"

class SubmitFTSAgent( AgentModule ):
  """
  .. class:: SubmitFTSAgent

  This class is submitting previously scheduled files to the FTS system using helper class FTSJob.

  Files to be transferred are read from FTSDB.FTSFile table, only those with Status = 'Waiting'.
  After submission FTSDB.FTSFile.Status is set to 'Submitted'. The rest of state propagation is
  done in FTSMonitorAgent.

  An information about newly created FTS jobs is hold in FTSDB.FTSJob.
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
  # # placeholder fot FTS client
  __ftsClient = None
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

  def updateLock( self ):
    """ update lock """
    if not self.__updateLock:
      self.__updateLock = LockRing().getLock( "SubmitFTSAgentLock" )
    return self.__updateLock

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
    log = gLogger.getSubLogger( "initialize" )

    self.FTSGRAPH_REFRESH = self.am_getOption( "FTSGraphValidityPeriod", self.FTSGRAPH_REFRESH )
    log.info( "FTSGraph validity period       = %s s" % self.FTSGRAPH_REFRESH )
    self.RW_REFRESH = self.am_getOption( "RWAccessValidityPeriod", self.RW_REFRESH )
    log.info( "SEs R/W access validity period = %s s" % self.RW_REFRESH )

    self.MAX_ACTIVE_JOBS = self.am_getOption( "MaxActiveJobsPerChannel", self.MAX_ACTIVE_JOBS )
    log.info( "Max active FTSJobs/route       = %s" % self.MAX_ACTIVE_JOBS )
    self.MAX_FILES_PER_JOB = self.am_getOption( "MaxFilesPerJob", self.MAX_FILES_PER_JOB )
    log.info( "Max FTSFiles/FTSJob            = %d" % self.MAX_FILES_PER_JOB )

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

    # # gMonitor stuff here
    gMonitor.registerActivity( "FTSJobsAtt", "FTSJob created",
                               "SubmitFTSAgent", "Created FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsOK", "FTSJobs submitted",
                               "SubmitFTSAgent", "Submitted FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsFail", "FTSJobs submissions failed",
                               "SubmitFTSAgent", "Failed FTSJobs/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "FTSFilesPerJob", "FTSFiles per FTSJob",
                               "SubmitFTSAgent", "Number of FTSFiles per FTSJob", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "FTSSizePerJob", "Average FTSFiles size per FTSJob",
                               "SubmitFTSAgent", "Average submitted size per FTSJob", gMonitor.OP_MEAN )
    return S_OK()

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
    # # update R/W access in FTSGraph if expired
    if now > self.__rwAccessValidStamp:
      log.info( "updating expired R/W access for SEs" )
      try:
        self.updateLock().acquire()
        self.__ftsGraph.updateRWAccess()
      finally:
        self.updateLock().release()
        self.__rwAccessValidStamp = now

    log.debug( "reading FTSFiles..." )
    ftsFileList = self.ftsClient().getFTSFileList( [ "Waiting.*" ] )
    if not ftsFileList["OK"]:
      log.error( "unable to read Waiting FTSFiles: %s" % ftsFileList["Message"] )
      return ftsFileList
    ftsFileList = ftsFileList["Value"]

    if not ftsFileList:
      log.info( "no waiting FTSFiles to submit found in FTSDB" )
      return S_OK()

    # #  ftsFileDict[sourceSE][targetSE][OperationID] = [ FTSFile, FTSFile, ... ]
    ftsFileDict = {}
    for ftsFile in ftsFileList:
      if ftsFile.SourceSE not in ftsFileDict:
        ftsFileDict[ftsFile.SourceSE] = {}
      if ftsFile.TargetSE not in ftsFileDict[ftsFile.SourceSE]:
        ftsFileDict[ftsFile.SourceSE][ftsFile.TargetSE] = {}
      if ftsFile.OperationID not in ftsFileDict[ftsFile.SourceSE][ftsFile.TargetSE]:
        ftsFileDict[ftsFile.SourceSE][ftsFile.TargetSE][ftsFile.OperationID] = []
      ftsFileDict[ftsFile.SourceSE][ftsFile.TargetSE][ftsFile.OperationID].append( ftsFile )

    # # thread job counter
    enqueued = 1
    # # entering sourceSE, targetSE, ftsFile loop
    for sourceSE, targetDict in ftsFileDict.items():

      sourceSite = self.__ftsGraph.findSiteForSE( sourceSE )
      if not sourceSite["OK"]:
        log.error( "unable to find source site for %s SE" % sourceSE )
        continue
      sourceSite = sourceSite["Value"]
      if not sourceSite.SEs[sourceSE]["read"]:
        log.error( "source SE %s is banned for reading" % sourceSE )
        continue

      for targetSE, operationDict in targetDict.items():
        targetSite = self.__ftsGraph.findSiteForSE( targetSE )
        if not targetSite["OK"]:
          log.error( "unable to find target site for %s SE" % targetSE )
          continue
        targetSite = targetSite["Value"]
        if not targetSite.SEs[targetSE]["write"]:
          log.error( "target SE %s is banned for writing" % sourceSE )
          continue

        log.info( "%s operations are waiting for transfer from %s to %s" % ( len( operationDict ),
                                                                             sourceSE, targetSE ) )

        route = self.__ftsGraph.findRoute( sourceSE, targetSE )
        if not route["OK"]:
          log.error( route["Message"] )
          return route
        route = route["Value"]

        minmax = min( self.MAX_ACTIVE_JOBS, route.toNode.MaxActiveJobs )
        if route.ActiveJobs > minmax:
          log.info( "maximal number of active jobs (%s) reached at FTS route %s" % ( minmax,
                                                                                     route.routeName ) )
          break

        for opID, ftsFileList in operationDict.items():
          log.info( "processing %s files from Operation %s" % ( len( ftsFileList ), opID ) )
          waitingFileList = [ ftsFile for ftsFile in ftsFileList if ftsFile.Status == "Waiting" ]
          if not waitingFileList:
            self.log.debug( "no waiting files for transfer found" )
            continue

          for ftsFileListChunk in getChunk( waitingFileList, self.MAX_FILES_PER_JOB ):

            sTJId = "submit-%s/%s/%s/%s" % ( enqueued, opID, sourceSE, targetSE )
            while True:
              queue = self.threadPool().generateJobAndQueueIt( self.submit,
                                                               args = ( ftsFileListChunk, targetSite.FTSServer,
                                                                        sourceSE, targetSE, route, sTJId ),
                                                               sTJId = sTJId )
              if queue["OK"]:
                log.info( "'%s' enqueued for execution" % sTJId )
                enqueued += 1
                gMonitor.addMark( "FTSJobsAtt", 1 )
                break
              time.sleep( 1 )

    # # process all results
    self.threadPool().processAllResults()
    return S_OK()

  def submit( self, ftsFileList, ftsServerURI, sourceSE, targetSE, route, sTJId ):
    """ create and submit FTSJob

    :param list ftsFileList: list with FTSFiles
    :param str ftsServerURI: FTS server URI
    :param str sourceSE: source SE
    :param str targetSE: targetSE
    :param Route route: FTSGraph.Route between source site and target site
    :param str sTJId: thread name for sublogger
    """
    log = gLogger.getSubLogger( sTJId, True )
    log.info( "%s FTSFiles to submit to FTS @ %s" % ( len( ftsFileList ), ftsServerURI ) )

    minmax = min( self.MAX_ACTIVE_JOBS, route.toNode.MaxActiveJobs )
    if route.ActiveJobs > minmax:
      log.info( "bailing out: maximal number of active jobs (%s) reached at route %s" % ( minmax,
                                                                                          route.routeName ) )
      return S_OK()

    # # create FTSJob instance
    ftsJob = FTSJob()
    ftsJob.FTSServer = ftsServerURI
    ftsJob.SourceSE = sourceSE
    ftsJob.TargetSE = targetSE

    # # metadata check is done during scheduling, so just add files to job
    for ftsFile in ftsFileList:
      # # reset error
      ftsFile.Error = ""
      # # add file to job
      ftsJob.addFile( ftsFile )

    log.debug( "submitting..." )
    submit = ftsJob.submitFTS2()
    if not submit["OK"]:
      gMonitor.addMark( "FTSJobsFail", 1 )
      log.error( submit["Message"] )
      return submit

    # # update FTS route
    try:
      self.updateLock().acquire()
      route.ActiveJobs += 1
    finally:
      self.updateLock().release()

    # # save newly created FTSJob
    log.info( "FTSJob %s submitted to FTS @ %s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )
    for ftsFile in ftsJob:
      ftsFile.Status = "Submitted"
      ftsFile.FTSGUID = ftsJob.FTSGUID
      ftsFile.Attempt = ftsFile.Attempt + 1

    putFTSJob = self.ftsClient().putFTSJob( ftsJob )
    if not putFTSJob["OK"]:
      log.error( putFTSJob["Message"] )
      gMonitor.addMark( "FTSJobsFail", 1 )
      return putFTSJob
    # # if we're here job was submitted and  saved
    gMonitor.addMark( "FTSJobsOK", 1 )

    gMonitor.addMark( "FTSFilesPerJob", ftsJob.Files )
    gMonitor.addMark( "FTSSizePerJob", ftsJob.Size )

    return S_OK()
