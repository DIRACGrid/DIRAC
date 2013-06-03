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
# # from CS
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
# # from Core
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.List import getChunk
from DIRAC.Core.Utilities.Time import fromString
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.private.FTSGraph import FTSGraph
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
# # from RMS
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
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

  Agent propagating Scheduled request to Done or Failed state in the FTS system.

  Requests and associated FTSJobs (and so FTSFiles) are kept in cache.

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
  # # request cache
  __reqCache = dict()

  def updateLock( self ):
    """ update lock """
    if not self.__updateLock:
      self.__updateLock = LockRing().getLock( "SubmitFTSAgentLock" )
    return self.__updateLock

  @classmethod
  def requestClient( cls ):
    """ request client getter """
    if not cls.__requestClient:
      cls.__requestClient = ReqClient()
    return cls.__requestClient

  @classmethod
  def ftsClient( cls ):
    """ FTS client """
    if not cls.__ftsClient:
      cls.__ftsClient = FTSClient()
    return cls.__ftsClient

  @classmethod
  def rssClient( cls ):
    """ RSS client getter """
    if not cls.__rssClient:
      cls.__rssClient = ResourceStatus()
    return cls.__rssClient

  @classmethod
  def getSE( cls, seName ):
    """ keep SEs in cache """
    if seName not in cls.__seCache:
      cls.__seCache[seName] = StorageElement( seName )
    return cls.__seCache[seName]

  @classmethod
  def getRequest( cls, reqName ):
    """ keep Requests in cache """
    if reqName not in cls.__reqCache:
      getRequest = cls.requestClient().getRequest( reqName )
      if not getRequest["OK"]:
        return getRequest
      getRequest = getRequest["Value"]
      if not getRequest:
        return S_ERROR( "request of name '%s' not found in ReqDB" % reqName )
      cls.__reqCache[reqName] = { "request": getRequest, "jobs": {} }

    request = cls.__reqCache[reqName]["request"]
    ftsJobs = cls.ftsClient().getFTSJobsForRequest( request.RequestID )
    if ftsJobs["OK"]:
      cls.__reqCache[reqName]["jobs"] = ftsJobs["Value"]

    return S_OK( cls.__reqCache[reqName] )

  @classmethod
  def putRequest( cls, requestName ):
    """ put request back to ReqDB

    :param str requestName: Request.RequestName value
    """
    requestDict = cls.__reqCache.get( requestName, None )
    if not requestDict:
      return S_ERROR( "request '%s' not cached" % requestName )

    request = requestDict.get( "request", None )

    # # put FTSJobs
    jobs = requestDict.get( "jobs", [] )
    if jobs:
      putJobs = cls.putFTSJobs( jobs )
      if not putJobs["OK"]:
        return putJobs
    # # put request
    put = cls.requestClient().putRequest( request )
    if not put["OK"]:
      return put
    # # del cache entry
    del cls.__reqCache[ requestName ]
    return S_OK()

  @classmethod
  def putFTSJobs( cls, ftsJobsList ):
    """ put back fts jobs to the FTSDB """
    for ftsJob in ftsJobsList:
      put = cls.ftsClient().putFTSJob( ftsJob )
      if not put["OK"]:
        return put
    return S_OK()

  @staticmethod
  def updateFTSFileDict( ftsFilesDict, toUpdateDict ):
    """ update :ftsFilesDict: with FTSFiles in :toUpdateDict: """
    for category, ftsFileList in ftsFilesDict.items():
      for ftsFile in toUpdateDict.get( category, [] ):
        if ftsFile not in ftsFileList:
          ftsFileList.append( ftsFile )
    return ftsFilesDict

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
    gMonitor.registerActivity( "RequestsAtt", "Attempted requests executions",
                               "FTSAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestsOK", "Successful requests executions",
                               "FTSAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestsFail", "Failed requests executions",
                               "FTSAgent", "Requests/min", gMonitor.OP_SUM )


    gMonitor.registerActivity( "FTSJobsSubAtt", "FTSJobs creation attempts",
                               "FTSAgent", "Created FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsSubOK", "FTSJobs submitted successfully",
                               "FTSAgent", "Successful FTSJobs submissions/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsSubFail", "FTSJobs submissions failed",
                               "FTSAgent", "Failed FTSJobs submissions/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "FTSJobsMonAtt", "FTSJobs monitored",
                               "FTSAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsMonOK", "FTSJobs monitored successfully",
                               "FTSAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSJobsMonFail", "FTSJobs attempts failed",
                               "FTSAgent", "FTSJobs/min", gMonitor.OP_SUM )

    pollingTime = self.am_getOption( "PollingTime", 60 )
    for status in list( FTSJob.INITSTATES + FTSJob.TRANSSTATES + FTSJob.FAILEDSTATES + FTSJob.FINALSTATES ):
      gMonitor.registerActivity( "FTSJobs%s" % status, "FTSJobs %s" % status ,
                                 "MonitorFTSAgent", "FTSJobs/cycle", gMonitor.OP_ACUM, pollingTime )

    gMonitor.registerActivity( "FtSJobsPerRequest", "Average FTSJobs per request",
                               "FTSAgent", "FTSJobs/Request", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "FTSFilesPerJob", "FTSFiles per FTSJob",
                               "FTSAgent", "Number of FTSFiles per FTSJob", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "FTSSizePerJob", "Average FTSFiles size per FTSJob",
                               "FTSAgent", "Average submitted size per FTSJob", gMonitor.OP_MEAN )
    return S_OK()


  def finalize( self ):
    """ finalize processing """
    log = self.log.getSubLogger( "finalize" )
    for request in self.__reqCache.values():
      put = self.requestClient().putRequest( request )
      if not put["OK"]:
        log.error( "unable to put back request '%s': %s" % ( request.RequestName, put["Message"] ) )
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
      requestDict = self.getRequest( requestName )
      if not requestDict["OK"]:
        log.error( requestDict["Message"] )
        continue
      requestDict = requestDict["Value"]
      sTJId = requestDict["request"].RequestName
      while True:
        queue = self.threadPool().generateJobAndQueueIt( self.processRequest,
                                                         args = ( requestDict, ),
                                                         sTJId = sTJId )
        if queue["OK"]:
          log.info( "'%s' enqueued for execution" % sTJId )
          gMonitor.addMark( "RequestsAtt", 1 )
          break
        time.sleep( 1 )

    # # process all results
    self.threadPool().processAllResults()
    return S_OK()

  def processRequest( self, requestDict ):
    """ process one request

    :param dict requestDict: { "request": ReqDB.Request, "jobs": [ FTSDB.FTSJob, FTSDB.FTSJob, ...] }
    """
    request = requestDict["request"]
    ftsJobs = requestDict["jobs"]

    log = self.log.getSubLogger( request.RequestName )

    operation = request.getWaiting()
    if not operation or operation.Status != "Scheduled":
      log.error( "unable to find 'Scheduled' ReplicateAndRegister operation in request" )
      return self.putRequest( request )

    if not ftsJobs:
      ftsJobs = self.ftsClient().getFTSJobsForRequest( request.RequestID )
      if not ftsJobs["OK"]:
        log.error( ftsJobs["Message"] )
        return ftsJobs
    ftsJobs = ftsJobs["Value"]

    # # dict keeping info about files to reschedule, submit, fail and register
    ftsFilesDict = dict( [ ( k, list() ) for k in ( "toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate" ) ] )

    toSubmit = []
    if not ftsJobs:
      log.info( "no active FTSJobs found, will try to collect waiting FTSFiles..." )
      ftsFiles = self.ftsClient().getFTSFilesForRequest( request.RequestID, [ "Waiting" ] )
      if not ftsFiles["OK"]:
        log.error( ftsFiles["Message"] )
      toSubmit = ftsFiles["Value"]
    ftsFilesDict["toSubmit"] = toSubmit

    # # monitor active jobs
    for ftsJob in ftsJobs:
      monitor = self.monitorJob( request, ftsJob )
      if not monitor["OK"]:
        log.error( "unable to monitor FTSJob %s: %s" % ( ftsJob.FTSJobID, monitor["Message"] ) )
        continue
      ftsFilesDict = self.updateFTSFileDict( ftsFilesDict, monitor["Value"] )

    # # TODO: process ftsFileDict

    # # fail
    toFail = ftsFilesDict.get( "toFail", [] )
    toReschedule = ftsFilesDict.get( "toReschedule", [] )
    toSubmit = ftsFilesDict.get( "toSubmit", [] )
    toRegister = ftsFilesDict.get( "toRegister", [] )
    toUpdate = ftsFilesDict.get( "toUpdate", [] )



    # # update statuses for waiting scheduled FTSFiles
    if toUpdate:
      bySource = {}
      for ftsFile in toUpdate:
        if ftsFile.SourceSE not in bySource:
          bySource.setdefault( ftsFile.SourceSE, [] )
        bySource[ftsFile.SourceSE].append( ftsFile.FileID )
      for sourceSE, fileIDList in bySource.items():
        update = self.ftsClient().setFTSFilesWaiting( operation.OperationID, sourceSE, fileIDList )
        if not update["OK"]:
          log.error( "update FTSFiles failed: %s" % update["Message"] )
          continue

    if toFail:
      log.error( "found %s failed FTSFiles, request execution cannot proceed..." % len( toFail ) )
      for opFile in operation:
        for ftsFile in toFail:
          if opFile.FileID == ftsFile.FileID:
            opFile.Error = ftsFile.Error
            opFile.Status = "Failed"
      # # requets.Status should be Failed at this stage "Failed"
      return self.putRequest( request )

    # # register
    if toRegister:
      log.info( "found %s FTSFiles waiting for registration, adding 'RegisterReplica' operations" )
      registerFiles = self.registerFiles( request, operation, toRegister )
      if not registerFiles["OK"]:
        log.error( "unable to create 'RegisterReplica' operations: %s" % registerFiles["Message"] )

    # # reschedule
    if toReschedule:
      log.info( "found %s files to reschedule" % len( toReschedule ) )

    # # submit
    if operation.Status == "Scheduled" and toSubmit:
      log.info( "found %s files to submit" % len( toSubmit ) )
      submit = self.submitJobs( request, operation, toSubmit )
      if not submit["OK"]:
        log.error( submit["Message"] )

    # # update graph

    return S_OK()

  def submitJobs( self, request, operation, toSubmit ):
    """ create and submit new FTSJobs using list of FTSFiles

    :param Request request: ReqDB.Request instance
    :param list ftsFiles: list of FTSFile instances
    """
    log = self.log.getSubLogger( "%s/submit" % request.RequestName )

    bySourceAndTarget = {}
    for ftsFile in toSubmit:
      if ftsFile.SourceSE not in bySourceAndTarget:
        bySourceAndTarget.setdefault( ftsFile.SourceSE, {} )
      if ftsFile.TargetSE not in bySourceAndTarget[ftsFile.SourceSE]:
        bySourceAndTarget[ftsFile.SourceSE].setdefault( ftsFile.TargetSE, [] )
      bySourceAndTarget[ftsFile.SourceSE][ftsFile.TargetSE].append( ftsFile )

    submittedJobs = 0

    for source, targetDict in bySourceAndTarget.items():

      for target, ftsFileList in targetDict.items():

        log.info( "found %s files to submit from %s to %s" % ( len( ftsFileList ), source, target ) )

        route = self.__ftsGraph.findRoute( source, target )
        if not route["OK"]:
          log.error( route["Message"] )
          continue
        route = route["Value"]

        sourceRead = route.fromNode["SEs"][source]["read"]
        if not sourceRead:
          log.error( "SourceSE %s is banned for reading right now" % source )
          continue

        targetWrite = route.toNode["SEs"][target]["write"]
        if not targetWrite:
          log.error( "TargetSE %s is banned for writing right now" % target )
          continue

        if route.toNode.ActiveJobs > route.toNode.MaxActiveJobs:
          log.warn( "unable to submit new FTS job, max active jobs reached" )
          continue

        # # create FTSJob
        ftsJob = FTSJob()
        ftsJob.RequestID = request.RequestID
        ftsJob.OperationID = operation.OperationID
        ftsJob.SourceSE = source
        ftsJob.TargetSE = target
        ftsJob.FTSServer = route.toNode.FTSServer

        for ftsFile in ftsFileList:
          ftsFile.Attempt += 1
          ftsFile.Error = ""
          ftsJob.addFile( ftsFile )

        submit = ftsJob.submit()
        if not submit["OK"]:
          log.error( "unable to submit FTSJob: %s" % submit["Message"] )
          continue
        submittedJobs += 1

        # # TODO: update graph

    log.info( "%s new FTSJobs have been submitted" % submittedJobs )
    return S_OK()


  def monitorJob( self, request, ftsJob ):
    """ execute FTSJob.monitorFTS2 for a given :ftsJob:
        if ftsJob is in a final state, finalize it

    :param Request request: ReqDB.Request instance
    :param FTSJob ftsJob: FTSDB.FTSJob instance
    """
    log = self.log.getSubLogger( "%s/monitor/%s" % ( request.RequestName, ftsJob.FTSJobID ) )
    log.info( "FTSJob %s@%s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

    # # this will be returned
    ftsFilesDict = dict( [ ( k, list() ) for k in ( "toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate" ) ] )

    monitor = ftsJob.monitorFTS2()
    if not monitor["OK"]:
      gMonitor.addMark( "FTSMonitorFail", 1 )
      log.error( monitor["Message"] )
      if "getTransferJobSummary2: Not authorised to query request" in monitor["Message"]:
        log.error( "FTSJob not known (expired on server?)" )
        for ftsFile in ftsJob:
          ftsFilesDict["toSubmit"] = ftsFile
        return S_OK( ftsFilesDict )
      return monitor

    monitor = monitor["Value"]
    log.info( "FTSJob Status = %s Completeness = %s" % ( ftsJob.Status, ftsJob.Completeness ) )

    # # monitor status change
    gMonitor.addMark( "FTSJobs%s" % ftsJob.Status, 1 )

    if ftsJob.Status in FTSJob.FINALSTATES:
      finalizeFTSJob = self.finalizeFTSJob( request, ftsJob )
      if not finalizeFTSJob["OK"]:
        log.error( finalizeFTSJob["Message"] )
        return finalizeFTSJob
      ftsFilesDict = self.updateFTSFileDict( ftsFilesDict, finalizeFTSJob["Value"] )

    return S_OK( ftsFilesDict )

  def finalizeFTSJob( self, request, ftsJob ):
    """ finalize FTSJob

    :param Request request: ReqDB.Request instance
    :param FTSJob ftsJob: FTSDB.FTSJob instance
    """
    log = self.log.getSubLogger( "%s/monitor/%s/finalize" % ( request.RequestName, ftsJob.FTSJobID ) )
    log.info( "finalizing FTSJob %s@%s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

    # # this will be returned
    ftsFilesDict = dict( [ ( k, list() ) for k in ( "toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate" ) ] )

    monitor = ftsJob.monitorFTS2( full = True )
    if not monitor["OK"]:
      log.error( monitor["Message"] )
      return monitor

    # # split FTSFiles to different categories
    processFiles = self.filterFiles( ftsJob )
    if not processFiles["OK"]:
      log.error( processFiles["Message"] )
      return processFiles
    ftsFilesDict = self.updateFTSFileDict( ftsFilesDict, processFiles["Value"] )

    # # send accounting record for this job
    self.sendAccounting( ftsJob, request.OwnerDN )

    # # TODO: update graph


    return S_OK( ftsFilesDict )

  def filterFiles( self, ftsJob ):
    """ process ftsFiles from finished ftsJob

    :param FTSJob ftsJob: monitored FTSJob instance
    """
    # # lists for different categories
    toUpdate = []
    toReschedule = []
    toRegister = []
    toSubmit = []
    toFail = []

    # # loop over files in fts job
    for ftsFile in ftsJob:
      # # successful files
      if ftsFile.Status == "Finished":
        if ftsFile.Error == "AddCatalogReplicaFailed":
          toRegister.append( ftsFile )
        toUpdate.append( ftsFile )
        continue
      if ftsFile.Status == "Failed":
        if ftsFile.Error == "MissingSource":
          toReschedule.append( ftsFile )
        else:
          if ftsFile.Attempt < self.MAX_ATTEMPT:
            toSubmit.append( ftsFile )
          else:
            toFail.append( ftsFile )
            ftsFile.Error = "Max attempts reached"

    return S_OK( { "toUpdate": toUpdate,
                   "toSubmit": toSubmit,
                   "toRegister": toRegister,
                   "toReschedule": toReschedule,
                   "toFail": toFail } )

  def registerFiles( self, request, operation, toRegister ):
    """ add RegisterReplica operation

    :param Request request: request instance
    :param Operation transferOp: 'ReplicateAndRegister' operation for this FTSJob
    :param list toRegister: [ FTSDB.FTSFile, ... ] - files that failed to register
    """
    log = self.log.getSubLogger( "%s/addRegisterReplica" % request.RequestName )

    byTarget = {}
    for ftsFile in toRegister:
      if ftsFile.TargetSE not in byTarget:
        byTarget.setdefault( ftsFile.TargetSE, [] )
      byTarget.append( ftsFile )
    log.info( "will create %s 'RegisterReplica' operations" % len( byTarget ) )

    for target, ftsFileList in byTarget.items():
      log.info( "creating 'RegisterReplica' operation for targetSE %s with %s files..." % ( target, len( ftsFileList ) ) )
      registerOperation = Operation()
      registerOperation.Type = "RegisterReplica"
      registerOperation.Status = "Waiting"
      registerOperation.TargetSE = target
      targetSE = self.getSE( target )
      for ftsFile in ftsFileList:
        opFile = File()
        opFile.LFN = ftsFile.LFN
        pfn = targetSE.getPfnForProtocol( ftsFile.TargetSURL, "SRM2", withPort = False )
        if not pfn["OK"]:
          continue
        opFile.PFN = pfn["Value"]
        registerOperation.addFile( opFile )
      request.insertBefore( registerOperation, operation )

    return S_OK()


  @staticmethod
  def sendAccounting( ftsJob, ownerDN ):
    """ prepare and send DataOperation to AccouringDB """

    dataOp = DataOperation()
    dataOp.setStartTime( fromString( ftsJob.SubmitTime ) )
    dataOp.setEndTime( fromString( ftsJob.LastUpdate ) )

    accountingDict = dict()
    accountingDict["OperationType"] = "ReplicateAndRegister"

    username = getUsernameForDN( ownerDN )
    if not username["OK"]:
      username = ownerDN
    else:
      username = username["Value"]

    accountingDict["User"] = username
    accountingDict["Protocol"] = "FTS"

    # accountingDict['RegistrationTime'] = 0
    # accountingDict['RegistrationOK'] = 0
    # accountingDict['RegistrationTotal'] = 0

    accountingDict["TransferOK"] = len( [ f for f in ftsJob if f.Status == "Finished" ] )
    accountingDict["TransferTotal"] = len( ftsJob )
    accountingDict["TransferSize"] = ftsJob.Size
    accountingDict["FinalStatus"] = ftsJob.Status
    accountingDict["Source"] = ftsJob.SourceSE
    accountingDict["Destination"] = ftsJob.TargetSE

    dt = ftsJob.LastUpdate - ftsJob.SubmitTime
    transferTime = dt.days * 86400 + dt.seconds
    accountingDict["TransferTime"] = transferTime
    dataOp.setValuesFromDict( accountingDict )
    dataOp.commit()
