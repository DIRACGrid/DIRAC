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

    DIRAC agent propagating scheduled RMS request in FTS

    Request processing phases (each in a separate thread):

    1. MONITOR
      ...active FTSJobs, prepare FTSFiles dictionary with files to submit, fail, register and reschedule
    2. CHECK REPLICAS
      ...just in case if all transfers are done, if yes, end processing
    3. FAILED FILES:
      ...if at least one Failed FTSFile is found, set Request.Operation.File to 'Failed', end processing
    4. UPDATE Waiting#SourceSE FTSFiles
      ...if any found in FTSDB
    5. REGISTER REPLICA
      ...insert RegisterReplica operation to request, if some FTSFiles failed to register, end processing
    6. RESCHEDULE FILES
      ...for FTSFiles failed with missing sources error
    7. SUBMIT
      ...but read 'Waiting' FTSFiles first from FTSDB and merge those with FTSFiles to retry

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
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
# # from CS
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN
# # from Core
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Time import fromString
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.private.FTSGraph import FTSGraph
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
# # from RMS
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # from RSS
# #from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog    import FileCatalog
from DIRAC.Resources.Utilities              import Utils
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
  # # stage flag
  STAGE_FILES = False

  # # placeholder for FTS client
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
      self.__updateLock = LockRing().getLock( "FTSAgentLock" )
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
  def getSECache( cls ):
    return cls.__seCache

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
      cls.__reqCache[reqName] = getRequest

    return S_OK( cls.__reqCache[reqName] )

  @classmethod
  def putRequest( cls, request, clearCache = True ):
    """ put request back to ReqDB

    :param Request request: Request instance
    :param bool clearCache: clear the cache?

    also finalize request if status == Done
    """
    # # put back request
    if request.RequestName not in cls.__reqCache:
      return S_OK()
    put = cls.requestClient().putRequest( request )
    if not put["OK"]:
      return put
    # # finalize first if possible
    if request.Status == "Done" and request.JobID:
      finalizeRequest = cls.requestClient().finalizeRequest( request.RequestName, request.JobID )
      if not finalizeRequest["OK"]:
        request.Status = "Scheduled"
    # # del request from cache if needed
    if clearCache:
      cls.__reqCache.pop( request.RequestName, None )
    else:
      # get back the request in order to set it Assigned in the DB
      cls.requestClient().getRequest( request.RequestName )
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

    ftsHistory = self.ftsClient().getFTSHistory()
    if not ftsHistory["OK"]:
      log.error( "unable to get FTS history: %s" % ftsHistory["Message"] )
      return ftsHistory
    ftsHistory = ftsHistory["Value"]

    try:
      self.updateLock().acquire()
      self.__ftsGraph = FTSGraph( "FTSGraph", ftsHistory )
    finally:
      self.updateLock().release()

    log.debug( "FTSSites: %s" % len( self.__ftsGraph.nodes() ) )
    for i, site in enumerate( self.__ftsGraph.nodes() ):
      log.debug( " [%02d] FTSSite: %-25s FTSServer: %s" % ( i, site.name, site.FTSServer ) )
    log.debug( "FTSRoutes: %s" % len( self.__ftsGraph.edges() ) )
    for i, route in enumerate( self.__ftsGraph.edges() ):
      log.debug( " [%02d] FTSRoute: %-25s Active FTSJobs (Max) = %s (%s)" % ( i,
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
    # # save rw access stamp
    self.__rwAccessValidStamp = datetime.datetime.now() + datetime.timedelta( seconds = self.RW_REFRESH )

    return S_OK()

  def initialize( self ):
    """ agent's initialization """


      # # data manager
    self.dataManager = DataManager()

    log = self.log.getSubLogger( "initialize" )

    self.FTSGRAPH_REFRESH = self.am_getOption( "FTSGraphValidityPeriod", self.FTSGRAPH_REFRESH )
    log.info( "FTSGraph validity period       = %s s" % self.FTSGRAPH_REFRESH )
    self.RW_REFRESH = self.am_getOption( "RWAccessValidityPeriod", self.RW_REFRESH )
    log.info( "SEs R/W access validity period = %s s" % self.RW_REFRESH )

    self.STAGE_FILES = self.am_getOption( "StageFiles", self.STAGE_FILES )
    log.info( "Stage files before submission  = %s" % {True: "yes", False: "no"}[bool( self.STAGE_FILES )] )

    self.MAX_ACTIVE_JOBS = self.am_getOption( "MaxActiveJobsPerRoute", self.MAX_ACTIVE_JOBS )
    log.info( "Max active FTSJobs/route       = %s" % self.MAX_ACTIVE_JOBS )
    self.MAX_FILES_PER_JOB = self.am_getOption( "MaxFilesPerJob", self.MAX_FILES_PER_JOB )
    log.info( "Max FTSFiles/FTSJob            = %d" % self.MAX_FILES_PER_JOB )

    self.MAX_ATTEMPT = self.am_getOption( "MaxTransferAttempts", self.MAX_ATTEMPT )
    log.info( "Max transfer attempts          = %s" % self.MAX_ATTEMPT )

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

    gMonitor.registerActivity( "FTSMonitorFail", "Failed FTS monitor executions",
                               "FTSAgent", "Execution/mins", gMonitor.OP_SUM )


    pollingTime = self.am_getOption( "PollingTime", 60 )
    for status in list( FTSJob.INITSTATES + FTSJob.TRANSSTATES + FTSJob.FAILEDSTATES + FTSJob.FINALSTATES ):
      gMonitor.registerActivity( "FTSJobs%s" % status, "FTSJobs %s" % status ,
                                 "FTSAgent", "FTSJobs/cycle", gMonitor.OP_ACUM, pollingTime )

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
    if self.__reqCache:
      log.info( 'putting back %d requests from cache' % len( self.__reqCache ) )
    else:
      log.info( 'no requests to put back' )
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
      log.info( "resetting expired FTS graph..." )
      resetFTSGraph = self.resetFTSGraph()
      if not resetFTSGraph["OK"]:
        log.error( "FTSGraph recreation error: %s" % resetFTSGraph["Message"] )
        return resetFTSGraph
      self.__ftsGraphValidStamp = now + datetime.timedelta( seconds = self.FTSGRAPH_REFRESH )
    # # update R/W access in FTSGraph if expired
    if now > self.__rwAccessValidStamp:
      log.info( "updating expired R/W access for SEs..." )
      try:
        self.updateLock().acquire()
        self.__ftsGraph.updateRWAccess()
      finally:
        self.updateLock().release()
        self.__rwAccessValidStamp = now + datetime.timedelta( seconds = self.RW_REFRESH )

    requestNames = self.requestClient().getRequestNamesList( [ "Scheduled" ] )
    if not requestNames["OK"]:
      log.error( "unable to read scheduled request names: %s" % requestNames["Message"] )
      return requestNames
    if not requestNames["Value"]:
      requestNames = self.__reqCache.keys()
    else:
      requestNames = [ req[0] for req in requestNames["Value"] ]
      requestNames = list( set ( requestNames + self.__reqCache.keys() ) )

    if not requestNames:
      log.info( "no 'Scheduled' requests to process" )
      return S_OK()

    log.info( "found %s requests to process:" % len( requestNames ) )
    log.info( " => from internal cache: %s" % ( len( self.__reqCache ) ) )
    log.info( " =>   new read from RMS: %s" % ( len( requestNames ) - len( self.__reqCache ) ) )

    for requestName in requestNames:
      request = self.getRequest( requestName )
      if not request["OK"]:
        log.error( request["Message"] )
        continue
      request = request["Value"]
      sTJId = request.RequestName
      while True:
        queue = self.threadPool().generateJobAndQueueIt( self.processRequest,
                                                         args = ( request, ),
                                                         sTJId = sTJId )
        if queue["OK"]:
          log.info( "request '%s' enqueued for execution" % sTJId )
          gMonitor.addMark( "RequestsAtt", 1 )
          break
        time.sleep( 1 )

    # # process all results
    self.threadPool().processAllResults()
    return S_OK()

  def processRequest( self, request ):
    """ process one request

    :param Request request: ReqDB.Request
    """
    log = self.log.getSubLogger( request.RequestName )

    operation = request.getWaiting()
    if not operation["OK"]:
      log.error( "unable to find 'Scheduled' ReplicateAndRegister operation in request" )
      return self.putRequest( request )
    operation = operation["Value"]
    if operation.Type != "ReplicateAndRegister":
      log.error( "operation to be executed is not a ReplicateAndRegister but %s" % operation.Type )
      return self.putRequest( request )
    if operation.Status != "Scheduled":
      log.error( "operation in a wrong state, expecting 'Scheduled', got %s" % operation.Status )
      return self.putRequest( request )

    log.info( 'start processRequest' )
    # # select  FTSJobs, by default all in TRANS_STATES and INIT_STATES
    ftsJobs = self.ftsClient().getFTSJobsForRequest( request.RequestID )
    if not ftsJobs["OK"]:
      log.error( ftsJobs["Message"] )
      return ftsJobs
    ftsJobs = [ftsJob for ftsJob in ftsJobs.get( "Value", [] ) if ftsJob.Status not in FTSJob.FINALSTATES]

    # # Use a try: finally: for making sure FTS jobs are put back before returnin
    try:
      # # dict keeping info about files to reschedule, submit, fail and register
      ftsFilesDict = dict( [ ( k, list() ) for k in ( "toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate" ) ] )

      if ftsJobs:
        log.info( "==> found %s FTSJobs to monitor" % len( ftsJobs ) )
        # # PHASE 0 = monitor active FTSJobs
        for ftsJob in ftsJobs:
          monitor = self.__monitorJob( request, ftsJob )
          if not monitor["OK"]:
            log.error( "unable to monitor FTSJob %s: %s" % ( ftsJob.FTSJobID, monitor["Message"] ) )
            ftsJob.Status = "Submitted"
          else:
            ftsFilesDict = self.updateFTSFileDict( ftsFilesDict, monitor["Value"] )

        log.info( "monitoring of FTSJobs completed" )
        for key, ftsFiles in ftsFilesDict.items():
          if ftsFiles:
            log.debug( " => %s FTSFiles to %s" % ( len( ftsFiles ), key[2:].lower() ) )

      # # PHASE ONE - check ready replicas
      missingReplicas = self.__checkReadyReplicas( request, operation )
      if not missingReplicas["OK"]:
        log.error( missingReplicas["Message"] )
      else:
        missingReplicas = missingReplicas["Value"]
        for opFile in operation:
          # Actually the condition below should never happen... Change printout for checking
          if opFile.LFN not in missingReplicas and opFile.Status not in ( 'Done', 'Failed' ):
            log.warn( "Should be set! %s is replicated at all targets" % opFile.LFN )
            opFile.Status = "Done"

      toFail = ftsFilesDict.get( "toFail", [] )
      toReschedule = ftsFilesDict.get( "toReschedule", [] )
      toSubmit = ftsFilesDict.get( "toSubmit", [] )
      toRegister = ftsFilesDict.get( "toRegister", [] )
      toUpdate = ftsFilesDict.get( "toUpdate", [] )

      # # PHASE TWO = Failed files? -> make request Failed and return
      if toFail:
        log.error( "==> found %s 'Failed' FTSFiles, but maybe other files can be processed..." % len( toFail ) )
        for opFile in operation:
          for ftsFile in toFail:
            if opFile.FileID == ftsFile.FileID:
              opFile.Error = ftsFile.Error
              opFile.Status = "Failed"
        operation.Error = "%s files are missing any replicas" % len( toFail )
        # # requets.Status should be Failed if all files in the operation "Failed"
        if request.Status == "Failed":
          request.Error = "ReplicateAndRegister %s failed" % operation.Order
          log.error( "request is set to 'Failed'" )
          return self.putRequest( request )

      # # PHASE THREE - update Waiting#TargetSE FTSFiles
      if toUpdate:
        log.info( "==> found %s possible FTSFiles to update..." % ( len( toUpdate ) ) )
        byTarget = {}
        for ftsFile in toUpdate:
          byTarget.setdefault( ftsFile.TargetSE, [] ).append( ftsFile.FileID )
        for targetSE, fileIDList in byTarget.items():
          update = self.ftsClient().setFTSFilesWaiting( operation.OperationID, targetSE, fileIDList )
          if not update["OK"]:
            log.error( "update FTSFiles failed: %s" % update["Message"] )

      # # PHASE FOUR - add 'RegisterReplica' Operations
      if toRegister:
        log.info( "==> found %d Files waiting for registration, adding 'RegisterReplica' operations" % len( toRegister ) )
        registerFiles = self.__register( request, operation, toRegister )
        if not registerFiles["OK"]:
          log.error( "unable to create 'RegisterReplica' operations: %s" % registerFiles["Message"] )
        if request.Status == "Waiting":
          log.info( "request is in 'Waiting' state, will put it back to RMS" )
          return self.putRequest( request )

      # # PHASE FIVE - reschedule operation files
      if toReschedule:
        log.info( "==> found %s Files to reschedule" % len( toReschedule ) )
        rescheduleFiles = self.__reschedule( request, operation, toReschedule )
        if not rescheduleFiles["OK"]:
          log.error( rescheduleFiles["Message"] )
        if request.Status == "Waiting":
          log.info( "request is in 'Waiting' state, will put it back to ReqDB" )
          return self.putRequest( request )

      # # PHASE SIX - read Waiting ftsFiles and submit new FTSJobs
      ftsFiles = self.ftsClient().getFTSFilesForRequest( request.RequestID, [ "Waiting" ] )
      if not ftsFiles["OK"]:
        log.error( ftsFiles["Message"] )
      else:
        retryIds = list( set ( [ ftsFile.FTSFileID for ftsFile in toSubmit ] ) )
        for ftsFile in ftsFiles["Value"]:
          if ftsFile.FTSFileID not in retryIds:
            toSubmit.append( ftsFile )
            retryIds.append( ftsFile.FTSFileID )

      # # submit new ftsJobs
      if operation.Status == "Scheduled" and toSubmit:
        log.info( "==> found %s FTSFiles to submit" % len( toSubmit ) )
        submit = self.__submit( request, operation, toSubmit )
        if not submit["OK"]:
          log.error( submit["Message"] )
        else:
          ftsJobs += submit["Value"]

      # # status change? - put back request
      if request.Status != "Scheduled":
        log.info( "request no longer in 'Scheduled' state, will put it back to ReqDB" )
        put = self.putRequest( request )
        if not put["OK"]:
          log.error( "unable to put back request: %s" % put["Message"] )
          return put

      return self.putRequest( request, clearCache = False ) if request.Status == 'Scheduled' else S_OK()
    except:
      pass
    finally:
      # #  put back jobs in all cases
      if ftsJobs:
        putJobs = self.putFTSJobs( ftsJobs )
        if not putJobs["OK"]:
          log.error( "unable to put back FTSJobs: %s" % putJobs["Message"] )
          return putJobs

  def __reschedule( self, request, operation, toReschedule ):
    """ reschedule list of :toReschedule: files in request for operation :operation:

    :param Request request:
    :param Operation operation:
    :param list toReschedule: list of FTSFiles
    """
    log = self.log.getSubLogger( "%s/reschedule" % request.RequestName )
    log.info( "found %s files to reschedule" % len( toReschedule ) )

    for opFile in operation:
      for ftsFile in toReschedule:
        if opFile.FileID == ftsFile.FileID:
          opFile.Status = "Waiting"

    toSchedule = []

    # # filter files
    for opFile in operation.getWaitingFilesList():

      replicas = self.__filterReplicas( opFile )
      if not replicas["OK"]:
        continue
      replicas = replicas["Value"]
      validReplicas = replicas["Valid"]
      bannedReplicas = replicas["Banned"]
      noReplicas = replicas["NoReplicas"]
      badReplicas = replicas['Bad']

      if not validReplicas:
        if bannedReplicas:
          log.warn( "unable to schedule '%s', replicas only at banned SEs" % opFile.LFN )
        elif noReplicas:
          log.warn( "unable to schedule %s, file doesn't exist" % opFile.LFN )
          opFile.Status = 'Failed'
        elif badReplicas:
          log.warn( "unable to schedule %s, all replicas have a bad checksum" % opFile.LFN )
          opFile.Status = 'Failed'
      else:
        validTargets = list( set( operation.targetSEList ) - set( validReplicas ) )
        if not validTargets:
          log.info( "file %s is already present at all targets" % opFile.LFN )
          opFile.Status = "Done"
        else:
          toSchedule.append( ( opFile.toJSON()["Value"], validReplicas, validTargets ) )

    # # do real schedule here
    if toSchedule:

      ftsSchedule = self.ftsClient().ftsSchedule( request.RequestID,
                                                  operation.OperationID,
                                                  toSchedule )
      if not ftsSchedule["OK"]:
        self.log.error( ftsSchedule["Message"] )
        return ftsSchedule

      ftsSchedule = ftsSchedule["Value"]
      for fileID in ftsSchedule["Successful"]:
        for opFile in operation:
          if fileID == opFile.FileID:
            opFile.Status = "Scheduled"

      for fileID, reason in ftsSchedule["Failed"]:
        for opFile in operation:
          if fileID == opFile.FileID:
            opFile.Error = reason

    return S_OK()


  def __submit( self, request, operation, toSubmit ):
    """ create and submit new FTSJobs using list of FTSFiles

    :param Request request: ReqDB.Request instance
    :param list ftsFiles: list of FTSFile instances

    :return: [ FTSJob, FTSJob, ...]
    """
    log = self.log.getSubLogger( "%s/submit" % request.RequestName )

    bySourceAndTarget = {}
    for ftsFile in toSubmit:
      if ftsFile.SourceSE not in bySourceAndTarget:
        bySourceAndTarget.setdefault( ftsFile.SourceSE, {} )
      if ftsFile.TargetSE not in bySourceAndTarget[ftsFile.SourceSE]:
        bySourceAndTarget[ftsFile.SourceSE].setdefault( ftsFile.TargetSE, [] )
      bySourceAndTarget[ftsFile.SourceSE][ftsFile.TargetSE].append( ftsFile )

    ftsJobs = []

    for source, targetDict in bySourceAndTarget.items():

      for target, ftsFileList in targetDict.items():

        log.info( "found %s files to submit from %s to %s" % ( len( ftsFileList ), source, target ) )

        route = self.__ftsGraph.findRoute( source, target )
        if not route["OK"]:
          log.error( route["Message"] )
          continue
        route = route["Value"]

        sourceRead = route.fromNode.SEs[source]["read"]
        if not sourceRead:
          log.error( "SourceSE %s is banned for reading right now" % source )
          continue

        targetWrite = route.toNode.SEs[target]["write"]
        if not targetWrite:
          log.error( "TargetSE %s is banned for writing right now" % target )
          continue

        if route.ActiveJobs > route.toNode.MaxActiveJobs:
          log.warn( "unable to submit new FTS job, max active jobs reached" )
          continue

        # # create FTSJob
        ftsJob = FTSJob()
        ftsJob.RequestID = request.RequestID
        ftsJob.OperationID = operation.OperationID
        ftsJob.SourceSE = source
        ftsJob.TargetSE = target

        sourceSE = self.getSE( source )
        sourceToken = sourceSE.getStorageParameters( "SRM2" )
        if not sourceToken["OK"]:
          log.error( "unable to get sourceSE '%s' parameters: %s" % ( source, sourceToken["Message"] ) )
          continue
        ftsJob.SourceToken = sourceToken["Value"].get( "SpaceToken", "" )

        targetSE = self.getSE( target )
        targetToken = targetSE.getStorageParameters( "SRM2" )
        if not targetToken["OK"]:
          log.error( "unable to get targetSE '%s' parameters: %s" % ( target, targetToken["Message"] ) )
          continue
        ftsJob.TargetToken = targetToken["Value"].get( "SpaceToken", "" )

        ftsJob.FTSServer = route.toNode.FTSServer

        for ftsFile in ftsFileList:
          ftsFile.Attempt += 1
          ftsFile.Error = ""
          ftsJob.addFile( ftsFile )

        submit = ftsJob.submitFTS2( self.STAGE_FILES )
        if not submit["OK"]:
          log.error( "unable to submit FTSJob: %s" % submit["Message"] )
          continue

        log.info( "FTSJob '%s'@'%s' has been submitted" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

        # # update statuses for job files
        for ftsFile in ftsJob:
          ftsFile.FTSGUID = ftsJob.FTSGUID
          ftsFile.Status = "Submitted"
          ftsFile.Attempt += 1

        # # update graph route
        try:
          self.updateLock().acquire()
          route.ActiveJobs += 1
        finally:
          self.updateLock().release()

        ftsJobs.append( ftsJob )

    log.info( "%s new FTSJobs have been submitted" % len( ftsJobs ) )
    return S_OK( ftsJobs )

  def __monitorJob( self, request, ftsJob ):
    """ execute FTSJob.monitorFTS2 for a given :ftsJob:
        if ftsJob is in a final state, finalize it

    :param Request request: ReqDB.Request instance
    :param FTSJob ftsJob: FTSDB.FTSJob instance
    """
    log = self.log.getSubLogger( "%s/monitor/%s" % ( request.RequestName, ftsJob.FTSGUID ) )
    log.info( "FTSJob '%s'@'%s'" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

    # # this will be returned
    ftsFilesDict = dict( [ ( k, list() ) for k in ( "toRegister", "toSubmit", "toFail", "toReschedule", "toUpdate" ) ] )

    monitor = ftsJob.monitorFTS2()
    if not monitor["OK"]:
      gMonitor.addMark( "FTSMonitorFail", 1 )
      log.error( monitor["Message"] )
      if "getTransferJobSummary2: Not authorised to query request" in monitor["Message"] or 'was not found' in monitor['Message']:
        log.error( "FTSJob not known (expired on server?): delete it" )
        for ftsFile in ftsJob:
          ftsFile.Status = "Waiting"
          ftsFilesDict["toSubmit"].append( ftsFile )
        # #  No way further for that job: delete it
        res = self.ftsClient().deleteFTSJob( ftsJob.FTSJobID )
        if not res['OK']:
          log.error( "Unable to delete FTSJob", res['Message'] )
        return S_OK( ftsFilesDict )
      return monitor

    monitor = monitor["Value"]
    log.info( "FTSJob Status = %s Completeness = %s" % ( ftsJob.Status, ftsJob.Completeness ) )

    # # monitor status change
    gMonitor.addMark( "FTSJobs%s" % ftsJob.Status, 1 )

    if ftsJob.Status in FTSJob.FINALSTATES:
      finalizeFTSJob = self.__finalizeFTSJob( request, ftsJob )
      if not finalizeFTSJob["OK"]:
        log.error( finalizeFTSJob["Message"] )
        return finalizeFTSJob
      ftsFilesDict = self.updateFTSFileDict( ftsFilesDict, finalizeFTSJob["Value"] )

    return S_OK( ftsFilesDict )

  def __finalizeFTSJob( self, request, ftsJob ):
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
    processFiles = self.__filterFiles( ftsJob )
    if not processFiles["OK"]:
      log.error( processFiles["Message"] )
      return processFiles
    ftsFilesDict = self.updateFTSFileDict( ftsFilesDict, processFiles["Value"] )

    # # send accounting record for this job
    self.__sendAccounting( ftsJob, request.OwnerDN )

    # # update graph - remove this job from graph
    route = self.__ftsGraph.findRoute( ftsJob.SourceSE, ftsJob.TargetSE )
    if route["OK"]:
      try:
        self.updateLock().acquire()
        route["Value"].ActiveJobs -= 1
      finally:
        self.updateLock().release()

    log.info( "FTSJob is finalized" )

    return S_OK( ftsFilesDict )

  def __filterFiles( self, ftsJob ):
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

  def __register( self, request, operation, toRegister ):
    """ add RegisterReplica operation

    :param Request request: request instance
    :param Operation transferOp: 'ReplicateAndRegister' operation for this FTSJob
    :param list toRegister: [ FTSDB.FTSFile, ... ] - files that failed to register
    """
    log = self.log.getSubLogger( "%s/registerFiles" % request.RequestName )

    byTarget = {}
    for ftsFile in toRegister:
      if ftsFile.TargetSE not in byTarget:
        byTarget.setdefault( ftsFile.TargetSE, [] )
      byTarget[ftsFile.TargetSE].append( ftsFile )
    log.info( "will create %s 'RegisterReplica' operations" % len( byTarget ) )

    for target, ftsFileList in byTarget.items():
      log.info( "creating 'RegisterReplica' operation for targetSE %s with %s files..." % ( target,
                                                                                            len( ftsFileList ) ) )
      registerOperation = Operation()
      registerOperation.Type = "RegisterReplica"
      registerOperation.Status = "Waiting"
      registerOperation.TargetSE = target
      targetSE = self.getSE( target )
      for ftsFile in ftsFileList:
        opFile = File()
        opFile.LFN = ftsFile.LFN
        pfn = Utils.executeSingleFileOrDirWrapper( targetSE.getPfnForProtocol( ftsFile.TargetSURL, protocol = "SRM2", withPort = False ) )
        if not pfn["OK"]:
          continue
        opFile.PFN = pfn["Value"]
        registerOperation.addFile( opFile )
      request.insertBefore( registerOperation, operation )

    return S_OK()

  @staticmethod
  def __sendAccounting( ftsJob, ownerDN ):
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
    accountingDict["Protocol"] = "FTS3" if 'fts3' in ftsJob.FTSServer.lower() else 'FTS'
    accountingDict['ExecutionSite'] = ftsJob.FTSServer

    accountingDict['RegistrationTime'] = ftsJob._regTime
    accountingDict['RegistrationOK'] = ftsJob._regSuccess
    accountingDict['RegistrationTotal'] = ftsJob._regTotal

    accountingDict["TransferOK"] = len( [ f for f in ftsJob if f.Status in FTSFile.SUCCESS_STATES ] )
    accountingDict["TransferTotal"] = len( ftsJob )
    accountingDict["TransferSize"] = ftsJob.Size - ftsJob.FailedSize
    accountingDict["FinalStatus"] = ftsJob.Status
    accountingDict["Source"] = ftsJob.SourceSE
    accountingDict["Destination"] = ftsJob.TargetSE

    dt = ftsJob.LastUpdate - ftsJob.SubmitTime
    transferTime = dt.days * 86400 + dt.seconds
    accountingDict["TransferTime"] = transferTime
    # accountingDict['TransferTime'] = sum( [f._duration for f in ftsJob])
    dataOp.setValuesFromDict( accountingDict )
    dataOp.commit()

  def __checkReadyReplicas( self, request, operation ):
    """ check ready replicas for transferOperation """
    log = self.log.getSubLogger( "%s/checkReadyReplicas" % request.RequestName )

    targetSESet = set( operation.targetSEList )

    # # { LFN: [ targetSE, ... ] }
    missingReplicas = {}

    scheduledFiles = dict( [ ( opFile.LFN, opFile ) for opFile in operation
                              if opFile.Status in ( "Scheduled", "Waiting" ) ] )
    # # get replicas
    replicas = FileCatalog().getReplicas( scheduledFiles )
    if not replicas["OK"]:
      self.log.error( replicas["Message"] )
      return replicas
    replicas = replicas["Value"]

    fullyReplicated = 0
    missingSEs = {}
    for successfulLFN in replicas["Successful"]:
      reps = set( replicas['Successful'][successfulLFN] )
      if targetSESet.issubset( reps ):
        log.info( "%s has been replicated to all targets" % successfulLFN )
        fullyReplicated += 1
        scheduledFiles[successfulLFN].Status = "Done"
      else:
        missingReplicas[successfulLFN] = sorted( targetSESet - reps )
        ses = ",".join( missingReplicas[ successfulLFN ] )
        missingSEs[ses] = missingSEs.setdefault( ses, 0 ) + 1
        log.verbose( "%s is still missing at %s" % ( successfulLFN, ses ) )
    if fullyReplicated:
      log.info( "%d new files have been replicated to all targets" % fullyReplicated )
    if missingSEs:
      for ses in missingSEs:
        log.info( "%d replicas still missing at %s" % ( missingSEs[ses], ses ) )

    reMissing = re.compile( "no such file or directory" )
    for failedLFN, errStr in replicas["Failed"].items():
      scheduledFiles[failedLFN].Error = errStr
      if reMissing.search( errStr.lower() ):
        log.error( "%s is missing, setting its status to 'Failed'" % failedLFN )
        scheduledFiles[failedLFN].Status = "Failed"
      else:
        log.warn( "unable to read replicas for %s: %s" % ( failedLFN, errStr ) )

    return S_OK( missingReplicas )

  def __filterReplicas( self, opFile ):
    """ filter out banned/invalid source SEs """
    from DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister import filterReplicas
    return filterReplicas( opFile, logger = self.log, datamanager = self.dataManager, seCache = self.getSEcache() )
