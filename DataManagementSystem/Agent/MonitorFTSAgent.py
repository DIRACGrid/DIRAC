########################################################################
# $HeadURL$
########################################################################
"""
  :mod: MonitorFTSAgent
  =====================

  .. module: MonitorFTSAgent
  :synopsis: agent monitoring FTS jobs at the external FTS services
  .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

  The MonitorFTSAgent takes FTS jobs from the FTSDB and monitors their execution.
"""
# # imports
import time
import re
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
# # from RMS
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement

# # RCSID
__RCSID__ = "$Id$"
# # agent's name
AGENT_NAME = 'DataManagement/MonitorFTSAgent'

class MonitorFTSAgent( AgentModule ):
  """
  .. class:: MonitorFTSAgent

  Monitor submitted FTS jobs.
  """
  # # FTS client
  __ftsClient = None
  # # thread pool
  __threadPool = None
  # # request client
  __requestClient = None
  # # replica manager
  __replicaManager = None
  # # SE cache
  __seCache = {}
  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10

  def ftsClient( self ):
    """ FTSClient getter """
    if not self.__ftsClient:
      self.__ftsClient = FTSClient()
    return self.__ftsClient

  def threadPool( self ):
    """ thread pool getter """
    if not self.__threadPool:
      self.__threadPool = ThreadPool( self.MIN_THREADS, self.MAX_THREADS )
      self.__threadPool.daemonize()
    return self.__threadPool

  def requestClient( self ):
    """ request client getter """
    if not self.__requestClient:
      self.__requestClient = ReqClient()
    return self.__requestClient

  def replicaManager( self ):
    """ replica manager getter """
    if not self.__replicaManager:
      self.__replicaManager = ReplicaManager()
    return self.__replicaManager

  @classmethod
  def getSE( cls, seName ):
    """ keep se in cache"""
    if seName not in cls.__seCache:
      cls.__seCache[seName] = StorageElement( seName )
    return cls.__seCache[seName]

  def initialize( self ):
    """ agent's initialization """

    # # gMonitor stuff over here
    gMonitor.registerActivity( "FTSMonitorAtt", "FTSJobs monitor attempts",
                               "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSMonitorOK", "Successful FTSJobs monitor attempts",
                               "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "FTSMonitorFail", "Failed FTSJobs monitor attempts",
                               "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_SUM )

    for status in list( FTSJob.INITSTATES + FTSJob.TRANSSTATES + FTSJob.FAILEDSTATES + FTSJob.FINALSTATES ):
      gMonitor.registerActivity( "FTSJobs%s" % status, "FTSJobs %s" % status ,
                                 "MonitorFTSAgent", "FTSJobs/min", gMonitor.OP_ACUM )

    self.am_setOption( "shifterProxy", "DataManager" )

    self.MIN_THREADS = self.am_getOption( "MinThreads", self.MIN_THREADS )
    self.MAX_THREADS = self.am_getOption( "MaxThreads", self.MAX_THREADS )
    minmax = ( abs( self.MIN_THREADS ), abs( self.MAX_THREADS ) )
    self.MIN_THREADS, self.MAX_THREADS = min( minmax ), max( minmax )
    self.log.info( "ThreadPool min threads = %s" % self.MIN_THREADS )
    self.log.info( "ThreadPool max threads = %s" % self.MAX_THREADS )

    return S_OK()

  def execute( self ):
    """ push FTS Jobs to the thread pool """

    ftsJobs = self.ftsClient().getFTSJobList()
    if not ftsJobs["OK"]:
      self.log.error( "execute: failed to get FTSJobs: %s" % ftsJobs["Message"] )
      return ftsJobs

    ftsJobs = ftsJobs["Value"]

    if not ftsJobs:
      self.log.info( "execute: no active FTS jobs found." )
      return S_OK()

    self.log.info( "execute: found %s FTSJobs to monitor" % len( ftsJobs ) )

    enqueued = 1
    for ftsJob in ftsJobs:
      sTJId = "monitor-%s/%s" % ( enqueued, ftsJob.FTSJobID )
      while True:
        self.log.debug( "execute: submitting FTSJob %s to monitor" % ( ftsJob.FTSJobID ) )
        ret = self.threadPool().generateJobAndQueueIt( self.monitorTransfer, args = ( ftsJob, sTJId ), sTJId = sTJId )
        if ret["OK"]:
          gMonitor.addMark( "FTSMonitorAtt", 1 )
          enqueued += 1
          break
        # # sleep 1 second to proceed
        time.sleep( 1 )
    self.threadPool().processAllResults()
    return S_OK()

  def monitorTransfer( self, ftsJob, sTJId ):
    """ monitors transfer obtained from FTSDB

    :param dict ftsReqDict: FTS job dictionary
    """
    log = gLogger.getSubLogger( sTJId )

    log.info( "%s at %s" % ( ftsJob.FTSGUID, ftsJob.FTSServer ) )

    monitor = ftsJob.monitorFTS2()
    if not monitor["OK"]:
      gMonitor.addMark( "FTSMonitorFail", 1 )
      log.error( monitor["Message"] )
      if "getTransferJobSummary2: Not authorised to query request" in monitor["Message"]:
        log.error( "FTSJob expired at server" )
        return self.resetFiles( ftsJob, "FTSJob expired on server", sTJId )
      return monitor
    monitor = monitor["Value"]

    # # monitor status change
    gMonitor.addMark( "FTSJobs%s" % ftsJob.Status, 1 )

    if ftsJob.Status in FTSJob.FINALSTATES:
      finalize = self.finalizeFTSJob( ftsJob, sTJId )
      if not finalize["OK"]:
        log.error( "unable to finalize ftsJob: %s" % finalize["Message"] )


    putFTSJob = self.ftsClient().putFTSJob( ftsJob )
    if not putFTSJob["OK"]:
      log.error( putFTSJob["Message"] )
      gMonitor.addMark( "FTSMonitorFail", 1 )
      return putFTSJob

    gMonitor.addMark( "FTSMonitorOK", 1 )
    return S_OK()

  def finalizeFTSJob( self, ftsJob, sTJId ):
    """ finalize FTSJob

    :param FTSJob ftsJob: FTSJob instance
    :param cTJId: thread id for logger
    """
    log = gLogger.getSubLogger( "%s/finalize" % sTJId )

    # # placeholder for request and transfer operation
    request = None
    transferOperation = None

    # # perform full monitor
    monitor = ftsJob.monitorFTS2( full = True )
    if not monitor["OK"]:
      log.error( monitor["Message"] )
      return monitor

    getRequest = self.getRequest( ftsJob )
    if not getRequest["OK"]:
      log.error( getRequest["Message"] )

      if "Request not found" in getRequest["Message"]:
        log.warn( "request not found, will cancel FTSJob" )
        for ftsFile in ftsJob:
          ftsFile.Status = "Canceled"
        ftsJob.Status = "Canceled"
        return getRequest
        # # will try again later - reset FTSJob status to 'Submitted'
      ftsJob.Status = "Submitted"

    getRequest = getRequest["Value"]

    request = getRequest["request"] if "request" in getRequest else None
    transferOperation = getRequest["operation"] if "operation" in getRequest else None

    if None in ( request, transferOperation ):
      log.error( "request or operation is missing" )
      for ftsFile in ftsJob:
        ftsFile.Status = "Canceled"
      ftsJob.Status = "Canceled"
      return S_ERROR( "unable to read request" )

    # # split FTSFiles to different categories
    processFiles = self.filterFiles( ftsJob )
    if not processFiles["OK"]:
      log.error( processFiles["Message"] )
      return processFiles
    processFiles = processFiles["Value"]

    # # ... and keep them for further processing
    toReschedule = processFiles.get( "toReschedule", [] )
    toUpdate = processFiles.get( "toUpdate", [] )
    toRetry = processFiles.get( "toRetry", [] )
    toRegister = processFiles.get( "toRegister", [] )

    # # update ftsFiles to retry
    if toRetry:
      for ftsFile in toRetry:
        ftsFile.Status = "Waiting"

    missingReplicas = self.checkReadyReplicas( transferOperation )
    if not missingReplicas["OK"]:
    # # bail out on error
      log.error( missingReplicas["Message"] )
      return missingReplicas

    missingReplicas = missingReplicas["Value"]
    if not missingReplicas:
      log.info( "all files replicated in OperationID=%s Request '%s'" % ( transferOperation.operationID,
                                                                          request.RequestName ) )
      transferOperation.Status = "Done"
      for ftsFile in ftsJob:
        ftsFile.Status = "Finished"
      ftsJob.Status = "Finished"
      return S_OK( ( request ) )
    else:
      if toRegister:
        self.registerFiles( request, transferOperation, toRegister )

      if toReschedule:
        # # remove ftsFIle from job
        for ftsFile in toReschedule:
          ftsJob.subFile( ftsFile )
        self.rescheduleFiles( transferOperation, toReschedule )

      if toUpdate:
        update = self.ftsClient().setFTSFilesWaiting( transferOperation.OperationID,
                                                      ftsJob.TargetSE,
                                                      [ ftsFile.FileID for ftsFile in toUpdate ] )
        if not update["OK"]:
          log.error( "unable to update descendants for finished FTSFiles: %s" % update["Message"] )
          ftsJob.Status = "Submitted"
          return update

    # # put back request if any
    if request:
      if request.Status == "Done":
        log.info( "request %s is done" % request.RequestName )
        if request.JobID:
          finalizeRequest = self.requestClient().finalizeRequest( request.RequestName, request.JobID )
          if not finalizeRequest["OK"]:
            log.error( "unable to finalize request %s, will reset it's status to Waiting" % request.RequestName )
            request.Status = "Waiting"

      putRequest = self.requestClient().putRequest( request )
      if not putRequest["OK"]:
        log.error( "unable to put back request: %s" % putRequest["Message"] )
      return putRequest

    return S_OK( request )


  def getRequest( self, ftsJob ):
    """ get request for this ftsJob

    :param FTSJob ftsJob: monitored FTSJob
    """
    opId = ftsJob[0].OperationID
    request = self.requestClient().getScheduledRequest( opId )
    if not request["OK"]:
      return request

    request = request["Value"]
    if not request:
      return S_ERROR( "Request not found" )

    for op in request:
      if op.OperationID == opId:
        return S_OK( { "request": request, "operation": op } )

    return S_ERROR( "Request '%s' is missing 'ReplicateAndRegister' OperationID=%s" % ( request.RequestName, opId ) )


  def checkReadyReplicas( self, transferOperation ):
    """ check ready replicas for transferOperation """
    targetSESet = set( transferOperation.targetSEList )

    # # { LFN: [ targetSE, ... ] }
    missingReplicas = {}

    scheduledFiles = dict( [ ( opFile.LFN, opFile ) for opFile in transferOperation
                              if opFile.Status in ( "Scheduled", "Waiting" ) ] )
    # # get replicas
    replicas = self.replicaManager().getCatalogReplicas( scheduledFiles.keys() )

    if not replicas["OK"]:
      self.log.error( replicas["Message"] )
      return replicas
    replicas = replicas["Value"]

    for successfulLFN, reps in replicas["Successful"]:
      if targetSESet.issubset( set( reps ) ):
        scheduledFiles[successfulLFN].Status = "Done"
      else:
        missingReplicas[successfulLFN] = list( set( reps ) - targetSESet )

    reMissing = re.compile( "no such file or directory" )
    for failedLFN, errStr in replicas["Failed"]:
      scheduledFiles[failedLFN].Error = errStr
      if reMissing.search( errStr.lower() ):
        scheduledFiles[failedLFN].Status = "Failed"

    return S_OK( missingReplicas )

  def rescheduleFiles( self, operation, toReschedule = None ):
    """ update statues for Operation.Files to waiting """
    if not operation:
      return S_OK()
    toReschedule = toReschedule if toReschedule else []
    ids = [ ftsFile.FileID for ftsFile in toReschedule ]
    if not ids:
      return S_OK()
    for opFile in operation:
      if opFile.FileID in ids:
        opFile.Status = "Waiting"
    return self.ftsClient().deleteFTSFiles( operation.OperationID, ids )

  def registerFiles( self, request = None, transferOp = None, toRegister = None ):
    """ add RegisterReplica operation

    :param Request request: request instance
    :param Operation transferOp: 'ReplicateAndRegister' operation for this FTSJob
    :param list toRegister: [ FTSDB.FTSFile, ... ] - files that failed to register
    """
    if not request or not transferOp:
      return S_OK()
    toRegister = toRegister if toRegister else []
    if toRegister:
      registerOperation = Operation()
      registerOperation.Type = "RegisterReplica"
      registerOperation.Status = "Waiting"
      registerOperation.TargetSE = toRegister[0].TargetSE
      targetSE = self.getSE( registerOperation.TargetSE )
      for ftsFile in toRegister:
        opFile = File()
        opFile.LFN = ftsFile.LFN
        pfn = targetSE.getPfnForProtocol( ftsFile.TargetSURL, "SRM2", withPort = False )
        if not pfn["OK"]:
          continue
        opFile.PFN = pfn["Value"]
        registerOperation.addFile( opFile )
      request.insertBefore( registerOperation, transferOp )
      return S_OK()

  @staticmethod
  def filterFiles( ftsJob ):
    """ process ftsFiles from finished ftsJob

    :param FTSJob ftsJob: monitored FTSJob instance
    """
    # # lists for different categories
    toUpdate = []
    toReschedule = []
    toRegister = []
    toRetry = []

    # #  read request
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
          toRetry.append( ftsFile )

    return S_OK( { "toUpdate": toUpdate,
                   "toRetry": toRetry,
                   "toRegister": toRegister,
                   "toReschedule": toReschedule } )

  def resetFiles( self, ftsJob, reason, sTJId ):
    """ clean up when FTS job had expired on the server side

    :param FTSJob ftsJob: FTSJob instance
    """
    log = gLogger.getSubLogger( "%s/resetFiles" % sTJId )
    for ftsFile in ftsJob:
      ftsFile.Status = "Waiting"
      # ftsFile.FTSGUID = ""
      ftsFile.Error = reason
      putFile = self.ftsClient().putFTSFile( ftsFile )
      if not putFile["OK"]:
        log.error( putFile["Message"] )
        return putFile
    ftsJob.Status = "Failed"
    putJob = self.ftsClient().putFTSJob( ftsJob )
    if not putJob["OK"]:
      log.error( putJob["Message"] )
      return putJob
    return S_OK()

