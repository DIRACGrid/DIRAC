########################################################################
# $HeadURL $
# File: RequestExecutingAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/12 15:36:47
########################################################################

""" :mod: RequestExecutingAgent
    ===========================

    .. module: RequestExecutingAgent
    :synopsis: request executing agent
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request processing agent
"""

__RCSID__ = "$Id $"

# #
# @file RequestExecutingAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/12 15:36:56
# @brief Definition of RequestExecutingAgent class.

# # imports
import time
# # from DIRAC
from DIRAC import gMonitor, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask

# # agent name
AGENT_NAME = "RequestManagement/RequestExecutingAgent"

class AgentConfigError( Exception ):
  """ misconfiguration error """
  def __init__( self, msg ):
    """ ctor
    :param str msg: error string
    """
    Exception.__init__( self )
    self.msg = msg
  def __str__( self ):
    """ str op """
    return self.msg

########################################################################
class RequestExecutingAgent( AgentModule ):
  """
  .. class:: RequestExecutingAgent

  request processing agent using ProcessPool, Operation handlers and RequestTask
  """
  # # process pool
  __processPool = None
  # # request cache
  __requestCache = {}
  # # requests/cycle
  __requestsPerCycle = 100
  # # minimal nb of subprocess running
  __minProcess = 2
  # # maximal nb of subprocess executed same time
  __maxProcess = 4
  # # ProcessPool queue size
  __queueSize = 20
  # # ProcessTask default timeout in seconds
  __taskTimeout = 900
  # # ProcessPool finalization timeout
  __poolTimeout = 900
  # # ProcessPool sleep time
  __poolSleep = 5
  # # placeholder for RequestClient instance
  __requestClient = None
  # # FTS scheduling flag
  __FTSMode = False

  def __init__( self, *args, **kwargs ):
    """ c'tor """
    # # call base class ctor
    AgentModule.__init__( self, *args, **kwargs )
    # # ProcessPool related stuff
    self.__requestsPerCycle = self.am_getOption( "RequestsPerCycle", self.__requestsPerCycle )
    self.log.info( "Requests/cycle = %d" % self.__requestsPerCycle )
    self.__minProcess = self.am_getOption( "MinProcess", self.__minProcess )
    self.log.info( "ProcessPool min process = %d" % self.__minProcess )
    self.__maxProcess = self.am_getOption( "MaxProcess", 4 )
    self.log.info( "ProcessPool max process = %d" % self.__maxProcess )
    self.__queueSize = self.am_getOption( "ProcessPoolQueueSize", self.__queueSize )
    self.log.info( "ProcessPool queue size = %d" % self.__queueSize )
    self.__poolTimeout = int( self.am_getOption( "ProcessPoolTimeout", self.__poolTimeout ) )
    self.log.info( "ProcessPool timeout = %d seconds" % self.__poolTimeout )
    self.__poolSleep = int( self.am_getOption( "ProcessPoolSleep", self.__poolSleep ) )
    self.log.info( "ProcessPool sleep time = %d seconds" % self.__poolSleep )
    self.__taskTimeout = int( self.am_getOption( "ProcessTaskTimeout", self.__taskTimeout ) )
    self.log.info( "ProcessTask timeout = %d seconds" % self.__taskTimeout )

    # # keep config path
    agentName = self.am_getModuleParam( "fullName" )
    self.__configPath = PathFinder.getAgentSection( agentName )

    # # operation handlers over here
    opHandlersPath = "%s/%s" % ( self.__configPath, "OperationHandlers" )
    opHandlers = gConfig.getSections( opHandlersPath )
    if not opHandlers["OK"]:
      self.log.error( opHandlers["Message" ] )
      raise AgentConfigError( "OperationHandlers section not found in CS under %s" % self.__configPath )
    opHandlers = opHandlers["Value"]

    self.operationHandlers = []
    for opHandler in opHandlers:
      opHandlerPath = "%s/%s/Location" % ( opHandlersPath, opHandler )
      opLocation = gConfig.getValue( opHandlerPath, "" )
      if not opLocation:
        self.log.error( "%s not set for %s operation handler" % ( opHandlerPath, opHandler ) )
        continue
      self.operationHandlers.append( opLocation )

    self.log.info( "Operation handlers:" )
    for itemTuple in enumerate ( self.operationHandlers ):
      self.log.info( "[%s] %s" % itemTuple )

    # # handlers dict
    self.handlersDict = dict()
    # # common monitor activity
    gMonitor.registerActivity( "Iteration", "Agent Loops",
                               "RequestExecutingAgent", "Loops/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Processed", "Request Processed",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "Done", "Request Completed",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )
    # # create request dict
    self.__requestCache = dict()

  def processPool( self ):
    """ facade for ProcessPool """
    if not self.__processPool:
      minProcess = max( 1, self.__minProcess )
      maxProcess = max( self.__minProcess, self.__maxProcess )
      queueSize = abs( self.__queueSize )
      self.log.info( "ProcessPool: minProcess = %d maxProcess = %d queueSize = %d" % ( minProcess,
                                                                                       maxProcess,
                                                                                       queueSize ) )
      self.__processPool = ProcessPool( minProcess,
                                        maxProcess,
                                        queueSize,
                                        poolCallback = self.resultCallback,
                                        poolExceptionCallback = self.exceptionCallback )
      self.__processPool.daemonize()
    return self.__processPool

  def requestClient( self ):
    """ RequestClient getter """
    if not self.__requestClient:
      self.__requestClient = ReqClient()
    return self.__requestClient

  def cleanCache( self, requestName = None ):
    """ delete request from requestCache

    :param str requestName: Request.RequestName
    """
    if requestName in self.__requestCache:
      del self.__requestCache[requestName]
    return S_OK()

  def cacheRequest( self, request ):
    """ put request into requestCache

    :param Request request: Request instance
    """
    self.__requestCache.setdefault( request.RequestName, request )
    return S_OK()

  def resetRequest( self, requestName ):
    """ put back :requestName: to RequestClient

    :param str requestName: request's name
    """
    if requestName in self.__requestCache:
      reset = self.requestClient().updateRequest( self.__requestCache[requestName] )
      if not reset["OK"]:
        return S_ERROR( "resetRequest: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
    return S_OK()

  def resetAllRequests( self ):
    """ put back all requests without callback called into requestClient

    :param self: self reference
    """
    self.log.info( "resetAllRequests: will put %s back requests" % len( self.__requestCache ) )
    for requestName, request in self.__requestCache.iteritems():
      reset = self.requestClient().updateRequest( request )
      if not reset["OK"]:
        self.log.error( "resetAllRequests: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
        continue
      self.log.debug( "resetAllRequests: request %s has been put back with its initial state" % requestName )
    return S_OK()

  def initialize( self ):
    """ initialize agent

    at the moment creates handlers dictionary
    """
    for opHandler in self.operationHandlers:
      handlerName = opHandler.split( "/" )[-1]
      self.handlersDict[ handlerName ] = opHandler
      self.log.debug( "handler '%s' for operation '%s' registered" % ( opHandler, handlerName ) )
    if not self.handlersDict:
      self.log.error( "operation handlers not set, check configuration option 'Operations'!" )
      return S_ERROR( "Operation handlers not set!" )
    return S_OK()

  def execute( self ):
    """ read requests from RequestClient and enqueue them into ProcessPool """
    gMonitor.addMark( "Iteration", 1 )
    # # requests (and so tasks) counter
    taskCounter = 0
    while taskCounter < self.__requestsPerCycle:
      self.log.debug( "execute: executing %d request in this cycle" % taskCounter )
      getRequest = self.requestClient().getRequest()
      if not getRequest["OK"]:
        self.log.error( "execute: %s" % getRequest["Message"] )
        break
      if not getRequest["Value"]:
        self.log.info( "execute: not more waiting requests to process" )
        break
      # # OK, we've got you
      request = getRequest["Value"]
      # # set task id
      taskID = request.RequestName
      # # save current request in cache
      self.cacheRequest( request )

      self.log.info( "processPool tasks idle = %s working = %s" % ( self.processPool().getNumIdleProcesses(),
                                                                    self.processPool().getNumWorkingProcesses() ) )
      while True:
        if not self.processPool().getFreeSlots():
          self.log.info( "No free slots available in processPool, will wait %d seconds to proceed" % self.__poolSleep )
          time.sleep( self.__poolSleep )
        else:
          self.log.info( "spawning task for request '%s'" % ( request.RequestName ) )
          enqueue = self.processPool().createAndQueueTask( RequestTask,
                                                           kwargs = { "requestXML" : request.toXML()["Value"],
                                                                      "handlersDict" : self.handlersDict,
                                                                      "csPath" : self.__configPath },
                                                           taskID = taskID,
                                                           blocking = True,
                                                           usePoolCallbacks = True,
                                                           timeOut = self.__taskTimeout )
          if not enqueue["OK"]:
            self.log.error( enqueue["Message"] )
          else:
            self.log.debug( "successfully enqueued task '%s'" % taskID )
            # # update monitor
            gMonitor.addMark( "Processed", 1 )
            # # update request counter
            taskCounter += 1
            # # task created, a little time kick to proceed
            time.sleep( 0.1 )
            break

    # # clean return
    return S_OK()

  def finalize( self ):
    """ agent finalization """
    if self.__processPool:
      self.processPool().finalize( timeout = self.__poolTimeout )
    self.resetAllRequests()
    return S_OK()

  def resultCallback( self, taskID, taskResult ):
    """ definition of request callback function

    :param str taskID: Reqiest.RequestName
    :param dict taskResult: task result S_OK/S_ERROR
    """
    self.log.info( "callback: %s result is %s(%s)" % ( taskID,
                                                      "S_OK" if taskResult["OK"] else "S_ERROR",
                                                      taskResult["Value"] if taskResult["OK"] else taskResult["Message"] ) )

    if not taskResult["OK"]:
      if taskResult["Message"] == "Timed out":
        self.resetRequest( taskID )
    # # clean cache
    self.cleanCache( taskID )

  def exceptionCallback( self, taskID, taskException ):
    """ definition of exception callback function

    :param str taskID: Request.RequestName
    :param Exception taskException: Exception instance
    """
    self.log.error( "exceptionCallback: %s was hit by exception %s" % ( taskID, taskException ) )
    self.resetRequest( taskID )
