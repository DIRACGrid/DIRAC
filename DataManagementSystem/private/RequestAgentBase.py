########################################################################
# $HeadURL $
# File: RequestAgentBase.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/05/31 07:40:33
########################################################################
""" :mod: RequestAgentBase
    =======================
 
    .. module: RequestAgentBase
    :synopsis: Implementation of base class for DMS agents working with Requests. 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Implementation of base class for DMS agents working with Requests and RequestTasks.

"""

__RCSID__ = "$Id $"

##
# @file RequestAgentBase.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/05/31 07:41:05
# @brief Definition of RequestAgentBase class.

## py imports 
import time

## DIRAC imports 
from DIRAC import gLogger, S_OK, S_ERROR, gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.Core.Base.AgentModule import AgentModule

def defaultCallback( task, ret ):
  """ default callback function

  on S_ERROR, error message is reported to the logger
  if S_OK["Value"]["monitor"] all values from it are passed to 
  monitoring services by calling gMonitor.addMark (and catching all 
  exceptions over there btw, in case some metric wasn't defined and so) 

  :param task: subprocess task
  :param ret: return from RequestTask.__call__ (S_OK/S_ERROR)
  """
  log = gLogger.getSubLogger( "defaultCallback/%s" % task.getTaskID() )
  log.showHeaders( True )
  log.setLevel( "INFO" )

  log.info("callback from task taskID=%s" % task.getTaskID() )
  if not ret["OK"]: 
    log.error( ret["Message"] )
    return
  log.info( ret["Value"] )

  ## remove request from agent cache
  for item in globals():
    if isinstance( item, RequestAgentBase ):
      item.deleteRequest( test.getTaskID() )

  if "monitor" in ret["Value"]:
    monitor = ret["Value"]["monitor"]
    for mark, value in monitor.items():
      try:
        gMonitor.addMark( mark, value )
      except Exception, error:
        log.exception( str(error) )
    
def defaultExceptionCallback( task, exc_info ):
  """ default exception callbak, just printing to gLogger.exception

  :param task: subprocess task
  :param exc_info: exception info
  """
  log = gLogger.getSubLogger( "exceptionCallback/%s" % task.getTaskID() )
  log.showHeaders( True )
  log.setLevel( "EXCEPTION" )
  log.exception( "exception %s from task taskID=%d" % ( str(exc_info), task.getTaskID() ) ) )

  ## remove request from agent cache
  for item in globals():
    if isinstance( item, RequestAgentBase ):
      item.deleteRequest( task.getTaskID() )



########################################################################
class RequestAgentBase( AgentModule ):
  """
  .. class:: RequestAgentBase

  Helper class for DIRAC agents dealing with RequestContainers and Requests.  
  """
  
  ## placeholder for thread pool
  __processPool = None
  ## requests/cycle 
  __requestsPerCycle = 50
  ## minimal nb of subprocess running 
  __minProcess = 2
  ## maximal nb of subprocess executed same time
  __maxProcess = 4
  ## ProcessPool queue size 
  __queueSize = 10
  ## placeholder for RequestClient instance
  __requestClient = None
  ## request type
  __requestType = ""
  ## placeholder for request task class definition 
  __requestTask = None
  ## placeholder for request callback function
  __requestCallback = None
  ## placeholder for exception callback function
  __exceptionCallback = None
  ## config path in CS
  __configPath = None
  ## read request holder 
  __requestHolder = None

  def __init__( self, agentName, baseAgentName=False, properties=dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: ???
    :param dict properties: whatever
    """
    AgentModule.__init__( self, agentName, baseAgentName, properties )

    ## save config path
    self.__configPath = PathFinder.getAgentSection( agentName )
    self.log.info( "Will use %s config path" % self.__configPath )
    

    self.__requestsPerCycle = self.am_getOption( "RequestsPerCycle", 10 )
    self.log.info("requests/cycle = %d" % self.__requestsPerCycle )
    self.__minProcess = self.am_getOption( "MinProcess", 1 )
    self.log.info("ProcessPool min process = %d" % self.__minProcess )
    self.__maxProcess = self.am_getOption( "MaxProcess", 4 )
    self.log.info("ProcessPool max process = %d" % self.__maxProcess )
    self.__queueSize = self.am_getOption( "ProcessPoolQueueSize", 10 )
    self.log.info("ProcessPool queue size = %d" % self.__queueSize )
    self.__requestType = self.am_getOption( "RequestType", None )
    self.log.info( "Will process '%s' request type." % str( self.__requestType ) )

    self.am_setOption( "shifterProxy", "DataManager" )
    self.log.info( "Will use DataManager proxy by default." )

    ## common monitor activity 
    self.monitor.registerActivity( "Iteration", "Agent Loops", self.__class__.__name__, "Loops/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Execute", "Request Processed", self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Done", "Request Completed", self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
      
    ## register callbacks
    self.registerCallBack( defaultCallback )
    self.registerExceptionCallBack( defaultExceptionCallback )

    ## create request dict
    self.__requestHolder = dict()

  @classmethod
  def deleteRequest( self, requestName ):
    """ delete request from requestHolder

    :param self: self reference
    """
    if requestName in self.__requestHolder:
      del self.__requestHolder[requestName]

  def saveRequest( self, requestName, requestString, requestServer ):
    if requestName not in self.__requestHolder:
      self.__requestHolder.setdefault( requestName, ( requestString, requestServer ) )
      return S_OK()
    return S_ERROR("saveRequest: request %s canot be saved, it's already in requestHolder")

  def resetRequests( self ):
    """ put back requests without callback called into requestClient 

    :param self: self reference
    """
    for requestName, requestTuple  in self.__requestHolder:
      requestString, requestServer = requestTuple
      reset = self.requestClient().updateRequest( requestName, requestString, requestServer )
      if not reset["OK"]:
        self.log.error("resetRequest: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
        continue
      self.log.debug("resetRequest: request %s has been put back with its initial state" % requestName )

    ## register callbacks
    self.registerCallBack( defaultCallback )
    self.registerExceptionCallBack( defaultExceptionCallback )

  def configPath( self ):
    """ config path getter

    :param self: self reference
    """
    return self.__configPath

  def requestsPerCycle( self ):
    """ get number of request to be processed in one cycle
    
    :param self: self reference
    """
    return self.__requestsPerCycle

  @classmethod
  def requestClient( cls ):
    """ RequestClient getter

    :param self: self reference
    """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  def processPool( self ):
    """ 'Live long and prosper, my dear ProcessPool'
                                        - Mr. Spock    
    :param self: self reference
    :return: brand new shiny ProcessPool instance on first call, the same instance
             on subsequent calls
    """
    if not self.__processPool:
      minProcess = max( 1, self.__minProcess ) 
      maxProcess = max( self.__minProcess, self.__maxProcess )
      queueSize = max( self.__requestsPerCycle, self.__queueSize )
      self.log.info("ProcessPool minProcess = %d maxProcess = %d queueSize = %d" % ( minProcess, 
                                                                                     maxProcess, 
                                                                                     queueSize ) )
      self.__processPool = ProcessPool( minProcess, maxProcess, queueSize )
      self.__processPool.daemonize()
    return self.__processPool

  def registerCallBack( self, callback ):
    """ register callback function executed after requestTask call
    
    :param self: self reference
    :param callback: function definition
    """
    if not callable( callback ):
      return S_ERROR("Request callback cannot be registered, passed object '%s' isn't callable" % str(callback) )
    self.__requestCallback = callback
    return S_OK()

  def requestCallback( self ):
    """ get definition of request callback function
    
    :param self: self reference
    """
    return self.__requestCallback

  def registerExceptionCallback( self, exceptionCallback ):
    """ register exception callback, executed when requestTask raise an exception
 
    :param self: self reference
    :param exceptionCallback: function definition
    """
    if not callable( exceptionCallback ):
      return S_ERROR("Exception callback cannot be registered, object '%s' isn't callable." % str(exceptionCallback) )
    self.__exceptionCallback = exceptionCallback

  def exceptionCallback( self ):
    """ get definition of exception callbak function
    
    :param self: self reference
    """
    return self.__exceptionCallback


  @classmethod
  def getRequest( cls, requestType ):
    """ retrive Request of type requestType from RequestDB

    :param cls: class reference
    :param str requestType: type of request
    :return: S_ERROR on error
    :return: S_OK with request dictionary::

      requestDict = { "requestString" : str,
                      "requestName" : str,
                      "sourceServer" : str,
                      "executionOrder" : list,
                      "jobID" : int }
    """
    ## prepare requestDict
    requestDict = { "requestString" : None,
                    "requestName" : None,
                    "sourceServer" : None,
                    "executionOrder" : None,
                    "jobID" : None }
    ## get request out of DB
    res = cls.requestClient().getRequest( requestType )
    if not res["OK"]:
      gLogger.error( res["Message"] )
      return res
    elif not res["Value"]:
      msg = "Request of type '%s' not found in RequestDB." % requestType
      gLogger.info( msg )
      return S_OK()
    ## store values
    requestDict["requestName"] = res["Value"]["RequestName"]
    requestDict["requestString"] = res["Value"]["RequestString"]
    requestDict["sourceServer"] = res["Value"]["Server"]
    ## get JobID
    try:
      requestDict["jobID"] = int( res["Value"]["JobID"] )
    except (ValueError, TypeError), exc:
      gLogger.warn( "Cannot read JobID for request %s, setting it to 0: %s" % ( requestDict["requestName"],
                                                                                str(exc) ) )
      requestDict["jobID"] = 0
    ## get the execution order
    res = cls.requestClient().getCurrentExecutionOrder( requestDict["requestName"],
                                                        requestDict["sourceServer"] )
    if not res["OK"]:
      msg = "Can not get the execution order for request %s." % requestDict["requestName"]
      gLogger.error( msg, res["Message"] )
      return res
    requestDict["executionOrder"] = res["Value"]
    ## save this request
    self.saveRequest( requestDict["requestName"], 
                      requestDict["requestString"], 
                      requestDict["sourceServer"] )
    ## return requestDict at least
    return S_OK( requestDict )

  def setRequestType( self, requestType ):
    """ set request type to process

    :param self: self reference
    :param str requestType: request type
    """
    self.__requestType = requestType

  def setRequestTask( self, requestTask ):
    """ set requestTask type

    :param self: self reference
    :param type requestTask: RequestTask-derived class definition
    """
    if issubclass( requestTask, RequestTask ):
      self.__requestTask = requestTask
      return S_OK()
    return S_ERROR("Wrong inheritance, requestTask should be derived from RequestTask class.")

  def execute( self ):
    """ one cycle execution 

    :param self: self reference
    """
    taskCounter = self.__requestsPerCycle 

    while taskCounter:
      requestDict = self.getRequest( self.__requestType ) 
      ## can't get request?
      if not requestDict["OK"]:
        self.log.error( requestDict["Message"] )
        break
      ## no more requests?
      if not requestDict["Value"]:
        self.log.info("No more waiting requests found.")
        break
      ## enqueue
      requestDict = requestDict["Value"]
      requestDict["configPath"] = self.__configPath
      taskID = self.__requestsPerCycle - taskCounter + 1
      while True:
        if not self.processPool().getFreeSlots():
          self.log.info("No free slots available in processPool, will wait a second to proceed...")
          time.sleep( 1 )
        else:
          self.log.always("spawning task %d for request %s" % ( taskID, requestDict["requestName"] ) )
          enqueue = self.processPool().createAndQueueTask( self.__requestTask, 
                                                           kwargs = requestDict,
                                                           taskID = requestDict["requestName"],
                                                           callback = self.requestCallback(),
                                                           exceptionCallback = self.exceptionCallback(),
                                                           blocking = True )
          ## can't enqueue new task?
          if not enqueue["OK"]:
            self.log.error( enqueue["Message"] )
          else:
            ## enqueued, decrease taskCounter
            taskCounter = taskCounter - 1
            ## time kick 
            time.sleep( 0.1 )
            ## break enqueue while
            break

    return S_OK()

  def finalize( self ):
    """ clean ending of one cycle execution

    :param self: self reference
    """
    ## finalize all processing
    self.processPool().processAllResults()
    ## reset failover requests for further processing 
    self.resetRequests()
    ## good bye, all done!
    return S_OK()
  
