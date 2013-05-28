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


    :deprecated:
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
from DIRAC import S_OK, S_ERROR, gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.Core.Base.AgentModule import AgentModule

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
  ## ProcessTask default timeout in seconds
  __taskTimeout = 300
  ## ProcessPool finalisation timeout 
  __poolTimeout = 300
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
  __requestHolder = dict()

  def __init__( self, *args, **kwargs ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param str loadName: name of module
    :param bool baseAgentName: whatever  
    :param dict properties: whatever else
    """
    
    AgentModule.__init__( self, *args, **kwargs )

    agentName = args[0]

    ## save config path
    self.__configPath = PathFinder.getAgentSection( agentName )
    self.log.info( "Will use %s config path" % self.__configPath )

    ## ProcessPool related stuff
    self.__requestsPerCycle = self.am_getOption( "RequestsPerCycle", 10 )
    self.log.info("requests/cycle = %d" % self.__requestsPerCycle )
    self.__minProcess = self.am_getOption( "MinProcess", 1 )
    self.log.info("ProcessPool min process = %d" % self.__minProcess )
    self.__maxProcess = self.am_getOption( "MaxProcess", 4 )
    self.log.info("ProcessPool max process = %d" % self.__maxProcess )
    self.__queueSize = self.am_getOption( "ProcessPoolQueueSize", 10 )
    self.log.info("ProcessPool queue size = %d" % self.__queueSize )
    self.__poolTimeout = int( self.am_getOption( "ProcessPoolTimeout", 300 ) )
    self.log.info("ProcessPool timeout = %d seconds" % self.__poolTimeout ) 
    self.__taskTimeout = int( self.am_getOption( "ProcessTaskTimeout", 300 ) )
    self.log.info("ProcessTask timeout = %d seconds" % self.__taskTimeout )
    ## request type
    self.__requestType = self.am_getOption( "RequestType", self.__requestType )
    self.log.info( "Will process '%s' request type." % str( self.__requestType ) )
    ## shifter proxy
    self.am_setOption( "shifterProxy", "DataManager" )
    self.log.info( "Will use DataManager proxy by default." )

    ## common monitor activity 
    self.monitor.registerActivity( "Iteration", "Agent Loops", 
                                   self.__class__.__name__, "Loops/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Execute", "Request Processed", 
                                   self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "Done", "Request Completed", 
                                   self.__class__.__name__, "Requests/min", gMonitor.OP_SUM )
      
    ## create request dict
    self.__requestHolder = dict()

  def poolTimeout( self ):
    """ poolTimeout getter

    :param self: self reference
    """
    return self.__poolTimeout

  def setPoolTimeout( self, timeout=300 ):
    """ poolTimeoit setter

    :param self: self reference
    :param int timeout: PP finalisation timeout in seconds 
    """
    self.__poolTimeout = int(timeout)
    
  def taskTimeout( self ):
    """ taskTimeout getter

    :param self: self reference
    """
    return self.__taskTimeout

  def setTaskTimeout( self, timeout=300 ):
    """ taskTimeout setter

    :param self: self reference
    :param int timeout: task timeout in seconds
    """
    self.__taskTimeout = int(timeout)

  def requestHolder( self ):
    """ get request holder dict
    
    :param self: self reference
    """
    return self.__requestHolder

  def deleteRequest( self, requestName ):
    """ delete request from requestHolder

    :param self: self reference
    """
    if requestName in self.__requestHolder:
      del self.__requestHolder[requestName]
      return S_OK()
    return S_ERROR("%s not found in requestHolder" % requestName )

  def saveRequest( self, requestName, requestString ):
    """ put request into requestHolder

    :param cls: class reference
    :param str requestName: request name
    :param str requestString: XML-serialised request
    :param str requestServer: server URL
    """
    if requestName not in self.__requestHolder:
      self.__requestHolder.setdefault( requestName, requestString )
      return S_OK()
    return S_ERROR("saveRequest: request %s cannot be saved, it's already in requestHolder")

  def resetRequest( self, requestName ):
    """ put back :requestName: to RequestClient

    :param self: self reference
    :param str requestName: request's name
    """
    if requestName in self.__requestHolder:
      requestString = self.__requestHolder[requestName]
      reset = self.requestClient().updateRequest( requestName, requestString )
      if not reset["OK"]:
        self.log.error("resetRequest: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
      self.log.debug("resetRequest: request %s has been put back with its initial state" % requestName )
    else:
      self.log.error("resetRequest: unable to reset request %s: request not found in requestHolder" % requestName )

  def resetAllRequests( self ):
    """ put back all requests without callback called into requestClient 

    :param self: self reference
    """
    self.log.info("resetAllRequests: will put %s back requests" % len(self.__requestHolder) )
    for requestName, requestString in self.__requestHolder.items():
      reset = self.requestClient().updateRequest( requestName, requestString )
      if not reset["OK"]:
        self.log.error("resetAllRequests: unable to reset request %s: %s" % ( requestName, reset["Message"] ) )
        continue
      self.log.debug("resetAllRequests: request %s has been put back with its initial state" % requestName )

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

  def requestClient( self ):
    """ RequestClient getter

    :param self: self reference
    """
    if not self.__requestClient:
      self.__requestClient = RequestClient()
    return self.__requestClient

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
      queueSize = abs(self.__queueSize) 
      self.log.info( "ProcessPool: minProcess = %d maxProcess = %d queueSize = %d" % ( minProcess, 
                                                                                       maxProcess, 
                                                                                       queueSize ) )
      self.log.info( "ProcessPool: tasks will use callbacks attached to ProcessPool" )
      self.__processPool = ProcessPool( minProcess, 
                                        maxProcess, 
                                        queueSize, 
                                        poolCallback = self.resultCallback,
                                        poolExceptionCallback = self.exceptionCallback )
      self.__processPool.daemonize()
      self.log.info( "ProcessPool: daemonized and ready")
    return self.__processPool

  def hasProcessPool( self ):
    """ check if ProcessPool exist to speed up finalization 

    :param self: self reference
    """
    return bool( self.__processPool )

  def resultCallback( self, taskID, taskResult ):
    """ definition of request callback function
    
    :param self: self reference
    """
    self.log.info("%s result callback" %  taskID ) 

    if not taskResult["OK"]:
      self.log.error( "%s result callback: %s" % ( taskID, taskResult["Message"] ) )
      if taskResult["Message"] == "Timed out":
        self.resetRequest( taskID )
      self.deleteRequest( taskID )
      return
    
    self.deleteRequest( taskID )
    taskResult = taskResult["Value"]
    ## add monitoring info
    monitor = taskResult["monitor"] if "monitor" in taskResult else {}
    for mark, value in monitor.items():
      try:
        gMonitor.addMark( mark, value )
      except Exception, error:
        self.log.exception( str(error) )
    
  def exceptionCallback( self, taskID, taskException ):
    """ definition of exception callbak function
    
    :param self: self reference
    """
    self.log.error( "%s exception callback" % taskID )
    self.log.error( taskException )

  def getRequest( self, requestType ):
    """ retrive Request of type requestType from RequestClient

    :param cls: class reference
    :param str requestType: type of request
    :return: S_ERROR on error
    :return: S_OK with request dictionary::

      requestDict = { "requestString" : str,
                      "requestName" : str,
                      "executionOrder" : list,
                      "jobID" : int }
    """
    ## prepare requestDict
    requestDict = { "requestString" : None,
                    "requestName" : None,
                    "executionOrder" : None,
                    "jobID" : None }
    ## get request out of RequestClient
    res = self.requestClient().getRequest( requestType )
    if not res["OK"]:
      self.log.error( res["Message"] )
      return res
    elif not res["Value"]:
      msg = "Request of type '%s' not found in RequestClient." % requestType
      self.log.debug( msg )
      return S_OK()
    ## store values
    requestDict["requestName"] = res["Value"]["RequestName"]
    requestDict["requestString"] = res["Value"]["RequestString"]
    ## get JobID
    try:
      requestDict["jobID"] = int( res["Value"]["JobID"] )
    except (ValueError, TypeError), exc:
      self.log.warn( "Cannot read JobID for request %s, setting it to 0: %s" % ( requestDict["requestName"],
                                                                                 str(exc) ) )
      requestDict["jobID"] = 0
    ## get the execution order
    res = self.requestClient().getCurrentExecutionOrder( requestDict["requestName"] )
    if not res["OK"]:
      msg = "Can not get the execution order for request %s." % requestDict["requestName"]
      self.log.error( msg, res["Message"] )
      return res
    requestDict["executionOrder"] = res["Value"]
    ## save this request
    self.saveRequest( requestDict["requestName"], requestDict["requestString"] ) 
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
      taskID = requestDict["requestName"]
      self.log.info( "processPool tasks idle = %s working = %s" % ( self.processPool().getNumIdleProcesses(), 
                                                                    self.processPool().getNumWorkingProcesses() ) )
      while True:
        if not self.processPool().getFreeSlots():
          self.log.info("No free slots available in processPool, will wait 2 seconds to proceed...")
          time.sleep(2)
        else:
          self.log.info("spawning task %s for request %s" % ( taskID, requestDict["requestName"] ) )
          enqueue = self.processPool().createAndQueueTask( self.__requestTask, 
                                                           kwargs = requestDict,
                                                           taskID = requestDict["requestName"],
                                                           blocking = True,
                                                           usePoolCallbacks = True,
                                                           timeOut = self.__taskTimeout )
          if not enqueue["OK"]:
            self.log.error( enqueue["Message"] )
          else:
            self.log.info("successfully enqueued task %s" % taskID )
            ## update request counter
            taskCounter = taskCounter - 1
            ## task created, a little time kick to proceed
            time.sleep( 0.1 )
            break
      
    return S_OK()

  def finalize( self ):
    """ clean ending of maxcycle execution

    :param self: self reference
    """
    ## finalize all processing
    if self.hasProcessPool():
      self.processPool().finalize( timeout = self.__poolTimeout )
    ## reset failover requests for further processing 
    self.resetAllRequests()
    ## good bye, all done!
    return S_OK()
  
