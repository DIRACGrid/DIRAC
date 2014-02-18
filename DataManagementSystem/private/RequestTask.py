########################################################################
# $HeadURL $
# File: RequestTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/10/12 12:08:51
########################################################################
""" :mod: RequestTask
    =================

    .. module: RequestTask
    :synopsis: base class for requests execution in separate subprocesses
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Base class for requests execution in a separate subprocesses.

    :deprecated:
"""

__RCSID__ = "$Id$"

# #
# @file RequestTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/10/12 12:09:18
# @brief Definition of RequestTask class.

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


class RequestTask( object ):
  """
  .. class:: RequestTask

  Base class for DMS 'transfer', 'removal' and 'register' Requests processing.
  This class is meant to be executed as a ProcessTask inside ProcessPool.

  The most important and common global DIRAC objects are created in RequestTask constructor.
  This includes gLogger, gConfig, gProxyManager, S_OK and S_ERROR. The constructor also
  imports a set of common modules: os, sys, re, time and everything from types module.
  
  All other DIRAC tools and clients (i.e. DataManager) are instance attributes of RequestTask class

  All currently proxied tools are::

  DataLoggingClient -- self.dataLoggingClient()
  RequestClient     -- self.requestClient()
  StorageFactory    -- self.storageFactory()

  SubLogger message handles for all levels are also proxied, so you can directly use them in your code, i.e.::

    self.info("An info message")
    self.debug("This will be shown only in debug")

  For handling sub-request one has to register their actions handlers using :self.addOperationAction:
  method. This method checks if handler is defined as a method of inherited class and then puts its
  definition into internal operation dispatcher dictionary with a key of sub-request's operation name.

  Each operation handler should have the signature::

    def operationName( self, index, requestObj, subRequestAttrs, subRequestFiles )

  where index is a sub-request counter, requestObj is a RequestContainer instance,
  subRequestAttrs is a dict with sub-request attributes and subRequestFiles is a dict with
  files attached to the sub-request.

  Handlers shoudl always return S_OK with value of (modified or not) requestObj, S_ERROR with some
  error message otherwise.

  Processing of request is done automatically in self.__call__, one doesn't have to worry about changing
  credentials, looping over subrequests or request finalizing -- only sub-request processing matters in
  the all inherited classes.

  Concerning :MonitringClient: (or better known its global instance :gMonitor:), if someone wants to send
  some metric over there, she has to put in agent's code registration of activity and then in a particular
  task use :RequestTask.addMark: to save monitoring data. All monitored activities are held in
  :RequestTask.__monitor: dict which at the end of processing is returned from :RequestTask.__call__:.
  The values are then processed and pushed to the gMonitor instance in the default callback function.
  """

  ## reference to DataLoggingClient
  __dataLoggingClient = None
  # # reference to RequestClient
  __requestClient = None
  # # reference to StotageFactory
  __storageFactory = None
  # # subLogger
  __log = None
  # # request type
  __requestType = None
  # # placeholder for request owner DB
  requestOwnerDN = None
  # # placeholder for Request owner group
  requestOwnerGroup = None

  # # operation dispatcher for SubRequests,
  # # a dictonary
  # # "operation" => methodToRun
  # #
  __operationDispatcher = {}
  # # holder for DataManager proxy file
  __dataManagerProxy = None
  # # monitoring dict
  __monitor = {}

  def __init__( self, requestString, requestName, executionOrder, jobID, configPath ):
    """ c'tor

    :param self: self reference
    :param str requestString: XML serialised RequestContainer
    :param str requestName: request name
    :param list executionOrder: request execution order
    :param int jobID: jobID
    :param str sourceServer: request's source server
    :param str configPath: path in CS for parent agent
    """
    # # fixtures

    # # python fixtures
    import os, os.path, sys, time, re, types
    self.makeGlobal( "os", os )
    self.makeGlobal( "os.path", os.path )
    self.makeGlobal( "sys", sys )
    self.makeGlobal( "time", time )
    self.makeGlobal( "re", re )
    # # export all Types from types
    [ self.makeGlobal( item, getattr( types, item ) ) for item in dir( types ) if "Type" in item ]

    # # DIRAC fixtures
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    self.__log = gLogger.getSubLogger( "%s/%s" % ( self.__class__.__name__, str( requestName ) ) )

    self.always = self.__log.always
    self.notice = self.__log.notice
    self.info = self.__log.info
    self.debug = self.__log.debug
    self.warn = self.__log.warn
    self.error = self.__log.error
    self.exception = self.__log.exception
    self.fatal = self.__log.fatal

    from DIRAC import S_OK, S_ERROR
    from DIRAC.ConfigurationSystem.Client.Config import gConfig
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsWithVOMSAttribute
    from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
    from DIRAC.DataManagementSystem.Client.Datamanager import DataManager
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


    # # export DIRAC global tools and functions
    self.makeGlobal( "S_OK", S_OK )
    self.makeGlobal( "S_ERROR", S_ERROR )
    self.makeGlobal( "gLogger", gLogger )
    self.makeGlobal( "gConfig", gConfig )
    self.makeGlobal( "gProxyManager", gProxyManager )
    self.makeGlobal( "getGroupsWithVOMSAttribute", getGroupsWithVOMSAttribute )
    self.makeGlobal( "gConfigurationData", gConfigurationData )

    # # save request string
    self.requestString = requestString
    # # build request object
    from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
    self.requestObj = RequestContainer( init = False )
    self.requestObj.parseRequest( request = self.requestString )
    # # save request name
    self.requestName = requestName
    # # .. and jobID
    self.jobID = jobID
    # # .. and execution order
    self.executionOrder = executionOrder

    # # save config path
    self.__configPath = configPath
    # # set requestType
    self.setRequestType( gConfig.getValue( os.path.join( configPath, "RequestType" ), "" ) )
    # # get log level
    self.__log.setLevel( gConfig.getValue( os.path.join( configPath, self.__class__.__name__, "LogLevel" ), "INFO" ) )
    # # clear monitoring
    self.__monitor = {}
    # # save DataManager proxy
    if "X509_USER_PROXY" in os.environ:
      self.info( "saving path to current proxy file" )
      self.__dataManagerProxy = os.environ["X509_USER_PROXY"]
    else:
      self.error( "'X509_USER_PROXY' environment variable not set" )

    self.fc = FileCatalog()
    self.dm = DataManager()

    self.dm = DataManager()
    self.fc = FileCatalog()


  def dataManagerProxy( self ):
    """ get dataManagerProxy file

    :param self: self reference
    """
    return self.__dataManagerProxy

  def addMark( self, name, value = 1 ):
    """ add mark to __monitor dict

    :param self: self reference
    :param name: mark name
    :param value: value to be

    """
    if name not in self.__monitor:
      self.__monitor.setdefault( name, 0 )
    self.__monitor[name] += value

  def monitor( self ):
    """ get monitoring dict

    :param cls: class reference
    """
    return self.__monitor

  def makeGlobal( self, objName, objDef ):
    """ export :objDef: to global name space using :objName: name

    :param self: self reference
    :param str objName: symbol name
    :param mixed objDef: symbol definition
    :throws: NameError if symbol of that name is already in
    """
    if objName not in __builtins__:
      if type( __builtins__ ) == type( {} ):
        __builtins__[objName] = objDef
      else:
        setattr( __builtins__, objName, objDef )
      return True

  def requestType( self ):
    """ get request type

    :params self: self reference
    """
    return self.__requestType

  def setRequestType( self, requestType ):
    """ set request type

    :param self: self reference
    """
    self.debug( "Setting requestType to %s" % str( requestType ) )
    self.__requestType = requestType


  @classmethod
  def dataLoggingClient( cls ):
    """ DataLoggingClient getter
    :param cls: class reference
    """
    if not cls.__dataLoggingClient:
      from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
      cls.__dataLoggingClient = DataLoggingClient()
    return cls.__dataLoggingClient

  @classmethod
  def requestClient( cls ):
    """ RequestClient getter
    :param cls: class reference
    """
    if not cls.__requestClient:
      from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  @classmethod
  def storageFactory( cls ):
    """ StorageFactory getter

    :param cls: class reference
    """
    if not cls.__storageFactory:
      from DIRAC.Resources.Storage.StorageFactory import StorageFactory
      cls.__storageFactory = StorageFactory()
    return cls.__storageFactory

  def changeProxy( self, ownerDN, ownerGroup ):
    """ get proxy from gProxyManager, save it to file

    :param self: self reference
    :param str ownerDN: request owner DN
    :param str ownerGroup: request owner group
    :return: S_OK with name of newly created owner proxy file
    """
    ownerProxy = gProxyManager.downloadVOMSProxy( str( ownerDN ), str( ownerGroup ) )
    if not ownerProxy["OK"] or not ownerProxy["Value"]:
      reason = ownerProxy["Message"] if "Message" in ownerProxy else "No valid proxy found in ProxyManager."
      return S_ERROR( "Change proxy error for '%s'@'%s': %s" % ( ownerDN, ownerGroup, reason ) )
    ownerProxyFile = ownerProxy["Value"].dumpAllToFile()
    if not ownerProxyFile["OK"]:
      return S_ERROR( ownerProxyFile["Message"] )
    ownerProxyFile = ownerProxyFile["Value"]
    os.environ["X509_USER_PROXY"] = ownerProxyFile
    return S_OK( ownerProxyFile )

  ######################################################################
  # operationDispatcher
  @classmethod
  def operationDispatcher( cls ):
    """ operation dispatcher getter

    :param cls: class reference
    """
    return cls.__operationDispatcher

  @classmethod
  def addOperationAction( cls, operation, methodToRun, overwrite = True ):
    """ register handler :methodToRun: for SubRequest operation :operation:
    :warn: all handlers should have the same signature
    :param self: self reference
    :param str operation: SubRequest operation name
    :param MethodType methodToRun: handler to be executed for SubRequest
    :param bool overwrite: flag to overwrite handler, if already present
    :return: S_OK/S_ERROR

    Every action handler should return S_OK with of a structure::

      { "OK" : True,
        "Value" : requestObj # that has been sent to operation handler
      }

    otherwise S_ERROR.

    """
    if operation in cls.__operationDispatcher and not overwrite:
      return S_ERROR( "addOperationAction: operation for '%s' is already registered" % operation )
    if type( methodToRun ) is not MethodType:
      return S_ERROR( "addOperationAction: wrong type (%s = types.MethodType) for '%s' operation" % \
                       ( str( type( methodToRun ) ), operation ) )
    cls.__operationDispatcher[operation] = methodToRun
    return S_OK()

  def __call__( self ):
    """ generic function to process one Request of a type requestType

    This method could be run in a thread.

    :param self: self reference
    :param str requestType: request type
    :return: S_OK/S_ERROR
    """
    self.always( "executing request %s" % self.requestName )

    ################################################################
    # # get ownerDN and ownerGroup
    ownerDN = self.requestObj.getAttribute( "OwnerDN" )
    if not ownerDN["OK"]:
      return ownerDN
    ownerDN = ownerDN["Value"]
    ownerGroup = self.requestObj.getAttribute( "OwnerGroup" )
    if not ownerGroup["OK"]:
      return ownerGroup
    ownerGroup = ownerGroup["Value"]

    # # save request owner
    self.requestOwnerDN = ownerDN if ownerDN else ""
    self.requestOwnerGroup = ownerGroup if ownerGroup else ""

    #################################################################
    # # change proxy
    ownerProxyFile = None
    if ownerDN and ownerGroup:
      ownerProxyFile = self.changeProxy( ownerDN, ownerGroup )
      if not ownerProxyFile["OK"]:
        self.error( "handleReuqest: unable to get proxy for '%s'@'%s': %s" % ( ownerDN,
                                                                               ownerGroup,
                                                                               ownerProxyFile["Message"] ) )
        # update = self.putBackRequest( self.requestName, self.requestString )
        # if not update["OK"]:
        #  self.error( "handleRequest: error when updating request: %s" % update["Message"] )
        #  return update
        # return ownerProxyFile
        ownerProxyFile = None
      else:
        ownerProxyFile = ownerProxyFile["Value"]
    if ownerProxyFile:
      # self.ownerProxyFile = ownerProxyFile
      self.info( "Will execute request for '%s'@'%s' using proxy file %s" % ( ownerDN, ownerGroup, ownerProxyFile ) )
    else:
      self.info( "Will execute request for DataManager using her/his proxy" )

    #################################################################
    # # execute handlers
    ret = { "OK" : False, "Message" : "" }
    useServerCert = gConfig.useServerCertificate()
    try:
      # Execute task with the owner proxy even for contacting DIRAC services
      if useServerCert:
        gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'false' )
      ret = self.handleRequest()
    finally:
      if useServerCert:
        gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )
      # # delete owner proxy
      if self.__dataManagerProxy:
        os.environ["X509_USER_PROXY"] = self.__dataManagerProxy
        if ownerProxyFile and os.path.exists( ownerProxyFile ):
          os.unlink( ownerProxyFile )
    if not ret["OK"]:
      self.error( "handleRequest: error during request processing: %s" % ret["Message"] )
      self.error( "handleRequest: will put original request back" )
      update = self.putBackRequest( self.requestName, self.requestString )
      if not update["OK"]:
        self.error( "handleRequest: error when putting back request: %s" % update["Message"] )
    # # return at least
    return ret

  def handleRequest( self ):
    """ read SubRequests and ExecutionOrder, fire registered handlers upon SubRequests operations

    :param self: self reference
    :param dict requestDict: request dictionary as read from self.readRequest
    """

    ##############################################################
    # here comes the processing
    ##############################################################
    res = self.requestObj.getNumSubRequests( self.__requestType )
    if not res["OK"]:
      errMsg = "handleRequest: failed to obtain number of '%s' subrequests." % self.__requestType
      self.error( errMsg, res["Message"] )
      return S_ERROR( res["Message"] )

    # # for gMonitor
    self.addMark( "Execute", 1 )
    # # process sub requests
    for index in range( res["Value"] ):
      self.info( "handleRequest: processing subrequest %s." % str( index ) )
      subRequestAttrs = self.requestObj.getSubRequestAttributes( index, self.__requestType )["Value"]
      if subRequestAttrs["ExecutionOrder"]:
        subExecutionOrder = int( subRequestAttrs["ExecutionOrder"] )
      else:
        subExecutionOrder = 0
      subRequestStatus = subRequestAttrs["Status"]
      if subRequestStatus != "Waiting":
        self.info( "handleRequest: subrequest %s has status '%s' and is not to be executed." % ( str( index ),
                                                                                                 subRequestStatus ) )
        continue

      if subExecutionOrder <= self.executionOrder:
        operation = subRequestAttrs["Operation"]
        if operation not in self.operationDispatcher():
          self.error( "handleRequest: '%s' operation not supported" % operation )
        else:
          self.info( "handleRequest: will execute %s '%s' subrequest" % ( str( index ), operation ) )

          # # get files
          subRequestFiles = self.requestObj.getSubRequestFiles( index, self.__requestType )["Value"]
          # # execute operation action
          ret = self.operationDispatcher()[operation].__call__( index,
                                                                self.requestObj,
                                                                subRequestAttrs,
                                                                subRequestFiles )
          ################################################
          # # error in operation action?
          if not ret["OK"]:
            self.error( "handleRequest: error when handling subrequest %s: %s" % ( str( index ), ret["Message"] ) )
            self.requestObj.setSubRequestAttributeValue( index, self.__requestType, "Error", ret["Message"] )
          else:
            # # update ref to requestObj
            self.requestObj = ret["Value"]
            # # check if subrequest status == Done, disable finalisation if not
            subRequestDone = self.requestObj.isSubRequestDone( index, self.__requestType )
            if not subRequestDone["OK"]:
              self.error( "handleRequest: unable to determine subrequest status: %s" % subRequestDone["Message"] )
            else:
              if not subRequestDone["Value"]:
                self.warn( "handleRequest: subrequest %s is not done yet" % str( index ) )

    ################################################
    #  Generate the new request string after operation
    newRequestString = self.requestObj.toXML()['Value']
    update = self.putBackRequest( self.requestName, newRequestString )
    if not update["OK"]:
      self.error( "handleRequest: error when updating request: %s" % update["Message"] )
      return update

    # # get request status
    if self.jobID:
      requestStatus = self.requestClient().getRequestStatus( self.requestName )
      if not requestStatus["OK"]:
        return requestStatus
      requestStatus = requestStatus["Value"]
      # # finalize request if jobID is present and request status = 'Done'
      self.info( "handleRequest: request status is %s" % requestStatus )

      if ( requestStatus["RequestStatus"] == "Done" ) and ( requestStatus["SubRequestStatus"] not in ( "Waiting", "Assigned" ) ):
        self.debug( "handleRequest: request is going to be finalised" )
        finalize = self.requestClient().finalizeRequest( self.requestName, self.jobID )
        if not finalize["OK"]:
          self.error( "handleRequest: error in request finalization: %s" % finalize["Message"] )
          return finalize
        self.info( "handleRequest: request is finalised" )
      # # for gMonitor
      self.addMark( "Done", 1 )

    # # should return S_OK with monitor dict
    return S_OK( { "monitor" : self.monitor() } )

  def putBackRequest( self, requestName, requestString ):
    """ put request back

    :param self: self reference
    :param str requestName: request name
    :param str requestString: XML-serilised request
    :param str sourceServer: request server URL
    """
    update = self.requestClient().updateRequest( requestName, requestString )
    if not update["OK"]:
      self.error( "putBackRequest: error when updating request: %s" % update["Message"] )
      return update
    return S_OK()
