########################################################################
# $HeadURL $
# File: RequestTask.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 12:42:45
########################################################################

""" :mod: RequestTask
    =================

    .. module: RequestTask
    :synopsis: request processing task
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    request processing task to be used inside ProcessTask created in RequestAgent
"""

__RCSID__ = "$Id $"

# #
# @file RequestTask.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 12:42:54
# @brief Definition of RequestTask class.

# # imports
import os
from DIRAC import gLogger, S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.BaseOperation import BaseOperation
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security import CS

########################################################################
class RequestTask( object ):
  """
  .. class:: RequestTask

  request's processing task
  """
  # # request client
  __requestClient = None

  def __init__( self, requestXML, handlersDict, csPath ):
    """c'tor

    :param self: self reference
    :param str requestXML: request serilised to XML
    :param dict opHandlers: operation handlers
    """
    self.request = Request.fromXML( requestXML )["Value"]
    # # csPath
    self.csPath = csPath
    # # handlers dict
    self.handlersDict = handlersDict
    # # handlers class def
    self.handlers = {}
    # # own sublogger
    self.log = gLogger.getSubLogger( self.request.RequestName )
    # # get shifters info
    self.__managersDict = {}
    self.__setupManagerProxies()
    # # own gMonitor activities
    gMonitor.registerActivity( "RequestAtt", "Requests processed",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestFail", "Requests failed",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "RequestOK", "Requests done",
                               "RequestExecutingAgent", "Requests/min", gMonitor.OP_SUM )


  def __setupManagerProxies( self ):
    """ setup grid proxy for all defined managers """
    oHelper = Operations()
    shifters = oHelper.getSections( "Shifter" )
    if not shifters["OK"]:
      self.log.error( shifters["Message"] )
      return shifters
    shifters = shifters["Value"]
    for shifter in shifters:
      shifterDict = oHelper.getOptionsDict( "Shifter/%s" % shifter )
      if not shifterDict["OK"]:
        self.log.error( shifterDict["Message"] )
        continue
      userName = shifterDict["Value"].get( "User", "" )
      userGroup = shifterDict["Value"].get( "Group", "" )

      userDN = CS.getDNForUsername( userName )
      if not userDN["OK"]:
        self.log.error( userDN["Message"] )
        continue
      userDN = userDN["Value"][0]
      vomsAttr = CS.getVOMSAttributeForGroup( userGroup )
      if vomsAttr:
        self.log.debug( "getting VOMS [%s] proxy for shifter %s@%s (%s)" % ( vomsAttr, userName,
                                                                             userGroup, userDN ) )
        getProxy = gProxyManager.downloadVOMSProxyToFile( userDN, userGroup,
                                                          requiredTimeLeft = 1200,
                                                          cacheTime = 4 * 43200 )
      else:
        self.log.debug( "getting proxy for shifter %s@%s (%s)" % ( userName, userGroup, userDN ) )
        getProxy = gProxyManager.downloadProxyToFile( userDN, userGroup,
                                                      requiredTimeLeft = 1200,
                                                      cacheTime = 4 * 43200 )
      if not getProxy["OK"]:
        self.log.error( getProxy["Message" ] )
      chain = getProxy["chain"]
      fileName = getProxy["Value" ]
      self.log.debug( "got %s: %s %s" % ( shifter, userName, userGroup ) )
      self.__managersDict[shifter] = { "ShifterDN" : userDN,
                                       "ShifterName" : userName,
                                       "ShifterGroup" : userGroup,
                                       "Chain" : chain,
                                       "ProxyFile" : fileName }
    return S_OK()

  def setupProxy( self ):
    """ download and dump request owner proxy to file and env

    :return: S_OK with name of newly created owner proxy file and shifter name if any
    """
    ownerDN = self.request.OwnerDN
    ownerGroup = self.request.OwnerGroup
    isShifter = []
    for shifter, creds in self.__managersDict.items():
      if creds["ShifterDN"] == ownerDN and creds["ShifterGroup"] == ownerGroup:
        isShifter.append( shifter )
    if isShifter:
      proxyFile = self.__managersDict[isShifter]["ProxyFile"]
      os.environ["X509_USER_PROXY"] = proxyFile
      return S_OK( {"Shifter": isShifter, "ProxyFile": proxyFile} )

    # # if we're here owner is not a shifter at all
    ownerProxy = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup )
    if not ownerProxy["OK"] or not ownerProxy["Value"]:
      reason = ownerProxy["Message"] if "Message" in ownerProxy else "No valid proxy found in ProxyManager."
      return S_ERROR( "Change proxy error for '%s'@'%s': %s" % ( ownerDN, ownerGroup, reason ) )
    ownerProxyFile = ownerProxy["Value"].dumpAllToFile()
    if not ownerProxyFile["OK"]:
      return S_ERROR( ownerProxyFile["Message"] )
    ownerProxyFile = ownerProxyFile["Value"]
    os.environ["X509_USER_PROXY"] = ownerProxyFile
    return S_OK( { "Shifter": isShifter, "ProxyFile": ownerProxyFile } )

  @staticmethod
  def loadHandler( pluginPath ):
    """ Create an instance of requested plugin class, loading and importing it when needed.
    This function could raise ImportError when plugin cannot be find or TypeError when
    loaded class object isn't inherited from BaseOperation class.
    :param str pluginName: dotted path to plugin, specified as in import statement, i.e.
    "DIRAC.CheesShopSystem.private.Cheddar" or alternatively in 'normal' path format
    "DIRAC/CheesShopSystem/private/Cheddar"

    :return: object instance
    This function try to load and instantiate an object from given path. It is assumed that:

    - :pluginPath: is pointing to module directory "importable" by python interpreter, i.e.: it's
    package's top level directory is in $PYTHONPATH env variable,
    - the module should consist a class definition following module name,
    - the class itself is inherited from DIRAC.RequestManagementSystem.private.BaseOperation.BaseOperation
    If above conditions aren't meet, function is throwing exceptions:

    - ImportError when class cannot be imported
    - TypeError when class isn't inherited from BaseOepration
    """
    if "/" in pluginPath:
      pluginPath = ".".join( [ chunk for chunk in pluginPath.split( "/" ) if chunk ] )
    pluginName = pluginPath.split( "." )[-1]
    if pluginName not in globals():
      mod = __import__( pluginPath, globals(), fromlist = [ pluginName ] )
      pluginClassObj = getattr( mod, pluginName )
    else:
      pluginClassObj = globals()[pluginName]
    if not issubclass( pluginClassObj, BaseOperation ):
      raise TypeError( "operation handler '%s' isn't inherited from BaseOperation class" % pluginName )
    for key, status in ( ( "Att", "Attempted" ), ( "OK", "Successful" ) , ( "Fail", "Failed" ) ):
      gMonitor.registerActivity( "%s%s" % ( pluginName, key ), "%s operations %s" % ( pluginName, status ),
                                 "RequestExecutingAgent", "Operations/min", gMonitor.OP_SUM )
    # # return an instance
    return pluginClassObj

  def getHandler( self, operation ):
    """ return instance of a handler for a given operation type on demand
        all created handlers are kept in self.handlers dict for further use

    :param Operation operation: Operation instance
    """
    if operation.Type not in self.handlersDict:
      return S_ERROR( "handler for operation '%s' not set" % operation.Type )
    handler = self.handlers.get( operation.Type, None )
    if not handler:
      try:
        handlerCls = self.loadHandler( self.handlersDict[operation.Type] )
        self.handlers[operation.Type] = handlerCls( csPath = "%s/OperationHandlers/%s" % ( self.csPath,
                                                                                           operation.Type ) )
        handler = self.handlers[ operation.Type ]
      except ( ImportError, TypeError ), error:
        self.log.exception( "getHandler: %s" % str( error ), lException = error )
        return S_ERROR( str( error ) )
    # # set operation for this handler
    handler.setOperation( operation )
    # # and return
    return S_OK( handler )

  @classmethod
  def requestClient( cls ):
    """ on demand request client """
    if not cls.__requestClient:
      cls.__requestClient = ReqClient()
    return cls.__requestClient

  def updateRequest( self ):
    """ put back request to the RequestDB """
    updateRequest = self.requestClient().putRequest( self.request )
    if not updateRequest["OK"]:
      self.log.error( updateRequest["Message"] )
    return updateRequest

  def __call__( self ):
    """ request processing """

    self.log.debug( "about to execute request" )
    gMonitor.addMark( "RequestAtt", 1 )

    # # setup proxy for request owner
    setupProxy = self.setupProxy()
    if not setupProxy["OK"]:
      self.log.error( setupProxy["Message"] )
      self.request.Error = setupProxy["Message"]
      return self.updateRequest()
    shifter = setupProxy["Value"]["Shifter"]
    proxyFile = setupProxy["Value"]["ProxyFile"]

    while self.request.Status == "Waiting":

      # # get waiting operation
      operation = self.request.getWaiting()
      if not operation["OK"]:
        self.log.error( operation["Message"] )
        return operation
      operation = operation["Value"]
      self.log.debug( "about to execute operation %s" % operation.Type )
      # gMonitor.addMark( "%s%s" % ( operation.Type, "Att" ), 1 )

      # # and handler for it
      handler = self.getHandler( operation )
      if not handler["OK"]:
        self.log.error( "unable to process operation %s: %s" % ( operation.Type, handler["Message"] ) )
        gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
        operation.Error = handler["Message"]
        break
      handler = handler["Value"]
      # # set shifters list in the handler
      handler.shifter = shifter
      # # and execute
      try:
        gMonitor.addMark( "%s%s" % ( operation.Type, "Att" ), 1 )
        exe = handler()
        if not exe["OK"]:
          self.log.error( "unable to process operation %s: %s" % ( operation.Type, exe["Message"] ) )
          gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
          gMonitor.addMark( "RequestFail", 1 )
          break
      except Exception, error:
        self.log.exception( "hit by exception: %s" % str( error ) )
        gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
        gMonitor.addMark( "RequestFail", 1 )
        break

      # # operation status check
      if operation.Status == "Done":
        gMonitor.addMark( "%s%s" % ( operation.Type, "OK" ), 1 )
      elif operation.Status == "Failed":
        gMonitor.addMark( "%s%s" % ( operation.Type, "Fail" ), 1 )
      elif operation.Status in ( "Waiting", "Scheduled" ):
        break

    # # not a shifter at all? delete temp proxy file
    if not shifter:
      os.unlink( proxyFile )

    # # request done?
    if self.request.Status == "Done":
      self.log.info( "request %s is done" % self.request.RequestName )
      gMonitor.addMark( "RequestOK", 1 )
      # # and there is a job waiting for it? finalize!
      if self.request.JobID:
        finalizeRequest = self.requestClient.finalize( self.request, self.request.JobID )
        if not finalizeRequest["OK"]:
          self.log.error( "unable to finalize request %s: %s" % ( self.request.RequestName,
                                                                  finalizeRequest["Message"] ) )
          return finalizeRequest
        else:
          self.log.info( "request %s is finalized" % self.request.RequestName )

    # # update request to the RequestDB
    update = self.updateRequest()
    if not update["OK"]:
      self.log.error( update["Message"] )
      return update
    return S_OK()
