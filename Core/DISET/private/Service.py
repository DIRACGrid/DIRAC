
import os
import time
import DIRAC
import threading
from DIRAC import gConfig, gMonitor, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import List, Time, MemStat
from DIRAC.Core.DISET.private.LockManager import LockManager
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.DISET.private.TransportPool import getGlobalTransportPool
from DIRAC.Core.DISET.private.MessageBroker import MessageBroker, MessageSender
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure
from DIRAC.Core.Security import CS
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.FrameworkSystem.Client.SecurityLogClient import SecurityLogClient

class Service:

  SVC_VALID_ACTIONS = { 'RPC' : 'export',
                        'FileTransfer': 'transfer',
                        'Message' : 'msg',
                        'Connection' : 'Message' }
  SVC_SECLOG_CLIENT = SecurityLogClient()

  def __init__( self, serviceName, activityMonitor = False ):
    self._name = serviceName
    self._startTime = Time.dateTime()
    self._cfg = ServiceConfiguration( serviceName )
    self._validNames = [ self._name ]
    if activityMonitor:
      self._monitor = activityMonitor
    else:
      self._monitor = MonitoringClient()
    self.__monitorLastStatsUpdate = time.time()
    self._stats = { 'queries' : 0, 'connections' : 0 }
    self._authMgr = AuthManager( "%s/Authorization" % self._cfg.getServicePath() )
    self._transportPool = getGlobalTransportPool()
    self.__cloneId = 0

  def setCloneProcessId( self, cloneId ):
    self.__cloneId = cloneId
    self._monitor.setComponentName( "%s-Clone:%s" % ( self._name, cloneId ) )

  def _isMetaAction( self, action ):
    referedAction = Service.SVC_VALID_ACTIONS[ action ]
    if referedAction in Service.SVC_VALID_ACTIONS:
      return referedAction
    return False

  def initialize( self ):
    #Build the URLs
    self._url = self._cfg.getURL()
    if not self._url:
      return S_ERROR( "Could not build service URL for %s" % self._name )
    gLogger.verbose( "Service URL is %s" % self._url )
    #Discover Handler
    self._handlerLocation = self._discoverHandlerLocation()
    if not self._handlerLocation:
      return S_ERROR( "Could not find handler location for %s" % self._name )
    gLogger.verbose( "Handler found at %s" % self._handlerLocation )
    #Load handler
    result = self._loadHandler()
    if not result[ 'OK' ]:
      return result
    self._handler = result[ 'Value' ]
    #Initialize lock manager
    self._lockManager = LockManager( self._cfg.getMaxWaitingPetitions() )
    #Load actions
    result = self._loadActions()
    if not result[ 'OK' ]:
      return result
    self._actions = result[ 'Value' ]
    self._initMonitoring()
    self._threadPool = ThreadPool( 1,
                                    max( 0, self._cfg.getMaxThreads() ),
                                    self._cfg.getMaxWaitingPetitions() )
    self._threadPool.daemonize()
    self._msgBroker = MessageBroker( "%sMSB" % self._name, threadPool = self._threadPool )
    #Create static dict
    self._serviceInfoDict = { 'serviceName' : self._name,
                               'URL' : self._cfg.getURL(),
                               'systemSectionPath' : self._cfg.getSystemPath(),
                               'serviceSectionPath' : self._cfg.getServicePath(),
                               'messageSender' : MessageSender( self._msgBroker )
                             }
    #Call static initialization function
    try:
      if self._handler[ 'init' ]:
        result = self._handler[ 'init' ]( dict( self._serviceInfoDict ) )
        if not isReturnStructure( result ):
          return S_ERROR( "Service initialization function must return S_OK/S_ERROR" )
        if not result[ 'OK' ]:
          return S_ERROR( "Error while initializing %s: %s" % ( self._name, result[ 'Message' ] ) )
    except Exception, e:
      errMsg = "Exception while intializing %s" % self._name
      gLogger.exception( errMsg )
      return S_ERROR( errMsg )

    gThreadScheduler.addPeriodicTask( 30, self.__reportThreadPoolContents )

    return S_OK()

  def _discoverHandlerLocation( self ):
    handlerLocation = self._cfg.getHandlerLocation()
    if handlerLocation:
      if handlerLocation.find( "Handler.py" ) != len( handlerLocation ) - 10:
        return S_ERROR( "CS defined file %s does not have a valid handler name" % handlerLocation )
      return handlerLocation
    fields = [ field.strip() for field in self._name.split( "/" ) if field.strip() ]
    if len( fields ) != 2:
      gLogger.error( "Oops. Invalid service name!", self._name )
      return False
    gLogger.debug( "Trying to auto discover handler" )
    rootModulesToLook = [ "%sDIRAC" % ext for ext in gConfig.getValue( "/DIRAC/Extensions", [] ) ] + [ 'DIRAC' ]
    for rootModule in rootModulesToLook:
      gLogger.debug( "Trying to find handler in %s root module" % rootModule )
      filePath = os.path.join( rootModule, "%sSystem" % fields[0], "Service", "%sHandler.py" % fields[1] )
      absPath = os.path.join ( DIRAC.rootPath, filePath )
      if os.path.isfile( absPath ):
        gLogger.debug( "Auto discovered handler %s" % filePath )
        return filePath
      gLogger.debug( "%s is not a valid file" % filePath )
    return False

  def _loadHandler( self ):
    handlerLocation = self._handlerLocation.replace( ".py", "" )
    lServicePath = List.fromChar( handlerLocation, "/" )
    handlerName = lServicePath[-1]
    try:
      handlerModule = __import__( ".".join( lServicePath ),
                                   globals(),
                                   locals(), handlerName )
      handlerClass = getattr( handlerModule, handlerName )
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Can't import handler: %s" % str( e ) )
    if not issubclass( handlerClass, RequestHandler ):
      return S_ERROR( "Handler class is not a request handler" )
    try:
      handlerInitMethod = getattr( handlerModule, "initialize%s" % handlerName )
      gLogger.debug( "Found initialization function for service" )
    except:
      handlerInitMethod = False
      gLogger.debug( "Not found initialization function for service" )

    handlerInfo = {}
    handlerInfo[ "name" ] = handlerName
    handlerInfo[ "module" ] = handlerModule
    handlerInfo[ "class" ] = handlerClass
    handlerInfo[ "init" ] = handlerInitMethod

    gLogger.info( "Loaded %s" % self._handlerLocation )
    return S_OK( handlerInfo )

  def _loadActions( self ):

    handlerClass = self._handler[ 'class' ]

    authRules = {}
    typeCheck = {}
    methodsList = {}
    for actionType in Service.SVC_VALID_ACTIONS:
      if self._isMetaAction( actionType ):
        continue
      authRules[ actionType ] = {}
      typeCheck[ actionType ] = {}
      methodsList[ actionType ] = []
    handlerAttributeList = dir( handlerClass )
    for actionType in Service.SVC_VALID_ACTIONS:
      if self._isMetaAction( actionType ):
        continue
      methodPrefix = '%s_' % Service.SVC_VALID_ACTIONS[ actionType ]
      for attribute in handlerAttributeList:
        if attribute.find( methodPrefix ) != 0:
          continue
        exportedName = attribute[ len( methodPrefix ) : ]
        methodsList[ actionType ].append( exportedName )
        gLogger.verbose( "+ Found %s method %s" % ( actionType, exportedName ) )
        #Create lock for method
        self._lockManager.createLock( "%s/%s" % ( actionType, exportedName ),
                                       self._cfg.getMaxThreadsForMethod( actionType, exportedName ) )
        #Look for type and auth rules
        if actionType == 'RPC':
          typeAttr = "types_%s" % exportedName
          authAttr = "auth_%s" % exportedName
        else:
          typeAttr = "types_%s_%s" % ( Service.SVC_VALID_ACTIONS[ actionType ], exportedName )
          authAttr = "auth_%s_%s" % ( Service.SVC_VALID_ACTIONS[ actionType ], exportedName )
        if typeAttr in handlerAttributeList:
          obj = getattr( handlerClass, typeAttr )
          gLogger.verbose( "|- Found type definition %s: %s" % ( typeAttr, str( obj ) ) )
          typeCheck[ actionType ][ exportedName ] = obj
        if authAttr in handlerAttributeList:
          obj = getattr( handlerClass, authAttr )
          gLogger.verbose( "|- Found auth rules %s: %s" % ( authAttr, str( obj ) ) )
          authRules[ actionType ][ exportedName ] = obj

    for actionType in Service.SVC_VALID_ACTIONS:
      referedAction = self._isMetaAction( actionType )
      if not referedAction:
        continue
      gLogger.verbose( "Action %s is a meta action for %s" % ( actionType, referedAction ) )
      authRules[ actionType ] = []
      for method in authRules[ referedAction ]:
        for prop in authRules[ referedAction ][ method ]:
          if prop not in authRules[ actionType ]:
            authRules[ actionType ].append( prop )
      gLogger.verbose( "Meta action %s props are %s" % ( actionType, authRules[ actionType ] ) )

    return S_OK( { 'methods' : methodsList, 'auth' : authRules, 'types' : typeCheck } )

  def _initMonitoring( self ):
    #Init extra bits of monitoring
    self._monitor.setComponentType( MonitoringClient.COMPONENT_SERVICE )
    self._monitor.setComponentName( self._name )
    self._monitor.setComponentLocation( self._cfg.getURL() )
    self._monitor.initialize()
    self._monitor.registerActivity( "Connections", "Connections received", "Framework", "connections", MonitoringClient.OP_RATE )
    self._monitor.registerActivity( "Queries", "Queries served", "Framework", "queries", MonitoringClient.OP_RATE )
    self._monitor.registerActivity( 'CPU', "CPU Usage", 'Framework', "CPU,%", MonitoringClient.OP_MEAN, 600 )
    self._monitor.registerActivity( 'MEM', "Memory Usage", 'Framework', 'Memory,MB', MonitoringClient.OP_MEAN, 600 )
    self._monitor.registerActivity( 'PendingQueries', "Pending queries", 'Framework', 'queries', MonitoringClient.OP_MEAN )
    self._monitor.registerActivity( 'ActiveQueries', "Active queries", 'Framework', 'threads', MonitoringClient.OP_MEAN )
    self._monitor.registerActivity( 'RunningThreads', "Running threads", 'Framework', 'threads', MonitoringClient.OP_MEAN )

    self._monitor.setComponentExtraParam( 'DIRACVersion', DIRAC.version )
    self._monitor.setComponentExtraParam( 'platform', DIRAC.platform )
    self._monitor.setComponentExtraParam( 'startTime', Time.dateTime() )
    for prop in ( ( "__RCSID__", "version" ), ( "__doc__", "description" ) ):
      try:
        value = getattr( self._handler[ 'module' ], prop[0] )
      except Exception, e:
        gLogger.error( "Missing %s" % prop[0] )
        value = 'unset'
      self._monitor.setComponentExtraParam( prop[1], value )
    for secondaryName in self._cfg.registerAlsoAs():
      if secondaryName not in self.servicesDict:
        gLogger.info( "Registering %s also as %s" % ( serviceName, secondaryName ) )
        self._validNames.append( secondaryName )
    return S_OK()

  def __reportThreadPoolContents( self ):
    self._monitor.addMark( 'PendingQueries', self._threadPool.pendingJobs() )
    self._monitor.addMark( 'ActiveQueries', self._threadPool.numWorkingThreads() )
    self._monitor.addMark( 'RunningThreads', threading.activeCount() )


  def getConfig( self ):
    return self._cfg

  #End of initialization functions

  def handleConnection( self, clientTransport ):
    self._stats[ 'connections' ] += 1
    gMonitor.setComponentExtraParam( 'queries', self._stats[ 'connections' ] )
    self._threadPool.generateJobAndQueueIt( self._processInThread,
                                             args = ( clientTransport, ) )

  #Threaded process function
  def _processInThread( self, clientTransport ):
    self._lockManager.lockGlobal()
    try:
      monReport = self.__startReportToMonitoring()
    except Exception, e:
      monReport = False
    try:
      #Handshake
      try:
        result = clientTransport.handshake()
        if not result[ 'OK' ]:
          clientTransport.close()
          return
      except:
        return
      #Add to the transport pool
      trid = self._transportPool.add( clientTransport )
      if not trid:
        return
      #Receive and check proposal
      result = self._receiveAndCheckProposal( trid )
      if not result[ 'OK' ]:
        self._transportPool.sendAndClose( trid, result )
        return
      proposalTuple = result[ 'Value' ]
      #Instantiate handler
      result = self._instantiateHandler( trid, proposalTuple )
      if not result[ 'OK' ]:
        self._transportPool.sendAndClose( trid, result )
        return
      handlerObj = result[ 'Value' ]
      #Execute the action
      result = self._processProposal( trid, proposalTuple, handlerObj )
      #Close the connection if required
      if result[ 'closeTransport' ] or not result[ 'OK' ]:
        self._transportPool.close( trid )
      return result
    finally:
      self._lockManager.unlockGlobal()
      if monReport:
        self.__endReportToMonitoring( *monReport )


  def _createIdentityString( self, credDict, clientTransport = False ):
    if 'username' in credDict:
      if 'group' in credDict:
        identity = "[%s:%s]" % ( credDict[ 'username' ], credDict[ 'group' ] )
      else:
        identity = "[%s:unknown]" % credDict[ 'username' ]
    else:
      identity = 'unknown'
    if clientTransport:
      addr = clientTransport.getRemoteAddress()
      if addr:
        addr = "{%s:%s}" % ( addr[0], addr[1] )
    if 'DN' in credDict:
      identity += "(%s)" % credDict[ 'DN' ]
    return identity

  def _receiveAndCheckProposal( self, trid ):
    clientTransport = self._transportPool.get( trid )
    #Get the peer credentials
    credDict = clientTransport.getConnectingCredentials()
    #Receive the action proposal
    retVal = clientTransport.receiveData( 1024 )
    if not retVal[ 'OK' ]:
      gLogger.error( "Invalid action proposal", "%s %s" % ( self._createIdentityString( credDict,
                                                                                        clientTransport ),
                                                            retVal[ 'Message' ] ) )
      return S_ERROR( "Invalid action proposal" )
    proposalTuple = retVal[ 'Value' ]
    gLogger.debug( "Received action from client", "/".join( list( proposalTuple[1] ) ) )
    #Check if there are extra credentials
    if proposalTuple[2]:
      clientTransport.setExtraCredentials( proposalTuple[2] )
    #Check if this is the requested service
    requestedService = proposalTuple[0][0]
    if requestedService not in self._validNames:
      return S_ERROR( "%s is not up in this server" % requestedService )
    #Check if the action is valid
    requestedActionType = proposalTuple[1][0]
    if requestedActionType not in Service.SVC_VALID_ACTIONS:
      return S_ERROR( "%s is not a known action type" % requestedActionType )
    #Check if it's authorized
    result = self._authorizeProposal( proposalTuple[1], trid, credDict )
    if not result[ 'OK' ]:
      return result
    #Proposal is OK
    return S_OK( proposalTuple )

  def _authorizeProposal( self, actionTuple, trid, credDict ):
    #Find CS path for the Auth rules
    referedAction = self._isMetaAction( actionTuple[0] )
    if referedAction:
      csAuthPath = "%s/Default" % actionTuple[0]
      hardcodedMethodAuth = self._actions[ 'auth' ][ actionTuple[0] ]
    else:
      if actionTuple[0] == 'RPC':
        csAuthPath = actionTuple[1]
      else:
        csAuthPath = "/".join( actionTuple )
      #Find if there are hardcoded auth rules in the code
      hardcodedMethodAuth = False
      if actionTuple[0] in self._actions[ 'auth' ]:
        hardcodedRulesByType = self._actions[ 'auth' ][ actionTuple[0] ]
        if actionTuple[0] == "FileTransfer":
          methodName = actionTuple[1][0].lower() + actionTuple[1][1:]
        else:
          methodName = actionTuple[1]

        if methodName in hardcodedRulesByType:
          hardcodedMethodAuth = hardcodedRulesByType[ methodName ]
    #Get the identity string
    identity = self._createIdentityString( credDict )
    #Auth time!
    if not self._authMgr.authQuery( csAuthPath, credDict, hardcodedMethodAuth ):
      gLogger.warn( "Unauthorized query", "to %s:%s by %s" % ( self._name,
                                                               "/".join( actionTuple ),
                                                               identity ) )
      result = S_ERROR( "Unautorized query" )
    else:
      result = S_OK()

    #Security log
    tr = self._transportPool.get( trid )
    if not tr:
      return S_ERROR( "Client disconnected" )
    sourceAddress = tr.getRemoteAddress()
    Service.SVC_SECLOG_CLIENT.addMessage( result[ 'OK' ], sourceAddress[0], sourceAddress[1], identity,
                                      self._cfg.getHostname(),
                                      self._cfg.getPort(),
                                      self._name, "/".join( actionTuple ) )
    return result

  def _instantiateHandler( self, trid, proposalTuple = False ):
    """
    Generate an instance of the handler for a given service
    """
    #Generate the client params
    clientParams = { 'serviceStartTime' : self._startTime }
    if proposalTuple:
      clientParams[ 'clientSetup' ] = proposalTuple[0][1]
      if len( proposalTuple[0] ) < 3:
        clientParams[ 'clientVO' ] = gConfig.getValue( "/DIRAC/VirtualOrganization", "unknown" )
      else:
        clientParams[ 'clientVO' ] = proposalTuple[0][2]
    clientTransport = self._transportPool.get( trid )
    if clientTransport:
      clientParams[ 'clientAddress' ] = clientTransport.getRemoteAddress()
    #Generate handler dict with per client info
    handlerInitDict = dict( self._serviceInfoDict )
    for key in clientParams:
      handlerInitDict[ key ] = clientParams[ key ]
    #Instantiate and initialize
    try:
      handlerInstance = self._handler[ 'class' ]( handlerInitDict,
                                                   trid,
                                                   self._lockManager,
                                                   self._msgBroker,
                                                   self._monitor )
      handlerInstance.initialize()
    except Exception, e:
      gLogger.exception( S_ERROR( "Server error while initializing handler: %s" % str( e ) ) )
      return S_ERROR( "Server error while intializing handler" )
    return S_OK( handlerInstance )

  def _processProposal( self, trid, proposalTuple, handlerObj ):
    #Notify the client we're ready to execute the action
    retVal = self._transportPool.send( trid, S_OK() )
    if not retVal[ 'OK' ]:
      return retVal

    messageConnection = False
    if proposalTuple[1] == ( 'Connection', 'new' ):
      messageConnection = True

    if messageConnection:

      if self._msgBroker.getNumConnections() > self._cfg.getMaxMessagingConnections():
        result = S_ERROR( "Maximum number of connections reached. Try later" )
        result[ 'closeTransport' ] = True
        return result

      #This is a stable connection
      self._msgBroker.addTransportId( trid, self._name,
                                       receiveMessageCallback = self._mbReceivedMsg,
                                       disconnectCallback = self._mbDisconnect,
                                       listenToConnection = False )

    result = self._executeAction( trid, proposalTuple, handlerObj )
    if result[ 'OK' ] and messageConnection:
      self._msgBroker.listenToTransport( trid )
      result = self._mbConnect( trid, handlerObj )

    result[ 'closeTransport' ] = not messageConnection
    return result

  def _mbConnect( self, trid, handlerObj = False ):
    if not handlerObj:
      result = self._instantiateHandler( trid )
      if not result[ 'OK' ]:
        return result
      handlerObj = result[ 'Value' ]
    return handlerObj._rh_executeConnectionCallback( 'connected' )

  def _executeAction( self, trid, proposalTuple, handlerObj ):
    try:
      return handlerObj._rh_executeAction( proposalTuple )
    except Exception, e:
      gLogger.exception( "Exception while executing handler action" )
      return S_ERROR( "Server error while executing action: %s" % str( e ) )

  def _mbReceivedMsg( self, trid, msgObj ):
    result = self._authorizeProposal( ( 'Message', msgObj.getName() ),
                                      trid,
                                      self._transportPool.get( trid ).getConnectingCredentials() )
    if not result[ 'OK' ]:
      return result
    result = self._instantiateHandler( trid )
    if not result[ 'OK' ]:
      return result
    handlerObj = result[ 'Value' ]
    return handlerObj._rh_executeMessageCallback( msgObj )

  def _mbDisconnect( self, trid ):
    result = self._instantiateHandler( trid )
    if not result[ 'OK' ]:
      return result
    handlerObj = result[ 'Value' ]
    return handlerObj._rh_executeConnectionCallback( 'drop' )


  def __startReportToMonitoring( self ):
    self._monitor.addMark( "Queries" )
    now = time.time()
    stats = os.times()
    cpuTime = stats[0] + stats[2]
    if now - self.__monitorLastStatsUpdate < 0:
      return ( now, cpuTime )
    # Send CPU consumption mark
    wallClock = now - self.__monitorLastStatsUpdate
    self.__monitorLastStatsUpdate = now
    # Send Memory consumption mark
    membytes = MemStat.VmB( 'VmRSS:' )
    if membytes:
      mem = membytes / ( 1024. * 1024. )
      self._monitor.addMark( 'MEM', mem )
    return ( now, cpuTime )

  def __endReportToMonitoring( self, initialWallTime, initialCPUTime ):
    wallTime = time.time() - initialWallTime
    stats = os.times()
    cpuTime = stats[0] + stats[2] - initialCPUTime
    percentage = cpuTime / wallTime * 100.
    if percentage > 0:
      self._monitor.addMark( 'CPU', percentage )
