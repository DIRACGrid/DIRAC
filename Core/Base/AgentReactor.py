
import time
import os
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities import ThreadScheduler

class AgentReactor:

  def __init__( self, baseAgentName ):
    self.__agentModules = {}
    self.__tasks = {}
    self.__baseAgentName = baseAgentName
    self.__scheduler = ThreadScheduler.ThreadScheduler( enableReactorThread = False,
                                                        minPeriod = 30 )
    self.__alive = True

  def loadAgentModules( self, modulesList, hideExceptions = False ):
    for module in modulesList:
      result = self.loadAgentModule( module, hideExceptions = hideExceptions )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def __checkHandler( self, oClass ):
    for oParent in oClass.__bases__:
      if self.__checkHandler( oParent ):
        return True
    if oClass.__name__ == "AgentModule":
      return True
    return False

  def loadAgentModule( self, fullName, hideExceptions = False ):
    modList = fullName.split( "/" )
    if len( modList ) != 2:
      return S_ERROR( "Can't load %s: Invalid agent name" % ( fullName ) )
    gLogger.info( "Loading %s" % fullName )
    system, agentName = modList
    rootModulesToLook = gConfig.getValue( "/LocalSite/Extensions", [] ) + [ 'DIRAC' ]
    for rootModule in rootModulesToLook:
      moduleLoaded = False
      try:
        gLogger.verbose( "Trying to load from root module %s" % rootModule )
        importString = '%s.%sSystem.Agent.%s' % ( rootModule, system, agentName )
        gLogger.debug( "Trying to load %s" % importString )
        agentModule = __import__( importString,
                                globals(),
                                locals(), agentName )
        agentClass = getattr( agentModule, agentName )
        if not self.__checkHandler( agentClass ):
          gLogger.error( "Invalid Agent module", "%s has to inherit from AgentModule" % fullName )
          continue
        agent = agentClass( fullName, self.__baseAgentName )
        result = agent.am_initialize()
        if not result[ 'OK' ]:
          return S_ERROR( "Error while calling initialize method of %s: %s" %( fullName, result[ 'Message' ] ) )
        moduleLoaded = True
      except Exception, e:
        if not hideExceptions:
          gLogger.exception( "Can't load agent %s" % fullName )
    if not moduleLoaded:
        return S_ERROR( "Can't load agent %s in root modules %s" % ( fullName, rootModulesToLook ) )
    self.__agentModules[ fullName ] = { 'instance' : agent,
                                        'class' : agentClass,
                                        'module' : agentModule }
    agentPeriod = agent.am_getPollingTime()
    result = self.__scheduler.addPeriodicTask( agentPeriod,
                                               agent.am_go,
                                               executions = agent.am_getMaxCycles(),
                                               elapsedTime = agentPeriod  )
    if not result[ 'OK' ]:
      return result
    taskId = result[ 'Value' ]
    self.__tasks[ result[ 'Value' ] ] = fullName
    self.__agentModules[ fullName ][ 'taskId' ] = taskId
    return S_OK()

  def go(self):
    while self.__alive:
      self.__checkControlDir()
      timeToNext = self.__scheduler.executeNextTask()
      if timeToNext == None:
        gLogger.info( "No more agent modules to execute. Exiting" )
        break
      time.sleep( min( max( timeToNext, 0.5 ), 5 ) )

  def __checkControlDir( self ):
    for agentName in self.__agentModules:
      agent = self.__agentModules[ agentName ][ 'instance' ]
      stopAgentFile = os.path.join( agent.am_getOption( 'ControlDirectory' ), 'stop_agent' )
      if os.path.exists( stopAgentFile ):
        gLogger.info( "Stopping agent module %s because of control file %s" % ( agentName, stopAgentFile ) )
        self.__scheduler.removeTask( self.__agentModules[ agentName ][ 'taskId ' ] )
        del( self.__tasks[ self.__agentModules[ agentName ][ 'taskId ' ] ] )



