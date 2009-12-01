
import time
import os
from DIRAC import S_OK, S_ERROR, gLogger, gConfig, rootPath
from DIRAC.Core.Utilities import ThreadScheduler
from DIRAC.Core.Base.AgentModule import AgentModule

class AgentReactor:

  def __init__( self, baseAgentName ):
    self.__agentModules = {}
    self.__tasks = {}
    self.__baseAgentName = baseAgentName
    self.__scheduler = ThreadScheduler.ThreadScheduler( enableReactorThread = False,
                                                        minPeriod = 30 )
    self.__alive = True
    self.__running = False

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
    rootModulesToLook = [ "%sDIRAC" % ext for ext in gConfig.getValue( "/DIRAC/Extensions", [] ) ] + [ 'DIRAC' ]
    moduleLoaded = False
    for rootModule in rootModulesToLook:
      if moduleLoaded:
        break
      gLogger.verbose( "Trying to load from root module %s" % rootModule )
      moduleFile = os.path.join( rootPath, rootModule, "%sSystem" % system, "Agent", "%s.py" % agentName )
      gLogger.verbose( "Looking for file %s" % moduleFile )
      if not os.path.isfile( moduleFile ):
        continue
      try:
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
        if not isinstance( agent, AgentModule ):
          gLogger.error( "%s is not a subclass of AgentModule" % moduleFile )
          continue
        result = agent.am_initialize()
        if not result[ 'OK' ]:
          return S_ERROR( "Error while calling initialize method of %s: %s" % ( fullName, result[ 'Message' ] ) )
        moduleLoaded = True
      except Exception, e:
        if not hideExceptions:
          gLogger.exception( "Can't load agent %s" % fullName )
    if not moduleLoaded:
        return S_ERROR( "Can't load agent %s in root modules %s" % ( fullName, ", ".join ( rootModulesToLook ) ) )
    self.__agentModules[ fullName ] = { 'instance' : agent,
                                        'class' : agentClass,
                                        'module' : agentModule,
                                        'running' : False }
    agentPeriod = agent.am_getPollingTime()
    result = self.__scheduler.addPeriodicTask( agentPeriod,
                                               agent.am_go,
                                               executions = agent.am_getMaxCycles(),
                                               elapsedTime = agentPeriod )
    if not result[ 'OK' ]:
      return result

    taskId = result[ 'Value' ]
    self.__tasks[ result[ 'Value' ] ] = fullName
    self.__agentModules[ fullName ][ 'taskId' ] = taskId
    self.__agentModules[ fullName ][ 'running' ] = True
    return S_OK()

  def runNumCycles( self, numCycles = 1 ):
    for agentName in self.__agentModules:
      self.setAgentModuleMaxCycles( agentName, numCycles )
    self.go()

  def go( self ):
    if self.__running:
      return
    self.__running = True
    try:
      while self.__alive:
        self.__checkControlDir()
        timeToNext = self.__scheduler.executeNextTask()
        if timeToNext == None:
          gLogger.info( "No more agent modules to execute. Exiting" )
          break
        time.sleep( min( max( timeToNext, 0.5 ), 5 ) )
    finally:
      self.__running = False
      
  def setAgentModuleMaxCycles( self, agentName, maxCycles ):
    if not agentName in self.__agentModules:
      return S_ERROR( "%s has not been loaded" % agentName )
    self.__agentModules[ agentName ][ 'instance' ].am_setOption( 'MaxCycles', maxCycles )
    self.__scheduler.setNumExecutionsForTask( self.__agentModules[ agentName ][ 'taskId' ],
                                              maxCycles )

  def __checkControlDir( self ):
    for agentName in self.__agentModules:
      if not self.__agentModules[ agentName ][ 'running' ]:
        continue
      agent = self.__agentModules[ agentName ][ 'instance' ]
      stopAgentFile = os.path.join( agent.am_getOption( 'ControlDirectory' ), 'stop_agent' )

      alive = agent.am_getModuleParam( 'Alive' )
      if alive:
        if os.path.isfile( stopAgentFile ):
          gLogger.info( "Found control file %s for agent" % ( stopAgentFile, agentName ) )
          alive = False

      if not alive:
        gLogger.info( "Stopping agent module %s" % ( agentName ) )
        self.__scheduler.removeTask( self.__agentModules[ agentName ][ 'taskId' ] )
        del( self.__tasks[ self.__agentModules[ agentName ][ 'taskId' ] ] )
        self.__agentModules[ agentName ][ 'running' ] = False
        if os.path.isfile( stopAgentFile ):
          os.unlink( stopAgentFile )
