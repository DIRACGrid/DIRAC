########################################################################
# $HeadURL$
# File :   AgentReactor.py
# Author : Adria Casajus
########################################################################
"""
  DIRAC class to execute Agents
  
  Agents are the active part any any DIRAC system, they execute in a cyclic
  manner looking at the state of the system and reacting to it by taken 
  appropriated actions
  
  All DIRAC Agents must inherit from the basic class AgentModule

  In the most common case, DIRAC Agents are executed using the dirac-agent command.
  dirac-agent accepts a list positional arguments. These arguments have the form:
  [DIRAC System Name]/[DIRAC Agent Name]
  dirac-agent then:
  - produces a instance of AgentReactor
  - loads the required modules using the AgentReactor.loadAgentModules method
  - starts the execution loop using the AgentReactor.go method

  Agent modules must be placed under the Agent directory of a DIRAC System. 
  DIRAC Systems are called XXXSystem where XXX is the "DIRAC System Name", and 
  must inherit from the base class AgentModule

"""
__RCSID__ = "$Id$"

import time
import os
from DIRAC import S_OK, S_ERROR, gLogger, gConfig, rootPath
from DIRAC.Core.Utilities import ThreadScheduler
from DIRAC.ConfigurationSystem.Client.Helpers import getInstalledExtensions
from DIRAC.Core.Base.AgentModule import AgentModule

class AgentReactor:
  """
    Main interface to DIRAC Agents. It allows to :
    - define a Agents modules to be executed
    - define the number of cycles to execute
    - steer the execution
    
    Agents are declared via:
    - loadAgentModule(): for a single Agent
    - loadAgentModules(): for a list of Agents
    
    The number of cycles to execute for a defined Agent can be set via:
    - setAgentModuleCyclesToExecute()
    
    The execution of the Agents is done with:
    - runNumCycles(): to execute an additional number of cycles
    - go():
    
    During the execution of the cycles, each of the Agents can be signaled to stop
    by creating a file named "stop_agent" in its Control Directory.
    
  """

  def __init__( self, baseAgentName ):
    self.__agentModules = {}
    self.__tasks = {}
    self.__baseAgentName = baseAgentName
    self.__scheduler = ThreadScheduler.ThreadScheduler( enableReactorThread = False,
                                                        minPeriod = 30 )
    self.__alive = True
    self.__running = False

  def loadAgentModules( self, modulesList, hideExceptions = False ):
    """
      Load all modules required in moduleList
    """
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
    """
      Load module fullName.
      fullName must take the form [DIRAC System Name]/[DIRAC Agent Name]
      then:
      - calls the am_initialize method of the imported Agent
      - determines the pooling interval: am_getPollingTime,
      - determines the number of executions: am_getMaxCycles
      - creates a periodic Task in the ThreadScheduler: am_go
      
    """
    modList = fullName.split( "/" )
    if len( modList ) != 2:
      return S_ERROR( "Can't load %s: Invalid agent name" % ( fullName ) )
    if fullName in self.__agentModules:
      gLogger.notice( "Agent already loaded:", fullName )
      return S_OK()
    gLogger.info( "Loading %s" % fullName )
    system, agentName = modList
    rootModulesToLook = getInstalledExtensions()
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

  def runNumCycles( self, agentName = None, numCycles = 1 ):
    """
      Run all defined agents a given number of cycles
    """
    if agentName:
      self.loadAgentModule( agentName )
    error = ''
    for aName in self.__agentModules:
      result = self.setAgentModuleCyclesToExecute( aName, numCycles )
      if not result['OK']:
        error = 'Failed to set cycles to execute'
        gLogger( '%s:' % error, aName )
        break
    if error:
      return S_ERROR( error )
    self.go()
    return S_OK()

  def __finalize( self ):
    """
      Execute the finalize method of all Agents
    """
    for agentName in self.__agentModules:
      try:
        self.__agentModules[agentName]['instance'].finalize()
      except:
        gLogger.exception( 'Failed to execute finalize for Agent:', agentName )

  def go( self ):
    """
      Main method to control the execution of all configured Agents
    """
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
    self.__finalize()

  def setAgentModuleCyclesToExecute( self, agentName, maxCycles = 1 ):
    """
      Set number of cycles to execute for a given agent (previously defined)
    """
    if not agentName in self.__agentModules:
      return S_ERROR( "%s has not been loaded" % agentName )
    if maxCycles:
      try:
        maxCycles += self.__agentModules[ agentName ][ 'instance' ].am_getCyclesDone()
      except:
        error = 'Can not determine number of cycles to execute'
        gLogger.exception( '%s:' % error, '"%s"' % maxCycles )
        return S_ERROR( error )
    self.__agentModules[ agentName ][ 'instance' ].am_setOption( 'MaxCycles', maxCycles )
    self.__scheduler.setNumExecutionsForTask( self.__agentModules[ agentName ][ 'taskId' ],
                                              maxCycles )
    return S_OK()

  def __checkControlDir( self ):
    """
      Check for the presence of stop_agent file to stop execution of the corresponding Agent
    """
    for agentName in self.__agentModules:
      if not self.__agentModules[ agentName ][ 'running' ]:
        continue
      agent = self.__agentModules[ agentName ][ 'instance' ]

      alive = agent.am_getModuleParam( 'alive' )
      if alive:
        if agent.am_checkStopAgentFile():
          gLogger.info( "Found StopAgent file for agent %s" % agentName )
          alive = False

      if not alive:
        gLogger.info( "Stopping agent module %s" % ( agentName ) )
        self.__scheduler.removeTask( self.__agentModules[ agentName ][ 'taskId' ] )
        del( self.__tasks[ self.__agentModules[ agentName ][ 'taskId' ] ] )
        self.__agentModules[ agentName ][ 'running' ] = False
        agent.am_removeStopAgentFile()
