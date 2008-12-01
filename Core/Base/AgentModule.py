########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/AgentModule.py,v 1.3 2008/12/01 17:50:15 rgracian Exp $
########################################################################
""" Base class for all agent modules

    The specific agents must provide the following methods:
    - initialize() for initial settings
    - execute() - the main method called in the agent cycle
    - finalize() - the graceful exit of the method, this one is usually used
               for the agent restart

    The agent can be stopped either by a signal or by creating a 'stop_agent' file
    in the controlDirectory defined in the agent configuration

"""

__RCSID__ = "$Id: AgentModule.py,v 1.3 2008/12/01 17:50:15 rgracian Exp $"

import os
import threading
import time
import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

class AgentModule:

  def __init__( self, agentName, baseAgentName, options = {} ):
    if agentName == baseAgentName:
      self.log = gLogger
      standaloneModule = True
    else:
      self.log = gLogger.getSubLogger( agentName, child = False )
      standaloneModule = False
    self.__moduleParams = { 'monitoringEnabled' : True,
                    'fullName' : agentName,
                    'section' : PathFinder.getAgentSection( agentName ),
                    'standalone' : standaloneModule }
    self.__moduleParams[ 'system' ], self.__moduleParams[ 'agentName' ] = agentName.split("/")
    self.__moduleParams[ 'enabled' ] = self.am_getCSOption( "Status", "Active" ).lower() in ( 'active' )
    self.__moduleParams[ 'pollingTime' ] = 120
    self.__moduleParams[ 'maxCycles' ] = 0
    self.__moduleParams[ 'cyclesDone' ] = 0
    self.__moduleParams[ 'setup' ] = gConfig.getValue( "/DIRAC/Setup", "Unknown" )
    self.__moduleParams[ 'controlDirectory' ] = os.path.join( DIRAC.rootPath,
                                                            'control',
                                                            os.path.join( *self.__moduleParams[ 'fullName' ].split("/") ) )
    self.__moduleParams[ 'workDirectory' ] = os.path.join( DIRAC.rootPath,
                                                            'work',
                                                            os.path.join( *self.__moduleParams[ 'fullName' ].split("/") ) )

    for key in options:
      self.__moduleParams[ key ] = self.options[ key ]
    self.__moduleParams[ 'executors' ] = [ ( self.execute, () ) ]
    self.__moduleParams[ 'shifterProxy' ] = False
    self.__moduleParams[ 'shifterProxyLocation' ] = os.path.join( self.__moduleParams[ 'workDirectory' ],
                                                                '.shifterCred' )
    self.__initializeMonitor()
    self.__initialized = False
    self.__translateAgentParamsToAttrs()

  def __translateAgentParamsToAttrs(self):
    for key in self.__moduleParams:
      setattr( self, key, self.__moduleParams[ key ] )

  def am_initialize( self, *initArgs ):
    result = self.initialize( *initArgs )
    if result == None:
      return S_ERROR( "Error while initializing %s module: initialize must return S_OK/S_ERROR" % agentName )
    if not result[ 'OK' ]:
      return S_ERROR( "Error while initializing %s: %s"  % ( agentName, result[ 'Message' ] ) )
    self.__checkAgentDir( 'controlDirectory' )
    self.__checkAgentDir( 'workDirectory' )
    if self.__moduleParams[ 'monitoringEnabled' ]:
      self.monitor.enable()
    if len( self.__moduleParams[ 'executors' ] ) < 1:
      return S_ERROR( "At least one executor method has to be defined" )
    if not self.__moduleParams[ 'enabled' ]:
      return S_ERROR( "Agent is disabled via the configuration")
    self.log.info( "="*40 )
    self.log.info( "Loaded agent module %s" % self.__moduleParams[ 'fullName' ] )
    self.log.info( " Site: %s" % gConfig.getValue( '/LocalSite/Site', 'Unknown' ) )
    self.log.info( " Setup: %s" % gConfig.getValue( "/DIRAC/Setup" ) )
    self.log.info( " Version: %s " % __RCSID__)
    self.log.info( " DIRAC version: %s" % DIRAC.version )
    self.log.info( " DIRAC platform: %s" % DIRAC.platform )
    self.log.info( " Polling time: %s" % self.am_getPollingTime() )
    self.log.info( " Control dir: %s" % self.__moduleParams[ 'controlDirectory' ] )
    self.log.info( " Work dir: %s" % self.__moduleParams[ 'workDirectory' ] )
    if self.am_getMaxCycles() > 0:
      self.log.info( " Cycles: %s" % self.am_getMaxCycles() )
    else:
      self.log.info( " Cycles: unlimited" )
    self.log.info( "="*40 )
    self.__translateAgentParamsToAttrs()
    self.__initialized = True
    return S_OK()

  def __checkAgentDir( self, name ):
    if name in self.__moduleParams:
      defaultPath = self.__moduleParams[ name ]
    else:
      defaultPath = os.path.join( DIRAC.rootPath, name, os.path.join( *self.__moduleParams[ 'fullName' ].split("/") ) )
    self.__moduleParams[ name ] = self.am_getCSOption( name.capitalize(), defaultPath )
    try:
      os.makedirs( self.__moduleParams[ name ] )
    except:
      pass
    if not os.path.isdir( self.__moduleParams[ name ] ):
      raise Exception('Can not create %s at %s' % ( name, self.__moduleParams[ name ] ) )

  def am_getCSOption( self, optionName, defaultValue = False ):
    return gConfig.getValue( "%s/%s" % ( self.__moduleParams[ 'section' ], optionName ), defaultValue )

  def am_getPollingTime( self ):
    return self.am_getCSOption( "PollingTime", self.__moduleParams[ 'pollingTime' ] )

  def am_getMaxCycles( self ):
    return self.am_getCSOption( "MaxCycles", self.__moduleParams[ 'maxCycles' ] )

  def am_getParam( self, optionName ):
    return self.__moduleParams[ optionName ]

  def am_setParam( self, optionName, value ):
    self.__moduleParams[ optionName ] = value

  def __initializeMonitor( self ):
    """
    Initialize the system monitor client
    """
    if self.am_getParam( 'standalone' ):
      self.monitor = gMonitor
    else:
      self.monitor = MonitoringClient()
    self.monitor.setComponentType( self.monitor.COMPONENT_AGENT )
    self.monitor.setComponentName( self.am_getParam( 'fullName' ) )
    self.monitor.initialize()
    self.monitor.registerActivity('CPU',"CPU Usage",'Framework',"CPU,%",self.monitor.OP_MEAN,600)
    self.monitor.registerActivity('MEM',"Memory Usage",'Framework','Memory,MB',self.monitor.OP_MEAN,600)
    self.monitor.disable()

  def __secureCall( self, functor, args = (), name = False ):
    if not name:
      name = str( functor )
    try:
      result = functor( *args )
      if result == None:
        return S_ERROR( "%s method for %s module has to return S_OK/S_ERROR" % ( name, self.__moduleParams[ 'fullName' ] ) )
      return result
    except Exception, e:
      self.log.exception( "Exception while calling %s method" % name )
      return S_ERROR( "Exception while calling %s method: %s" % ( name, str(e) ) )

  def am_go( self ):
    #Set the shifter proxy if required
    if self.__moduleParams[ 'shifterProxy' ]:
      result = setupShifterProxyInEnv( self.__moduleParams[ 'shifterProxy' ],
                                       self.__moduleParams[ 'shifterProxyLocation' ] )
      if not result[ 'OK' ]:
        return result
    self.log.info( "-"*40 )
    self.log.info( "Starting cycle for module %s" % self.__moduleParams[ 'fullName' ] )
    mD = self.am_getMaxCycles()
    if mD > 0:
      cD = self.self.__moduleParams[ 'cyclesDone' ]
      self.log.info( "Remaining %s of % cycles" % ( mD - cD, mD ) )
    self.log.info( "-"*40 )
    elapsedTime = time.time()
    cycleResult = self.__executeModuleCycle()
    #Incrmenent counters
    self.__moduleParams[ 'cyclesDone' ] += 1
    #Show status
    elapsedTime = time.time() - elapsedTime
    self.log.info( "-"*40 )
    self.log.info( "Agent module %s run summary" % self.__moduleParams[ 'fullName' ] )
    self.log.info( " Executed %s times previously" % self.__moduleParams[ 'cyclesDone' ] )
    self.log.info( " Cycle took %.2f seconds" % elapsedTime )
    if cycleResult[ 'OK' ]:
      self.log.info( " Cycle was successful" )
    else:
      self.log.error( " Cycle had an error:", cycleResult[ 'Message' ] )
    self.log.info( "-"*40 )
    return cycleResult

  def __executeModuleCycle(self):
    #Execute the beginExecution function
    result = self.__secureCall( self.beginExecution, name = "beginExecution" )
    if not result[ 'OK' ]:
      return result
    #Launch executor functions
    executors = self.__moduleParams[ 'executors' ]
    if len( executors ) == 1:
      result = self.__secureCall( executors[0][0], executors[0][1] )
      if not result[ 'OK' ]:
        return result
    else:
      exeThreads = [ threading.Thread( target = executor[0], args = executor[1] ) for executor in executors ]
      for thread in exeThreads:
        thread.setDaemon(1)
        thread.start()
      for thread in exeThreads:
        thread.join()
    #Execute the endExecution function
    return  self.__secureCall( self.endExecution, name = "endExecution" )

  def initialize(self):
    return S_OK()

  def beginExecution(self):
    return S_OK()

  def endExecution(self):
    return S_OK()

  def finalize(self):
    return S_OK()

  def execute(self):
    return S_ERROR( "Execute method has to be overwritten by agent module" )