
import os
import threading
import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

class AgentModule:

  def __init__( self, agentName, options = {}, logger = None ):
    if not logger:
      self.log = gLogger
    else:
      self.log = logger
    self.moduleParams = { 'monitoringEnabled' : True,
                    'fullName' : agentName,
                    'section' : PathFinder.getAgentSection( agentName ) }
    self.moduleParams[ 'system' ], self.moduleParams[ 'agentName' ] = agentName.split("/")
    self.moduleParams[ 'enabled' ] = self.getCSOption( "Status", "Active" ).lower() in ( 'active' )
    self.moduleParams[ 'pollingTime' ] = 120
    self.moduleParams[ 'maxCycles' ] = 0
    self.moduleParams[ 'cyclesDone' ] = 0
    self.moduleParams[ 'setup' ] = gConfig.getValue( "/DIRAC/Setup", "Unknown" )
    self.moduleParams[ 'controlDirectory' ] = os.path.join( DIRAC.rootPath,
                                                            'control',
                                                            os.path.join( self.moduleParams[ 'fullName' ].split("/") ) )
    self.moduleParams[ 'workDirectory' ] = os.path.join( DIRAC.rootPath,
                                                            'work',
                                                            os.path.join( self.moduleParams[ 'fullName' ].split("/") ) )

    for key in params:
      self.moduleParams[ key ] = self.params[ key ]
    self.__checkAgentDir( 'controDirectory' )
    self.moduleParams[ 'executors' ] = [ ( self.execute, () ) ]
    self.moduleParams[ 'shifterProxy' ] = False
    self.moduleParams[ 'shifterProxyLocation' ] = os.join( self.info[ 'self.workDirectory' ], '.shifterCred' )
    self.__initializeMonitor()
    result = self.initialize()
    if not result[ 'OK' ]:
      raise Exception( "Error while initializing %s: %s"  % ( agentName, result[ 'Message' ] ) )
    if self.moduleParams[ 'monitoringEnabled' ]:
      self.monitor.enable()
    if len( self.moduleParams[ 'executors' ] ) < 1:
      raise Exception( "At least one executor method has to be defined" )
    if not self.moduleParams[ 'enabled' ]:
      raise Exception( "Agent is disabled via the configuration")

  def __checkAgentDir( self, name, defaultPath ):
    if name in self.moduleParams:
      defaultPath = self.moduleParams[ name ]
    else:
      defaultPath = os.path.join( DIRAC.rootPath, name, os.path.join( self.moduleParams[ 'fullName' ].split("/") ) )
    self.moduleParams[ name ] = self.getCSOption( name.capitalize(), defaultPath )
    try:
      os.makedirs( self.moduleParams[ name ] )
    except:
      pass
    if not os.path.isdir( self.moduleParams[ name ] ):
      raise Exception('Can not create %s at %s' % ( name, self.moduleParams[ name ] ) )

  def getCSOption( self, optionName, defaultValue = False ):
    return gConfig.getValue( "%s/%s" % ( self.moduleParams[ 'section' ], optionName ), defaultValue )

  def getPollingTime( self ):
    return self.getCSOption( "PollingTime", self.moduleParams[ 'pollingTime' ] )

  def getMaxCycles( self ):
    return self.getCSOption( "MaxCycles", self.moduleParams[ 'maxCycles' ] )

  def __initializeMonitor( self ):
    """
    Initialize the system monitor client
    """
    self.monitor = MonitoringClient()
    self.monitor.setComponentType( gMonitor.COMPONENT_AGENT )
    self.monitor.setComponentName( self.getFullNameProperty() )
    self.monitor.initialize()
    self.monitor.registerActivity('CPU',"CPU Usage",'Framework',"CPU,%",gMonitor.OP_MEAN,600)
    self.monitor.registerActivity('MEM',"Memory Usage",'Framework','Memory,MB',gMonitor.OP_MEAN,600)
    self.monitor.disable()

  def __secureCall( self, functor, args = (), name = False ):
    if not name:
      name = str( functor )
    try:
      result = functor( *args )
      if not result[ 'OK' ]:
        return result
    except Exception, e:
      self.log.exception( "Exception while calling %s method" % name )
      return S_ERROR( "Exception while calling %s method: %s" % ( name, str(e) ) )

  def run( self ):
    #Set the shifter proxy if required
    if self.moduleParams[ 'shifterProxy' ]:
      result = setupShifterProxyInEnv( self.moduleParams[ 'shifterProxy' ],
                                       self.moduleParams[ 'shifterProxyLocation' ] )
      if not result[ 'OK' ]:
        return result
    #Execute the beginExecution function
    result = self.__secureCall( self.beginExecution, name = "beginExecution" )
    if not result[ 'OK' ]:
      return result
    #Launch executor functions
    executors = self.moduleParams[ 'executors' ]
    if len( executors ) == 1:
      result = self.__secureCall( executors[0][0], executors[0][1] )
      if not result[ 'OK' ]:
        return res
    else:
      exeThreads = [ threading.Thread( target = executor[0], args = executor[1] ) for executor in executors ]
      for thread in exeThreads:
        thread.setDaemon(1)
        thread.start()
      for thread in exeThreads:
        thread.join()
    #Execute the endExecution function
    result =  self.__secureCall( self.endExecution, name = "endExecution" )
    if not result[ 'OK' ]:
      return result
    #Incrmenent counters
    self.moduleParams[ 'cyclesDone' ] += 1

  def initialize(self):
    return S_OK()

  def beginExecution(self):
    return S_OK()

  def endExecution(self):
    return S_OK()

  def execute(self):
    return S_ERROR( "Execute method has to be overwritten by agent module" )