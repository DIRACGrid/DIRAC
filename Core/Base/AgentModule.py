########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/AgentModule.py,v 1.8 2008/12/04 18:24:15 acasajus Exp $
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

__RCSID__ = "$Id: AgentModule.py,v 1.8 2008/12/04 18:24:15 acasajus Exp $"

import os
import threading
import types
import time
import DIRAC
from DIRAC import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

class AgentModule:

  def __init__( self, agentName, baseAgentName, properties = {} ):
    if agentName == baseAgentName:
      self.log = gLogger
      standaloneModule = True
    else:
      self.log = gLogger.getSubLogger( agentName, child = False )
      standaloneModule = False

    self.__moduleProperties = { 'fullName' : agentName,
                                'section' : PathFinder.getAgentSection( agentName ),
                                'standalone' : standaloneModule,
                                'cyclesDone' : 0,
                                'setup' : gConfig.getValue( "/DIRAC/Setup", "Unknown" ) }
    self.__moduleProperties[ 'system' ], self.__moduleProperties[ 'agentName' ] = agentName.split("/")
    self.__configDefaults = {}
    self.__configDefaults[ 'MonitoringEnabled'] = True
    self.__configDefaults[ 'Enabled'] = self.am_getOption( "Status", "Active" ).lower() in ( 'active' )
    self.__configDefaults[ 'PollingTime'] = self.am_getOption( "PollingTime", 120 )
    self.__configDefaults[ 'MaxCycles'] = self.am_getOption( "MaxCycles", 0 )
    self.__configDefaults[ 'ControlDirectory' ] = os.path.join( DIRAC.rootPath,
                                                            'control',
                                                            os.path.join( *self.__moduleProperties[ 'fullName' ].split("/") ) )
    self.__configDefaults[ 'WorkDirectory' ] = os.path.join( DIRAC.rootPath,
                                                            'work',
                                                            os.path.join( *self.__moduleProperties[ 'fullName' ].split("/") ) )

    for key in properties:
      self.__moduleProperties[ key ] = self.properties[ key ]
    self.__moduleProperties[ 'executors' ] = [ ( self.execute, () ) ]
    self.__moduleProperties[ 'shifterProxy' ] = False
    self.__moduleProperties[ 'shifterProxyLocation' ] = False
    self.__initializeMonitor()
    self.__initialized = False

  def am_initialize( self, *initArgs ):
    result = self.initialize( *initArgs )
    if result == None:
      return S_ERROR( "Error while initializing %s module: initialize must return S_OK/S_ERROR" % agentName )
    if not result[ 'OK' ]:
      return S_ERROR( "Error while initializing %s: %s"  % ( agentName, result[ 'Message' ] ) )
    self.__checkAgentDir( 'ControlDirectory' )
    self.__checkAgentDir( 'WorkDirectory' )
    if not self.__moduleProperties[ 'shifterProxyLocation' ]:
      self.__moduleProperties[ 'shifterProxyLocation' ] = os.path.join( self.am_getOption( 'WorkDirectory' ),
                                                                        '.shifterCred' )
    if self.am_MonitoringEnabled():
      self.monitor.enable()
    if len( self.__moduleProperties[ 'executors' ] ) < 1:
      return S_ERROR( "At least one executor method has to be defined" )
    if not self.am_Enabled():
      return S_ERROR( "Agent is disabled via the configuration")
    self.log.info( "="*40 )
    self.log.info( "Loaded agent module %s" % self.__moduleProperties[ 'fullName' ] )
    self.log.info( " Site: %s" % gConfig.getValue( '/LocalSite/Site', 'Unknown' ) )
    self.log.info( " Setup: %s" % gConfig.getValue( "/DIRAC/Setup" ) )
    self.log.info( " Version: %s " % __RCSID__)
    self.log.info( " DIRAC version: %s" % DIRAC.version )
    self.log.info( " DIRAC platform: %s" % DIRAC.platform )
    self.log.info( " Polling time: %s" % self.am_getOption( 'PollingTime' ) )
    self.log.info( " Control dir: %s" % self.am_getOption( 'ControlDirectory' ) )
    self.log.info( " Work dir: %s" % self.am_getOption( 'WorkDirectory' ) )
    if self.am_getOption( 'MaxCycles' ) > 0:
      self.log.info( " Cycles: %s" % self.am_getMaxCycles() )
    else:
      self.log.info( " Cycles: unlimited" )
    self.log.info( "="*40 )
    self.__initialized = True
    return S_OK()

  def __checkAgentDir( self, name ):
    path = self.am_getOption( name )
    try:
      os.makedirs( path )
    except:
      pass
    if not os.path.isdir( path ):
      raise Exception('Can not create %s at %s' % ( name, path ) )

  def am_getOption( self, optionName, defaultValue = False ):
    if optionName and optionName[0] == "/":
      return gConfig.getValue( optionName, defaultValue )
    if not defaultValue:
      if optionName in self.__configDefaults:
        defaultValue = self.__configDefaults[ optionName ]
    return gConfig.getValue( "%s/%s" % ( self.__moduleProperties[ 'section' ], optionName ), defaultValue )

  def am_setOption( self, optionName, value ):
    self.__configDefaults[ optionName ] = value

  def am_getModuleParam( self, optionName ):
    return self.__moduleProperties[ optionName ]

  def am_setModuleParam( self, optionName, value ):
    self.__moduleProperties[ optionName ] = value

  def am_getPollingTime(self):
    return self.am_getOption( "PollingTime" )

  def am_getMaxCycles(self):
    return self.am_getOption( "MaxCycles" )

  def am_Enabled(self):
    enabled = self.am_getOption( "Enabled" )
    return self.am_getOption( "Enabled" )

  def am_MonitoringEnabled(self):
    return self.am_getOption( "MonitoringEnabled" )

  def __initializeMonitor( self ):
    """
    Initialize the system monitor client
    """
    if self.__moduleProperties[ 'standalone' ]:
      self.monitor = gMonitor
    else:
      self.monitor = MonitoringClient()
    self.monitor.setComponentType( self.monitor.COMPONENT_AGENT )
    self.monitor.setComponentName( self.__moduleProperties[ 'fullName' ] )
    self.monitor.initialize()
    self.monitor.registerActivity('CPU',"CPU Usage",'Framework',"CPU,%",self.monitor.OP_MEAN,600)
    self.monitor.registerActivity('MEM',"Memory Usage",'Framework','Memory,MB',self.monitor.OP_MEAN,600)
    self.monitor.disable()
    self.__monitorLastStatsUpdate = time.time()

  def am_secureCall( self, functor, args = (), name = False ):
    if not name:
      name = str( functor )
    try:
      result = functor( *args )
      if result == None:
        return S_ERROR( "%s method for %s module has to return S_OK/S_ERROR" % ( name, self.__moduleProperties[ 'fullName' ] ) )
      return result
    except Exception, e:
      self.log.exception( "Exception while calling %s method" % name )
      return S_ERROR( "Exception while calling %s method: %s" % ( name, str(e) ) )

  def am_go( self ):
    #Set the shifter proxy if required
    if self.__moduleProperties[ 'shifterProxy' ]:
      result = setupShifterProxyInEnv( self.__moduleProperties[ 'shifterProxy' ],
                                       self.__moduleProperties[ 'shifterProxyLocation' ] )
      if not result[ 'OK' ]:
        return result
    self.log.info( "-"*40 )
    self.log.info( "Starting cycle for module %s" % self.__moduleProperties[ 'fullName' ] )
    mD = self.am_getMaxCycles()
    if mD > 0:
      cD = self.self.__moduleProperties[ 'cyclesDone' ]
      self.log.info( "Remaining %s of % cycles" % ( mD - cD, mD ) )
    self.log.info( "-"*40 )
    elapsedTime = time.time()
    cpuStats = self.__startReportToMonitoring()
    cycleResult = self.__executeModuleCycle()
    if cpuStats:
        self.__endReportToMonitoring( *cpuStats )
    #Incrmenent counters
    self.__moduleProperties[ 'cyclesDone' ] += 1
    #Show status
    elapsedTime = time.time() - elapsedTime
    self.log.info( "-"*40 )
    self.log.info( "Agent module %s run summary" % self.__moduleProperties[ 'fullName' ] )
    self.log.info( " Executed %s times previously" % self.__moduleProperties[ 'cyclesDone' ] )
    self.log.info( " Cycle took %.2f seconds" % elapsedTime )
    if cycleResult[ 'OK' ]:
      self.log.info( " Cycle was successful" )
    else:
      self.log.error( " Cycle had an error:", cycleResult[ 'Message' ] )
    self.log.info( "-"*40 )
    return cycleResult

  def __startReportToMonitoring(self):
    try:
      now = time.time()
      stats = os.times()
      cpuTime = stats[0] + stats[2]
      if now - self.__monitorLastStatsUpdate < 10:
        return ( now, cpuTime )
      # Send CPU consumption mark
      wallClock = now - self.__monitorLastStatsUpdate
      self.__monitorLastStatsUpdate = now
      # Send Memory consumption mark
      membytes = self.__VmB('VmRSS:')
      if membytes:
        mem = membytes / ( 1024. * 1024. )
        gMonitor.addMark('MEM', mem )
      return( now, cpuTime )
    except:
      return False

  def __endReportToMonitoring( self, initialWallTime, initialCPUTime ):
    wallTime = time.time() - initialWallTime
    stats = os.times()
    cpuTime = stats[0] + stats[2] - initialCPUTime
    percentage = cpuTime / wallTime * 100.
    if percentage > 0:
      gMonitor.addMark( 'CPU', percentage )

  def __VmB(self, VmKey):
      '''Private.
      '''
      __memScale = {'kB': 1024.0, 'mB': 1024.0*1024.0, 'KB': 1024.0, 'MB': 1024.0*1024.0}
      procFile = '/proc/%d/status' % os.getpid()
       # get pseudo file  /proc/<pid>/status
      try:
          t = open( procFile )
          v = t.read()
          t.close()
      except:
          return 0.0  # non-Linux?
       # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
      i = v.index( VmKey )
      v = v[i:].split(None, 3)  # whitespace
      if len(v) < 3:
          return 0.0  # invalid format?
       # convert Vm value to bytes
      return float(v[1]) * __memScale[v[2]]

  def __executeModuleCycle(self):
    #Execute the beginExecution function
    result = self.am_secureCall( self.beginExecution, name = "beginExecution" )
    if not result[ 'OK' ]:
      return result
    #Launch executor functions
    executors = self.__moduleProperties[ 'executors' ]
    if len( executors ) == 1:
      result = self.am_secureCall( executors[0][0], executors[0][1] )
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
    return  self.am_secureCall( self.endExecution, name = "endExecution" )

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