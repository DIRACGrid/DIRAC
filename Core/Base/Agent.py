########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/Agent.py,v 1.24 2008/07/09 12:54:58 rgracian Exp $
########################################################################
""" Base class for all the Agents.

    The specific agents must provide the following methods:
    - initialize() for initial settings
    - execute() - the main method called in the agent cycle
    - finalize() - the graceful exit of the method, this one is usually used
               for the agent restart

    The agent can be stopped either by a signal or by creating a 'stop_agent' file
    in the ControlDirectory defined in the agent configuration

"""

__RCSID__ = "$Id: Agent.py,v 1.24 2008/07/09 12:54:58 rgracian Exp $"

import os
import threading
import time
import signal

import DIRAC
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getAgentSection
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.MonitoringSystem.Client.MonitoringClient import gMonitor
from DIRAC.Core.Utilities.Subprocess import shellCall

class Agent:

  def __init__( self, name, initializeMonitor = False ):
    """ Standard constructor takes the full name of the agent as its argument.
        The full name consists of the system name and the Agent name separated
        by /, e.g. WorkloadManagement/Optimizer
    """
    self.fullname = name
    self.system,self.name = name.split('/')
    self.log = gLogger
    self.monitorFlag = False

    self.section = getAgentSection(self.fullname)

    if initializeMonitor:
      self.monitorFlag = True

    if gConfig.getValue('/LocalSite/EnableAgentMonitoring',"no").lower() in ( 'y', 'yes', '1' ):
      self.monitorFlag = True

    agentFlag = gConfig.getValue(self.section+'/EnableAgentMonitoring',"notSet").lower()
    if agentFlag in ( 'y', 'yes', '1' ):
      self.monitorFlag = True
    elif agentFlag in ( 'n', 'no', '0' ):
      self.monitorFlag = False

    if self.monitorFlag:
      self.__initializeMonitor()

  def __initializeMonitor( self ):
    """
    Initialize the system monitor client
    """
    gMonitor.setComponentType( gMonitor.COMPONENT_AGENT )
    gMonitor.setComponentName( self.fullname )
    gMonitor.initialize()

  def initialize(self):
    """ Default common agent initialization
    """

    status = gConfig.getValue(self.section+'/Status','Active')
    if status == "Stopped":
      return S_ERROR('The agent is disactivated in its configuration')

    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120 )
    controlDir = os.path.join( DIRAC.rootPath, 'control', self.fullname )
    self.controlDir = gConfig.getValue(self.section+'/ControlDirectory', controlDir )
    try:
      os.makedirs( self.controlDir )
    except:
      pass
    if not os.path.isdir( self.controlDir ):
      return S_ERROR('Can not create control directory: %s' % self.controlDir )
    
    self.maxcount = gConfig.getValue(self.section+'/MaxCycles',0)
    workDir = os.path.join( DIRAC.rootPath, 'work' )
    workDir = gConfig.getValue(self.section+'/WorkDir', workDir )
    self.workDir = os.path.join( workDir, self.fullname )
    self.diracSetup = gConfig.getValue('/DIRAC/Setup','Unknown')

    gLogger.always( 'Starting Agent', self.fullname )
    gLogger.info('')
    gLogger.info('==========================================================')
    gLogger.info('Starting %s Agent' % self.fullname)
    gLogger.info('At site '+ gConfig.getValue('/LocalSite/Site','Unknown'))
    gLogger.info('Within the '+ self.diracSetup +' setup')
    gLogger.info('CVS version '+__RCSID__)
    gLogger.info('DIRAC version v%dr%d build %d' % \
                    (DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel) )
    gLogger.info('Polling time %d' % self.pollingTime)
    gLogger.info('Control directory %s' % self.controlDir)
    gLogger.info('Working directory %s' % self.workDir)
    if self.maxcount == 1:
      gLogger.info('Single execution cycle')
    elif self.maxcount > 1:
      gLogger.info('%d execution cycles requested' % self.maxcount)
    else:
      gLogger.info('No cycle limit')
    if self.monitorFlag:
      gLogger.info('Activity monitoring enabled')
    gLogger.info('==========================================================')
    gLogger.info('')

    self.runFlag = True
    self.maxCountFlag = False
    self.signalFlag = 0
    self.count = 0   # Counter of the number of cycles
    self.start = time.time()
    self.startStats = os.times()
    self.lastStats = os.times()
    self.lastWallClock = time.time()
    self.exit_status = 'OK'

    # Set the signal handler
    signal.signal(signal.SIGINT, self.__signal_handler)

    self.monitorName = "%s/%s" % (self.name,self.diracSetup)
    if self.monitorFlag:
      gLogger.verbose("Registering CPU & Memory consumption activity")
      gMonitor.registerActivity('CPU',"CPU Usage",'Framework',"CPU,%",gMonitor.OP_MEAN,600)
      gMonitor.registerActivity('MEM',"Memory Usage",'Framework','Memory,MB',gMonitor.OP_MEAN,600)

    return S_OK()

  def getAgentName(self):
    """ Get the agent name, this is the name of the agent class
    """

    return self.name

  def getSystemName(self):
    """ Get the agent system name - to which system agent belongs
    """

    return self.system

  def getFullName(self):
    """ Get the agent full name consisting of the system and the agent names
    """

    return self.fullname

  def __signal_handler (self, signum, frame):
    """ Handler of the interruption signals
    """

    gLogger.info("Received interruption signal %d" % signum)
    self.signalFlag = signum
    self.exit_status = 'Signal %d' % signum

  def finalize(self):
    """ Last actions before exiting
    """

    # Print the final summary
    gLogger.always('')
    gLogger.always('==========================================================')
    gLogger.always('Agent run summary:')
    exec_time = time.time() - self.start
    gLogger.always('Execution time %.2f seconds' % exec_time)
    gLogger.always('Number of cycles %d' % self.count)
    gLogger.always('Exit status %s' % self.exit_status)
    gLogger.always('==========================================================')
    gLogger.always('')

    if os.path.exists(self.controlDir+'/stop_agent'):
      os.remove(self.controlDir+'/stop_agent')

  def run(self, ncycles=0 ):
    """ The main agent execution method
    """

    self.maxcount = ncycles
    result = self.initialize()
    if not result['OK']:
      gLogger.always('Can not start agent for the following reason')
      gLogger.always(result['Message'])
      return result

    # Create the working thread
    job = threading.Thread()
    job.run = self.__run_thread
    job.start()

    # Check the agent instructions
    while True:

      # Check if the working thread failed
      if not self.runFlag:
        gLogger.error('Agent stopped because of internal error')
        self.finalize()
        return S_ERROR('Agent stopped because of internal error')

      # Check for the max cycle count
      if self.maxCountFlag:
        gLogger.info('Maximum number of cycles reached %d' % self.maxcount)
        self.finalize()
        return S_OK()

      # check for signal
      if self.signalFlag:
        gLogger.info('Stopping agent ...')
        self.runFlag = False
        # wait for the working thread
        while job.isAlive():
          time.sleep(1)
        self.finalize()
        return S_ERROR('Agent signalled to stop')

      stopFlag = False
      if os.path.exists(self.controlDir+'/stop_agent'):
        stopFlag = True
        self.exit_status = 'Stopped'
      if stopFlag:
        gLogger.info('Agent stopped by the control flag')
        self.runFlag = False
        # Let the workin thread stop gracefully
        gLogger.info('Waiting for the working thread to finish')
        while job.isAlive():
          time.sleep(1)
        gLogger.info('The working thread is stopped')
        self.finalize()
        return S_ERROR('The agent is stopped by the external control')
      time.sleep(1)

  def __run_thread(self):
    """ Execute the agent method in a separate thread
    """

    try:
      while self.runFlag:
        gLogger.debug('Starting agent loop # %d' % self.count)
        start_cycle_time = time.time()
        result = self.execute()
        exec_cycle_time = time.time() - start_cycle_time
        self.count += 1
        if self.count >= self.maxcount and self.maxcount > 0:
          self.exit_status = 'Max # of cycles reached %d' % self.maxcount
          self.maxCountFlag = True
          break
        if result['OK']:
          status = 'OK'
          if exec_cycle_time < self.pollingTime:
            time.sleep(self.pollingTime)
        else:
          gLogger.error(result['Message'])
          status = 'Error'
          break
        if self.monitorFlag:
          # Send CPU consumption mark
          stats = os.times()
          cpuTime = stats[0]+stats[2]-self.lastStats[0]-self.lastStats[2]
          wallClock = time.time() - self.lastWallClock
          percentage = cpuTime/wallClock*100.
          gLogger.verbose("Sending CPU consumption %.2f" % percentage)
          gMonitor.addMark('CPU',percentage)
          self.lastStats = os.times()
          self.lastWallClock = time.time()
          # Send Memory consumption mark
          pid = os.getpid()
          result = shellCall(0,'ps -p %d -o rsz=' % pid)
          if result['OK']:
            returnCode,stdOut,stdErr = result['Value']
            mem = float(stdOut)
            gLogger.verbose("Sending Memory consumption %.2f MB" % (mem/1024.,))
            gMonitor.addMark('MEM',mem/1024.)
          else:
            gLogger.warn('Failed to get memory consumption')
    except Exception,x:
      gLogger.exception(str(x))
      self.runFlag = False
      self.exit_status = 'Exception'
      return

  def run_once(self):
    """ Runs the agent just once
    """

    return self.run(1)

  def execute(self):
    """ This method should be overridden in the specific agent classes
    """

    gLogger.error('AgentBase: execute method should be implemented in a subclass')
    return S_ERROR('AgentBase: execute method should be implemented in a subclass')

####################################################################################

def createAgent(agentName):

  try:
    system,name = agentName.split('/')
  except ValueError:
    print "Invalid agent name",agentName
    return None

  try:
    # print "Importing",'DIRAC.'+system+'System.Agent'
    module = __import__('DIRAC.'+system+'System.Agent',globals(),locals(),[name])
    agent = eval("module."+name+'.'+name+"()")
  except Exception,x:
    try:
      # print 'Importing',system+'System.Agent'
      module = __import__(system+'System.Agent',globals(),locals(),[name])
      agent = eval("module."+name+'.'+name+"()")
    except Exception,y:
      print 'Importing DIRAC.'+system+'System Agent failed with exception:'
      print x
      print 'Importing '+system+'System Agent failed with exception:'
      print y
      return None

  return agent
