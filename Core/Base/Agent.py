########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Base/Agent.py,v 1.7 2007/12/21 10:08:12 paterson Exp $
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

__RCSID__ = "$Id: Agent.py,v 1.7 2007/12/21 10:08:12 paterson Exp $"

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

class Agent:

  def __init__(self,name,initializeMonitor=False):
    """ Standard constructor takes the full name of the agent as its argument.
        The full name consists of the system name and the Agent name separated
        by /, e.g. WorkloadManagement/Optimizer
    """
    self.fullname = name
    self.system,self.name = name.split('/')
    self.monitorFlag = initializeMonitor
    self.log = gLogger

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

    self.section = getAgentSection(self.fullname)
    print self.name,self.section

    status = gConfig.getValue(self.section+'/Status','Active')
    if status == "Stopped":
      return S_ERROR('The agent is disactivated in its configuration')

    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',60)
    self.controlDir = gConfig.getValue(self.section+'/ControlDirectory','')
    self.maxcount = gConfig.getValue(self.section+'/MaxCycles',0)

    gLogger.always('')
    gLogger.always('==========================================================')
    gLogger.always('Starting %s Agent' % self.fullname)
    gLogger.always('At site '+ gConfig.getValue('/DIRAC/Site','Unknown'))
    gLogger.always('Within the '+ gConfig.getValue('/DIRAC/Setup','Unknown') +' setup')
    gLogger.always('CVS version '+__RCSID__)
    gLogger.always('DIRAC version v%dr%d build %d' % \
                    (DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel) )
    gLogger.always('Polling time %d' % self.pollingTime)
    gLogger.always('Control directory %s' % self.controlDir)
    if self.maxcount == 1:
      gLogger.always('Single execution cycle')
    elif self.maxcount > 1:
      gLogger.always('%d execution cycles requested' % self.maxcount)
    else:
      gLogger.always('No cycle limit')
    gLogger.always('==========================================================')
    gLogger.always('')

    self.runFlag = True
    self.maxCountFlag = False
    self.signalFlag = 0
    self.count = 0   # Counter of the number of cycles
    self.start = time.time()
    self.exit_status = 'OK'

    if self.monitorFlag:
      self.__initializeMonitor()

    # Set the signal handler
    signal.signal(signal.SIGINT, self.__signal_handler)

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

  print "Importing",'DIRAC.'+system+'System.Agent'
  module = __import__('DIRAC.'+system+'System.Agent',globals(),locals(),[name])
  agent = eval("module."+name+'.'+name+"()")
  return agent

