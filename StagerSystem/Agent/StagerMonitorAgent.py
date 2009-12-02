########################################################################
# $HeadURL$
# File :   StagerMonitorAgent.py
# Author : Stuart Paterson
########################################################################

"""  The StagerMonitorAgent controls the monitoring of staging requests via
     the SiteMonitor. This is a simple wrapper that performs the instantiation and monitoring
     of the SiteMonitor instances. The StagerMonitorAgent also manages the proxy environment.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                           import AgentModule
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Shifter                          import setupShifterProxyInEnv
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client                      import PathFinder
import os, sys, re, string, time

AGENT_NAME = 'Stager/StagerMonitorAgent'

class StagerMonitorAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """

    self.section = PathFinder.getAgentSection( AGENT_NAME )
    self.pollingTime = self.am_getOption('PollingTime',60)
    self.threadStartDelay = self.am_getOption('ThreadStartDelay',5)
    self.siteMonitor = self.am_getOption('ModulePath','DIRAC.StagerSystem.Agent.SiteMonitor')
    self.started = False
    try:
      self.importModule = __import__(self.siteMonitor,globals(),locals(),['SiteMonitor'])
    except Exception, x:
      msg = 'Could not import %s' %(self.siteMonitor)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)
    
    self.proxyLocation = self.am_getOption('ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    self.am_setModuleParam('shifter','ProductionManager')
    self.am_setModuleParam('shifterProxyLocation',self.proxyLocation)
    
    return S_OK()

  #############################################################################
  def execute(self):
    """The StagerMonitorAgent execution method.
    """
    # Update polling time
    self.pollingTime = self.am_getOption('PollingTime',60)

    agent = {}
    if not self.started:
      sites = self.am_getOption('Sites','LCG.CERN.ch')
      if not type(sites)==type([]):
        sites = [x.strip() for x in string.split(sites,',')]

      for site in sites:
        csPath = '%s/%s' %(self.section,site)
        gLogger.info('Starting SiteMonitor thread for site %s' %(site))
        gLogger.verbose('SiteMonitor CS path: %s' %(csPath))
        try:
          moduleStr = 'self.importModule.SiteMonitor(csPath,site)'
          agent[site] = eval(moduleStr)
        except Exception, x:
          msg = 'Could not instantiate SiteMonitor()'
          self.log.warn(x)
          self.log.warn(msg)
          return S_ERROR(msg)

        agent[site].start()
        time.sleep(self.threadStartDelay)

      self.started=True

    for site,th in agent.items():
      if th.isAlive():
        gLogger.verbose('SiteMonitor thread for %s is alive' %(site))
      else:
        gLogger.verbose('%s SiteMonitor thread isAlive() = %s' %(site,th.isAlive()))
        gLogger.warn('SiteMonitor thread for %s is dead, restarting ...' %(site))
        th.start()

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
