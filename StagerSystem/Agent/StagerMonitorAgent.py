########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/StagerMonitorAgent.py,v 1.10 2008/07/14 16:23:54 acasajus Exp $
# File :   StagerMonitorAgent.py
# Author : Stuart Paterson
########################################################################

"""  The StagerMonitorAgent controls the monitoring of staging requests via
     the SiteMonitor. This is a simple wrapper that performs the instantiation and monitoring
     of the SiteMonitor instances. The StagerMonitorAgent also manages the proxy environment.
"""

__RCSID__ = "$Id: StagerMonitorAgent.py,v 1.10 2008/07/14 16:23:54 acasajus Exp $"

from DIRAC.Core.Base.Agent                                 import Agent
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Shifter                          import setupShifterProxyInEnv
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

AGENT_NAME = 'Stager/StagerMonitorAgent'

class StagerMonitorAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',24) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/StagerMonitorAgent/shiftProdProxy')
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',60)
    self.threadStartDelay = gConfig.getValue(self.section+'/ThreadStartDelay',5)
    self.siteMonitor = gConfig.getValue(self.section+'/ModulePath','DIRAC.StagerSystem.Agent.SiteMonitor')
    self.started = False
    try:
      self.importModule = __import__(self.siteMonitor,globals(),locals(),['SiteMonitor'])
    except Exception, x:
      msg = 'Could not import %s' %(self.siteMonitor)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)
    return result

  #############################################################################
  def execute(self):
    """The StagerMonitorAgent execution method.
    """
    # Update polling time
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',60)

    result = setupShifterProxyInEnv( "ProductionManager" )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't get shifter's proxy: %s" % result[ 'Message' ] )

    agent = {}
    if not self.started:
      sites = gConfig.getValue(self.section+'/Sites','LCG.CERN.ch')
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
