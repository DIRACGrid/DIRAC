########################################################################
# $HeadURL$
# File :   StagerAgent.py
# Author : Stuart Paterson
########################################################################

"""  The StagerAgent controls staging requests via the SiteStager class.
     This is a simple wrapper that performs the instantiation and monitoring
     of the SiteStager instances for each site. The appropriate CS section
     and site name are passed to the SiteStager instances.  The StagerAgent
     also manages the proxy environment for the SiteStager instances.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                           import AgentModule
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Shifter                          import setupShifterProxyInEnv
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client                      import PathFinder
import os, sys, re, string, time

AGENT_NAME = 'Stager/StagerAgent'

class StagerAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """

    self.section = PathFinder.getAgentSection( AGENT_NAME )
    self.pollingTime = self.am_getOption('PollingTime',120)
    self.threadStartDelay = self.am_getOption('ThreadStartDelay',5)
    self.siteStager = self.am_getOption('ModulePath','DIRAC.StagerSystem.Agent.SiteStager')
    self.started = False
    try:
      self.importModule = __import__(self.siteStager,globals(),locals(),['SiteStager'])
    except Exception, x:
      msg = 'Could not import %s' %(self.siteStager)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)
    return result
  
    self.proxyLocation = self.am_getOption('ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    self.am_setModuleParam('shifter','ProductionManager')
    self.am_setModuleParam('shifterProxyLocation',self.proxyLocation)
    
    return S_OK()

  #############################################################################
  def execute(self):
    """The StagerAgent execution method.
    """

    agent = {}
    if not self.started:
      sites = gConfig.getValue(self.section+'/Sites','LCG.CERN.ch')
      if not type(sites)==type([]):
        sites = [x.strip() for x in string.split(sites,',')]

      for site in sites:
        csPath = '%s/%s' %(self.section,site)
        gLogger.info('Starting SiteStager thread for site %s' %(site))
        gLogger.verbose('SiteStager CS path: %s' %(csPath))
        try:
          moduleStr = 'self.importModule.SiteStager(csPath,site)'
          agent[site] = eval(moduleStr)
        except Exception, x:
          msg = 'Could not instantiate SiteStager()'
          self.log.warn(x)
          self.log.warn(msg)
          return S_ERROR(msg)

        agent[site].start()
        time.sleep(self.threadStartDelay)

      self.started=True

    for site,th in agent.items():
      if th.isAlive():
        gLogger.verbose('SiteStager thread for %s is alive' %(site))
      else:
        gLogger.verbose('%s SiteStager thread isAlive() = %s' %(site,th.isAlive()))
        gLogger.warn('SiteStager thread for %s is dead, restarting ...' %(site))
        th.start()

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
