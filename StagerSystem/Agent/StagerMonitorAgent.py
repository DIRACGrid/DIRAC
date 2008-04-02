########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/StagerMonitorAgent.py,v 1.1 2008/04/02 10:57:09 paterson Exp $
# File :   StagerMonitorAgent.py
# Author : Stuart Paterson
########################################################################

"""  The StagerMonitorAgent controls the monitoring of staging requests via
     the SiteMonitor base class.  The SiteMonitor is overridden by system-specific
     sub-classes and is a simple wrapper that performs the instantiation and monitoring
     of the SiteMonitor instances. The StagerMonitorAgent also manages the proxy environment.
"""

__RCSID__ = "$Id: StagerMonitorAgent.py,v 1.1 2008/04/02 10:57:09 paterson Exp $"

from DIRAC.Core.Base.Agent                                 import Agent
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
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
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.threadStartDelay = gConfig.getValue(self.section+'/ThreadStartDelay',5)
    self.modulePath = gConfig.getValue(self.section+'/ModulePath','DIRAC.StagerSystem.Agent')
    self.started = False
    return result

  #############################################################################
  def execute(self):
    """The StagerMonitorAgent execution method.
    """
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','')
    if not prodDN:
      self.log.warn('Production shift manager DN not defined (/Operations/Production/ShiftManager)')
      return S_OK('Production shift manager DN is not defined')

    self.log.verbose('Checking proxy for %s' %(prodDN))
    result = self.__getProdProxy(prodDN)
    if not result['OK']:
      self.log.warn('Could not set up proxy for shift manager %s %s' %(prodDN))
      return S_OK('Production shift manager proxy could not be set up')

    agent = {}
    if not self.started:
      systemMonitors = gConfig.getValue(self.section+'/SystemMonitors','WMSMonitor')
      if re.search(',',systemMonitors):
        systemMonitors = [ x.strip() for x in string.split(systemMonitors,',')]
      elif type(systemMonitors)==type(' '):
        systemMonitors = [systemMonitors]

      self.log.verbose('Starting Stager Monitoring Modules: %s' %(string.join(systemMonitors,', ')))
      sites = gConfig.getValue(self.section+'/Sites','LCG.CERN.ch')
      if not type(sites)==type([]):
        sites = [x.strip() for x in string.split(sites,',')]

      for system in systemMonitors:
        agent[system]={}
        siteMonitor = '%s.%s' %(self.modulePath,system)
        for site in sites:
          try:
            self.importModule = __import__(siteMonitor,globals(),locals(),[system])
          except Exception, x:
            msg = 'Could not import %s' %(siteMonitor)
            self.log.warn(x)
            self.log.warn(msg)
            return S_ERROR(msg)
          csPath = '%s/%s' %(self.section,site)
          gLogger.info('Starting SiteMonitor thread for site %s and %s' %(site,system))
          gLogger.verbose('%s %s CS path: %s' %(site,system,csPath))
          try:
            moduleStr = 'self.importModule.SiteMonitor(csPath,site)'
            agent[system][site] = eval(moduleStr)
          except Exception, x:
            msg = 'Could not instantiate %s() for %s' %(system,site)
            self.log.warn(x)
            self.log.warn(msg)
            return S_ERROR(msg)

          agent[system][site].start()
          time.sleep(self.threadStartDelay)

      self.started=True

    for system,sites in agent.items():
      for site,th in sites.items():
        if th.isAlive():
          gLogger.verbose('%s thread for %s is alive' %(system,site))
        else:
          gLogger.verbose('%s thread for %s isAlive() = %s' %(system,site,th.isAlive()))
          gLogger.warn('%s thread for %s is dead, restarting ...' %(system,site))
          th.start()

    return S_OK()

  #############################################################################
  def __getProdProxy(self,prodDN):
    """This method sets up the proxy for immediate use if not available, and checks the existing
       proxy if this is available.
    """
    prodGroup = gConfig.getValue(self.section+'/ProductionGroup','lhcb_prod')
    self.log.info("Determining the length of proxy for DN %s" %prodDN)
    obtainProxy = False
    if not os.path.exists(self.proxyLocation):
      self.log.info("No proxy found")
      obtainProxy = True
    else:
      currentProxy = open(self.proxyLocation,'r')
      oldProxyStr = currentProxy.read()
      res = getProxyTimeLeft(oldProxyStr)
      if not res["OK"]:
        self.log.error("Could not determine the time left for proxy", res['Message'])
        res = S_OK(0) # force update of proxy

      proxyValidity = int(res['Value'])
      self.log.debug('Current proxy found to be valid for %s seconds' %proxyValidity)
      self.log.info('%s proxy found to be valid for %s seconds' %(prodDN,proxyValidity))
      if proxyValidity <= self.minProxyValidity:
        obtainProxy = True

    if obtainProxy:
      self.log.info('Attempting to renew %s proxy' %prodDN)
      res = self.wmsAdmin.getProxy(prodDN,prodGroup,self.proxyLength)
      if not res['OK']:
        self.log.error('Could not retrieve proxy from WMS Administrator', res['Message'])
        return S_OK()
      proxyStr = res['Value']
      if not os.path.exists(os.path.dirname(self.proxyLocation)):
        os.makedirs(os.path.dirname(self.proxyLocation))
      res = setupProxy(proxyStr,self.proxyLocation)
      if not res['OK']:
        self.log.error('Could not create environment for proxy.', res['Message'])
        return S_OK()

      setDIRACGroup(prodGroup)
      self.log.info('Successfully renewed %s proxy' %prodDN)

    #os.system('voms-proxy-info -all')
    return S_OK('Active proxy available')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#