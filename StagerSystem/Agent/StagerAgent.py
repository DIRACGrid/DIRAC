########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/StagerAgent.py,v 1.1 2008/03/31 16:30:21 paterson Exp $
# File :   StagerAgent.py
# Author : Stuart Paterson
########################################################################

"""  The StagerAgent controls staging requests via the SiteStager class.
     This is a simple wrapper that performs the instantiation and monitoring
     of the SiteStager instances for each site. The appropriate CS section
     and site name are passed to the SiteStager instances.  The StagerAgent
     also manages the proxy environment for the SiteStager instances.
"""

__RCSID__ = "$Id: StagerAgent.py,v 1.1 2008/03/31 16:30:21 paterson Exp $"

from DIRAC.Core.Base.Agent                                 import Agent
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

AGENT_NAME = 'Stager/StagerAgent'

class StagerAgent(Agent):

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
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/StagerAgent/shiftProdProxy')
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.threadStartDelay = gConfig.getValue(self.section+'/ThreadStartDelay',5)
    self.siteStager = gConfig.getValue(self.section+'/ModulePath','DIRAC.StagerSystem.Agent.SiteStager')
    self.started = False
    try:
      self.importModule = __import__(self.siteStager,globals(),locals(),['SiteStager'])
    except Exception, x:
      msg = 'Could not import %s' %(self.siteStager)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)
    return result

  #############################################################################
  def execute(self):
    """The StagerAgent execution method.
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