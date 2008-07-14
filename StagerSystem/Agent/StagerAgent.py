########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/StagerAgent.py,v 1.5 2008/07/14 16:19:25 acasajus Exp $
# File :   StagerAgent.py
# Author : Stuart Paterson
########################################################################

"""  The StagerAgent controls staging requests via the SiteStager class.
     This is a simple wrapper that performs the instantiation and monitoring
     of the SiteStager instances for each site. The appropriate CS section
     and site name are passed to the SiteStager instances.  The StagerAgent
     also manages the proxy environment for the SiteStager instances.
"""

__RCSID__ = "$Id: StagerAgent.py,v 1.5 2008/07/14 16:19:25 acasajus Exp $"

from DIRAC.Core.Base.Agent                                 import Agent
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Shifter                          import setupShifterProxyInEnv
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
