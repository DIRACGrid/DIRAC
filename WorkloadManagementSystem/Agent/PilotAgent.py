########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/PilotAgent.py,v 1.3 2009/04/18 18:26:57 rgracian Exp $
# File :   PilotAgent.py
# Author : Stuart Paterson
########################################################################

"""  The Pilot Agent controls the submission of pilots via the Pilot Director and Grid-specific Pilot
     Director sub-classes.  This is a simple wrapper that performs the instantiation and monitoring
     of the PilotDirector instances.
"""

__RCSID__ = "$Id: PilotAgent.py,v 1.3 2009/04/18 18:26:57 rgracian Exp $"

from DIRAC.Core.Base.Agent                                      import Agent
from DIRAC.Core.Utilities                                       import List
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

AGENT_NAME = 'WorkloadManagement/PilotAgent'

class PilotAgent(Agent):

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
    #Can be passed via a command line .cfg file for gLite
    self.type = gConfig.getValue(self.section+'/Middleware','LCG')
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.threadStartDelay = gConfig.getValue(self.section+'/ThreadStartDelay',5)
    self.pdName = '%sPilotDirector' %(self.type)
    self.pdSection = '/%s/%s' % ( '/'.join( List.fromChar(self.section, '/' )[:-2] ), 'PilotAgent/%s' %(self.pdName))
    self.log.debug('%sPilotDirector CS section is: %s' %(self.type,self.pdSection))
    self.pdPath = gConfig.getValue(self.section+'/ModulePath','DIRAC.WorkloadManagementSystem.PilotAgent')
    self.started = False
    try:
      self.importModule = __import__('%s.%s' %(self.pdPath,self.pdName),globals(),locals(),[self.pdName])
    except Exception, x:
      msg = 'Could not import %s.%s' %(self.pdPath,self.pdName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    return result

  #############################################################################
  def execute(self):
    """The PilotAgent execution method.
    """
    agent = {}
    if not self.started:
      resourceBrokers = gConfig.getValue(self.pdSection+'/ResourceBrokers','lhcb-lcg-rb04.cern.ch')
  #  resourceBrokers = gConfig.getValue('LCGPilotDirector/ResourceBrokers','lcgrb03.gridpp.rl.ac.uk,lhcb-lcg-rb03.cern.ch')
      if not type(resourceBrokers)==type([]):
        resourceBrokers = resourceBrokers.split(',')

      for rb in resourceBrokers:
        gLogger.verbose('Starting thread for %s RB %s' %(self.type,rb))
        try:
          moduleStr = 'self.importModule.%s(self.pdSection,rb,self.type)' %(self.pdName)
          agent[rb] = eval(moduleStr)
        except Exception, x:
          msg = 'Could not instantiate %s()' %(self.pdName)
          self.log.warn(x)
          self.log.warn(msg)
          return S_ERROR(msg)

        agent[rb].start()
        time.sleep(self.threadStartDelay)

      self.started=True

    for rb,th in agent.items():
      if th.isAlive():
        gLogger.verbose('Thread for %s RB %s is alive' %(self.type,rb))
      else:
        gLogger.verbose('Thread isAlive() = %s' %(th.isAlive()))
        gLogger.warn('Thread for %s RB %s is dead, restarting ...' %(self.type,rb))
        th.start()

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
