########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/LCGPilotAgent.py,v 1.2 2008/01/16 10:51:33 paterson Exp $
# File :   LCGPilotAgent.py
# Author : Stuart Paterson
########################################################################

"""  The LCG Pilot Agent controls the submission of pilots via the Pilot Director and LCG Pilot
     Director classes.  This is a simple wrapper that performs the instantiation and monitoring
     of the PilotDirector instance for either LCG or gLite.
"""

__RCSID__ = "$Id: LCGPilotAgent.py,v 1.2 2008/01/16 10:51:33 paterson Exp $"

from DIRAC.Core.Base.Agent                                      import Agent
from DIRAC.WorkloadManagementSystem.PilotAgent.LCGPilotDirector import LCGPilotDirector
from DIRAC.Core.Utilities                                       import List
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

AGENT_NAME = 'WorkloadManagement/LCGPilotAgent'

class LCGPilotAgent(Agent):

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
    self.started = False
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
        agent[rb] = LCGPilotDirector(self.pdSection,rb,self.type)
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