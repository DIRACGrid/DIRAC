########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/PilotMonitor.py,v 1.1 2008/01/16 13:46:10 paterson Exp $
# File :   PilotMonitor.py
# Author : Stuart Paterson
########################################################################

"""  The Pilot Monitor Agent controls the tracking of pilots via the AgentMonitor and Grid
     specific sub-classes. This is a simple wrapper that performs the instantiation and monitoring
     of the AgentMonitor instance for all Grids.
"""

__RCSID__ = "$Id: PilotMonitor.py,v 1.1 2008/01/16 13:46:10 paterson Exp $"

from DIRAC.Core.Base.Agent                                      import Agent
from DIRAC.Core.Utilities                                       import List
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

AGENT_NAME = 'WorkloadManagement/PilotMonitor'

class PilotMonitor(Agent):

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
    #Can be passed via a command line .cfg file for gLite and others
    self.type = gConfig.getValue(self.section+'/Middleware','LCG')
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.threadStartDelay = gConfig.getValue(self.section+'/ThreadStartDelay',5)
    self.pmName = '%sPilotMonitor' %(self.type)
    self.pmSection = '/%s/%s' % ( '/'.join( List.fromChar(self.section, '/' )[:-2] ), 'PilotAgent/%s' %(self.pdName))
    self.log.debug('%sPilotMonitor CS section is: %s' %(self.type,self.pdSection))
    self.pmPath = gConfig.getValue(self.section+'/ModulePath','DIRAC.WorkloadManagementSystem.PilotAgent')

    try:
      importModule = __import__('%s.%s' %(self.pmPath,self.pmName),globals(),locals(),[self.pmName])
    except Exception, x:
      msg = 'Could not import %s.%s' %(self.pmPath,self.pmName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    try:
      moduleStr = 'importModule.%s(self.pmSection,self.type)' %(self.pmName)
      self.agentMonitor = eval(moduleStr)
    except Exception, x:
      msg = 'Could not instantiate %s()' %(self.pmName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    return result

  #############################################################################
  def execute(self):
    """The PilotAgent execution method.
    """
    self.agentMonitor.run()
    return S_OK('Monitoring cycle complete.')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#