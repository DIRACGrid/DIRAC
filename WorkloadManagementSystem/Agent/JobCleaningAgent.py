########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobCleaningAgent.py,v 1.1 2008/10/20 07:08:29 atsareg Exp $
# File :   JobCleaningAgent.py
# Author : A.T.
########################################################################

"""  The Job Cleaning Agent controls removing jobs from the WMS in the end of their life cycle.
"""

__RCSID__ = "$Id: JobCleaningAgent.py,v 1.1 2008/10/20 07:08:29 atsareg Exp $"

from DIRAC.Core.Base.Agent                   import Agent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC                                   import S_OK, S_ERROR, gConfig, gLogger

AGENT_NAME = 'WorkloadManagement/JobCleaningAgent'

class JobCleaningAgent(Agent):

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
    self.jobDB = JobDB()

    return result

  #############################################################################
  def execute(self):
    """The PilotAgent execution method.
    """

    result = self.removeDeletedJobs()
    return S_OK()

  def removeDeletedJobs(self):
    """ Remove deleted jobs
    """

    result = self.jobDB.selectJobsWithStatus('Deleted')
    if not result['OK']:
      return result

    jobList = result['Value']
    for jobID in jobList:
      result = self.jobDB.removeJobFromDB(jobID)
      if not result['OK']:
        gLogger.warn('Failed to remove job %d from JobDB' % jobID, result['Message'])

    return S_OK()
  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
