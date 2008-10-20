########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobCleaningAgent.py,v 1.2 2008/10/20 13:16:48 atsareg Exp $
# File :   JobCleaningAgent.py
# Author : A.T.
########################################################################

"""  The Job Cleaning Agent controls removing jobs from the WMS in the end of their life cycle.
"""

__RCSID__ = "$Id: JobCleaningAgent.py,v 1.2 2008/10/20 13:16:48 atsareg Exp $"

from DIRAC.Core.Base.Agent                   import Agent
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC                                   import S_OK, S_ERROR, gConfig, gLogger
import DIRAC.Core.Utilities.Time as Time

AGENT_NAME = 'WorkloadManagement/JobCleaningAgent'
REMOVE_STATUS_DELAY = {'Deleted':0,
                       'Done':14,
                       'Killed':7,
                       'Failed':14 }

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

    # Remove jobs with final status
    for status,delay in REMOVE_STATUS_DELAY.items():
      delTime = Time.dateTime() - delay*Time.day
      result = self.removeJobsByStatus(status,delTime)
      if not result['OK']:
        gLogger.warn('Failed to remove jobs in status %s' % status)
    return S_OK()

  def removeJobsByStatus(self,status,delay):
    """ Remove deleted jobs
    """

    result = self.jobDB.selectJobs({'Status':status},older=delay)
    if not result['OK']:
      return result

    jobList = result['Value']
    count = 0
    for jobID in jobList:
      result = self.jobDB.removeJobFromDB(jobID)
      if not result['OK']:
        gLogger.warn('Failed to remove job %d from JobDB' % jobID, result['Message'])
      else:
        count += 1

    if count > 0:
      gLogger.info('Deleted %d jobs from JobDB' % count)
    return S_OK()