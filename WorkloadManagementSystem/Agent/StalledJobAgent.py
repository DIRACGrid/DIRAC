########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/StalledJobAgent.py,v 1.1 2007/11/07 18:12:47 paterson Exp $
# File :   StalledJobAgent.py
########################################################################

"""  The StalledJobAgent hunts for stalled jobs in the Job database. Jobs in "running"
     state not receiving a heartbeat signal for more than self.stalledTime
     seconds will be assigned "stalled" state. Jobs in "stalled" state
     having the last heartbeat no older than self.revivalTime will
     be assigned "running" state.
"""

__RCSID__ = "$Id: StalledJobAgent.py,v 1.1 2007/11/07 18:12:47 paterson Exp $"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC                                          import S_OK, S_ERROR

import time

AGENT_NAME = 'WorkloadManagement/StalledJobAgent'

class StalledJobAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets default parameters
    """
    result = Agent.initialize(self)
    self.jobDB = JobDB()

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','INFO') == 'DEBUG':
      self.dbg = True
      self.log.setLevel(logging.DEBUG)

    self.pollingTime   = gConfig.getValue(self.section+'/PollingTime',600)
    self.stalledTime   = gConfig.getValue(self.section+'/StalledTime',7200)
    self.revivalTime   = gConfig.getValue(self.section+'/RevivalTime',3600)
    self.enable        = gConfig.getValue(self.section+'/EnableFlag',1)

    self.log.debug( '==========================================='          )
    self.log.debug( 'DIRAC Stalled Job Agent is started with     '         )
    self.log.debug( 'the following parameters:           '                 )
    self.log.debug( '==========================================='          )
    self.log.debug( 'Polling Time       ==> %s' % self.pollingTime         )
    self.log.debug( 'Stalled Time       ==> %s' % self.stalledTime         )
    self.log.debug( 'Revival Time       ==> %s' % self.revivalTime         )
    if not self.enable:
      self.log.debug('Stalled Job Agent running in disabled mode')
    self.log.debug( '==========================================='          )

  #############################################################################
  def execute(self):
    """ The main agent execution method
    """

    self.log.debug( 'Waking up Stalled Job Agent' )
    result = self.markStalledJobs()
    if not result['OK']:
      return result

    result = self.reviveJobs()
    if not result['OK']:
      return result

    return S_OK('Stalled Job Agent cycle complete')

 #############################################################################
  def markStalledJobs(self):
    """ Identifies stalled jobs running longer than self.stalledTime.
    """
    result = self.jobDB.selectJobs(status='running',old=self.stalledTime)
    if not result['OK']:
      print result
    else:
      jobs = result['Value']
      if jobs:
        msg = 'The following %s jobs will be marked as stalled:' % (len(jobs))
        self.log.info(msg)
        print jobs
        if self.enable:
          for job in jobs:
            result = self.updateJobStatus(job,'stalled')
            if not result['OK']:
              if result.has_key('Message'):
                self.log.error('Problem updating status for job %s' % (job) )
                self.log.error(result['Message'])
              else:
                print result

 #############################################################################
  def reviveJobs(self):
    """ Revives jobs inadvertently marked as stalled.
    """
    result = self.jobDB.selectJobs(status='stalled',recent=self.revivalTime)
    if not result['OK']:
      print result
    else:
      jobs = result['Value']
      if jobs:
        msg = 'The following %s jobs will be revived:' % (len(jobs))
        self.log.info(msg)
        print jobs
        if self.enable:
          for job in jobs:
            result = self.updateJobStatus(job,'running')
            if not result['OK']:
              if result.has_key('Message'):
                self.log.error('Problem updating status for job %s' % (job) )
                self.log.error(result['Message'])
              else:
                print result

  #############################################################################
  def updateJobStatus(self,job,status):
    """This method updates the job status in the JobDB.
    """
    self.log.debug("self.jobDB.setJobAttribute("+str(job)+",Status,"+status+" update=False)")
    result = self.jobDB.setJobAttribute(job,'Status',status, update=False)
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
