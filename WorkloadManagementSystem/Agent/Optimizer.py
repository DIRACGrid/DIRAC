########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Optimizer.py,v 1.12 2008/05/14 18:55:54 rgracian Exp $
# File :   Optimizer.py
# Author : Stuart Paterson
########################################################################

"""  The Optimizer base class is an agent that polls for jobs with a specific
     status and minor status pair.  The checkJob method is overridden for all
     optimizer instances and associated actions are performed there.
"""

__RCSID__ = "$Id: Optimizer.py,v 1.12 2008/05/14 18:55:54 rgracian Exp $"

from DIRAC.WorkloadManagementSystem.DB.JobDB        import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import ClassAd
from DIRAC.Core.Base.Agent                          import Agent
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Utilities.Subprocess                import shellCall
from DIRAC                                          import S_OK, S_ERROR

import os, re, time, string

class Optimizer(Agent):

  #############################################################################
  def __init__(self, name, initialStatus=None, enableFlag=True, system=None):
    self.optimizerName = name
    self.initialStatus = initialStatus
    self.enableFlag    = enableFlag
    if not system:
      system='WorkloadManagement'
    Agent.__init__(self,'%s/%s' %(system,self.optimizerName))

  #############################################################################
  def initialize(self):
    """ Initialization of the Optimizer Agent.
    """
    result = Agent.initialize(self)
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    #In disabled mode, no statuses will be updated to allow debugging.

    if self.enableFlag:
      self.enable               = True
    else:
      self.enable               = False

    #Other parameters
    self.pollingTime            = gConfig.getValue(self.section+'/PollingTime',60)
    if not self.initialStatus:
      self.jobStatus            = gConfig.getValue(self.section+'/JobStatus','Checking')
    else:
      self.jobStatus            = gConfig.getValue(self.section+'/JobStatus',self.initialStatus)

    self.minorStatus            = gConfig.getValue(self.section+'/JobMinorStatus',self.optimizerName)
    self.failedStatus           = gConfig.getValue(self.section+'/FailedJobStatus','Failed')

    self.log.info( '==========================================='           )
    self.log.info( 'DIRAC '+self.optimizerName+' Agent is started with'    )
    if self.enable:
      self.log.info( 'the following parameters:'                           )
    else:
      self.log.info( 'the following parameters, in DISABLED mode:'         )
    self.log.info( '==========================================='           )
    self.log.info( 'Polling Time        ==> %s' % self.pollingTime         )
    self.log.info( 'Job Status          ==> %s' % self.jobStatus           )
    self.log.info( 'Job Minor Status    ==> %s' % self.minorStatus         )
    self.log.info( 'Failed Job Status   ==> %s' % self.failedStatus        )
    self.log.info( '==========================================='           )

    return result

  #############################################################################
  def execute(self):
    """ The main agent execution method
    """
    condition = {}

    if not self.initialStatus:
      condition = {'Status':self.jobStatus,'MinorStatus':self.minorStatus}
    else:
      condition = {'Status':self.jobStatus}

    result = self.jobDB.selectJobs(condition)
    if not result['OK']:
      self.log.warn('Failed to get a job list from the JobDB')
      return S_ERROR('Failed to get a job list from the JobDB')

    if not len(result['Value']):
      self.log.verbose('No pending jobs to process')
      return S_OK('No work to do')

    jobList = result['Value']
    for job in jobList:
      result = self.checkJob(job)
      if not result['OK']:
        self.updateJobStatus(job,self.failedStatus,result['Message'])

    return S_OK()

  #############################################################################
  def getOptimizerJobInfo(self,job,reportName):
    """This method gets job optimizer information that will
       be used for
    """
    self.log.verbose("self.jobDB.getJobOptParameter(%s,'%s')" %(job,reportName))
    result = self.jobDB.getJobOptParameter(job,reportName)
    if result['OK']:
      value = result['Value']
      if not value:
        self.log.warn('JobDB returned null value for %s %s' %(job,reportName))
        return S_ERROR('No optimizer info returned')
      else:
        pyValue = eval(value)
        return S_OK(pyValue)

    return result

  #############################################################################
  def setOptimizerJobInfo(self,job,reportName,value):
    """This method sets the job optimizer information that will subsequently
       be used for job scheduling and TURL queries on the WN.
    """
    self.log.verbose("self.jobDB.setJobOptParameter(%s,'%s','%s')" %(job,reportName,value))
    if self.enable:
      result = self.jobDB.setJobOptParameter(job,reportName,str(value))
    else:
      result = S_OK('DisabledMode')

    return result

  #############################################################################
  def setOptimizerChain(self,job,value):
    """This method sets the job optimizer chain, in principle only needed by
       one of the optimizers.
    """
    self.log.verbose("self.jobDB.setOptimizerChain(%s,%s)" %(job,value))
    if self.enable:
      result = self.jobDB.setOptimizerChain(job,value)
    else:
      result = S_OK('DisabledMode')

    return result

  #############################################################################
  def setNextOptimizer(self,job):
    """This method is executed when the optimizer instance has successfully
       processed the job.  The next optimizer in the chain will subsequently
       start to work on the job.
    """
    self.log.verbose("self.jobDB.setNextOptimizer(%s,'%s')" %(job,self.optimizerName))
    if self.enable:
      result = self.jobDB.setNextOptimizer(job,self.optimizerName)
    else:
      result = S_OK('DisabledMode')

    if self.enable: # this will set the logging record to the current optimizer at present :(
      result = self.logDB.addLoggingRecord(job,status=self.jobStatus,minor=self.optimizerName,source=self.optimizerName+'Agent')
      if not result['OK']:
        self.log.warn(result['Message'])

    return result

  #############################################################################
  def updateJobStatus(self,job,status,minorstatus=None):
    """This method updates the job status in the JobDB, this should only be
       used to fail jobs due to the optimizer chain.
    """
    self.log.verbose("self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" %(job,status))
    if self.enable:
      result = self.jobDB.setJobAttribute(job,'Status',status, update=True)
    else:
      result = S_OK('DisabledMode')

    if result['OK']:
      if minorstatus:
        self.log.verbose("self.jobDB.setJobAttribute(%s,'MinorStatus','%s',update=True)" %(job,minorstatus))
        if self.enable:
          result = self.jobDB.setJobAttribute(job,'MinorStatus',minorstatus,update=True)
        else:
          result = S_OK('DisabledMode')

    if self.enable:
      logStatus=status
      result = self.logDB.addLoggingRecord(job,status=logStatus,minor=minorstatus,source=self.optimizerName+'Agent')
      if not result['OK']:
        self.log.warn(result['Message'])

    return result

  #############################################################################
  def setJobParam(self,job,reportName,value):
    """This method updates a job parameter in the JobDB.
    """
    self.log.verbose("self.jobDB.setJobParameter(%s,'%s','%s')" %(job,reportName,value))
    if self.enable:
      result = self.jobDB.setJobParameter(job,reportName,value)
    else:
      result = S_OK('DisabledMode')

    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job, should be overridden in a subclass
    """
    self.log.warn('Optimizer: checkJob method should be implemented in a subclass')
    return S_ERROR('Optimizer: checkJob method should be implemented in a subclass')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
