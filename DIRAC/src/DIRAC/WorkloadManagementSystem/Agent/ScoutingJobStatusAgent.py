#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

""" Agent for scout job framework to monitor scout status
    and update main job status according to scout status.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB


class ScoutingJobStatusAgent(AgentModule):
  """
      The specific agents must provide the following methods:
        - initialize() for initial settings
        - beginExecution()
        - execute() - the main method called in the agent cycle
        - endExecution()
        - finalize() - the graceful exit of the method, this one is usually used
                   for the agent restart
  """

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    AgentModule.__init__(self, *args, **kwargs)

    self.jobDB = None
    self.logDB = None

  #############################################################################
  def initialize(self):
    """Sets defaults
    """

    self.am_setOption('PollingTime', 120)
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()

    return S_OK()

  #############################################################################
  def beginExecution(self):

    self.totalScoutJobs = Operations().getValue('WorkloadManagement/Scouting/totalScoutJobs', 10)
    self.criteriaFailedRate = Operations().getValue('WorkloadManagement/Scouting/criteriaFailedRate', 0.5)
    self.criteriaSucceededRate = Operations().getValue('WorkloadManagement/Scouting/criteriaSucceededRate', 0.3)
    self.criteriaStalledRate = Operations().getValue('WorkloadManagement/Scouting/criteriaStalledRate', 1.0)
    self.criteriaFailed = Operations().getValue('WorkloadManagement/Scouting/criteriaFailed', int(self.totalScoutJobs * self.criteriaFailedRate))
    self.criteriaSucceeded = Operations().getValue('WorkloadManagement/Scouting/criteriaSucceeded', int(self.totalScoutJobs * self.criteriaSucceededRate))
    self.criteriaStalled = Operations().getValue('WorkloadManagement/Scouting/criteriaStalled', int(self.totalScoutJobs * self.criteriaStalledRate))

    if int(self.totalScoutJobs * self.criteriaFailedRate) > self.criteriaFailed:
      self.criteriaFailedRate = int(self.criteriaFailed / self.totalScoutJobs)
    if int(self.totalScoutJobs * self.criteriaSucceededRate) > self.criteriaSucceeded:
      self.criteriaSucceededRate = int(self.criteriaSucceeded / self.totalScoutJobs)
    if int(self.totalScoutJobs * self.criteriaStalledRate) > self.criteriaStalled:
      self.criteriaStalledRate = int(self.criteriaStalled / self.totalScoutJobs)

    self.log.info('Scouting parameters: Total: %s, Succeeded: %s(%s), Failed: %s(%s), Stalled: %s(%s), '
                  % (self.totalScoutJobs, self.criteriaSucceeded, self.criteriaSucceededRate, self.criteriaFailed, self.criteriaFailedRate, self.criteriaStalled, self.criteriaStalledRate))

    return S_OK()

  def execute(self):
    """The PilotAgent execution method.
    """
    result = self.jobDB.selectJobs({'Status': 'Scouting'})
    if not result['OK']:
      return S_ERROR()

    joblist = result['Value']
    if not joblist:
      self.log.info('No Jobs with scouting status. Skipping this cycle')
      return S_OK()

    self.log.info('Check %s Scouting jobs' % len(joblist))
    self.log.debug('joblist: ' % joblist)

    scoutIDdict = {}
    for jobID in joblist:
      result = self.jobDB.getJobParameters(int(jobID), ['ScoutID'])  # <lowest jobID>:<Highest jobID>
      if not result['OK']:
        self.log.warn(result['Message'])
        continue
      if not result['Value'].get(int(jobID)):
        continue

      scoutID = result['Value'][int(jobID)]['ScoutID']
      scoutStatus = scoutIDdict.get(scoutID)
      if not scoutStatus:
        result = self.__getScoutStatus(scoutID)
        if not result['OK']:
          self.log.warn(result['Message'])
          continue
        scoutStatus = result['Value']

      scoutIDdict[scoutID] = scoutStatus
      if scoutStatus['Status'] == 'NotComplete':
        self.log.verbose("%s: skipping since corresponding scout does not complete yet." % jobID)
        continue
      else:
        result = self.__updateJobStatus(jobID, status=scoutStatus['Status'],
                                        minorstatus=scoutStatus['MinorStatus'],
                                        appstatus=scoutStatus['appstatus'])
        if not result['OK']:
          self.log.warn(result['Message'])

    self.log.info('final scoutIDdict:%s' % scoutIDdict)
    return S_OK()

  def __getScoutStatus(self, scoutid):
    ids = scoutid.split(':')
    scoutjoblist = list(range(int(ids[0]), int(ids[1]) + 1))

    result = self.jobDB.getJobsAttributes(scoutjoblist, ['Status', 'Site'])
    if not result['OK']:
      return S_ERROR(result['Message'])

    donejoblist = []
    donesitelist = []
    failedjoblist = []
    failedsitelist = []
    stalledjoblist = []
    scoutjobs = result['Value'].keys()
    for scoutjob in scoutjobs:
      status = result['Value'][scoutjob]['Status']
      site = result['Value'][scoutjob]['Site']
      jobid = scoutjob
      if status == 'Done':
        donejoblist.append(jobid)
        donesitelist.append(site)
      elif status == 'Failed':
        failedjoblist.append(jobid)
        failedsitelist.append(site)
      elif status == 'Stalled':
        stalledjoblist.append(jobid)

    if self.criteriaSucceeded > len(scoutjobs):
      criteriaSucceeded = max(int(len(scoutjobs) * self.criteriaSucceededRate), 1)
      self.log.verbose('criteriaSucceeded = %s' % self.criteriaSucceeded)
    else:
      criteriaSucceeded = self.criteriaSucceeded
      self.log.debug('criteriaSucceeded = %s' % self.criteriaSucceeded)

    if self.criteriaFailed > len(scoutjobs):
      criteriaFailed = max(int(len(scoutjobs) * self.criteriaFailedRate), 1)
      self.log.verbose('criteriaFailed = %s' % self.criteriaFailed)
    else:
      criteriaFailed = self.criteriaFailed
      self.log.debug('criteriaFailed = %s' % self.criteriaFailed)

    if self.criteriaStalled > len(scoutjobs):
      criteriaStalled = max(int(len(scoutjobs) * self.criteriaStalledRate), 1)
      self.log.verbose('criteriaStalled = %s' % self.criteriaStalled)
    else:
      criteriaStalled = self.criteriaStalled
      self.log.debug('criteriaStalled = %s' % self.criteriaStalled)

    if len(donejoblist) >= criteriaSucceeded:
      self.log.verbose('Scout (ID = %s) are done.' % scoutid)
      scoutStatus = {'Status': 'Checking', 'MinorStatus': 'Scouting', 'appstatus': 'Scout Complete'}

    elif len(failedjoblist) >= criteriaFailed:
      self.log.verbose('Scout (ID = %s) are failed.' % scoutid)
      msg = 'Failed scout job ' + str(failedjoblist)
      scoutStatus = {'Status': 'Failed', 'MinorStatus': 'Failed in scouting', 'appstatus': msg}

    elif len(stalledjoblist) >= criteriaStalled:
      self.log.verbose('Scout (ID = %s) are stalled.' % scoutid)
      msg = 'Stalled scout job ' + str(stalledjoblist)
      scoutStatus = {'Status': 'Stalled', 'MinorStatus': 'Stalled in scouting', 'appstatus': msg}

    else:
      self.log.verbose('Scout (ID = %s) did not completed.' % scoutid)
      scoutStatus = {'Status': 'NotComplete'}

    return S_OK(scoutStatus)

  def __updateJobStatus(self, job, status=None, minorstatus=None, appstatus=None):
    """ This method updates the job status in the JobDB.
    """
    self.log.info('Job %s set Status="%s", MinorStatus="%s", ApplicationStatus="%s".'
                  % (job, status, minorstatus, appstatus))
    if not self.am_getOption('Enable', True):
      result = S_OK('DisabledMode')

    # Update ApplicationStatus
    if not appstatus:
      result = self.jobDB.getJobAttributes(job, ['ApplicationStatus'])
      if result['OK']:
        minorstatus = result['Value']['ApplicationStatus']

    self.log.verbose("self.jobDB.setJobAttribute(%s,'ApplicationStatus','%s',update=True)"
                     % (job, appstatus))
    result = self.jobDB.setJobAttribute(job, 'ApplicationStatus', appstatus, update=True)
    if not result['OK']:
      return S_ERROR(result['Message'])

    # Update MinorStatus
    if not minorstatus:
      result = self.jobDB.getJobAttributes(job, ['MinorStatus'])
      if result['OK']:
        minorstatus = result['Value']['MinorStatus']

    self.log.verbose("self.jobDB.setJobAttribute(%s,'MinorStatus','%s',update=True)" % (job, minorstatus))
    result = self.jobDB.setJobAttribute(job, 'MinorStatus', minorstatus, update=True)
    if not result['OK']:
      return S_ERROR(result['Message'])

    # Update ScoutFlag
    result = self.jobDB.setJobParameter(int(job), 'ScoutFlag', 1)
    if not result['OK']:
      return S_ERROR(result['Message'])

    # Update Status
    if not status:  # Retain last minor status for stalled jobs
      result = self.jobDB.getJobAttributes(job, ['Status'])
      if result['OK']:
        status = result['Value']['Status']

    self.log.verbose("self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" % (job, status))
    result = self.jobDB.setJobAttribute(job, 'Status', status, update=True)
    if not result['OK']:
      return S_ERROR(result['Message'])

    logStatus = status
    result = self.logDB.addLoggingRecord(job, status=logStatus, minor=minorstatus,
                                         source='ScoutingJobStatusAgent')
    if not result['OK']:
      self.log.warn(result)

    return result
