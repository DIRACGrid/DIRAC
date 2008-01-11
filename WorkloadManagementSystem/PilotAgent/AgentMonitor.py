########################################################################
# $Id: AgentMonitor.py,v 1.1 2008/01/11 18:17:13 paterson Exp $
# File :   AgentMonitor.py
# Author : Stuart Paterson
########################################################################

""" The Agent Monitor base class provides the infrastructure to manage submitted
    Pilot Agents to the Grid and trigger further submissions as required.  Grid
    specific monitoring commands are overridden in subclasses.
"""

__RCSID__ = "$Id: AgentMonitor.py,v 1.1 2008/01/11 18:17:13 paterson Exp $"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Utilities.Subprocess                       import shellCall
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,destroyProxy
from DIRAC.Core.Utilities.ThreadPool                       import *
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB        import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB   import ProxyRepositoryDB
#sfrom DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB       import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.JobDB               import JobDB
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time, shutil

class AgentMonitor:

  #############################################################################
  def __init__(self,type='LCG',enableFlag=True):
    """ Standard constructor
    """
    self.log = gLogger
    self.enable = enableFlag
    self.type = type
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    self.proxyDB = ProxyRepositoryDB()
    self.log.setLevel('debug')
#    self.pilotAgentsDB = PilotAgentsDB()
    self.selectJobLimit = 100
    self.maxWaitingTime = 5*60
    self.maxPilotAgents = 4
    self.pollingTime = 180
    self.minThreads = 1
    self.maxThreads = 1
    self.name = '%sAgentMonitor' %(self.type)
    self.workingDirectory = '/opt/dirac/work/%s' %(self.name)
    self.threadPool = ThreadPool( self.minThreads, self.maxThreads )

  #############################################################################
  def run(self):
    """ The run method of the Agent Monitor
    """
    while True:
      self.log.info('%sAgentMonitor: %s working threads, %s pending jobs' %(self.type,self.threadPool.numWorkingThreads(),self.threadPool.pendingJobs()))
      result = self.__getWaitingJobs()
      if not result['OK']:
        self.log.warn(result['Message'])
        return result

      jobsList = result['Value']
      if jobsList:
        for job in jobsList:
          self.log.verbose('Monitoring starts for job %s' %(job))
          self.__checkPilot(job)

      self.log.info('%sAgentMonitor: %s working threads, %s pending jobs' %(self.type,self.threadPool.numWorkingThreads(),self.threadPool.pendingJobs()))
      self.threadPool.processResults()
      time.sleep(self.pollingTime)

  #############################################################################
  def __checkPilot(self,jobID):
    """ This method creates and queues a ThreadedJob to be executed by the TheadPool.
    """
    threadedMonitor = ThreadedJob(self.monitorPilot,args=(jobID,),oCallback=self.reportResult,oExceptionCallback=self.reportException)
    self.threadPool.queueJob(threadedMonitor)

  #############################################################################
  def monitorPilot(self,jobID):
    """Method to be called by the ThreadPool
    """
    atts = self.jobDB.getJobAttributes(jobID)
    if not atts['OK']:
      self.log.warn(result['Message'])
      return atts

    attributes = atts['Value']
    for param in ['OwnerDN','OwnerGroup','Status','MinorStatus']:
      if not attributes.has_key(param):
        self.__updateJobStatus(jobID,'Failed','%s Undefined' %(param))
        result = S_ERROR('%s undefined for job %s' %(param,jobID))
        result['Attributes']=attributes
        return result

    ownerDN = attributes['OwnerDN']
    jobGroup = attributes['OwnerGroup']

    workingDir = '%s/%s' %(self.workingDirectory,jobID)

    if os.path.exists(workingDir):
      shutil.rmtree(workingDir)

    if not os.path.exists(workingDir):
      os.makedirs(workingDir)

    currentStatus = attributes['Status']
    if not currentStatus == 'Waiting':
      msg = 'Job %s has changed status to %s and will be ignored by %s' %(jobID,currentStatus,self.name)
      return S_ERROR(msg)

    currentMinorStatus = attributes['MinorStatus']
    if not currentMinorStatus == 'Pilot Agent Response':
      msg = 'Job %s has changed minor status to %s and will be ignored by %s' %(jobID,currentMinorStatus,self.name)
      return S_ERROR(msg)

    result = self.__setupProxy(jobID,ownerDN,jobGroup,workingDir)
    if not result['OK']:
      return result

    #extract pilot ID if any
    result =  self.jobDB.getJobParameters(jobID,['SubmittedAgents','AbortedAgents'])
    if not result['OK']:
      return result

    submittedAgents = []
    abortedAgents = []
    agents = result['Value']
    if agents.has_key('SubmittedAgents'):
      pilotIDs = agents['SubmittedAgents']['Value'].split(',')
      submittedAgents = pilotIDs

    if agents.has_key('AbortedAgents'):
      pilotIDs = agents['AbortedAgents']['Value'].split(',')
      abortedAgents = pilotIDs

    if not submittedAgents:
      #self.__updateJobStatus('Waiting','Pilot Agent Submission')
      return S_OK('No submitted pilots found for job %s updated to Pilot Agent Submission' % jobID)

    pilotSummary = ''
    for pilotID in submittedAgents:
      pilotStatus = self.getPilotStatus(jobID,pilotID)
      if not pilotStatus['OK']:
        return pilotStatus
      if pilotStatus['Aborted']:
        #have the information to report to pilotDB here
        abortedAgents.append(pilotID)
        pilotSummary += '%s Aborted\n' %(pilotID)
      else:
        #report to pilotDB here
        pilotSummary += '%s %s\n' %(pilotID,pilotStatus['PilotStatus'])

    if submittedAgents:
      self.__setJobParam(jobID,'SubmittedAgents',submittedAgents.join(','))
    if abortedAgents:
      self.__setJobParam(jobID,'AbortedAgents',abortedAgents.join(','))

    if len(submittedAgents) < self.maxPilotAgents:
      self.__updateJobStatus('Waiting','Pilot Agent Submission')
      pilotSummary += 'Job %s is updated to Pilot Agent Submission\n' % jobID
    else:
      self.__updateJobStatus('Waiting','Pilot Agent Response')
      pilotSummary += 'Max pilots submitted for job %s' % jobID

    self.__cleanUp(workingDir)
    return S_OK(pilotSummary)

  #############################################################################
  def reportResult(self,threadedJob,result):
    """CallBack function for reporting the thread result.
    """
    #print threadedJob
    #print result
    if result['OK']:
      self.log.info(result['Value'])
    else:
      self.log.warn(result['Message'])
      self.log.debug(result)

  #############################################################################
  def reportException(self,threadedJob,result):
    """CallBack function for reporting a thread exception.
    """
    self.log.warn(result[1])

  #############################################################################
  def __cleanUp(self,jobDirectory):
    """  Clean up all the remnant files after checking the pilot status.
    """
    destroyProxy()
    if os.path.exists(jobDirectory):
      if self.enable:
        shutil.rmtree(jobDirectory, True )
        self.log.debug('Cleaning up working directory: %s' %(jobDirectory))

  #############################################################################
  def __setupProxy(self,job,ownerDN,jobGroup,workingDir):
    """Retrieves user proxy with correct role for job and sets up environment for
       checking pilot agent status.
    """
    result = self.proxyDB.getProxy(ownerDN,jobGroup)
    if not result['OK']:
      self.log.warn('Could not retrieve proxy from ProxyRepositoryDB')
      self.log.debug(result)
      self.__updateJobStatus(job,'Failed','Valid Proxy Not Found')
      return S_ERROR('Error retrieving proxy')

    proxyStr = result['Value']
    proxyFile = '%s/proxy%s' %(workingDir,job)
    setupResult = setupProxy(proxyStr,proxyFile)
    if not setupResult['OK']:
      self.log.warn('Could not create environment for proxy')
      self.log.debug(setupResult)
      self.__updateJobStatus(job,'Failed','Proxy WMS Error')
      return S_ERROR('Error setting up proxy')

    self.log.debug(setupResult)
    return setupResult

  #############################################################################
  def __getWaitingJobs(self):
    """Returns the list of waiting jobs for which pilots should be tracked
    """
    selection = {'Status':'Waiting','MinorStatus':'Pilot Agent Response'}
    delay  = time.localtime( time.time() - self.maxWaitingTime )
    delay = time.strftime( "%Y-%m-%d %H:%M:%S", delay )
    result = self.jobDB.selectJobs(selection, older=delay, limit=self.selectJobLimit)
    if not result['OK']:
      return result

    jobs = result['Value']
    if not jobs:
      self.log.info('No eligible jobs selected from DB')
    else:
      if len(jobs)>15:
        self.log.info( 'Selected jobs %s...' % string.join(jobs[0:14],', ') )
      else:
        self.log.info('Selected jobs %s' % string.join(jobs,', '))

    return S_OK(jobs)

  #############################################################################
  def __updateJobStatus(self,job,status,minorstatus=None,logRecord=False):
    """This method updates the job status in the JobDB when enable flag is true.
    """
    self.log.debug("self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" %(job,status))
    if self.enable:
      result = self.jobDB.setJobAttribute(job,'Status',status, update=True)
    else:
      result = S_OK('DisabledMode')

    if result['OK']:
      if minorstatus:
        self.log.debug("self.jobDB.setJobAttribute(%s,'MinorStatus','%s',update=True)" %(job,minorstatus))
        if self.enable:
          result = self.jobDB.setJobAttribute(job,'MinorStatus',minorstatus,update=True)
          if not result['OK']:
            self.log.warn(result['Message'])
        else:
          result = S_OK('DisabledMode')

    if self.enable:
      if logRecord:
        logStatus=status
        result = self.logDB.addLoggingRecord(job,status=logStatus,minor=minorstatus,source=self.name)
        if not result['OK']:
          self.log.warn(result['Message'])

    return result

  #############################################################################
  def __setJobParam(self,job,reportName,value):
    """This method updates a job parameter in the JobDB.
    """
    self.log.debug("self.jobDB.setJobParameter(%s,'%s','%s')" %(job,reportName,value))
    if self.enable:
      result = self.jobDB.setJobParameter(job,reportName,value)
      if not result['OK']:
        self.log.warn(result['Message'])
    else:
      result = S_OK('DisabledMode')

    return result

  #############################################################################
  def getPilotStatus(self,jobID,pilotID):
    """This method should be overridden in a subclass
    """
    self.log.error('AgentMonitor: getJobStatus method should be implemented in a subclass')
    return S_ERROR('AgentMonitor: getJobStatus method should be implemented in a subclass')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

