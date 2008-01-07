########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/AgentDirector.py,v 1.4 2008/01/07 15:47:26 paterson Exp $
# File :   AgentDirector.py
# Author : Stuart Paterson
########################################################################

"""  The Agent Director base class provides the infrastructure to submit pilots
     to various compute resources (Grids).  The specific submission commands
     are overridden in Grid specific subclasses.
"""

__RCSID__ = "$Id: AgentDirector.py,v 1.4 2008/01/07 15:47:26 paterson Exp $"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Utilities.Subprocess                       import shellCall
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB        import JobLoggingDB
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time, shutil
from threading import Thread, Lock

lock = Lock()

COMPONENT_NAME = 'WorkloadManagement/AgentDirector'

class AgentDirector(Thread):

  #############################################################################
  def __init__(self,jobDB,resourceBroker=None,enableFlag=True):
    """ Constructor for Agent Director
    """
    self.enable = enableFlag
    self.resourceBroker = resourceBroker
    self.jobDB = jobDB
    self.logDB = JobLoggingDB()
    self.name = '%sAgentDirector' %(self.type)
    #self.log = self.log.getSubLogger('AgentDirector')
    self.log = gLogger
    self.log.info('Creating %s' %(self.name))
    self.failed = False
    self.pollingTime = 120
    self.selectJobLimit = 1000
    self.workingDirectory = '/opt/dirac/work/%s' %(self.name)
    self.diracSetup = gConfig.getValue('DIRAC/Setup','LHCb-Development')
    Thread.__init__(self)

  #############################################################################
  def run(self):
    """ The run method of the Agent Director thread
    """
    if self.failed:
      return
    while True:
      self.log.info( 'Waking up Agent Director thread for %s' %(self.resourceBroker))
      try:
        result = self.__checkJobs()
        if not result['OK']:
          self.log.warn(result['Message'])
      except Exception,x:
        self.log.warn(str(x))

      time.sleep(self.pollingTime)

  #############################################################################
  def __checkJobs(self):
    """Retrieves waiting jobs and loops over the selection.
    """
    self.log.debug('Preparing list of waiting jobs')
    workload = self.__getWaitingJobs()
    if not workload['OK']:
      return workload
    jobs = workload['Value']

    if not jobs:
      return S_ERROR('No work to do')

    #self.log.debug('restricting jobs for debugging')
    #jobs = jobs[0:2]
    for job in jobs:
      lock.acquire()
      attributes = self.jobDB.getJobAttributes(job)
      if not attributes['OK']:
        self.log.warn(result['Message'])
        lock.release()
        continue

      currentStatus = attributes['Value']['Status']
      if not currentStatus == 'Waiting':
        self.log.warn('Job %s has changed status to %s and will be ignored by %s' %(job,currentStatus,self.name))
        lock.release()
        continue

      currentMinorStatus = attributes['Value']['MinorStatus']
      if not currentMinorStatus == 'Pilot Agent Submission':
#      if not currentMinorStatus == 'Pilot Agent Response':
        self.log.warn('Job %s has changed minor status to %s and will be ignored by %s' %(job,currentMinorStatus,self.name))
        lock.release()
        continue

      result = self.jobDB.getJobJDL(job)
      if not result['OK']:
        self.log.warn(result['Message'])
        self.__updateJobStatus(job,'Failed','No Job JDL Available',logRecord=True)
        lock.release()
        continue

      jdl = result['Value']
      classadJob = ClassAd(jdl)
      if not classadJob.isOK():
        self.log.warn('Illegal JDL for job %d ' % int(job))
        self.__updateJobStatus(job,'Failed','Job JDL Illegal',logRecord=True)
        lock.release()
        continue

      platform = string.replace(classadJob.get_expression("Platform"),'"','')
      if not platform:
        self.log.warn('Job %s has no platform defined' % job)
        self.__updateJobStatus(job,'Failed','No Platform Specified',logRecord=True)
        lock.release()
        continue

      if platform.lower() == 'dirac':
        self.__updateJobStatus(job,'Waiting','DIRAC Site Response',logRecord=True)
        lock.release()
        continue
      elif platform.lower() == self.type.lower():
        self.log.debug('Job %s is of type %s and will be considered by %s' %(job,platform,self.name))
      else:          
        self.log.verbose('Job %s is of type %s and will be ignored by %s' %(job,platform,self.name))
        lock.release()
        continue

      self.log.info( 'Changing Status to "Waiting Pilot Agent Response" for Job %s' % job )
      self.__updateJobStatus(job,'Waiting','Pilot Agent Response')
      lock.release()
      result = self.__preparePilot(job,classadJob,attributes['Value'])
      if not result['OK']:
        self.log.warn(result['Message'])

    return S_OK('Jobs considered')

  #############################################################################
  def __preparePilot(self,job,classadJob,attributes):
    """The pilot agent job can now be formulated for a job that is deemed
       eligible.
    """
    jdlCPU = 0
    if classadJob.lookupAttribute("MaxCPUTime"):
      jdlCPU = int(string.replace(classadJob.get_expression("MaxCPUTime"),'"','') )

    softwareTag = ''
    if classadJob.lookupAttribute("SoftwareTag"):
      jdlTag = string.replace(classadJob.get_expression("SoftwareTag"),'"','')
      softwareTag = string.replace(string.replace(string.replace(jdlTag,'{',''),'}',''),' ','').split(',')

    requirements = ''
    if (classadJob.lookupAttribute("Requirements")):
      requirements = classadJob.get_expression("Requirements")
    else:
      self.log.warn('Job %s has no JDL requirements and will not be processed' %(job))

    for param in ['Owner','OwnerDN','JobType']:
      if not attributes.has_key(param):
        self.__updateJobStatus(job,'Failed','%s Undefined' %(param))
        return S_ERROR('%s Undefined' %(param))

    owner = attributes['Owner']
    ownerDN = attributes['OwnerDN']
    jobType = attributes['JobType']

    self.log.debug('JobID: %s' %(job))
    self.log.debug('Owner: %s' %(owner))
    self.log.debug('OwnerDN: %s' %(ownerDN))
    self.log.debug('JobType: %s' %(jobType))
    self.log.debug('MaxCPUTime: %s' %(jdlCPU))
    self.log.debug('Requirements: %s' %(requirements))

    executable = None
    if classadJob.lookupAttribute("GridExecutable"):
      executable = string.replace(classadJob.get_expression("GridExecutable"),'"','')
      self.log.verbose('Will attempt to use pilot script %s for job %s' %(executable,job))

    gridRequirements = None
    if classadJob.lookupAttribute("GridRequirements"):
      gridRequirements = string.replace(classadJob.get_expression("GridRequirements"),'"','')
      self.log.verbose('Will attempt to use pilot script %s for job %s' %(gridRequirements,job))

    candidateSites = None
    if classadJob.lookupAttribute("Site"):
      candidateSites = string.replace(classadJob.get_expression("Site"),'"','')
      self.log.verbose('Candidate sites for job %s are %s' %(job,candidateSites))

    bannedSites = None
    if classadJob.lookupAttribute("BannedSites"):
      candidateSites = string.replace(classadJob.get_expression("BannedSites"),'"','')
      self.log.verbose('Banned sites for job %s are %s' %(job,candidateSites))

    sites = self.__resolveSiteCandidates(job,candidateSites,bannedSites,gridRequirements)
    if not sites['OK']:
      return sites

    siteList = sites['Value']
    self.log.verbose('Candidate %s Sites for job %s: %s' %(self.type,job,string.join(siteList,', ')))
    workingDirectory = '%s/%s' %(self.workingDirectory,job)

    if os.path.exists(workingDirectory):
      shutil.rmtree(workingDirectory)

    if not os.path.exists(workingDirectory):
      os.makedirs(workingDirectory)

    #determine whether a generic or specific pilot should be submitted
    #for initial instance this is ignored and pilots will pick up specific jobs
    #if jobType.lower() == 'sam':
    inputSandbox = []
    ownerFile = self.__addOwner(workingDirectory,owner)
    if not ownerFile['OK']:
      return ownerFile
    inputSandbox.append(ownerFile['Value'])
    jobIDFile = self.__addJobID(workingDirectory,job)
    if not jobIDFile['OK']:
      return jobIDFile
#    inputSandbox.append(jobIDFile['Value'])
    self.log.verbose('Submitting %s Pilot Agent for job %s' %(self.type,job))
    result = self.submitJob(job,self.workingDirectory,siteList,jdlCPU,inputSandbox,gridRequirements,executable,softwareTag)
    self.__cleanUp(workingDirectory)

    if not result['OK']:
      self.__updateJobStatus(job,'Waiting','Pilot Agent Submission')
      return result

    submittedPilot = result['Value']
    report = self.__reportSubmittedPilot(job,submittedPilot,ownerDN)
    if not report['OK']:
      self.log.warn(report['Message'])

    return S_OK('Pilot Submitted')

  #############################################################################
  def __reportSubmittedPilot(self,job,submittedPilot,ownerDN):
    """The pilot reference is added to the JobDB and appended to the
        SubmittedAgents job parameter.
    """
    #can add pilot reference to DB here when available
    existingParam = self.jobDB.getJobParameters(int(job),['SubmittedAgents'])
    if not existingParam['OK']:
      return existingParam

    if not existingParam['Value']:
      self.log.debug('Adding first submitted pilot parameter for job %s' %job)
      if self.enable:
        self.__setJobParam(job,'SubmittedAgents',submittedPilot)
    else:
      pilots = len(existingParam['Value'])
      self.log.debug('Adding submitted pilot number %s for job %s' %(pilots,job))
      pilots += ',%s' %(submittedPilot)
      if self.enable:
        self.__setJobParam(job,'SubmittedAgents',submittedPilot)

    return S_OK()

  #############################################################################
  def __addOwner(self,workingDirectory,owner):
    """Adds owner ID to pilot requirements
    """
    self.log.verbose( 'Adding Owner.cfg to be used in agent requirements' )
    path = '%s/Owner.cfg' % workingDirectory
    try:
      if os.path.exists( path ):
        os.remove( path )
      ownerCFG = open( path ,'w')
      ownerCFG.write( 'AgentJobRequirements\n{\nOwner=%s\n}\n' %(owner))
      ownerCFG.close()
    except Exception, x:
      self.log.warn( str(x) )
      return S_ERROR('Cannot create Owner.cfg')
    return S_OK(path)

  #############################################################################
  def __addJobID(self,workingDirectory,jobID):
    """Adds jobID to pilot requirements
    """
    self.log.verbose( 'Adding JobID.cfg to be used in agent requirements' )
    path = '%s/JobID.cfg' % workingDirectory
    try:
      if os.path.exists( path ):
        os.remove( path )
      jobCFG = open( path ,'w')
      jobCFG.write( 'AgentJobRequirements\n{\nJobID=%s\n}\n' %(jobID))
      jobCFG.close()
    except Exception, x:
      self.log.warn( str(x) )
      return S_ERROR('Cannot create JobID.cfg')
    return S_OK(path)

  #############################################################################
  def __resolveSiteCandidates(self,job,candidateSites,bannedSites,gridRequirements):
    """This method takes all client-provided inputs and resolves them into a
       list of candidate sites by checking against the allowed site mask
       for the Grid being used.
    """
    #assume user knows what they're doing and avoid site mask e.g. sam jobs
    if gridRequirements:
      return S_OK(gridRequirements)

    mask = self.jobDB.getMask()
    if not mask['OK']:
      return mask

    siteMask = mask['Value']
    if not siteMask:
      return S_ERROR('Returned site mask is empty for %s' %(self.type))

    self.log.debug('Site Mask: %s' %(string.join(siteMask,', ')))
    candidates = siteMask

    if bannedSites:
      for site in bannedSites:
        if site in candidates:
          candidates.remove(site)
          self.log.verbose('Removing banned site %s from site candidate list for job %s' %(site,job))

    if candidateSites:
      for site in candidates:
        if not site in candidateSites:
          candidates.remove(site)
        else:
          self.log.verbose('Site %s is a candidate site in the mask for job %s' %(site,job))

    if not candidates:
      self.__updateJobStatus(job,'Failed','No Candidate Sites in Mask')
      return S_ERROR('No Candidate Sites in Mask')

    self.log.verbose('Candidate Sites: %s' %(string.join(siteMask,', ')))
    finalSites = self.__getGridSites(candidates)
    return finalSites

  #############################################################################
  def __getGridSites(self,candidates):
    """Converts candidates sites from canonical DIRAC site names into those
       of the Grid.
    """
    section = '/Resources/GridSites/%s' % (self.type)
    sites = gConfig.getOptionsDict(section)
    if not sites['OK']:
      self.log.warn(sites['Message'])
      return S_ERROR('Could not obtain %s section from CS' %(section))

    if not sites['Value']:
      return S_ERROR('Empty CS section %s' %(section))

    gridSites = sites['Value']
    self.log.debug('%s Grid Sites are: %s' %(self.type,string.join(gridSites,', ')))

    candidateList = []

    for ce,siteName in gridSites.items():
      if siteName in candidates:
        candidateList.append(ce)

    if not candidateList:
      return S_ERROR('No Grid site names found for DIRAC sites %s' %(string.join(candidates,', ')))

    return S_OK(candidateList)

  #############################################################################
  def __cleanUp(self,jobDirectory):
    """  Clean up all the remnant files of the job submission
    """
    if os.path.exists(jobDirectory):
      if self.enable:
        shutil.rmtree(jobDirectory, True )
        self.log.debug('Cleaning up working directory: %s' %(jobDirectory))

  #############################################################################
  def __getWaitingJobs(self):
    """Returns the list of waiting jobs for which pilots should be submitted
    """
    selection = {'Status':'Waiting','MinorStatus':'Pilot Agent Submission'}
    #selection = {'Status':'Waiting','MinorStatus':'Pilot Agent Response'}
    result = self.jobDB.selectJobs(selection, limit=self.selectJobLimit)
    if not result['OK']:
      return result

    jobs = result['Value']
    if jobs > 0:
      if len(jobs)>15:
        self.log.info( 'Selected jobs %s...' % string.join(jobs[0:14],', ') )
      else:
        self.log.info('Selected jobs %s' % string.join(jobs,', '))

    return S_OK(jobs)

  #############################################################################
  def __updateJobStatus(self,job,status,minorstatus=None,logRecord=False):
    """This method updates the job status in the JobDB, this should only be
       used to fail jobs due to the optimizer chain.
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
  def submitJob(self,job):
    """This method should be overridden in a subclass
    """
    self.log.error('AgentDirector: submitJob method should be implemented in a subclass')
    return S_ERROR('Optimizer: submitJob method should be implemented in a subclass')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
