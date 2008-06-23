########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/PilotDirector.py,v 1.16 2008/06/23 21:39:30 atsareg Exp $
# File :   PilotDirector.py
# Author : Stuart Paterson
########################################################################

"""  The Pilot Director base class provides the infrastructure to submit pilots
     to various compute resources (Grids).  The specific submission commands
     are overridden in Grid specific subclasses.
"""

__RCSID__ = "$Id: PilotDirector.py,v 1.16 2008/06/23 21:39:30 atsareg Exp $"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Utilities.Subprocess                       import shellCall
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,destroyProxy
from DIRAC.WorkloadManagementSystem.DB.JobDB               import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB        import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB   import ProxyRepositoryDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB       import PilotAgentsDB
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time, shutil
from threading import Thread, Lock

lock = Lock()

class PilotDirector(Thread):

  #############################################################################
  def __init__(self,configPath,resourceBroker,enableFlag=True):
    """ Constructor for Pilot Director
    """
    self.log = gLogger.getSubLogger('PilotDirector')
    self.enable = enableFlag
    self.resourceBroker = resourceBroker
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    self.proxyDB = ProxyRepositoryDB()
    self.pilotDB = PilotAgentsDB()
    self.name = '%sPilotDirector' %(self.type)
    self.configSection = configPath
    self.pollingTime = gConfig.getValue(self.configSection+'/PollingTime',120)
    self.selectJobLimit = gConfig.getValue(self.configSection+'/JobSelectLimit',100)
    self.scratchDir = gConfig.getValue(self.configSection+'/ScratchDir','/opt/dirac/work')
    self.genericPilotDN = gConfig.getValue(self.configSection+'/GenericPilotDN','/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=paterson/CN=607602/CN=Stuart Paterson')
    self.genericPilotGroup = gConfig.getValue(self.configSection+'/GenericPilotGroup','lhcb_pilot')
    self.defaultPilotType = gConfig.getValue(self.configSection+'/DefaultPilotType','generic')
    self.log.info('Generic Pilot DN is: %s Group: %s' %(self.genericPilotDN,self.genericPilotGroup))
    self.workingDirectory = '%s/%s' %(self.scratchDir,self.name)
    self.diracSetup = gConfig.getValue('/DIRAC/Setup','LHCb-Development')
    Thread.__init__(self)

  #############################################################################
  def run(self):
    """ The run method of the Pilot Director thread
    """
    while True:
      self.log.info( 'Waking up Pilot Director thread for %s' %(self.resourceBroker))
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
    self.log.verbose('Preparing list of waiting jobs')
    workload = self.__getWaitingJobs()
    if not workload['OK']:
      return workload

    jobs = workload['Value']
    if not jobs:
      return S_ERROR('No work to do')

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
        #self.__updateJobStatus(job,'Failed','No Platform Specified',logRecord=True)
        platform = 'ANY'
        lock.release()
        continue

      if platform.lower() == 'dirac':
        self.__updateJobStatus(job,'Waiting','DIRAC Site Response',logRecord=True)
        lock.release()
        continue
      elif platform.lower() == self.type.lower():
        self.log.verbose('Job %s is of type %s and will be considered by %s' %(job,platform,self.name))
      elif platform.lower() == 'any':
        self.log.verbose('Job %s is of type %s and will be considered by %s' %(job,platform,self.name))
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
      self.__updateJobStatus(job,'Failed','Undefined JDL Requirements')
      return S_ERROR('Undefined JDL Requirements')

    pilotType = ''
    if classadJob.lookupAttribute("PilotType"):
      pilotType = str(string.replace(classadJob.get_expression("PilotType"),'"','') )

    if not pilotType:
      self.log.warn('PilotType is not defined for job %s' %(job))
      pilotType = self.defaultPilotType

    if pilotType.lower() == 'generic' or pilotType.lower() == 'private':
      self.log.warn('Job %s has %s PilotType specified' %(job,pilotType))
    else:
      self.__updateJobStatus(job,'Failed','PilotType not defined')
      return S_ERROR('Undefined PilotType %s for job %s' %(pilotType,job))

    for param in ['Owner','OwnerDN','JobType','OwnerGroup']:
      if not attributes.has_key(param):
        self.__updateJobStatus(job,'Failed','%s Undefined' %(param))
        return S_ERROR('%s Undefined' %(param))

    owner = attributes['Owner']
    ownerDN = attributes['OwnerDN']
    jobType = attributes['JobType']
    ownerGroup = attributes['OwnerGroup']

    self.log.verbose('JobID: %s' %(job))
    self.log.verbose('Owner: %s' %(owner))
    self.log.verbose('OwnerDN: %s' %(ownerDN))
    self.log.verbose('JobType: %s' %(jobType))
    self.log.verbose('MaxCPUTime: %s' %(jdlCPU))
    self.log.verbose('Requirements: %s' %(requirements))

    executable = None
    if classadJob.lookupAttribute("GridExecutable"):
      executable = string.replace(classadJob.get_expression("GridExecutable"),'"','')
      self.log.verbose('Will attempt to use pilot script %s for job %s' %(executable,job))

    gridRequirements = None
    if classadJob.lookupAttribute("GridRequirements"):
      gridRequirements = string.replace(classadJob.get_expression("GridRequirements"),'"','')
      self.log.verbose('Will attempt to use pilot script %s for job %s' %(gridRequirements,job))

    candidateSites = None  #although only one site candidate is normally specified, can imagine having >1 ;)
    if classadJob.lookupAttribute("Site"):
      candidateSites = classadJob.get_expression("Site").replace('{','').replace('}','').replace('"','').split(',')
      self.log.verbose('Candidate sites for job %s is %s' %(job,string.join(candidateSites,',')))

    bannedSites = None
    if classadJob.lookupAttribute("BannedSites"):
      bannedSites = classadJob.get_expression("BannedSites").replace('{','').replace('}','').replace('"','').split()
      self.log.verbose('Banned sites for job %s are %s' %(job,string.join(bannedSites,',')))

    sites = self.__resolveSiteCandidates(job,candidateSites,bannedSites,gridRequirements)
    if not sites['OK']:
      return sites

    siteList = sites['Value']
    self.log.info('Resolved candidate %s Sites for job %s: %s' %(self.type,job,string.join(siteList,', ')))
    workingDirectory = '%s/%s' %(self.workingDirectory,job)

    if os.path.exists(workingDirectory):
      shutil.rmtree(workingDirectory)

    if not os.path.exists(workingDirectory):
      os.makedirs(workingDirectory)

    inputSandbox = []
    if pilotType.lower()=='private':
      self.log.verbose('Found private PilotType requirement, adding Owner Requirement for Pilot Agent')
      ownerFile = self.__addPilotCFGParameter(workingDirectory,'Owner',owner)
      if not ownerFile['OK']:
        return ownerFile
      inputSandbox.append(ownerFile['Value'])
    else:
      self.log.verbose('Job %s will be submitted with a generic pilot' %(job))
      ownerGroup=self.genericPilotGroup
      ownerDN = self.genericPilotDN

    self.log.verbose('Setting up proxy for job %s group %s and owner %s' %(job,ownerGroup,ownerDN))
    proxyResult = self.__setupProxy(job,ownerDN,ownerGroup,workingDirectory)
    self.log.verbose('Submitting %s Pilot Agent for job %s' %(self.type,job))

    result = self.submitJob(job,self.workingDirectory,
                            siteList,jdlCPU,ownerGroup,inputSandbox,
                            gridRequirements,executable,softwareTag)
    if self.enable:
      self.__cleanUp(workingDirectory)
    if not result['OK']:
      self.log.warn('Pilot submission failed for job %s with message:' %(job))
      self.log.warn(result['Message'])
      return result

    submittedPilot = result['Value']['PilotReference']
    pilotRequirements = result['Value']['PilotRequirements']
    report = self.__reportSubmittedPilot(job,submittedPilot,ownerDN,ownerGroup,pilotRequirements)
    if not report['OK']:
      self.log.warn(report['Message'])

    return S_OK('Pilot Submitted')

  #############################################################################
  def __setupProxy(self,job,ownerDN,ownerGroup,workingDir):
    """Retrieves user proxy with correct role for job and sets up environment for
       pilot agent submission.
    """
    result = self.proxyDB.getProxy(ownerDN,ownerGroup)
    if not result['OK']:
      self.log.warn('Could not retrieve proxy from ProxyRepositoryDB')
      self.log.verbose(result)
      self.__updateJobStatus(job,'Failed','Valid Proxy Not Found')
      return S_ERROR('Error retrieving proxy')

    proxyStr = result['Value']
    proxyFile = '%s/proxy%s' %(workingDir,job)
    setupResult = setupProxy(proxyStr,proxyFile)
    if not setupResult['OK']:
      self.log.warn('Could not create environment for proxy')
      self.log.verbose(setupResult)
      self.__updateJobStatus(job,'Failed','Proxy WMS Error')
      return S_ERROR('Error setting up proxy')

    self.log.verbose(setupResult)
    return setupResult

  #############################################################################
  def __reportSubmittedPilot(self,job,submittedPilot,ownerDN,ownerGroup,
                             jdl_requirements):
    """The pilot reference is added to the JobDB and appended to the
        SubmittedAgents job parameter.
    """

#    This info is now taken from the PilotAgentsDB
#
#    existingParam = self.jobDB.getJobParameters(int(job),['SubmittedAgents'])
#    if not existingParam['OK']:
#      return existingParam
#
#    if not existingParam['Value']:
#      self.log.verbose('Adding first submitted pilot parameter for job %s' %job)
#      if self.enable:
#        self.__setJobParam(job,'SubmittedAgents',submittedPilot)
#    else:
#      pilots = len(existingParam['Value'])
#      self.log.verbose('Adding submitted pilot number %s for job %s' %(pilots,job))
#      pilots += ',%s' %(submittedPilot)
#      if self.enable:
#        self.__setJobParam(job,'SubmittedAgents',submittedPilot)

    if self.enable:
      result = self.pilotDB.addPilotReference(submittedPilot,job,ownerDN,
                                              ownerGroup,
                                              self.resourceBroker,self.type,
                                              jdl_requirements)
      if not result['OK']:
        self.log.warn('Problem reporting to PilotAgentsDB:')
        self.log.warn(result['Message'])
      self.log.verbose(result)

    return S_OK()

  #############################################################################
  def __addPilotCFGParameter(self,workingDirectory,option,value):
    """Adds optional pilot requirements from the job e.g. owner or jobid
    """
    self.log.verbose( 'Adding %s.cfg to be used in agent requirements' %(option))
    path = '%s/%s.cfg' % (workingDirectory,option)
    try:
      if os.path.exists( path ):
        os.remove( path )
      optionCFG = open( path ,'w')
      optionCFG.write( 'AgentJobRequirements\n{\n%s=%s\n}\n' %(option,value))
      optionCFG.close()
    except Exception, x:
      self.log.warn( str(x) )
      return S_ERROR('Cannot create %s.cfg' %(option))
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

    mask = self.jobDB.getSiteMask()
    if not mask['OK']:
      return mask

    siteMask = mask['Value']
    if not siteMask:
      return S_ERROR('Returned site mask is empty for %s' %(self.type))

    self.log.verbose('Site Mask: %s' %(string.join(siteMask,', ')))
    candidates = siteMask

    tmpSites = []

    if bannedSites:
      for i in candidates:
        tmpSites.append(i)
      for site in bannedSites:
        if site in candidates:
          tmpSites.remove(site)
          self.log.verbose('Removing banned site %s from site candidate list for job %s' %(site,job))
      candidates = tmpSites

    if candidateSites==['ANY']:
      candidateSites=candidates

    tmpSites = []
    if candidateSites:
      for i in candidates:
        tmpSites.append(i)
      for site in candidates:
        if site in candidateSites:
          self.log.verbose('Site %s is a candidate site in the mask for job %s' %(site,job))
        else:
          tmpSites.remove(site)
          self.log.verbose('Removing %s as a candidate site for job %s' %(site,job))

      candidates = tmpSites

    if not candidates:
      self.__updateJobStatus(job,'Failed','No Candidate Sites in Mask')
      return S_ERROR('No Candidate Sites in Mask')

    self.log.info('Candidate sites for job %s: %s' %(job,string.join(candidates,', ')))
    finalSites = self.__getGridSites(candidates)
    return finalSites

  #############################################################################
  def __getGridSites(self,candidates):
    """Converts candidates sites from canonical DIRAC site names into those
       of the Grid.
    """

    ceCandidateList = []

    for site in candidates:
      gridType = site.split('.')[0]
      siteName = site.replace(gridType+'.','')
      ceList = gConfig.getValue('/Resources/Sites/%s/%s/CE' % (gridType,siteName),[])
      ceCandidateList += ceList

    if not ceCandidateList:
      return S_ERROR('No Grid site names found for DIRAC sites %s' %(string.join(candidates,', ')))

    return S_OK(ceCandidateList)

  #############################################################################
#  def __getGridSites(self,candidates):
#    """Converts candidates sites from canonical DIRAC site names into those
#       of the Grid.
#    """
#    section = '/Resources/GridSites/%s' % (self.type)
#    sites = gConfig.getOptionsDict(section)
#    if not sites['OK']:
#      #To avoid duplicating sites listed in LCG for gLite for example.  This could be passed as a parameter from
#      #the sub class to avoid below...
#      section = '/Resources/GridSites/LCG'
#      sites = gConfig.getOptionsDict(section)
#
#    if not sites['OK']:
#      self.log.warn(sites['Message'])
#      return S_ERROR('Could not obtain %s section from CS' %(section))
#
#    if not sites['Value']:
#      return S_ERROR('Empty CS section %s' %(section))
#
#    gridSites = sites['Value']
#    self.log.verbose('%s Grid Sites are: %s' %(self.type,string.join(gridSites,', ')))
#
#    candidateList = []
#
#    for ce,siteName in gridSites.items():
#      if siteName in candidates:
#        candidateList.append(ce)
#
#    if not candidateList:
#      return S_ERROR('No Grid site names found for DIRAC sites %s' %(string.join(candidates,', ')))
#
#    return S_OK(candidateList)

  #############################################################################
  def __cleanUp(self,jobDirectory):
    """  Clean up all the remnant files of the job submission
    """
    destroyProxy()
    if os.path.exists(jobDirectory):
      if self.enable:
        shutil.rmtree(jobDirectory, True )
        self.log.verbose('Cleaning up working directory: %s' %(jobDirectory))

  #############################################################################
  def __getWaitingJobs(self):
    """Returns the list of waiting jobs for which pilots should be submitted
    """
    selection = {'Status':'Waiting','MinorStatus':'Pilot Agent Submission'}
    result = self.jobDB.selectJobs(selection, limit=self.selectJobLimit, orderAttribute='LastUpdateTime')
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
    self.log.verbose("self.jobDB.setJobParameter(%s,'%s','%s')" %(job,reportName,value))
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
    self.log.error('PilotDirector: submitJob() method should be implemented in a subclass')
    return S_ERROR('PilotDirector: submitJob() method should be implemented in a subclass')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
