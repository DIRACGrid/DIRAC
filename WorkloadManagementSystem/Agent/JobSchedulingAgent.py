########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/JobSchedulingAgent.py,v 1.30 2008/07/22 16:00:58 acasajus Exp $
# File :   JobSchedulingAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Scheduling Agent takes the information gained from all previous
      optimizers and makes a scheduling decision for the jobs.  Subsequent to this
      jobs are added into a Task Queue by the next optimizer and pilot agents can
      be submitted.

      All issues preventing the successful resolution of a site candidate are discovered
      here where all information is available.  This Agent will fail affected jobs
      meaningfully.

"""
__RCSID__ = "$Id: JobSchedulingAgent.py,v 1.30 2008/07/22 16:00:58 acasajus Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.Core.Utilities.SiteSEMapping                    import getSEsForSite
from DIRAC.ConfigurationSystem.Client.PathFinder           import getAgentSection
from DIRAC                                                 import gConfig,S_OK,S_ERROR

import random,string,re, types

OPTIMIZER_NAME = 'JobScheduling'

class JobSchedulingAgent(Optimizer):
  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """ Initialization of the Agent.
    """
    result = Optimizer.initialize(self)

    self.dataAgentName        = gConfig.getValue(self.section+'/InputDataAgent','InputData')
    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """
    self.log.verbose('Job %s will be processed' % (job))
    #First check whether the job has an input data requirement
    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.warn('Failed to get input data from JobDB for %s' % (job))
      self.log.error(result['Message'])

    if not result['Value']:
      result = self.__sendJobToTaskQueue(job)
      return result

    hasInputData=False
    for i in result['Value']:
      if i:
        hasInputData=True

    if not hasInputData:
      #With no input data requirement, job can proceed directly to task queue
      self.log.verbose('Job %s has no input data requirement' % (job))
      result = self.__sendJobToTaskQueue(job)
      return result

    self.log.verbose('Job %s has an input data requirement ' % (job))

    #Check all optimizer information
    optInfo = self.__checkOptimizerInfo(job)
    if not optInfo['OK']:
      msg = optInfo['Message']
      self.log.info(msg)
      self.updateJobStatus(job,self.failedStatus,msg)
      return S_OK(msg)

    #Compare site candidates with current mask
    siteCandidates = optInfo['Value']['SiteCandidates'].keys()
    self.log.info('Input Data Site Candidates: %s' % (siteCandidates))
    maskSiteCandidates = self.__checkSitesInMask(job,siteCandidates)
    if not maskSiteCandidates['OK']:
      msg = 'Could not get sites in mask'
      self.log.error(msg)
      return S_OK(msg)

    siteCandidates = maskSiteCandidates['Value']
    if not siteCandidates:
      msg = 'No Candidate Sites in Mask'
      result = self.__getJobSiteRequirement(job)
      if not result['OK']:
        self.log.warn('Could not determine site candidates for job, will leave as ANY')
      else:
        chosenSite = result['ChosenSite']
        if len(chosenSite)==1:
          result = self.jobDB.setJobAttribute(job,'Site',chosenSite[0])
          if not result['OK']:
            self.log.warn('Problem setting job site parameter:\n%s' %result)
      self.log.info(msg)
      self.updateJobStatus(job,self.failedStatus,msg)
      return S_OK(msg)

    #Compare site candidates with site requirement / banned sites in JDL
    jobReqtCandidates = self.__checkJobSiteRequirement(job,siteCandidates)
    if not jobReqtCandidates['OK']:
      msg=jobReqtCandidates['Message']
      self.log.warn(msg)
      self.updateJobStatus(job,self.failedStatus,msg)
      return S_OK(msg)

    siteCandidates = jobReqtCandidates['Value']
    if not siteCandidates:
      msg = 'Conflict with job site requirement'
      self.log.info(msg)
      self.updateJobStatus(job,self.failedStatus,msg)
      return S_OK(msg)

    #Set stager request as necessary, optimize for smallest #files on tape if
    #more than one site candidate left at this point
    checkStaging = self.__resolveSitesForStaging(job,siteCandidates,optInfo['Value']['SiteCandidates'])
    if not checkStaging['OK']:
      self.log.warn(result['Message'])
      return checkStaging

    destinationSites = checkStaging['SiteCandidates']
    if not destinationSites:
      return S_ERROR('No destination sites available')

    stagingFlag = checkStaging['Value']
    if stagingFlag:
      #Single site candidate chosen and staging required
      self.log.verbose('Job %s requires staging of input data' %(job))
      stagerDict = self.__setStagingRequest(job,destinationSites[0],optInfo['Value'])
      if not stagerDict['OK']:
        return stagerDict
      #Staging request is saved as job optimizer parameter
    else:
      #No staging required, can proceed to task queue agent and then waiting status
      self.log.verbose('Job %s does not require staging of input data' %(job))

    #Finally send job to TaskQueueAgent
    result = self.__sendJobToTaskQueue(job,destinationSites)
    if not result['OK']:
      return result

    return S_OK('Job successfully scheduled')

  #############################################################################
  def __checkOptimizerInfo(self,job):
    """This method aggregates information from optimizers to return a list of
       site candidates and all information regarding input data.
    """
    dataDict = {}
    #Check input data agent result and limit site candidates accordingly
    siteCandidates = {}
    dataResult = self.getOptimizerJobInfo(job,self.dataAgentName)
    if dataResult['OK'] and len(dataResult['Value']):
      self.log.verbose(dataResult)
      dataResult = dataResult['Value']
      if not dataResult.has_key('SiteCandidates'):
        return S_ERROR('No possible site candidates')
      siteCandidates = dataResult['SiteCandidates'].keys()
    else:
      self.log.warn('No information available for optimizer %s' %(self.dataAgentName))

    if not siteCandidates:
      msg = 'File Catalog Access Failure'
      self.log.info(msg)
      return S_ERROR(msg)

    return S_OK(dataResult)

  #############################################################################
  def __resolveSitesForStaging(self, job, siteCandidates, inputDataDict):
    """Site candidates are resolved from potential candidates and any job site
       requirement is compared at this point.
    """
    self.log.verbose(inputDataDict)
    finalSiteCandidates = []
    tapeCount = 0
    tapeList  = []
    stagingFlag = 0
    numberOfCandidates = len(siteCandidates)
    self.log.verbose('Job %s has %s candidate sites' %(job,numberOfCandidates))
    for site in siteCandidates:
      tape = inputDataDict[site]['tape']
      tapeList.append(tape)
      if tape > 0:
        self.log.verbose('%s replicas on tape storage for %s' %(tape,site))
        tapeCount += 1

    if not tapeCount:
      self.log.verbose('All replicas on disk, no staging required')
      finalSiteCandidates = siteCandidates
      result = S_OK()
      result['SiteCandidates']=finalSiteCandidates
      return result

    if tapeCount < numberOfCandidates:
      self.log.verbose('All replicas on disk for some candidate sites, restricting to those')
      for site in siteCandidates:
        tape = inputDataDict[site]['tape']
        if tape==0:
          finalSiteCandidates.append(site)

    if tapeCount == numberOfCandidates:
      self.log.verbose('Staging is required for job')
      tapeList.sort()
      minTapeValue = tapeList[0]
      minTapeSites = []
      for site in siteCandidates:
        if inputDataDict[site]['tape']==minTapeValue:
          minTapeSites.append(site)

      if not minTapeSites:
        return S_ERROR('No possible site candidates')

      if len(minTapeSites) > 1:
        self.log.verbose('The following sites have %s tape replicas: %s' %(minTapeValue,minTapeSites))
        random.shuffle(minTapeSites)
        randomSite = minTapeSites[0]
        finalSiteCandidates.append(randomSite)
        self.log.verbose('Site %s has been randomly chosen for job' %(randomSite))
        stagingFlag = 1
      else:
        self.log.verbose('%s is the candidate site with smallest number of tape replicas (=%s)' %(minTapeSites[0],minTapeValue))
        finalSiteCandidates.append(minTapeSites[0])
        stagingFlag = 1

    result = S_OK()
    if stagingFlag:
      result['Value'] = 1

    result['SiteCandidates']=finalSiteCandidates

    return result

  #############################################################################
  def __setStagingRequest(self,job,destination,inputDataDict):
    """A Staging request is formulated and saved as a job optimizer parameter.
    """

    self.log.verbose('Destination site %s' % (destination))
    self.log.verbose('Input Data: %s' % (inputDataDict))

    destinationSEs = getSEsForSite(destination)
    if not destinationSEs['OK']:
      return S_ERROR('Could not determine SEs for site %s' %destination)
    destinationSEs = destinationSEs['Value']

    #Ensure only tape SE files are staged
    tapeSEs = gConfig.getValue(self.section+'/TapeSE','-tape,-RDST,-RAW')
    if type(tapeSEs) == type(' '):
      tapeSEs = [x.strip() for x in string.split(tapeSEs,',')]

    siteTapeSEs = []
    for se in destinationSEs:
      for tapeSE in tapeSEs:
        if re.search('%s$' %tapeSE,se):
          siteTapeSEs.append(se)

    destinationSEs = siteTapeSEs
    if not destinationSEs:
      return S_ERROR('No LocalSEs For Site')

    self.log.verbose('Site tape SEs: %s' % (string.join(destinationSEs,', ')))
    stageSURLs = {}
    inputData = inputDataDict['Value']['Value']['Successful']
    for lfn,reps in inputData.items():
      for se,surl in reps.items():
        for destSE in destinationSEs:
          if se==destSE:
            if not lfn in stageSURLs.keys():
              stageSURLs[lfn]={}
              stageSURLs[lfn].update({se:surl})

    stagingRequest = {'Sites':destination,'Files':stageSURLs}
    self.log.verbose('Staging Request: %s' %stagingRequest)
    storeRequest = self.setOptimizerJobInfo(job,'StagingRequest',stagingRequest)
    if not storeRequest['OK']:
      return S_ERROR('Storing StagingRequest')

    return S_OK('StagingRequest saved')

  #############################################################################
  def __checkJobSiteRequirement(self,job,siteCandidates):
    """Get Grid site list from the DIRAC CS, choose only those which are allowed
       in the Matcher mask for the scheduling decision.
    """
    result = self.__getJobSiteRequirement(job)
    if not result['OK']:
      return result
    bannedSites = result['BannedSites']
    chosenSite = result['ChosenSite']

    if chosenSite:
      chosen = chosenSite[0]
      if not chosen in siteCandidates:
        self.log.info('%s is not a possible site candidate for %s' %(chosen,siteCandidates))
        result = self.jobDB.setJobAttribute(job,'Site',chosen)
        if not result['OK']:
          self.log.warn('Problem setting job site parameter:\n%s' %result)
        return S_ERROR('Chosen site is not eligible')
      else:
        siteCandidates = chosenSite

    if bannedSites:
      badSiteCandidates = []
      for site in siteCandidates:
        for banned in bannedSites:
          if site == banned:
            self.log.info('Candidate site %s is a banned site' %(banned))
            badSiteCandidates.append(banned)

      if badSiteCandidates:
        for removeSite in badSiteCandidates:
          siteCandidates.remove(removeSite)

    if not siteCandidates:
      result = S_ERROR('No eligible sites for job')

    return S_OK(siteCandidates)

  #############################################################################
  def __getJobSiteRequirement(self,job):
    """Returns any candidate sites specified by the job or sites that have been
       banned and could affect the scheduling decision.
    """
    result = self.jobDB.getJobJDL(job)
    if not result['OK']:
      return result
    if not result['Value']:
      self.log.warn('No JDL found for job')
      self.log.verbose(result)
      return S_ERROR('No JDL found for job')

    jdl = result['Value']
    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.verbose("Warning: illegal JDL for job %s, will be marked problematic" % (job))
      result = S_ERROR()
      result['Value'] = "Illegal Job JDL"
      return result

    result = S_OK()
    site = classadJob.get_expression('Site').replace('"','').replace('Unknown','')
    bannedSites = classadJob.get_expression("BannedSites").replace('{','').replace('}','').replace('"','').replace(' ','').split(',')
    if site and site!='ANY' and len(string.split(site,','))==1:
      self.log.info('Job %s has single chosen site %s specified in JDL' %(job,site))
      result['ChosenSite']=[site]
    else:
      result['ChosenSite']=[]

    if bannedSites:
      self.log.info('Job %s has JDL requirement to ban %s' %(job,bannedSites))
      result['BannedSites']=bannedSites
    else:
      result['BannedSites']=[]

    return result

  #############################################################################
  def __checkSitesInMask(self,job,siteCandidates):
    """Returns list of site candidates that are in current mask.
    """

    result = self.jobDB.getSiteMask()
    if not result['OK']:
      return S_ERROR('Could not get site mask')

    sites = []
    allowedSites = result['Value']
    for candidate in siteCandidates:
      if not candidate in allowedSites:
        self.log.verbose('%s is a candidate site for job %s but not in mask' %(candidate,job))
      else:
        sites.append(candidate)

    self.log.info('Candidate sites in Mask are %s' %(sites))

    return S_OK(sites)

  #############################################################################
  def __sendJobToTaskQueue(self,job,siteCandidates=[]):
    """This method sends jobs to the task queue agent and if candidate sites
       are defined, updates job JDL accordingly.
    """
    result = self.jobDB.getJobJDL(job)
    #means that reqts field will not be changed by any other optimizer
    if not result['OK']:
      return result
    if not result['Value']:
      self.log.warn('No JDL found for job')
      return S_ERROR('No JDL found for job')

    jdl = result['Value']
    classAdJob = ClassAd(jdl)
    if not classAdJob.isOK():
      self.log.verbose("Warning: illegal JDL for job %s, will be marked problematic" % (job))
      result = S_ERROR()
      result['Value'] = "Illegal Job JDL"
      return result

    requirements = classAdJob.get_expression('Requirements').replace('Unknown','')
    self.log.verbose('Existing job requirements: %s' % (requirements))
    reqsToAdd = {}
    if not requirements:
      newRequirements = self.__resolveJobJDLRequirement('True',siteCandidates)
    else:
      newRequirements = self.__resolveJobJDLRequirement(requirements,siteCandidates)
    #GridMiddleware requirements
    if not classAdJob.lookupAttribute( "SubmitPool" ):
      classAdJob.insertAttributeString( 'SubmitPool', 'ANY' )
    else:
      submitPool = classAdJob.getListFromExpression('SubmitPool')[0]
      if submitPool != "ANY":
        directorSection = getAgentSection( "WorkloadManagement/Director" )
        gridMiddleware = gConfig.getValue( "%s/%s/GridMiddleware" % ( directorSection, submitPool ), '' )
        if gridMiddleware:
          reqsToAdd[ 'other.GridMiddleware' ] = gridMiddleware
    #Required CE's requirements
    gridCEs = [ ce for ce in classAdJob.getListFromExpression( 'GridRequiredCEs' ) if ce ]
    if gridCEs:
      reqsToAdd[ 'other.GridCE' ] = gridCEs
    #Add reqs
    reqList = []
    for reqField in reqsToAdd:
      reqValue = reqsToAdd[ reqField ]
      if type( reqValue ) in ( types.ListType, types.TupleType ):
        reqList.append( "(%s)" % " || ".join( [ '%s=="%s"' %( reqField, val ) for val in reqValue ] ) )
      else:
        reqList.append( '%s=="%s"' % ( reqField, reqValue ) )
    if reqList:
      newRequirements += " && %s" % " && ".join( reqList )

    if newRequirements:
      self.log.verbose('Resolved requirements for job: %s' %(newRequirements))
      classAdJob.set_expression ("Requirements", newRequirements)
      if siteCandidates:
        sites = string.join(siteCandidates,',')
        classAdJob.insertAttributeString("Site",sites)
      jdl = classAdJob.asJDL()
      result = self.jobDB.setJobJDL(int(job),jdl)
      if not result['OK']:
        return result

    if siteCandidates:
      if len(siteCandidates)==1:
        self.log.verbose('Individual site candidate for job %s is %s' %(job,siteCandidates[0]))

    #To assign site if single candidate
    result = self.__setSiteCandidate(job,classAdJob)
    if not result['OK']:
      self.log.warn(result['Message'])

    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.warn(result['Message'])

    return result

  #############################################################################
  def __setSiteCandidate(self,job,classAdJob):
    """Sets the candidate site if a single site such that it appears in the monitoring
       webpage.
    """
    result = S_OK()
    siteRequirement = classAdJob.get_expression('Site').replace('Unknown','').replace(' ','').replace('"','')
    self.log.verbose('Final site requirement is: %s' %(siteRequirement))
    if not re.search(',',siteRequirement):
      result = self.jobDB.setJobAttribute(job,'Site',siteRequirement)
      if not result['OK']:
        self.log.warn('Problem setting job site parameter')
        self.log.warn(result)

    return result

  #############################################################################
  def __resolveJobJDLRequirement(self,requirements,siteCandidates):
    """Returns the job site requirement for the final candidate sites. Any existing
       site requirements are replaced.
    """
    requirementsList = string.split(requirements,'&&')
    #First check whether site is already assigned
    for req in requirementsList:
      if re.search('other.Site==',req.replace(' ','')):
        return requirements

    if siteCandidates:
      jdlsite = ' && ( '
      for site in siteCandidates:
        if not re.search(site,jdlsite):
          jdlsite = jdlsite + ' other.Site == "'+site+'" || '

      jdlsite = jdlsite[0:-4]
      jdlsite = jdlsite + " )"

      requirements += jdlsite

    return requirements

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
