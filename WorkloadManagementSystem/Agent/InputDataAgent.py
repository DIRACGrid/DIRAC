########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/InputDataAgent.py,v 1.18 2008/02/15 14:02:10 paterson Exp $
# File :   InputDataAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Input Data Agent queries the file catalogue for specified job input data and adds the
      relevant information to the job optimizer parameters to be used during the
      scheduling decision.

"""

__RCSID__ = "$Id: InputDataAgent.py,v 1.18 2008/02/15 14:02:10 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.ConfigurationSystem.Client.Config               import gConfig
from DIRAC                                                 import S_OK, S_ERROR

import os, re, time, string

OPTIMIZER_NAME = 'InputData'

class InputDataAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for JobSanityAgent.
    """
    result = Optimizer.initialize(self)

    self.failedMinorStatus = gConfig.getValue(self.section+'/FailedJobStatus','Input Data Not Available')
    #this will ignore failover SE files
    self.diskSE            = gConfig.getValue(self.section+'/DiskSE','-disk,-DST,-USER')
    self.tapeSE            = gConfig.getValue(self.section+'/TapeSE','-tape,-RDST,-RAW')
    if type(self.diskSE) == type(' '):
      self.diskSE = self.diskSE.split(',')
    if type(self.tapeSE) == type(' '):
      self.tapeSE = self.tapeSE.split(',')

    self.site_se_mapping = {}
    mappingKeys = gConfig.getOptions('/Resources/SiteLocalSEMapping')
    for site in mappingKeys['Value']:
      seStr = gConfig.getValue('/Resources/SiteLocalSEMapping/%s' %(site))
      self.log.verbose('Site: %s, SEs: %s' %(site,seStr))
      self.site_se_mapping[site] = [ x.strip() for x in string.split(seStr,',')]

    try:
      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.fileCatalog = LcgFileCatalogCombinedClient()
    except Exception,x:
      msg = 'Failed to create LcgFileCatalogClient with exception:'
      self.log.fatal(msg)
      self.log.fatal(str(x))
      result = S_ERROR(msg)

    return result

  #############################################################################
  def checkJob(self,job):
    """This method controls the checking of the job.
    """

    result = self.jobDB.getInputData(job)
    if result['OK']:
      if result['Value']:
        self.log.verbose('Job %s has an input data requirement and will be processed' % (job))
        inputData = result['Value']
        result = self.resolveInputData(job,inputData)
        if not result['OK']:
          self.log.warn(result['Message'])
          return result
        resolvedData = result['Value']
        result = self.setOptimizerJobInfo(job,self.optimizerName,resolvedData)
        if not result['OK']:
          self.log.warn(result['Message'])
          return result
        result = self.setNextOptimizer(job)
        if not result['OK']:
          self.log.warn(result['Message'])
        return result
      else:
        self.log.verbose('Job %s has no input data requirement' % (job) )
        result = self.setNextOptimizer(job)
        if not result['OK']:
          self.log.warn(result['Message'])
        return result
    else:
      self.log.warn('Failed to get input data from JobdB for %s' % (job) )
      self.log.warn(result['Message'])
      return result

  #############################################################################
  def resolveInputData(self,job,inputData):
    """This method checks the file catalogue for replica information.
    """
    lfns = [string.replace(fname,'LFN:','') for fname in inputData]
    start = time.time()
    replicas = self.fileCatalog.getReplicas(lfns)
    timing = time.time() - start
    self.log.info('LFC Lookup Time: %.2f seconds ' % (timing) )
    if not replicas['OK']:
      self.log.warn(replicas['Message'])
      return replicas

    badLFNCount = 0
    badLFNs = []
    catalogResult = replicas['Value']

    if catalogResult.has_key('Failed'):
      for lfn,cause in catalogResult['Failed'].items():
        badLFNCount+=1
        badLFNs.append('LFN:%s Problem: %s' %(lfn,cause))

    if catalogResult.has_key('Successful'):
      for lfn,reps in catalogResult['Successful'].items():
        if not reps:
          badLFNs.append('LFN:%s Problem: Null replica value' %(lfn))
          badLFNCount+=1

    if badLFNCount:
      self.log.info('Found %s problematic LFN(s) for job %s' % (badLFNCount,job) )
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.setJobParam(job,self.optimizerName,param)
      if not result['OK']:
        self.log.warn(result['Message'])
      return S_ERROR('Input Data Not Available')

    inputData = catalogResult['Successful']
    siteCandidates = self.getSiteCandidates(inputData)
    if not siteCandidates['OK']:
      self.log.warn(siteCandidates['Message'])
      return siteCandidates

    guids = True
    guidDict = self.fileCatalog.getFileMetadata(lfns)
    if not guidDict['OK']:
      self.log.warn(guidDict['Message'])
      guids = False

    failed = guidDict['Value']['Failed']
    if failed:
      self.log.warn('Failed to establish some GUIDs')
      self.log.warn(failed)
      guids = False

    if guids:
      for lfn,reps in replicas['Value']['Successful'].items():
        guidDict['Value']['Successful'][lfn].update(reps)

      replicas = guidDict

    result = {}
    result['Value'] = replicas
    result['SiteCandidates'] = siteCandidates['Value']
    return S_OK(result)

  #############################################################################
  def getSiteCandidates(self,inputData):
    """This method returns a list of possible site candidates based on the
       job input data requirement.  For each site candidate, the number of files
       on disk and tape is resolved.
    """
    fileSEs = {}
    siteSEMapping = self.site_se_mapping
    for lfn,replicas in inputData.items():
      siteList = []
      for se in replicas.keys():
        sites = self._getSitesForSE(se)
        if sites:
          siteList += sites
      fileSEs[lfn] = siteList

    siteCandidates = []
    i = 0
    for file,sites in fileSEs.items():
      if not i:
        siteCandidates = sites
      else:
        tempSite = []
        for site in siteCandidates:
          if site in sites:
            tempSite.append(site)
        siteCandidates = tempSite
      i += 1

    if not len(siteCandidates):
      return S_ERROR('No candidate sites available')

    #In addition, check number of files on tape and disk for each site
    #for optimizations during scheduling
    siteResult = {}
    for site in siteCandidates: siteResult[site]={'disk':0,'tape':0}

    for lfn,replicas in inputData.items():
      for se,surl in replicas.items():
        sites = self._getSitesForSE(se)
        for site in sites:
          if site in siteCandidates:
            for disk in self.diskSE:
              if re.search(disk+'$',se):
                siteResult[site]['disk'] = siteResult[site]['disk']+1
            for tape in self.tapeSE:
              if re.search(tape+'$',se):
                siteResult[site]['tape'] = siteResult[site]['tape']+1

    return S_OK(siteResult)

  #############################################################################
  def _getSitesForSE(self,se):
    """Returns a list of sites via the site SE mapping for a given SE.
    """
    sites = []
    for site,ses in self.site_se_mapping.items():
      if se in ses:
        sites.append(site)

    return sites

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
