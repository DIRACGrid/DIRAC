########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/SiteStager.py,v 1.8 2008/11/22 14:39:28 acsmith Exp $
# File :   SiteStager.py
# Author : Stuart Paterson
########################################################################

"""  The SiteStager performs staging requests for a given site and triggers
     resetting of stage requests as necessary.
"""

__RCSID__ = "$Id: SiteStager.py,v 1.8 2008/11/22 14:39:28 acsmith Exp $"

from DIRAC.StagerSystem.Client.StagerClient                import StagerClient
from DIRAC.DataManagementSystem.Client.StorageElement      import StorageElement
from DIRAC.Core.Utilities.SiteSEMapping                    import getSEsForSite
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time, shutil
from threading import Thread

class SiteStager(Thread):

  #############################################################################
  def __init__(self,configPath,siteName,enableFlag=True):
    """ Constructor for SiteStager
    """
    logSiteName = string.split(siteName,'.')[1]
    self.log = gLogger.getSubLogger(logSiteName)
    self.site = siteName
    self.enable = enableFlag
    self.configSection = configPath
    self.pollingTime = gConfig.getValue(self.configSection+'/PollingTime',120) #Seconds
    self.fileSelectLimit = gConfig.getValue(self.configSection+'/FileSelectLimit',1000)
    self.maxRequests = gConfig.getValue(self.configSection+'/MaxFiles',20000)
    self.stageRepeatTime = gConfig.getValue(self.configSection+'/StageRepeatTime',6) #Hours
    self.stagerClient = StagerClient()
    Thread.__init__(self)
    self.setDaemon( True )

  #############################################################################
  def run(self):
    """ The run method of the SiteStager thread
    """
    while True:
      self.log.info( 'Waking up SiteStager thread for %s' %(self.site))
      try:
        result = self.__pollSite()
        if not result['OK']:
          self.log.warn(result['Message'])
      except Exception,x:
        self.log.warn('Site thread failed with exception, will restart...')
        self.log.warn(str(x))

      time.sleep(self.pollingTime)


  def __get_site_se_mapping(self):
    """ Helper function to prepare a dictionary of local SEs per site defined
        in the Configuration Service
    """

    mappingDict = {}

    result = gConfig.getSections('/Resources/Sites')
    if not result['OK']:
      return result
    gridTypes = result['Value']
    for gridType in gridTypes:
      result = gConfig.getSections('/Resources/Sites/'+gridType)
      if not result['OK']:
        continue
      siteList = result['Value']
      for site in siteList:
        ses = gConfig.getValue('/Resources/Sites/%s/%s/SE' % (gridType,site),[])
        if ses:
          mappingDict[site] = ses

    return S_OK(mappingDict)

  #############################################################################
  def __pollSite(self):
    """ This method starts the staging loop for a given site thread.  The initial
        action is to repeat any staging requests that are not yet completed before
        polling for new staging requests.
    """
    self.log.verbose('Checking for stage requests to repeat')
    self.__repeatStageRequests()
    self.log.verbose('Querying StagerDB for pending requests')
    result = self.stagerClient.getFilesForState(self.site,'New',limit=self.fileSelectLimit)
    if not result['OK']:
      return result

    replicas = result['Files']
    siteSEs = getSEsForSite(self.site)
    if not siteSEs['OK']:
      return S_ERROR('Could not determine SEs for site %s' %self.site)
    siteSEs = siteSEs['Value']

    seFilesDict = {}
    pfnLfnDict = {}
    for localSE in siteSEs:
      for lfn,reps in replicas.items():
        if reps.has_key(localSE):
          pfn = reps[localSE]
          if seFilesDict.has_key(localSE):
            currentFiles = seFilesDict[localSE]
            currentFiles.append(pfn)
            seFilesDict[localSE] = currentFiles
            pfnLfnDict[pfn]=lfn
          else:
            seFilesDict[localSE] = [pfn]
            pfnLfnDict[pfn]=lfn

    self.log.verbose('Files grouped by LocalSE are: \n%s' %seFilesDict)
    result = self.__prestageFiles(seFilesDict,pfnLfnDict)
    if not result['OK']:
      self.log.warn(result['Message'])
      self.log.debug(result)

    self.log.verbose('Checking for stage requests to repeat')
    self.__repeatStageRequests()
    self.log.verbose('End of site stager loop')
    return S_OK()

  #############################################################################
  def __prestageFiles(self,seFilesDict,pfnLfnDict):
    """ This method instantiates the StorageElement class and prestages the SURLs.
    """
    submitted = [] #even if files fail, still report them as submitted since they will fail after several retries
    for se,pfnList in seFilesDict.items():
      storageElement = StorageElement(se)
      if not storageElement.isValid()['Value']:
        return S_ERROR('%s SiteStager Failed to instantiate StorageElement for: %s' %(self.site,se))

      start = time.time()
      stagingRequest = storageElement.prestageFile(pfnList)
      timing = time.time()-start
      self.log.info('Stage request for %s files took %.1f secs' %(len(pfnList),timing))

      if not stagingRequest['OK']:
        self.log.warn('Staging request failed for %s %s' %(self.site,se))
        return stagingRequest
      else:
        stagingRequest = stagingRequest['Value']

      if stagingRequest.has_key('Failed'):
        for pfn,cause in stagingRequest['Failed'].items():
          self.log.warn('PFN %s failed to stage with message: %s' %(pfn,cause))
          #submitted.append(pfnLfnDict[pfn])

      if stagingRequest.has_key('Successful'):
        for pfn,protPFN in stagingRequest['Successful'].items():
          self.log.verbose('Stage request made for PFN %s at SE %s' %(pfn,se))
          submitted.append(pfnLfnDict[pfn])

    if submitted:
      result = self.stagerClient.setFilesState(submitted,self.site,'Submitted')
      if not result['OK']:
        self.log.warn('Setting file status failed with error:\n%s' %result)

    return S_OK('Files prestaged')

  #############################################################################
  def __repeatStageRequests(self):
    """ Checks and resubmits stage requests after waiting a
        specified amount of time.
    """
    result = self.stagerClient.resetStageRequest(self.site,self.stageRepeatTime*60*60)
    if not result['OK']:
      self.log.warn('Resetting stage requests failed')
      self.log.warn(result)

    return S_OK('Stage requests reset')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
