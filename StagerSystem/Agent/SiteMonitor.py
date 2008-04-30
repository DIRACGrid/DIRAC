########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/SiteMonitor.py,v 1.7 2008/04/30 10:17:03 paterson Exp $
# File :   SiteMonitor.py
# Author : Stuart Paterson
########################################################################

"""  The SiteMonitor base-class monitors staging requests for a given site.
"""

__RCSID__ = "$Id: SiteMonitor.py,v 1.7 2008/04/30 10:17:03 paterson Exp $"

from DIRAC.StagerSystem.Client.StagerClient                import StagerClient
from DIRAC.DataManagementSystem.Client.StorageElement      import StorageElement
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time
from threading import Thread

class SiteMonitor(Thread):

  #############################################################################
  def __init__(self,configPath,siteName):
    """ Constructor for SiteMonitor
    """
    logSiteName = string.split(siteName,'.')[1]
    self.log = gLogger.getSubLogger(logSiteName)
    self.site = siteName
    self.configSection = configPath
    self.pollingTime = gConfig.getValue(self.configSection+'/PollingTime',10) #Seconds
    self.fileSelectLimit = gConfig.getValue(self.configSection+'/FileSelectLimit',1000)
    self.stageRepeatTime = gConfig.getValue(self.configSection+'/StageRepeatTime',21600) # e.g. 6hrs
    self.stageRetryMax = gConfig.getValue(self.configSection+'/StageRetryMax',4) # e.g. after 4 * 6 hrs
    self.taskSelectLimit = gConfig.getValue(self.configSection+'/TaskSelectLimit',100) # e.g. after 24hrs
    self.stagerClient = StagerClient()
    Thread.__init__(self)

  #############################################################################
  def run(self):
    """ The run method of the SiteMonitor thread
    """
    while True:
      self.log.info( 'Waking up SiteMonitor thread for %s' %(self.site))
      try:
        result = self.__pollSite()
        if not result['OK']:
          self.log.warn(result['Message'])
      except Exception,x:
        self.log.warn('Site thread failed with exception, will restart...')
        self.log.warn(str(x))

      time.sleep(self.pollingTime)

  #############################################################################
  def __pollSite(self):
    """ This method starts the monitoring loop for a given site thread.
    """
    self.log.verbose('Checking for tasks that are completed at %s' %(self.site))
    result = self.__updateCompletedTasks()
    self.log.verbose('Monitoring files for status "New" at %s' %(self.site))
    result = self.__monitorStageRequests('New')
    self.log.verbose('Checking for tasks that are completed at %s' %(self.site))
    result = self.__updateCompletedTasks()
    self.log.verbose('Monitoring files for status "Submitted" at %s' %(self.site))
    result = self.__monitorStageRequests('Submitted')
    self.log.verbose('Checking for tasks that are completed at %s' %(self.site))
    result = self.__updateCompletedTasks()
    self.log.verbose('Checking for tasks to retry at %s' %(self.site))
    result = self.__getTasksForRetry()
    return S_OK('Monitoring loop completed')

  #############################################################################
  def __monitorStageRequests(self,status):
    """ This method instantiates the StorageElement class and prestages the SURLs.
    """
    result = self.stagerClient.getFilesForState(self.site,status,limit=self.fileSelectLimit)
    if not result['OK']:
      return result

    replicas = result['Files']
    siteSEs = []
    mappingKeys = gConfig.getOptions('/Resources/SiteLocalSEMapping')
    for possible in mappingKeys['Value']:
      if possible==self.site:
        seStr = gConfig.getValue('/Resources/SiteLocalSEMapping/%s' %(self.site))
        self.log.verbose('Site: %s, SEs: %s' %(self.site,seStr))
        siteSEs = [ x.strip() for x in string.split(seStr,',')]

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

    self.log.verbose('Files grouped by LocalSE for state "%s" are: \n%s' %(status,seFilesDict))
    if seFilesDict:
      result = self.__getStagedFiles(seFilesDict,pfnLfnDict)
      if not result['OK']:
        self.log.warn('Problem while getting file metadata:\n%s' %(result))

    return S_OK('Monitoring updated')

  #############################################################################
  def __getStagedFiles(self,seFilesDict,pfnLfnDict):
    """ Checks whether files are cached.
    """
    staged = []
    failed = []
    unstaged = []
    totalFiles = len(pfnLfnDict.keys())
    for se,pfnList in seFilesDict.items():
      storageElement = StorageElement(se)
      if not storageElement.isValid()['Value']:
        return S_ERROR('%s SiteMonitor Failed to instantiate StorageElement for: %s' %(self.site,se))

      start = time.time()
      metadataRequest = storageElement.getFileMetadata(pfnList)
      timing = time.time()-start
      self.log.verbose(metadataRequest)
      self.log.info('Metadata request for %s files took %.1f secs' %(len(pfnList),timing))

      if not metadataRequest['OK']:
        self.log.warn('Metadata request failed for %s %s' %(self.site,se))
        return metadataRequest
      else:
        metadataRequest = metadataRequest['Value']

      self.log.verbose('Setting timing information for gfal.prestage at site %s for %s files' %(self.site,len(pfnList)))
      result = self.stagerClient.setTiming(self.site,'gfal.prestage',float(timing),len(pfnList))
      if not result['OK']:
        self.log.warn('Failed to enter timing information for site %s with error:\n%s' %(self.site,result))

      if metadataRequest.has_key('Failed'):
        for pfn,cause in metadataRequest['Failed'].items():
          self.log.warn('Metadata request for PFN %s failed with message: %s' %(pfn,cause))
          failed.append(pfnLfnDict[pfn])

      if metadataRequest.has_key('Successful'):
        for pfn,metadata in metadataRequest['Successful'].items():
          self.log.debug('Metadata call successful for PFN %s SE %s' %(pfn,se))
          if metadata.has_key('Cached'):
            if metadata['Cached']:
              staged.append(pfnLfnDict[pfn])
              self.log.verbose('PFN %s is staged' %(pfn))
            else:
              self.log.verbose('PFN %s is not yet staged' %(pfn))
              unstaged.append(pfnLfnDict[pfn])
          else:
            self.log.warn('Unexpected metadata result for PFN %s' %(pfn))
            self.log.warn(metadata)

    if staged:
      result = self.stagerClient.setFilesState(staged,self.site,'ToUpdate')
      if not result['OK']:
        self.log.warn(result)

    self.log.info('Metadata query found: Staged=%s, UnStaged=%s, Failed=%s, out of Total=%s files' %(len(staged),len(unstaged),len(failed),totalFiles))
    return S_OK(staged)

  #############################################################################
  def __updateCompletedTasks(self):
    """ Checks for completed tasks and triggers the update of their status.
    """
    result = self.stagerClient.getJobsForState(self.site,'Staged',limit=self.taskSelectLimit)
    if not result['OK']:
      return result

    lfns = []
    for jobID in result['JobIDs']:
      result = self.stagerClient.getLFNsForJob(jobID)
      if not result['OK']:
        self.log.warn('Problem getting LFNs for ID %s with result:\n%s' %(jobID,result))
      else:
        for lfn in result['LFNs']:
          lfns.append(lfn)

    if lfns:
      self.log.info('Updating %s LFNs to successful status' %(len(lfns)))
      result = self.stagerClient.setFilesState(lfns,self.site,'Successful')
      if not result['OK']:
        self.log.warn('Problem updating successful task with ID %s:\n%s' %(jobID,result))
    else:
      self.log.verbose('No successfully staged tasks to update')

    return S_OK('Completed tasks updated')

  #############################################################################
  def __getTasksForRetry(self):
    """ Checks for failed tasks and triggers the update of their status.
    """
    result = self.stagerClient.getJobsForRetry(self.stageRetryMax,self.site)
    if not result['OK']:
      return result
    for jobID,lfns in result['JobIDs'].items():
      self.log.info('Updating %s LFNs to failed status for job %s' %(len(lfns),jobID))
      result = self.stagerClient.setFilesState(lfns,self.site,'Failed')
      if not result['OK']:
        self.log.warn('Problem updating failed task with ID %s:\n%s' %(jobID,result))

    return S_OK('Failed tasks updated')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
