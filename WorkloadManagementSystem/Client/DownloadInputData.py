########################################################################
# $Id: DownloadInputData.py,v 1.8 2009/04/18 18:26:56 rgracian Exp $
# File :   DownloadInputData.py
# Author : Stuart Paterson
########################################################################

""" The Download Input Data module wraps around the Replica Management
    components to provide access to datasets by available site protocols as
    defined in the CS for the VO.
"""

__RCSID__ = "$Id: DownloadInputData.py,v 1.8 2009/04/18 18:26:56 rgracian Exp $"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageElement               import StorageElement
from DIRAC.Core.Utilities.Os                                        import getDiskSpace
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger

import os,sys,re,string

COMPONENT_NAME = 'DownloadInputData'

class DownloadInputData:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)
    self.inputData = argumentsDict['InputData']
    self.configuration = argumentsDict['Configuration']
    self.fileCatalogResult = argumentsDict['FileCatalog']
    self.jobID = None
    self.rm = ReplicaManager()

  #############################################################################
  def execute(self,dataToResolve=None):
    """This method is called to download the requested files in the case where
       enough local disk space is available.  A buffer is left in this calculation
       to leave room for any produced files.
    """

    #Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']
    diskSEList = self.configuration['DiskSEList']
    tapeSEList = self.configuration['TapeSEList']

    if self.configuration.has_key('JobID'):
      self.jobID = self.configuration['JobID']

    #Problematic files will be returned and can be handled by another module
    failedReplicas = []

    if dataToResolve:
      self.log.verbose('Data to resolve passed directly to DownloadInputData module')
      self.inputData = dataToResolve #e.g. list supplied by another module


    self.inputData = [x.replace('LFN:','') for x in self.inputData]
    self.log.info('InputData to be downloaded is:')
    for i in self.inputData:
      self.log.verbose(i)

    replicas = self.fileCatalogResult['Value']['Successful']

    #For the unlikely case that a file is found on two SEs at the same site
    #disk-based replicas are favoured.
    downloadReplicas = {}
    for lfn,reps in replicas.items():
      localStorage = {}
      localTapeSE = ''
      if lfn in self.inputData:
        for localSE in localSEList:
          if reps.has_key(localSE):
            for diskSE in diskSEList:
              if re.search(diskSE,localSE):
                localStorage[localSE]=1
            for tapeSE in tapeSEList:
              if re.search(tapeSE,localSE):
                localTapeSE = localSE
                localStorage[localTapeSE]=0

        for se,flag in localStorage.items():
          if flag:
            pfn = replicas[lfn][se]
            if replicas[lfn].has_key('Size') and replicas[lfn].has_key('GUID'):
              size = replicas[lfn]['Size']
              guid = replicas[lfn]['GUID']
              downloadReplicas[lfn] = {'SE':se,'PFN':pfn,'Size':size,'GUID':guid}

        if not downloadReplicas.has_key(lfn):
          pfn = replicas[lfn][localTapeSE]
          if replicas[lfn].has_key('Size') and replicas[lfn].has_key('GUID'):
            size = replicas[lfn]['Size']
            guid = replicas[lfn]['GUID']
            downloadReplicas[lfn] = {'SE':localTapeSE,'PFN':pfn,'Size':size,'GUID':guid}
      else:
        self.log.verbose('LFN %s is not in requested input data to download')

    totalSize = 0
    self.log.verbose('Replicas to download from local SEs are:')
    for lfn,reps in downloadReplicas.items():
      self.log.verbose(lfn)
      for n,v in reps.items():
        self.log.verbose('%s %s' %(n,v))
        if n=='Size':
          totalSize+=int(v) #bytes

    self.log.info('Total size of files to be downloaded is %s bytes' %(totalSize))
    for i in self.inputData:
      if not downloadReplicas.has_key(i):
        self.log.warn('Not all file metadata (SE,PFN,Size,GUID) was available for LFN %s' %(i))
        failedReplicas.append(i)

    #Now need to check that the list of replicas to download fits into
    #the available disk space. Initially this is a simple check and if there is not
    #space for all input data, no downloads are attempted.
    result = self.__checkDiskSpace(totalSize)
    if not result['OK']:
      self.log.warn('Problem checking available disk space:\n%s' %(result))
      return result

    if not result['Value']:
      report = 'Not enough disk space available for download: %s / %s bytes' %(result['Value'],totalSize)
      self.log.warn(report)
      self.__setJobParam(COMPONENT_NAME,report)
      result = S_OK()
      result['Failed'] = self.inputData
      result['Successful'] = {}
      return result

    resolvedData = {}
    localSECount = 0
    for lfn in downloadReplicas.keys():
      result = self.__getPFN(downloadReplicas[lfn]['PFN'], downloadReplicas[lfn]['SE'],downloadReplicas[lfn]['Size'],downloadReplicas[lfn]['GUID'])
      if not result['OK']:
        self.log.warn('Download of file from localSE failed with message:\n%s' %(result))
        result = self.__getLFN(lfn,downloadReplicas[lfn]['PFN'], downloadReplicas[lfn]['SE'],downloadReplicas[lfn]['Size'],downloadReplicas[lfn]['GUID'])
        if not result['OK']:
          self.log.warn('Download of file from any SE failed with message:\n%s' %(result))
          failedReplicas.append(lfn)
        else:
          resolvedData[lfn] = result['Value']
      else:
        resolvedData[lfn] = result['Value']
        localSECount+=1

    #Report datasets that could not be downloaded
    report = ''
    if failedReplicas:
      report =  'The following LFN(s) could not be downloaded to the WN:\n'
      for lfn in failedReplicas:
        report+='%s\n' %(lfn)
        self.log.warn(report)

    if resolvedData:
      report = 'Successfully downloaded LFN(s):\n'
      for lfn,reps in resolvedData.items():
        report+='%s\n' %(lfn)
      totalLFNs = len(resolvedData.keys())
      report+='\nDownloaded %s / %s files from local Storage Elements on first attempt.' %(localSECount,totalLFNs)
      self.__setJobParam(COMPONENT_NAME,report)

    result = S_OK()
    result['Successful'] = resolvedData
    result['Failed'] = failedReplicas #lfn list to be passed to another resolution mechanism
    return result

  #############################################################################
  def __checkDiskSpace(self,totalSize):
    """Compare available disk space to the file size reported from the catalog
       result.
    """
    diskSpace = getDiskSpace() #MB
    availableBytes = diskSpace*1024*1024 #bytes
    #below can be a configuration option sent via the job wrapper in the future
    buffer = 3*1024*1024*1024 # 3GB in bytes
    if (buffer+totalSize) < availableBytes:
      msg = 'Enough disk space available (%s bytes)' %(availableBytes)
      self.log.verbose(msg)
      return S_OK(msg)
    else:
      msg = 'Not enough disk space available for input data download (%s > %s bytes)' %((buffer+totalSize),availableBytes)
      self.log.warn(msg)
      return S_ERROR(msg)

  #############################################################################
  def __getLFN(self,lfn,pfn,se,size,guid):
    """ Download a local copy of a single LFN from the specified Storage Element.
        This is used as a last resort to attempt to retrieve the file.  The Replica
        Manager will perform an LFC lookup to refresh the stored result.
    """
    self.log.verbose('Attempting to ReplicaManager.getFile for %s' %(lfn))
    result = self.rm.getFile(lfn)
    fileName = os.path.basename(pfn)
    self.log.verbose(result)
    if os.path.exists('%s/%s' %(os.getcwd(),fileName)):
      self.log.verbose('File %s exists in current directory' %(fileName))
      fileDict = {'turl':'Downloaded','protocol':'Downloaded','se':se,'pfn':pfn,'guid':guid}
      return S_OK(fileDict)
    else:
      self.log.warn('File does not exist in local directory after download')
      return S_ERROR('OK download result but file missing in current directory')

  #############################################################################
  def __getPFN(self,pfn,se,size,guid):
    """ Download a local copy of a single PFN from the specified Storage Element.
    """
    fileName = os.path.basename(pfn)
    if os.path.exists('%s/%s' %(os.getcwd(),fileName)):
      self.log.verbose('File already %s exists in current directory' %(fileName))
      fileDict = {'turl':'LocalData','protocol':'LocalData','se':se,'pfn':pfn,'guid':guid}
      return S_OK(fileDict)

    storageElement = StorageElement(se)
    if not storageElement.isValid()['Value']:
      return S_ERROR('Failed to instantiate StorageElement for: %s' %(se))

    result = storageElement.getFile(pfn,size)
    if not result['OK']:
      self.log.warn('Problem getting PFN %s with size %s bytes:\n%s' %(pfn,size,result))
      return result

    self.log.verbose(result)
    if os.path.exists('%s/%s' %(os.getcwd(),fileName)):
      self.log.verbose('File %s exists in current directory' %(fileName))
      fileDict = {'turl':'Downloaded','protocol':'Downloaded','se':se,'pfn':pfn,'guid':guid}
      return S_OK(fileDict)
    else:
      self.log.warn('File does not exist in local directory after download')
      return S_ERROR('OK download result but file missing in current directory')

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_ERROR('JobID not defined')

    jobReport = RPCClient('WorkloadManagement/JobStateUpdate',timeout=120)
    jobParam = jobReport.setJobParameter(int(self.jobID),str(name),str(value))
    self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#