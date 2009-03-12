########################################################################
# $Id: InputDataByProtocol.py,v 1.12 2009/03/12 08:50:10 paterson Exp $
# File :   InputDataByProtocol.py
# Author : Stuart Paterson
########################################################################

""" The Input Data By Protocol module wraps around the Replica Management
    components to provide access to datasets by available site protocols as
    defined in the CS for the VO.
"""

__RCSID__ = "$Id: InputDataByProtocol.py,v 1.12 2009/03/12 08:50:10 paterson Exp $"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger

import os,sys,re,string

COMPONENT_NAME = 'InputDataByProtocol'

class InputDataByProtocol:

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
    """This method is called to obtain the TURLs for all requested input data
       firstly by available site protocols and redundantly via TURL construction.
       If TURLs are missing these are conveyed in the result to
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
      self.log.verbose('Data to resolve passed directly to InputDataByProtocol module')
      self.inputData = dataToResolve #e.g. list supplied by another module

    self.inputData = [x.replace('LFN:','') for x in self.inputData]
    self.log.info('InputData requirement to be resolved by protocol is:')
    for i in self.inputData:
      self.log.verbose(i)

    #First make a check in case replicas have been removed or are not accessible
    #from the local site (remove these from consideration for local protocols)
    replicas = self.fileCatalogResult['Value']['Successful']
    self.log.verbose('File Catalogue result is:')
    self.log.verbose(replicas)
    for lfn,reps in replicas.items():
      localReplica = False
      for localSE in localSEList:
        if reps.has_key(localSE):
          localReplica = True
      if not localReplica:
        failedReplicas.append(lfn)

    #Check that all LFNs have at least one replica and GUID
    if failedReplicas:
      #in principle this is not a failure but depends on the policy of the VO
      #datasets could be downloaded from another site
      self.log.info('The following file(s) were found not to have replicas for available LocalSEs:\n%s' %(string.join(failedReplicas,',\n')))

    #For the unlikely case that a file is found on two SEs at the same site
    #disk-based replicas are favoured.
    newReplicasDict = {}
    for lfn,reps in replicas.items():
      localStorage = {}
      localTapeSE = ''
      if not lfn in failedReplicas:
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
            newReplicasDict[lfn] = {se:pfn}

        if not newReplicasDict.has_key(lfn) and localTapeSE:
          pfn = replicas[lfn][localTapeSE]
          newReplicasDict[lfn] = {localTapeSE:pfn}

    replicas = newReplicasDict

    #Need to group files by SE in order to stage optimally
    #we know from above that all remaining files have a replica
    #(preferring disk if >1) in the local storage.
    #IMPORTANT, only add replicas for input data that is requested
    #since this module could have been executed after another.
    seFilesDict = {}
    trackLFNs = {}
    for localSE in localSEList:
      for lfn,reps in replicas.items():
        if lfn in self.inputData:
          if reps.has_key(localSE):
            pfn = reps[localSE]
            if seFilesDict.has_key(localSE):
              currentFiles = seFilesDict[localSE]
              if not lfn in trackLFNs.keys():
                currentFiles.append(pfn)
              seFilesDict[localSE] = currentFiles
              trackLFNs[lfn] = pfn
            else:
              seFilesDict[localSE] = [pfn]
              trackLFNs[lfn] = pfn

    self.log.verbose('Files grouped by LocalSE are:')
    self.log.verbose(seFilesDict)
    for se,pfnList in seFilesDict.items():
      seTotal = len(pfnList)
      self.log.info(' %s SURLs found from catalog for LocalSE %s' %(seTotal,se))
      for pfn in pfnList:
        self.log.info('%s %s' % (se,pfn))

    #Construct resolvedData dictionary with final metadata for selected replicas
    #knowing each file has one site replica from above
    resolvedData = {}
    for lfn,reps in replicas.items():
      for se,rep in reps.items():
        if seFilesDict.has_key(se):
          pfnList = seFilesDict[se]
          for pfn in pfnList:
            if rep == pfn:
              guid = self.fileCatalogResult['Value']['Successful'][lfn]['GUID']
              resolvedData[lfn] = {'pfn':pfn,'se':se,'guid':guid}

    #Can now start to obtain TURLs for files grouped by localSE
    #for requested input data
    for se,pfnList in seFilesDict.items():
      result = self.rm.getPhysicalFileAccessUrl(pfnList,se)
      self.log.debug(result)
      if not result['OK']:
        self.log.warn(result['Message'])
        return result

      badTURLCount = 0
      badTURLs = []
      seResult = result['Value']

      if seResult.has_key('Failed'):
        for pfn,cause in seResult['Failed'].items():
          badTURLCount+=1
          badTURLs.append('Failed to obtain TURL for %s\n Problem: %s' %(pfn,cause))
          for lfn,reps in replicas.items():
            for se,rep in reps.items():
              if rep == pfn:
                if not lfn in failedReplicas:
                  failedReplicas.append(lfn)

      if badTURLCount:
        self.log.warn('Found %s problematic TURL(s) for job %s' % (badTURLCount,self.jobID))
        param = string.join(badTURLs,'\n')
        self.log.info(param)
        result = self.__setJobParam('ProblematicTURLs',param)
        if not result['OK']:
          self.log.warn(result)

      pfnTurlDict = seResult['Successful']
      for lfn,reps in replicas.items():
        if lfn in self.inputData:
          for se,rep in reps.items():
            for pfn in pfnTurlDict.keys():
              if rep == pfn:
                turl = pfnTurlDict[pfn]
                resolvedData[lfn]['turl'] = turl
                self.log.info('Resolved input data\n>>>> SE: %s\n>>>>LFN: %s\n>>>>PFN: %s\n>>>>TURL: %s' %(se,lfn,pfn,turl))

    self.log.verbose(resolvedData)
    for lfn,mdata in resolvedData.items():
      if not mdata.has_key('turl'):
        self.log.verbose('%s: No TURL resolved for %s' %(COMPONENT_NAME,lfn))

    #Remove any failed replicas from the resolvedData dictionary
    if failedReplicas:
      self.log.verbose('The following LFN(s) were not resolved by protocol:\n%s' %(string.join(failedReplicas,'\n')))
      for lfn in failedReplicas:
        if resolvedData.has_key(lfn):
          del resolvedData[lfn]

    result = S_OK()
    result['Successful'] = resolvedData
    result['Failed'] = failedReplicas #lfn list to be passed to another resolution mechanism
    return result

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
