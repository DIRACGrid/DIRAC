########################################################################
# $Id$
# File :   OnlineInputData.py
# Author : Ricardo Graciani
########################################################################

""" The Online Input module wraps access to Input Data fro the Online farm
    defined in the CS for the VO.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger

import os,sys,re,string

COMPONENT_NAME = 'OnlineInputData'

class OnlineInputData:

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

  #############################################################################
  def execute(self,dataToResolve=None):
    """This method is called to obtain the TURLs for all requested input data
       firstly by available site protocols and redundantly via TURL construction.
       If TURLs are missing these are conveyed in the result to
    """

    #Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']
    #Problematic files will be returned and can be handled by another module
    failedReplicas = []

    if dataToResolve:
      self.log.verbose('Data to resolve passed directly to InputDataByProtocol module')
      self.inputData = dataToResolve #e.g. list supplied by another module

    self.inputData = [x.replace('LFN:','') for x in self.inputData]

    self.log.info('InputData requirement is:')
    for i in self.inputData:
      self.log.info(i)
      # Make sure the files are available
      # we need to exclude the first "/" to get os.path.join to work
      localPath = os.path.join('/castorfs/cern.ch/grid/',i[1:])
      if not os.path.isfile( localPath ):
        self.log.error( 'Can not find Input Data: ', i )
        return S_ERROR( 'Can not find Input Data: %s' % i )
    
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
              pfn = os.path.join('/castorfs/cern.ch/grid/',lfn[1:])
              resolvedData[lfn] = {'pfn':pfn, 'se':se, 'guid':guid, 'turl':'file:%s' % pfn }

    result = S_OK()
    result['Successful'] = resolvedData
    result['Failed'] = failedReplicas #lfn list to be passed to another resolution mechanism
    return result
