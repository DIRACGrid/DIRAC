""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 

__RCSID__ = "$Id$"

import re, time, commands, random,os
import types

from DIRAC import S_OK, S_ERROR, gLogger,gConfig
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList

class DataIntegrityClient:

  def __init__(self):
    """ Constructor function.
    """
    pass

  ##########################################################################
  #
  # This section contains the specific methods for interacting with the IntegrityDB service
  #
  def removeProblematic(self,fileID):
    """ This removes the specified file ID from the integrity DB
    """
    if type(fileID) == ListType:
      fileIDs = fileID
    else:
      fileIDs = [int(fileID)]
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.removeProblematic(fileIDs)

  def getProblematic(self):
    """ Obtains a problematic file from the IntegrityDB based on the LastUpdate time
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.getProblematic()

  def getPrognosisProblematics(self,prognosis):
    """ Obtains all the problematics of a particular prognosis from the integrityDB
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.getPrognosisProblematics(prognosis)

  def getProblematicsSummary(self):
    """ Obtains a count of the number of problematics for each prognosis found
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.getProblematicsSummary()

  def getDistinctPrognosis(self):
    """ Obtains the distinct prognosis found in the integrityDB
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.getDistinctPrognosis()

  def getProductionProblematics(self,prodID):
    """ Obtains the problematics for a given production
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.getProductionProblematics(prodID)

  def incrementProblematicRetry(self,fileID):
    """ Increments the retry count for the supplied file ID
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.incrementProblematicRetry(fileID)

  def changeProblematicPrognosis(self,fileID,newPrognosis):
    """ Changes the prognosis of the supplied file to the new prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.changeProblematicPrognosis(fileID,newPrognosis)

  def setProblematicStatus(self,fileID,status):
    """ Updates the status of a problematic in the integrityDB
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    return integrityDB.setProblematicStatus(fileID,status)

  ##########################################################################
  #
  # This section contains the specific methods for BK->LFC checks
  #
  
  def productionToCatalog(self,productionID):
    """  This obtains the file information from the BK and checks these files are present in the LFC.
    """
    gLogger.info("-" * 40)
    gLogger.info("Performing the BK->LFC check")
    gLogger.info("-" * 40)
    res = self.__getProductionFiles(productionID)
    if not res['OK']:
      return res
    bkMetadata = res['Value']['BKMetadata']
    noReplicaFiles = res['Value']['GotReplicaNo']
    yesReplicaFiles = res['Value']['GotReplicaYes']
    # For the files marked as existing we perfom catalog check
    res = self.__getCatalogMetadata(yesReplicaFiles)
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    # Try and get the metadata for files that shouldn't exist in the catalog
    if noReplicaFiles:
      res = self.__checkCatalogForBKNoReplicas(noReplicaFiles)
      if not res['OK']:
        return res    
      catalogMetadata.update(res['Value'])
    # Get the replicas for the files found to exist in the catalog
    res = self.__getCatalogReplicas(catalogMetadata.keys())
    if not res['OK']:
      return res
    replicas = res['Value']
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    return S_OK(resDict)

  def __checkCatalogForBKNoReplicas(self,lfns):
    gLogger.info('Checking the catalog existence of %s files' % len(lfns))
    rm = ReplicaManager()
    res = rm.getCatalogFileMetadata(lfns)
    if not res['OK']:
      gLogger.error('Failed to get catalog metadata',res['Message'])
      return res
    allMetadata = res['Value']['Successful']
    existingCatalogFiles = allMetadata.keys()
    if existingCatalogFiles:
      self.__reportProblematicFiles(existingCatalogFiles,'BKReplicaNo')
    gLogger.info('Checking the catalog existence of files complete')
    return S_OK(allMetadata)

  def __getProductionFiles(self,productionID):
    """ This method queries the bookkeeping and obtains the file metadata for the given production
    """
    gLogger.info("Attempting to get files for production %s" % productionID)
    bk = RPCClient('Bookkeeping/BookkeepingManager')
    res = bk.getProductionFiles(productionID,'ALL')
    if not res['OK']:
      return res
    yesReplicaFiles = []
    noReplicaFiles = []
    badReplicaFiles = []
    badBKFileSize = []
    badBKGUID = []
    allMetadata = res['Value']
    gLogger.info("Obtained at total of %s files" % len(allMetadata.keys()))
    totalSize = 0
    for lfn,bkMetadata in allMetadata.items():
      if (bkMetadata['FileType'] != 'LOG'):
        if (bkMetadata['GotReplica'] == 'Yes'):
          yesReplicaFiles.append(lfn)
          if bkMetadata['FileSize']:
            totalSize+= long(bkMetadata['FileSize'])
        elif (bkMetadata['GotReplica'] == 'No'):
          noReplicaFiles.append(lfn)
        else:
          badReplicaFiles.append(lfn)
        if not bkMetadata['FileSize']:
          badBKFileSize.append(lfn)
        if not bkMetadata['GUID']:
          badBKGUID.append(lfn)
    if badReplicaFiles:
      self.__reportProblematicFiles(badReplicaFiles,'BKReplicaBad')
    if badBKFileSize:
      self.__reportProblematicFiles(badBKFileSize,'BKSizeBad')
    if badBKGUID:
      self.__reportProblematicFiles(badBKGUID,'BKGUIDBad')
    gLogger.info("%s files marked with replicas with total size %s bytes" % (len(yesReplicaFiles),totalSize))
    gLogger.info("%s files marked without replicas" % len(noReplicaFiles))
    resDict = {'BKMetadata':allMetadata,'GotReplicaYes':yesReplicaFiles,'GotReplicaNo':noReplicaFiles}
    return S_OK(resDict)

  ##########################################################################
  #
  # This section contains the specific methods for LFC->BK checks
  #

  def catalogDirectoryToBK(self,lfnDir):
    """ This obtains the replica and metadata information from the catalog for the supplied directory and checks against the BK.
    """
    gLogger.info("-" * 40)
    gLogger.info("Performing the LFC->BK check") 
    gLogger.info("-" * 40)
    if type(lfnDir) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getCatalogDirectoryContents(lfnDir)
    if not res['OK']:
      return res
    replicas = res['Value']['Replicas']
    catalogMetadata = res['Value']['Metadata']
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    if not catalogMetadata:
      return S_ERROR('No files found in directory')
    res = self.__checkBKFiles(replicas,catalogMetadata)
    if not res['OK']:
      return res       
    return S_OK(resDict)

  def catalogFileToBK(self,lfns):
    """ This obtains the replica and metadata information from the catalog and checks against the storage elements.
    """
    gLogger.info("-" * 40)
    gLogger.info("Performing the LFC->BK check")
    gLogger.info("-" * 40)
    if type(lfns) in types.StringTypes:
      lfns = [lfns]
    res = self.__getCatalogMetadata(lfns)
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    res = self.__getCatalogReplicas(catalogMetadata.keys())
    if not res['OK']:
      return res
    replicas = res['Value']
    res = self.__checkBKFiles(replicas,catalogMetadata)
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    return S_OK(resDict)

  def __checkBKFiles(self,replicas,catalogMetadata):
    """ This takes the supplied replica and catalog metadata information and ensures the files exist in the BK with the correct metadata.
    """
    gLogger.info('Checking the bookkeeping existence of %s files' % len(catalogMetadata))
    rm = ReplicaManager()
    res = rm.getCatalogFileMetadata(catalogMetadata.keys(),catalogs=['BookkeepingDB'])
    if not res['OK']:
      gLogger.error('Failed to get bookkeeping metadata',res['Message'])
      return res
    allMetadata = res['Value']['Successful']
    missingBKFiles = []
    sizeMismatchFiles = []
    guidMismatchFiles = []
    noBKReplicaFiles = []
    withBKReplicaFiles = []
    for lfn,error in res['Value']['Failed'].items():
      if re.search('No such file or directory',error):
        missingBKFiles.append(lfn)
    for lfn,bkMetadata in allMetadata.items():
      if not bkMetadata['FileSize'] == catalogMetadata[lfn]['Size']:
        sizeMismatchFiles.append(lfn)
      if not bkMetadata['GUID'] ==  catalogMetadata[lfn]['GUID']:
        guidMismatchFiles.append(lfn)
      gotReplica = bkMetadata['GotReplica'].lower()
      if (gotReplica == 'yes') and (not replicas.has_key(lfn)):
        withBKReplicaFiles.append(lfn)
      if (gotReplica != 'yes') and (replicas.has_key(lfn)):
        noBKReplicaFiles.append(lfn)
    if missingBKFiles:
      self.__reportProblematicFiles(missingBKFiles,'LFNBKMissing')
    if sizeMismatchFiles:
      self.__reportProblematicFiles(sizeMismatchFiles,'BKCatalogSizeMismatch')
    if guidMismatchFiles:
      self.__reportProblematicFiles(guidMismatchFiles,'BKCatalogGUIDMismatch')
    if withBKReplicaFiles:
      self.__reportProblematicFiles(withBKReplicaFiles,'BKReplicaYes')
    if noBKReplicaFiles:
      self.__reportProblematicFiles(noBKReplicaFiles,'BKReplicaNo')
    gLogger.info('Checking the bookkeeping existence of files complete')
    return S_OK(allMetadata)

  ##########################################################################
  #
  # This section contains the specific methods for LFC->SE checks
  #

  def catalogDirectoryToSE(self,lfnDir):
    """ This obtains the replica and metadata information from the catalog for the supplied directory and checks against the storage elements.
    """ 
    gLogger.info("-" * 40)
    gLogger.info("Performing the LFC->SE check")
    gLogger.info("-" * 40)
    if type(lfnDir) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getCatalogDirectoryContents(lfnDir)
    if not res['OK']:
      return res
    replicas = res['Value']['Replicas']
    catalogMetadata = res['Value']['Metadata']
    res = self.__checkPhysicalFiles(replicas,catalogMetadata)
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    return S_OK(resDict)

  def catalogFileToSE(self,lfns):
    """ This obtains the replica and metadata information from the catalog and checks against the storage elements.
    """
    gLogger.info("-" * 40)    
    gLogger.info("Performing the LFC->SE check")    
    gLogger.info("-" * 40)    
    if type(lfns) in types.StringTypes:
      lfns = [lfns]
    res = self.__getCatalogMetadata(lfns)
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    res = self.__getCatalogReplicas(catalogMetadata.keys())
    if not res['OK']:
      return res
    replicas = res['Value']
    res = self.__checkPhysicalFiles(replicas,catalogMetadata)
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    return S_OK(resDict)

  def checkPhysicalFiles(self,replicas,catalogMetadata,ses=[]):
    """ This obtains takes the supplied replica and metadata information obtained from the catalog and checks against the storage elements.
    """   
    gLogger.info("-" * 40)    
    gLogger.info("Performing the LFC->SE check")    
    gLogger.info("-" * 40)
    return self.__checkPhysicalFiles(replicas,catalogMetadata,ses=ses)

  def __checkPhysicalFiles(self,replicas,catalogMetadata,ses=[]):
    """ This obtains the physical file metadata and checks the metadata against the catalog entries
    """
    sePfns = {}
    pfnLfns = {}
    for lfn,replicaDict in replicas.items():
      for se,pfn in replicaDict.items():
        if (ses) and (se not in ses):
          continue
        if not sePfns.has_key(se):
          sePfns[se] = []
        sePfns[se].append(pfn)
        pfnLfns[pfn] = lfn
    gLogger.info('%s %s' % ('Storage Element'.ljust(20), 'Replicas'.rjust(20)))
    for site in sortList(sePfns.keys()):
      files = len(sePfns[site])
      gLogger.info('%s %s' % (site.ljust(20), str(files).rjust(20)))

    physicalFileMetadata = {}
    for se in sortList(sePfns.keys()):
      pfns = sePfns[se]
      pfnDict = {}
      for pfn in pfns:
        pfnDict[pfn] = pfnLfns[pfn]
      sizeMismatch = []
      res = self.__checkPhysicalFileMetadata(pfnDict,se)
      if not res['OK']:
        gLogger.error('Failed to get physical file metadata.', res['Message'])
        return res
      for pfn,metadata in res['Value'].items():
        if catalogMetadata.has_key(pfnLfns[pfn]):
          if (metadata['Size'] != catalogMetadata[pfnLfns[pfn]]['Size']) and (metadata['Size'] != 0):
            sizeMismatch.append((pfnLfns[pfn],pfn,se,'CatalogPFNSizeMismatch'))
      if sizeMismatch:
        self.__reportProblematicReplicas(sizeMismatch,se,'CatalogPFNSizeMismatch')
    return S_OK() 

  def __checkPhysicalFileMetadata(self,pfnLfns,se):
    """ Check obtain the physical file metadata and check the files are available
    """
    gLogger.info('Checking the integrity of %s physical files at %s' % (len(pfnLfns),se))
    rm = ReplicaManager()
    res = rm.getStorageFileMetadata(pfnLfns.keys(),se)
    if not res['OK']:
      gLogger.error('Failed to get metadata for pfns.', res['Message'])
      return res
    pfnMetadataDict = res['Value']['Successful']
    # If the replicas are completely missing
    missingReplicas = []
    for pfn,reason in res['Value']['Failed'].items():
      if re.search('File does not exist',reason):
        missingReplicas.append((pfnLfns[pfn],pfn,se,'PFNMissing'))
    if missingReplicas:
      self.__reportProblematicReplicas(missingReplicas,se,'PFNMissing')
    lostReplicas = []
    unavailableReplicas = []
    zeroSizeReplicas = []
    # If the files are not accessible
    for pfn,pfnMetadata in pfnMetadataDict.items():
      if pfnMetadata['Lost']:
        lostReplicas.append((pfnLfns[pfn],pfn,se,'PFNLost'))
      if pfnMetadata['Unavailable']:
        unavailableReplicas.append((pfnLfns[pfn],pfn,se,'PFNUnavailable'))
      if pfnMetadata['Size'] == 0:
        zeroSizeReplicas.append((pfnLfns[pfn],pfn,se,'PFNZeroSize'))
    if lostReplicas:
      self.__reportProblematicReplicas(lostReplicas,se,'PFNLost')       
    if unavailableReplicas:
      self.__reportProblematicReplicas(unavailableReplicas,se,'PFNUnavailable')  
    if zeroSizeReplicas:
      self.__reportProblematicReplicas(zeroSizeReplicas,se,'PFNZeroSize') 
    gLogger.info('Checking the integrity of physical files at %s complete' % se)
    return S_OK(pfnMetadataDict)

  ##########################################################################
  # 
  # This section contains the specific methods for SE->LFC checks
  #
  
  def storageDirectoryToCatalog(self,lfnDir,storageElement):
    """ This obtains the file found on the storage element in the supplied directories and determines whether they exist in the catalog and checks their metadata elements
    """
    gLogger.info("-" * 40)    
    gLogger.info("Performing the SE->LFC check at %s" % storageElement)    
    gLogger.info("-" * 40)     
    if type(lfnDir) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getStorageDirectoryContents(lfnDir,storageElement)
    if not res['OK']:
      return res
    storageFileMetadata = res['Value']
    if storageFileMetadata:
     return self.__checkCatalogForSEFiles(storageFileMetadata,storageElement)
    return S_OK({'CatalogMetadata':{},'StorageMetadata':{}})

  def __checkCatalogForSEFiles(self,storageMetadata,storageElement):
    gLogger.info('Checking %s storage files exist in the catalog' % len(storageMetadata))
    rm = ReplicaManager()
    # First get all the PFNs as they should be registered in the catalog
    res = rm.getPfnForProtocol(storageMetadata.keys(),storageElement,withPort=False)
    if not res['OK']:
      gLogger.error("Failed to get registered PFNs for physical files",res['Message'])
      return res
    for pfn, error in res['Value']['Failed'].items():
      gLogger.error('Failed to obtain registered PFN for physical file','%s %s' % (pfn,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to obtain registered PFNs from physical file')
    for original,registered in res['Value']['Successful'].items():
      storageMetadata[registered] = storageMetadata.pop(original)
    # Determine whether these PFNs are registered and if so obtain the LFN
    res = rm.getCatalogLFNForPFN(storageMetadata.keys())
    if not res['OK']:
      gLogger.error("Failed to get registered LFNs for PFNs",res['Message'])
      return res
    failedPfns = res['Value']['Failed']
    notRegisteredPfns = []
    for pfn,error in failedPfns.items():
      if re.search('No such file or directory',error):
        notRegisteredPfns.append((storageMetadata[pfn]['LFN'],pfn,storageElement,'PFNNotRegistered'))
        failedPfns.pop(pfn)
    if notRegisteredPfns:
      self.__reportProblematicReplicas(notRegisteredPfns,storageElement,'PFNNotRegistered')
    if failedPfns:
      return S_ERROR('Failed to obtain LFNs for PFNs')
    pfnLfns = res['Value']['Successful']
    for pfn in storageMetadata.keys():
      pfnMetadata = storageMetadata.pop(pfn)
      if pfn in pfnLfns.keys():
        lfn = pfnLfns[pfn]
        storageMetadata[lfn] = pfnMetadata
        storageMetadata[lfn]['PFN'] = pfn
    # For the LFNs found to be registered obtain the file metadata from the catalog and verify against the storage metadata 
    res = self.__getCatalogMetadata(storageMetadata.keys())
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    sizeMismatch = []
    for lfn,lfnCatalogMetadata in catalogMetadata.items():
      lfnStorageMetadata = storageMetadata[lfn]
      if (lfnStorageMetadata['Size'] != lfnCatalogMetadata['Size']) and (lfnStorageMetadata['Size'] != 0):
        sizeMismatch.append((lfn,lfnPfns[lfn],storageElement,'CatalogPFNSizeMismatch'))
    if sizeMismatch:
     self.__reportProblematicReplicas(sizeMismatch,storageElement,'CatalogPFNSizeMismatch')
    gLogger.info('Checking storage files exist in the catalog complete')
    resDict = {'CatalogMetadata':catalogMetadata,'StorageMetadata':storageMetadata}
    return S_OK(resDict)

  def getStorageDirectoryContents(self,lfnDir,storageElement):
    """ This obtains takes the supplied lfn directories and recursively obtains the files in the supplied storage element
    """
    return self.__getStorageDirectoryContents(lfnDir,storageElement)

  def __getStorageDirectoryContents(self,lfnDir,storageElement):
    """ Obtians the contents of the supplied directory on the storage
    """
    gLogger.info('Obtaining the contents for %s directories at %s' % (len(lfnDir),storageElement))
    rm = ReplicaManager()
    res = rm.getPfnForLfn(lfnDir,storageElement)
    if not res['OK']:
      gLogger.error("Failed to get PFNs for directories",res['Message'])
      return res
    for directory, error in res['Value']['Failed'].items():
      gLogger.error('Failed to obtain directory PFN from LFNs','%s %s' % (directory,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to obtain directory PFN from LFNs')
    storageDirectories = res['Value']['Successful'].values()
    res = rm.getStorageFileExists(storageDirectories,storageElement)
    if not res['OK']:
      gLogger.error("Failed to obtain existance of directories",res['Message'])
      return res
    for directory, error in res['Value']['Failed'].items():
       gLogger.error('Failed to determine existance of directory','%s %s' % (directory,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to determine existance of directory')
    directoryExists = res['Value']['Successful']
    activeDirs = []
    for directory in sortList(directoryExists.keys()):
      exists = directoryExists[directory]
      if exists:
        activeDirs.append(directory)
    allFiles = {}
    while len(activeDirs) > 0:
      currentDir = activeDirs[0]
      res = rm.getStorageListDirectory(currentDir,storageElement)
      activeDirs.remove(currentDir)
      if not res['OK']:
        gLogger.error('Failed to get directory contents',res['Message'])
        return res
      elif res['Value']['Failed'].has_key(currentDir):
        gLogger.error('Failed to get directory contents','%s %s' % (currentDir,res['Value']['Failed'][currentDir]))
        return S_ERROR(res['Value']['Failed'][currentDir])
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend(dirContents['SubDirs'])
        fileMetadata = dirContents['Files']
        res = rm.getLfnForPfn(fileMetadata.keys(),storageElement)
        if not res['OK']:
          gLogger.error('Failed to get directory content LFNs',res['Message'])
          return res
        for pfn,error in res['Value']['Failed'].items():
          gLogger.error("Failed to get LFN for PFN","%s %s" % (pfn,error)) 
        if res['Value']['Failed']:
          return S_ERROR("Failed to get LFNs for PFNs")
        pfnLfns = res['Value']['Successful']
        for pfn,lfn in pfnLfns.items():
          fileMetadata[pfn]['LFN'] = lfn
        allFiles.update(fileMetadata)
    zeroSizeFiles = []
    lostFiles = []  
    unavailableFiles = []
    for pfn in sortList(allFiles.keys()):
      if os.path.basename(pfn) == 'dirac_directory':
        allFiles.pop(pfn)
      else: 
        metadata = allFiles[pfn] 
        if metadata['Size'] == 0:
          zeroSizeFiles.append((metadata['LFN'],pfn,storageElement,'PFNZeroSize'))
        #if metadata['Lost']:
        #  lostFiles.append((metadata['LFN'],pfn,storageElement,'PFNLost'))
        #if metadata['Unavailable']:
        #  unavailableFiles.append((metadata['LFN'],pfn,storageElement,'PFNUnavailable'))
    if zeroSizeFiles:  
      self.__reportProblematicReplicas(zeroSizeFiles,storageElement,'PFNZeroSize')
    if lostFiles:  
      self.__reportProblematicReplicas(lostFiles,storageElement,'PFNLost')
    if unavailableFiles:
      self.__reportProblematicReplicas(unavailableFiles,storageElement,'PFNUnavailable')
    gLogger.info('Obtained at total of %s files for directories at %s' % (len(allFiles),storageElement))
    return S_OK(allFiles)

  def __getStoragePathExists(self,lfnPaths,storageElement):
    gLogger.info('Determining the existance of %d files at %s' % (len(lfnPaths),storageElement))
    rm = ReplicaManager()
    res = rm.getPfnForLfn(lfnPaths,storageElement)
    if not res['OK']:
      gLogger.error("Failed to get PFNs for LFNs",res['Message'])
      return res
    for lfnPath, error in res['Value']['Failed'].items():
      gLogger.error('Failed to obtain PFN from LFN','%s %s' % (lfnPath,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to obtain PFNs from LFNs')
    lfnPfns = res['Value']['Successful']
    pfnLfns = {}
    for lfn,pfn in lfnPfns.items():
      pfnLfns[pfn] = lfn
    res = rm.getStorageFileExists(pfnLfns.keys(),storageElement)
    if not res['OK']:
      gLogger.error("Failed to obtain existance of paths",res['Message'])
      return res
    for lfnPath, error in res['Value']['Failed'].items():
       gLogger.error('Failed to determine existance of path','%s %s' % (lfnPath,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to determine existance of paths')
    pathExists = res['Value']['Successful']
    resDict = {}
    for pfn,exists in pathExists.items():
      if exists:
        resDict[pfnLfns[pfn]] = pfn
    return S_OK(resDict)

  ##########################################################################
  #
  # This section contains the specific methods for obtaining replica and metadata information from the catalog
  #

  def __getCatalogDirectoryContents(self,lfnDir):
    """ Obtain the contents of the supplied directory 
    """
    gLogger.info('Obtaining the catalog contents for %s directories' % len(lfnDir))
    rm = ReplicaManager()
    activeDirs = lfnDir
    allFiles = {}
    while len(activeDirs) > 0:
      currentDir = activeDirs[0]
      res = rm.getCatalogListDirectory(currentDir)
      activeDirs.remove(currentDir)
      if not res['OK']:
        gLogger.error('Failed to get directory contents',res['Message'])
        return res
      elif res['Value']['Failed'].has_key(currentDir):
        gLogger.error('Failed to get directory contents','%s %s' % (currentDir,res['Value']['Failed'][currentDir]))
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend(dirContents['SubDirs'])
        allFiles.update(dirContents['Files'])

    zeroReplicaFiles = []
    zeroSizeFiles = []
    allReplicaDict = {}
    allMetadataDict = {}    
    for lfn,lfnDict in allFiles.items():
      lfnReplicas = {}
      for se,replicaDict in lfnDict['Replicas'].items():
        lfnReplicas[se] = replicaDict['PFN']
      if not lfnReplicas:
        zeroReplicaFiles.append(lfn)
      allReplicaDict[lfn] = lfnReplicas
      allMetadataDict[lfn] = lfnDict['MetaData']
      if lfnDict['MetaData']['Size'] == 0:
        zeroSizeFiles.append(lfn)
    if zeroReplicaFiles:
      self.__reportProblematicFiles(zeroReplicaFiles,'LFNZeroReplicas')
    if zeroSizeFiles:
      self.__reportProblematicFiles(zeroSizeFiles,'LFNZeroSize')
    gLogger.info('Obtained at total of %s files for the supplied directories' % len(allMetadataDict))
    resDict = {'Metadata':allMetadataDict,'Replicas':allReplicaDict}
    return S_OK(resDict)

  def __getCatalogReplicas(self,lfns):
    """ Obtain the file replicas from the catalog while checking that there are replicas
    """
    gLogger.info('Obtaining the replicas for %s files' % len(lfns))
    rm = ReplicaManager()
    zeroReplicaFiles = []
    res = rm.getCatalogReplicas(lfns,allStatus=True)
    if not res['OK']:
      gLogger.error('Failed to get catalog replicas',res['Message'])
      return res
    allReplicas = res['Value']['Successful']
    for lfn,error in res['Value']['Failed'].items():
      if re.search('File has zero replicas',error):
        zeroReplicaFiles.append(lfn)
    if zeroReplicaFiles:
      self.__reportProblematicFiles(zeroReplicaFiles,'LFNZeroReplicas')
    gLogger.info('Obtaining the replicas for files complete')
    return S_OK(allReplicas)

  def __getCatalogMetadata(self,lfns):
    """ Obtain the file metadata from the catalog while checking they exist
    """
    gLogger.info('Obtaining the catalog metadata for %s files' % len(lfns))
    rm = ReplicaManager()
    missingCatalogFiles = []
    zeroSizeFiles = []
    res = rm.getCatalogFileMetadata(lfns)
    if not res['OK']:
      gLogger.error('Failed to get catalog metadata',res['Message'])
      return res
    allMetadata = res['Value']['Successful']
    for lfn,error in res['Value']['Failed'].items():
      if re.search('No such file or directory',error):
        missingCatalogFiles.append(lfn)
    if missingCatalogFiles:
      self.__reportProblematicFiles(missingCatalogFiles,'LFNCatalogMissing')
    for lfn,metadata in allMetadata.items():
      if metadata['Size'] == 0:
        zeroSizeFiles.append(lfn)
    if zeroSizeFiles:
      self.__reportProblematicFiles(zeroSizeFiles,'LFNZeroSize')
    gLogger.info('Obtaining the catalog metadata complete')
    return S_OK(allMetadata)

  ##########################################################################
  #
  # This section contains the methods for inserting problematic files into the integrity DB
  #

  def __reportProblematicFiles(self,lfns,reason):
    """ Simple wrapper function around setFileProblematic """
    gLogger.info('The following %s files were found with %s' % (len(lfns),reason))
    for lfn in sortList(lfns):     
      gLogger.info(lfn)
    res = self.setFileProblematic(lfns,reason,sourceComponent='DataIntegrityClient')
    if not res['OK']:
      gLogger.info('Failed to update integrity DB with files',res['Message'])
    else:
      gLogger.info('Successfully updated integrity DB with files')

  def setFileProblematic(self,lfn,reason,sourceComponent=''):
    """ This method updates the status of the file in the FileCatalog and the IntegrityDB
        
        lfn - the lfn of the file
        reason - this is given to the integrity DB and should reflect the problem observed with the file
        
        sourceComponent is the component issuing the request.
    """  
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "DataIntegrityClient.setFileProblematic: Supplied file info must be list or a single LFN."
      gLogger.error(errStr) 
      return S_ERROR(errStr)
    gLogger.info("DataIntegrityClient.setFileProblematic: Attempting to update %s files." % len(lfns))
    successful = {}
    failed = {}
    fileMetadata = {}
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    for lfn in lfns:
      fileMetadata[lfn] = {'Prognosis':reason,'LFN':lfn,'PFN':'','SE':''}
    res = integrityDB.insertProblematic(sourceComponent,fileMetadata)
    if not res['OK']:
      gLogger.error("DataIntegrityClient.setReplicaProblematic: Failed to insert problematics to integrity DB")
    return res

  def __reportProblematicReplicas(self,replicaTuple,se,reason):
    """ Simple wrapper function around setReplicaProblematic """
    gLogger.info('The following %s files had %s at %s' % (len(replicaTuple),reason,se))
    for lfn,pfn,se,reason in sortList(replicaTuple):
      if lfn:
        gLogger.info(lfn)
      else:
        gLogger.info(pfn)
    res = self.setReplicaProblematic(replicaTuple,sourceComponent='DataIntegrityClient')
    if not res['OK']:
      gLogger.info('Failed to update integrity DB with replicas',res['Message'])
    else:
      gLogger.info('Successfully updated integrity DB with replicas')

  def setReplicaProblematic(self,replicaTuple,sourceComponent=''):
    """ This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaDict should be of the form {lfn :{'PFN':pfn,'SE':se,'Prognosis':prognosis}

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if type(replicaTuple) == types.TupleType:
     replicaTuple = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      pass
    else:
      errStr = "DataIntegrityClient.setReplicaProblematic: Supplied replica info must be a tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("DataIntegrityClient.setReplicaProblematic: Attempting to update %s replicas." % len(replicaTuple))
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    replicaDict = {}
    for lfn,pfn,se,reason in replicaTuple:
      replicaDict[lfn] = {'Prognosis':reason,'LFN':lfn,'PFN':pfn,'SE':se}
    res = integrityDB.insertProblematic(sourceComponent,replicaDict)
    if not res['OK']:
      gLogger.error("DataIntegrityClient.setReplicaProblematic: Failed to insert problematic to integrity DB")
      return res
    for lfn in replicaDict.keys():
      replicaDict[lfn]['Status'] = 'Problematic'
    rm = ReplicaManager()
    res = rm.setCatalogReplicaStatus(replicaDict)
    if not res['OK']:
      errStr = "DataIntegrityClient.setReplicaProblematic: Completely failed to update replicas."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  ##########################################################################
  #
  # This section contains the resolution methods for various prognoses
  #

  def __updateCompletedFiles(self,prognosis,fileID):
    gLogger.info("%s file (%d) is resolved" % (prognosis,fileID))
    #return integrityDB.removeProblematic(fileID)
    return self.setProblematicStatus(fileID,'Resolved')

  def __returnProblematicError(self,fileID,res):
    self.incrementProblematicRetry(fileID)
    gLogger.error(res['Message'])
    return res

  def __getRegisteredPFNLFN(self,pfn,storageElement):
    rm = ReplicaManager()
    res = rm.getPfnForProtocol([pfn],storageElement,withPort=False)
    if not res['OK']:
      gLogger.error("Failed to get registered PFN for physical files",res['Message'])
      return res
    for pfn, error in res['Value']['Failed'].items():
      gLogger.error('Failed to obtain registered PFN for physical file','%s %s' % (pfn,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to obtain registered PFNs from physical file')
    registeredPFN = res['Value']['Successful'][pfn]
    res = rm.getCatalogLFNForPFN(registeredPFN,singleFile=True)
    if (not res['OK']) and re.search('No such file or directory',res['Message']):
      return S_OK(False)
    return S_OK(res['Value'])

  def __updateReplicaToChecked(self,problematicDict):
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    prognosis = problematicDict['Prognosis']
    problematicDict['Status'] = 'Checked'
    rm = ReplicaManager()
    res = rm.setCatalogReplicaStatus({lfn:problematicDict},singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    gLogger.info("%s replica (%d) is updated to Checked status" % (prognosis,fileID))
    return self.__updateCompletedFiles(prognosis,fileID)

  def resolveCatalogPFNSizeMismatch(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the CatalogPFNSizeMismatch prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    pfn = problematicDict['PFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogFileSize(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    catalogSize = res['Value']
    res = rm.getStorageFileSize(pfn,se,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    storageSize = res['Value']
    res = rm.getCatalogFileSize(lfn,singleFile=True,catalogs=['BookkeepingDB'])
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    bookkeepingSize = res['Value']
    if bookkeepingSize == catalogSize == storageSize:
      gLogger.info("CatalogPFNSizeMismatch replica (%d) matched all registered sizes." % fileID)
      return self.__updateReplicaToChecked(problematicDict)
    if (catalogSize == bookkeepingSize):
      gLogger.info("CatalogPFNSizeMismatch replica (%d) found to mismatch the bookkeeping also" % fileID)
      res = rm.getCatalogReplicas(lfn,singleFile=True)
      if not res['OK']:
        return self.__returnProblematicError(fileID,res)
      if len(res['Value']) <= 1:
        gLogger.info("CatalogPFNSizeMismatch replica (%d) has no other replicas." % fileID)
        return S_ERROR("Not removing catalog file mismatch since the only replica")
      else:
        gLogger.info("CatalogPFNSizeMismatch replica (%d) has other replicas. Removing..." % fileID)
        res = rm.removeReplica(se,lfn)
        if not res['OK']:
          return self.__returnProblematicError(fileID,res)
        return self.__updateCompletedFiles('CatalogPFNSizeMismatch',fileID)
    if (catalogSize != bookkeepingSize) and (bookkeepingSize == storageSize):
      gLogger.info("CatalogPFNSizeMismatch replica (%d) found to match the bookkeeping size" % fileID)  
      res = self.__updateReplicaToChecked(problematicDict)
      if not res['OK']:
        return self.__returnProblematicError(fileID,res)
      return self.changeProblematicPrognosis(fileID,'BKCatalogSizeMismatch')
    gLogger.info("CatalogPFNSizeMismatch replica (%d) all sizes found mismatch. Updating retry count" % fileID)
    return self.incrementProblematicRetry(fileID)

  def resolvePFNNotRegistered(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNNotRegistered prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    pfn = problematicDict['PFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if not res['Value']:
      # The file does not exist in the catalog
      res = rm.removeStorageFile(pfn,se,singleFile=True)
      if not res['OK']:
        return self.__returnProblematicError(fileID,res)
      return self.__updateCompletedFiles('PFNNotRegistered',fileID)
    res = rm.getStorageFileMetadata(pfn,se,singleFile=True)
    if (not res['OK']) and (re.search('File does not exist',res['Message'])):
      gLogger.info("PFNNotRegistered replica (%d) found to be missing." % fileID)
      return self.__updateCompletedFiles('PFNNotRegistered',fileID)
    elif not res['OK']: 
      return self.__returnProblematicError(fileID,res)
    storageMetadata = res['Value']
    if storageMetadata['Lost']:
      gLogger.info("PFNNotRegistered replica (%d) found to be Lost. Updating prognosis" % fileID)
      return self.changeProblematicPrognosis(fileID,'PFNLost')
    if storageMetadata['Unavailable']:
      gLogger.info("PFNNotRegistered replica (%d) found to be Unavailable. Updating retry count" % fileID)
      return self.incrementProblematicRetry(fileID) 
     
    # HACK until we can obtain the space token descriptions through GFAL
    site = se.split('_')[0].split('-')[0]
    if not storageMetadata['Cached']:
      if lfn.endswith('.raw'):
        se = '%s-RAW' % site
      else:
        se = '%s-RDST' % site
    elif storageMetadata['Migrated']:
      if lfn.startswith('/lhcb/data'):
        se = '%s_M-DST' % site
      else:
        se = '%s_MC_M-DST' % site
    else:
      if lfn.startswith('/lhcb/data'):
        se = '%s-DST' % site
      else:
        se = '%s_MC-DST' % site

    problematicDict['SE'] = se
    res = rm.getPfnForProtocol([pfn],se,withPort=False)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    for pfn, error in res['Value']['Failed'].items():
      gLogger.error('Failed to obtain registered PFN for physical file','%s %s' % (pfn,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to obtain registered PFNs from physical file')
    problematicDict['PFN'] = res['Value']['Successful'][pfn]
    res = rm.addCatalogReplica({lfn:problematicDict},singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    res = rm.getCatalogFileMetadata(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if res['Value']['Size'] != storageMetadata['Size']:
      gLogger.info("PFNNotRegistered replica (%d) found with catalog size mismatch. Updating prognosis" % fileID)
      return self.changeProblematicPrognosis(fileID,'CatalogPFNSizeMismatch')
    return self.__updateCompletedFiles('PFNNotRegistered',fileID)

  def resolveLFNCatalogMissing(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the LFNCatalogMissing prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if res['Value']:
      return self.__updateCompletedFiles('LFNCatalogMissing',fileID)
    # Remove the file from all catalogs
    res = rm.removeCatalogFile(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    return self.__updateCompletedFiles('LFNCatalogMissing',fileID)

  def resolvePFNMissing(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNMissing prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    pfn = problematicDict['PFN']
    se = problematicDict['SE']
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if not res['Value']:
      gLogger.info("PFNMissing file (%d) no longer exists in catalog" % fileID)
      return self.__updateCompletedFiles('PFNMissing',fileID)
    res = rm.getStorageFileExists(pfn,se,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if res['Value']:
      gLogger.info("PFNMissing replica (%d) is no longer missing" % fileID)
      return self.__updateReplicaToChecked(problematicDict)
    gLogger.info("PFNMissing replica (%d) does not exist" % fileID)
    res = rm.getCatalogReplicas(lfn,allStatus=True,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    replicas = res['Value']
    seSite = se.split('_')[0].split('-')[0]
    found = False
    print replicas
    for replicaSE in replicas.keys():
      if re.search(seSite,replicaSE):
        found = True
        problematicDict['SE'] = replicaSE
        se = replicaSE
    if not found:
      gLogger.info("PFNMissing replica (%d) is no longer registered at SE. Resolved." % fileID)
      return self.__updateCompletedFiles('PFNMissing',fileID)
    gLogger.info("PFNMissing replica (%d) does not exist. Removing from catalog..." % fileID)
    res = rm.removeCatalogReplica({lfn:problematicDict},singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if len(replicas) == 1:
      gLogger.info("PFNMissing replica (%d) had a single replica. Updating prognosis" % fileID)
      return self.changeProblematicPrognosis(fileID,'LFNZeroReplicas')
    res = rm.replicateAndRegister(problematicDict['LFN'],se)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    # If we get here the problem is solved so we can update the integrityDB
    return self.__updateCompletedFiles('PFNMissing',fileID)

  def resolvePFNUnavailable(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNUnavailable prognosis
    """
    pfn = problematicDict['PFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getStorageFileMetadata(pfn,se,singleFile=True)
    if (not res['OK']) and (re.search('File does not exist',res['Message'])):
      # The file is no longer Unavailable but has now dissapeared completely
      gLogger.info("PFNUnavailable replica (%d) found to be missing. Updating prognosis" % fileID)
      return self.changeProblematicPrognosis(fileID,'PFNMissing')
    if (not res['OK']) or res['Value']['Unavailable']:
      gLogger.info("PFNUnavailable replica (%d) found to still be Unavailable" % fileID)
      return self.incrementProblematicRetry(fileID)
    if res['Value']['Lost']:
      gLogger.info("PFNUnavailable replica (%d) is now found to be Lost. Updating prognosis" % fileID)
      return self.changeProblematicPrognosis(fileID,'PFNLost')
    gLogger.info("PFNUnavailable replica (%d) is no longer Unavailable" % fileID) 
    # Need to make the replica okay in the Catalog
    return self.__updateReplicaToChecked(problematicDict)

  def resolveBKReplicaYes(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaYes prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    removeBKFile = False
    # If the file does not exist in the catalog
    if not res['Value']:
      gLogger.info("BKReplicaYes file (%d) does not exist in the catalog. Removing..." % fileID)
      removeBKFile = True
    else:
      gLogger.info("BKReplicaYes file (%d) found to exist in the catalog" % fileID)
      # If the file has no replicas in the catalog
      res = rm.getCatalogReplicas(lfn,singleFile=True)
      if (not res['OK']) and (res['Message'] == 'File has zero replicas'):
        gLogger.info("BKReplicaYes file (%d) found to exist without replicas. Removing..." % fileID)
        removeBKFile = True
    if removeBKFile:
      # Remove the file from the BK because it does not exist
      res = rm.removeCatalogFile(lfn,singleFile=True,catalogs=['BookkeepingDB'])
      if not res['OK']:
        return self.__returnProblematicError(fileID,res)
      gLogger.info("BKReplicaYes file (%d) removed from bookkeeping" % fileID)
    return self.__updateCompletedFiles('BKReplicaYes',fileID)

  def resolveBKReplicaNo(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaNo prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    # If the file exists in the catalog
    if not res['Value']:
      return self.__updateCompletedFiles('BKReplicaNo',fileID)
    gLogger.info("BKReplicaNo file (%d) found to exist in the catalog" % fileID)
    # and has available replicas
    res = rm.getCatalogReplicas(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if not res['Value']:
      gLogger.info("BKReplicaNo file (%d) found to have no replicas" % fileID)
      return self.changeProblematicPrognosis(fileID,'LFNZeroReplicas')
    gLogger.info("BKReplicaNo file (%d) found to have replicas" % fileID)
    res = rm.addCatalogFile(lfn,singleFile=True,catalogs=['BookkeepingDB'])
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    return self.__updateCompletedFiles('BKReplicaNo',fileID)
   
  def resolvePFNZeroSize(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolves the PFNZeroSize prognosis
    """
    pfn = problematicDict['PFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getStorageFileSize(pfn,se,singleFile=True)
    if (not res['OK']) and (re.search('File does not exist',res['Message'])):
      gLogger.info("PFNZeroSize replica (%d) found to be missing. Updating prognosis" % problematicDict['FileID'])
      return self.changeProblematicPrognosis(fileID,'PFNMissing')
    storageSize = res['Value']
    if storageSize == 0:
      res = rm.removeStorageFile(pfn,se,singleFile=True)
      if not res['OK']:
        return self.__returnProblematicError(fileID,res)
      gLogger.info("PFNZeroSize replica (%d) removed. Updating prognosis" % problematicDict['FileID'])
      return self.changeProblematicPrognosis(fileID,'PFNMissing')
    res = self.__getRegisteredPFNLFN(pfn,se)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    lfn = res['Value']
    if not lfn:
      gLogger.info("PFNZeroSize replica (%d) not registered in catalog. Updating prognosis" % problematicDict['FileID'])
      return self.changeProblematicPrognosis(fileID,'PFNNotRegistered')
    rm = res.getCatalogMetadata(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    catalogSize = res['Value']['Size']
    if catalogSize != storageSize:
      gLogger.info("PFNZeroSize replica (%d) size found to differ from registered metadata. Updating prognosis" % problematicDict['FileID']) 
      return self.changeProblematicPrognosis(fileID,'CatalogPFNSizeMismatch')
    return self.__updateCompletedFiles('PFNZeroSize',fileID)

  ############################################################################################
     
  def resolveLFNZeroReplicas(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolves the LFNZeroReplicas prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    rm = ReplicaManager()
    res = rm.getCatalogReplicas(lfn,allStatus=True,singleFile=True)
    if res['OK'] and res['Value']:
      gLogger.info("LFNZeroReplicas file (%d) found to have replicas" % fileID)
    else:
      gLogger.info("LFNZeroReplicas file (%d) does not have replicas. Checking storage..." % fileID)
      pfnsFound = False
      for storageElementName in sortList(gConfig.getValue('Resources/StorageElementGroups/Tier1_MC_M-DST',[])):
        res = self.__getStoragePathExists([lfn],storageElementName)
        if res['Value'].has_key(lfn):
          gLogger.info("LFNZeroReplicas file (%d) found storage file at %s" % (fileID,storageElementName))
          pfn = res['Value'][lfn]
          self.__reportProblematicReplicas([(lfn,pfn,storageElementName,'PFNNotRegistered')],storageElementName,'PFNNotRegistered')
          pfnsFound = True
      if not pfnsFound:
        gLogger.info("LFNZeroReplicas file (%d) did not have storage files. Removing..." % fileID)
        res = rm.removeCatalogFile(lfn,singleFile=True)
        if not res['OK']:
          gLogger.error(res['Message'])  
          # Increment the number of retries for this file
          integrityDB.incrementProblematicRetry(fileID)
          return res
        gLogger.info("LFNZeroReplicas file (%d) removed from catalog" % fileID)
    # If we get here the problem is solved so we can update the integrityDB
    #return integrityDB.removeProblematic(fileID)   
    return self.__updateCompletedFiles('LFNZeroReplicas',fileID)
