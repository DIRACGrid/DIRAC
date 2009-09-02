""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 

__RCSID__ = "$Id: DataIntegrityClient.py,v 1.9 2009/09/02 21:15:31 acsmith Exp $"

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
          if bkMetadata['FilesSize']:
            totalSize+= long(bkMetadata['FilesSize'])
        elif (bkMetadata['GotReplica'] == 'No'):
          noReplicaFiles.append(lfn)
        else:
          badReplicaFiles.append(lfn)
        if not bkMetadata['FilesSize']:
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
      self.__reportProblematicFiles(missingBKFiles,'LFNMissingBK')
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

  def checkPhysicalFiles(self,replicas,catalogMetadata):
    """ This obtains takes the supplied replica and metadata information obtained from the catalog and checks against the storage elements.
    """   
    gLogger.info("-" * 40)    
    gLogger.info("Performing the LFC->SE check")    
    gLogger.info("-" * 40)    
    return self.__checkPhysicalFiles(replicas,catalogMetadata)

  def __checkPhysicalFiles(self,replicas,catalogMetadata):
    """ This obtains the physical file metadata and checks the metadata against the catalog entries
    """
    sePfns = {}
    pfnLfns = {}
    for lfn,replicaDict in replicas.items():
      for se,pfn in replicaDict.items():
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
          if not metadata['Size'] == catalogMetadata[pfnLfns[pfn]]['Size']:
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
      gLoger.error('Failed to get metadata for pfns.', res['Message'])
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
    # If the files are not accessible
    for pfn,pfnMetadata in pfnMetadataDict.items():
      if pfnMetadata['Lost']:
        lostReplicas.append((pfnLfns[pfn],pfn,se,'PFNLost'))
      if pfnMetadata['Unavailable']:
        unavailableReplicas.append((pfnLfns[pfn],pfn,se,'PFNUnavailable'))
    if lostReplicas:
      self.__reportProblematicReplicas(lostReplicas,se,'PFNLost')       
    if unavailableReplicas:
      self.__reportProblematicReplicas(unavailableReplicas,se,'PFNUnavailable')  
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
    return self.__checkCatalogForSEFiles(storageFileMetadata,storageElement)

  def __checkCatalogForSEFiles(self,storageMetadata,storageElement):
    gLogger.info('Checking %s storage files exist in the catalog' % len(storageMetadata))
    rm = ReplicaManager()
    # First get all the PFNs as they should be registered in the catalog
    res = rm.getPfnForProtocol(storageMetadata.keys(),storageElement,withPort=False)
    if not res['OK']:
      gLogger.error("Failed to get registered PFNs for physical files",res['Message'])
      return res
    for pfn, error in res['Value']['Failed']:
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
        notRegisteredPfns.append(('',pfn,storageElement,'PFNNotRegistered'))
        failedPfns.pop(pfn)
    if notRegisteredPfns:
      self.__reportProblematicReplicas(notRegisteredPfns,storageElement,'PFNNotRegistered')
    if failedPfns:
      return S_ERROR('Failed to obtain LFNs for PFNs')
    pfnLfns = res['Value']['Successful']
    for pfn,lfn in pfnLfns.items():
      storageMetadata[lfn] = storageMetadata.pop(pfn)
      storageMetadata[lfn]['PFN'] = pfn
    # For the LFNs found to be registered obtain the file metadata from the catalog and verify against the storage metadata 
    res = self.__getCatalogMetadata(storageMetadata.keys())
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    sizeMismatch = []
    for lfn,lfnCatalogMetadata in catalogMetadata.items():
      lfnStorageMetadata = storageMetadata[lfn]
      if lfnStorageMetadata['Size'] != lfnCatalogMetadata['Size']:
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
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend(dirContents['SubDirs'])
        allFiles.update(dirContents['Files'])
    zeroSizeFiles = []
    for pfn in sortList(allFiles.keys()):
      if os.path.basename(pfn) == 'dirac_directory':
        allFiles.pop(pfn)
      else: 
        metadata = allFiles[pfn] 
        if metadata['Size'] == 0:
          zeroSizeFiles.append(('',pfn),storageElementName,'PFNZeroSize')
    if zeroSizeFiles:  
      self.__reportProblematicReplicas(zeroSizeFiles,se,'PFNZeroSize')
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
    if zeroReplicaFiles:
      self.__reportProblematicFiles(zeroReplicaFiles,'LFNZeroReplicas')
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
    gLogger.info('Checking the catalog existence of %s files' % len(lfns))
    rm = ReplicaManager()
    missingCatalogFiles = []
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
    gLogger.info('Checking the catalog existence of files complete')
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
      gLogger.info(lfn)
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

  def resolveLFNZeroReplicas(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the LFNZeroReplicas prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    rm = ReplicaManager()
    res = rm.getCatalogReplicas(lfn,allStatus=True,singleFile=True)
    if res['OK'] and res['Value']:
      gLogger.info("LFNZeroReplicas file (%d) found to have replicas" % problematicDict['FileID'])
    else:
      gLogger.info("LFNZeroReplicas file (%d) does not have replicas. Checking storage..." % problematicDict['FileID'])
      pfnsFound = False
      for storageElementName in sortList(gConfig.getValue('Resources/StorageElementGroups/Tier1_MC_M-DST',[])):
        res = self.__getStoragePathExists([lfn],storageElementName)
        if res['Value'].has_key(lfn):
          gLogger.info("LFNZeroReplicas file (%d) found storage file at %s" % (problematicDict['FileID'],storageElementName))
          pfn = res['Value'][lfn]
          self.__reportProblematicReplicas([(lfn,pfn,storageElementName,'PFNNotRegistered')],storageElementName,'PFNNotRegistered')
          pfnsFound = True
      if not pfnsFound:
        gLogger.info("LFNZeroReplicas file (%d) did not have storage files. Removing..." % problematicDict['FileID'])
        res = rm.removeCatalogFile(lfn,singleFile=True)
        if not res['OK']:
          gLogger.error(res['Message'])  
          # Increment the number of retries for this file
          integrityDB.incrementProblematicRetry(problematicDict['FileID'])
          return res
        gLogger.info("LFNZeroReplicas file (%d) removed from catalog" % problematicDict['FileID'])
    gLogger.info("LFNZeroReplicas file (%d) is resolved" % problematicDict['FileID'])
    # If we get here the problem is solved so we can update the integrityDB
    return integrityDB.removeProblematic(problematicDict['FileID'])   
  
  def resolvePFNMissing(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNMissing prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    pfn = problematicDict['PFN']  
    se = problematicDict['SE']
    rm = ReplicaManager()
    res = rm.getStorageFileExists(pfn,se,singleFile=True)
    if not res['OK']:
      gLogger.error(res['Message'])
      # Increment the number of retries for this file
      integrityDB.incrementProblematicRetry(problematicDict['FileID'])
      return res
    if res['Value']:
      gLogger.info("PFNMissing replica (%d) is no longer missing" % problematicDict['FileID'])
      problematicDict['Status'] = 'Checked'
      res = rm.setCatalogReplicaStatus({problematicDict['LFN']:problematicDict},singleFile=True)
      if not res['OK']:
        gLogger.error(res['Message'])
        # Increment the number of retries for this file
        integrityDB.incrementProblematicRetry(problematicDict['FileID'])
        return res
      gLogger.info("PFNUnavailable replica (%d) is updated to Checked status" % problematicDict['FileID'])
    else:
      res = rm.getCatalogReplicas(problematicDict['LFN'],singleFile=True)
      if not res['OK']:
        gLogger.error(res['Message'])
        integrityDB.incrementProblematicRetry(problematicDict['FileID'])
        return res 
      replicas = res['Value']
      if len(replicas) > 1:
        gLogger.info("PFNMissing replica (%d) does not exist. Removing..." % problematicDict['FileID'])
        res = rm.removeCatalogReplica({problematicDict['LFN']:problematicDict},singleFile=True)
        if not res['OK']:
          gLogger.error(res['Message'])
          integrityDB.incrementProblematicRetry(problematicDict['FileID'])
          return res
        gLogger.info("PFNMissing replica (%d) does not exist. Re-replicating..." % problematicDict['FileID'])
        res = rm.replicateAndRegister(problematicDict['LFN'],se)
        if not res['OK']:
          gLogger.error(res['Message'])
          integrityDB.incrementProblematicRetry(problematicDict['FileID'])
          return res
      else:
        gLogger.info("PFNMissing replica (%d) does not exist and is the only replica. Completely removing..." % problematicDict['FileID'])
        res = rm.removeFile(problematicDict['LFN'])
        if not res['OK']:
          gLogger.error(res['Message'])
          integrityDB.incrementProblematicRetry(problematicDict['FileID'])
          return res
    gLogger.info("PFNMissing replica (%d) is resolved" % problematicDict['FileID'])
    # If we get here the problem is solved so we can update the integrityDB
    return integrityDB.removeProblematic(problematicDict['FileID'])

  def resolvePFNUnavailable(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNUnavailable prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    pfn = problematicDict['PFN']
    se = problematicDict['SE']
    rm = ReplicaManager()
    res = rm.getStorageFileMetadata(pfn,se,singleFile=True)
    if (not res['OK']) and (re.search('File does not exist',res['Message'])):
      # The file is no longer Unavailable but has now dissapeared completely
      gLogger.info("PFNUnavailable replica (%d) found to be missing. Updating prognosis" % problematicDict['FileID'])
      integrityDB.changeProblematicPrognosis(problematicDict['FileID'],'PFNMissing')
      return S_OK()
    if res['Value']['Unavailable']:
      # The file is still Unavailable, increment the number of retries for this file
      gLogger.info("PFNUnavailable replica (%d) found to still be Unavailable" % problematicDict['FileID'])
      integrityDB.incrementProblematicRetry(problematicDict['FileID'])
      return S_OK()
    gLogger.info("PFNUnavailable replica (%d) is no longer Unavailable" % problematicDict['FileID']) 
    # Need to make the replica okay in the Catalog
    problematicDict['Status'] = 'Checked'
    res = rm.setCatalogReplicaStatus({problematicDict['LFN']:problematicDict},singleFile=True)
    if not res['OK']:
      gLogger.error(res['Message'])
      # Increment the number of retries for this file
      integrityDB.incrementProblematicRetry(problematicDict['FileID'])
      return res
    gLogger.info("PFNUnavailable replica (%d) is updated to Checked status" % problematicDict['FileID'])
    # If we get here the problem is solved so we can update the integrityDB
    return integrityDB.removeProblematic(problematicDict['FileID'])

  def resolveBKReplicaNo(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaNo prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      gLogger.error(res['Message'])
      # Increment the number of retries for this file
      integrityDB.incrementProblematicRetry(problematicDict['FileID'])
      return res
    # If the file exists in the catalog
    if res['Value']:
      gLogger.info("BKReplicaNo file (%d) found to exist in the catalog" % problematicDict['FileID'])
      # and has available replicas
      res = rm.getCatalogReplicas(lfn,singleFile=True)
      if res['OK'] and res['Value']:
        gLogger.info("BKReplicaNo file (%d) found to have replicas" % problematicDict['FileID'])
        res = rm.addCatalogFile(lfn,singleFile=True,catalogs=['BookkeepingDB'])
        if not res['OK']:
          gLogger.error(res['Message'])
          # Increment the number of retries for this file
          integrityDB.incrementProblematicRetry(problematicDict['FileID'])
          return res
      else:
        gLogger.info("BKReplicaNo file (%d) found to have no replicas" % problematicDict['FileID'])  
    gLogger.info("BKReplicaNo replica (%d) is resolved" % problematicDict['FileID'])
    # If we get here the problem is solved so we can update the integrityDB
    return integrityDB.removeProblematic(problematicDict['FileID'])
   
  def resolveBKReplicaYes(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaYes prognosis
    """
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    lfn = problematicDict['LFN']
    rm = ReplicaManager()
    res = rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      gLogger.error(res['Message'])
      # Increment the number of retries for this file
      integrityDB.incrementProblematicRetry(problematicDict['FileID'])
      return res
    removeBKFile = False
    # If the file does not exist in the catalog
    if not res['Value']:
      gLogger.info("BKReplicaYes file (%d) does not exist in the catalog. Removing..." % problematicDict['FileID'])
      removeBKFile = True
    else:
      gLogger.info("BKReplicaYes file (%d) found to exist in the catalog" % problematicDict['FileID'])
      # If the file has no replicas in the catalog
      res = rm.getCatalogReplicas(lfn,singleFile=True)
      if (not res['OK']) and (res['Message'] == 'File has zero replicas'):
        gLogger.info("BKReplicaYes file (%d) found to exist without replicas. Removing..." % problematicDict['FileID'])
        removeBKFile = True
    # If we are to remove the file from the BK because it does not exist
    if removeBKFile:
      res = rm.removeCatalogFile(lfn,singleFile=True,catalogs=['BookkeepingDB'])
      if not res['OK']:
        gLogger.error(res['Message'])
        # Increment the number of retries for this file
        integrityDB.incrementProblematicRetry(problematicDict['FileID'])
        return res
      gLogger.info("BKReplicaYes file (%d) removed from bookkeeping" % problematicDict['FileID'])
    gLogger.info("BKReplicaYes file (%d) is resolved" % problematicDict['FileID'])
    # If we get here the problem is solved so we can update the integrityDB
    return integrityDB.removeProblematic(problematicDict['FileID'])
