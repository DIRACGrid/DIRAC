""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 

__RCSID__ = "$Id: DataIntegrityClient.py,v 1.6 2009/08/28 15:38:39 acsmith Exp $"

import re, time, commands, random,os
import types

from DIRAC import S_OK, S_ERROR, gLogger
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
    res = rm.getPhysicalFileMetadata(pfnLfns.keys(),se)
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
  # This section contains the specific methods for obtaining replica and metadata information from the catalog
  #

  def __getCatalogDirectoryContents(self,lfnDir):
    """ Obtain the contents of the supplied directory 
    """
    gLogger.info('Obtaining the contents for %s directories' % len(lfnDir))
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
