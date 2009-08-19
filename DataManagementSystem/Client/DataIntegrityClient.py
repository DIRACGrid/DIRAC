""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 

__RCSID__ = "$Id: DataIntegrityClient.py,v 1.3 2009/08/19 17:10:35 acsmith Exp $"

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
      gLogger.info('The following %s files were found with zero replicas in the catalog' % len(zeroReplicaFiles))
      for lfn in sortList(zeroReplicaFiles):
        gLogger.info(lfn)
      res = self.setFileProblematic(zeroReplicaFiles,'LFNZeroReplicas',sourceComponent='DataIntegrityClient')
      if not res['OK']:
        gLogger.info('Failed to update integrity DB with files',res['Message'])
      else:
        gLogger.info('Successfully updated integrity DB with files')
    gLogger.info('Obtaining at total of %s files for the supplied directories' % len(allMetadataDict))
    resDict = {'Metadata':allMetadataDict,'Replicas':allReplicaDict}
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
    for se,pfns in sePfns.items():
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
        gLoger.info('The following %s files had size mis-matches at %s' % (len(sizeMismatch),se))
        for lfn,pfn,se,reason in sortList(sizeMismatch):
          gLogger.info(lfn)
        res = self.setReplicaProblematic(sizeMismatch,sourceComponent='DataIntegrityClient')
        if not res['OK']:
          gLogger.info('Failed to update integrity DB with replicas',res['Message'])
        else:
          gLogger.info('Successfully updated integrity DB with replicas')
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
      gLogger.info('The following %s physical files are not present at %s' % (len(missingReplicas),se))
      for lfn,pfn,se,reason in sortList(missingReplicas):
        gLogger.info(lfn)
      res = self.setReplicaProblematic(missingReplicas,sourceComponent='DataIntegrityClient')
      if not res['OK']:
        gLogger.info('Failed to update integrity DB with replicas',res['Message'])
      else:
        gLogger.info('Successfully updated integrity DB with replicas')
    lostReplicas = []
    unavailableReplicas = []
    # If the files are not accessible
    for pfn,pfnMetadata in pfnMetadataDict.items():
      if pfnMetadata['Lost']:
        lostReplicas.append((pfnLfns[pfn],pfn,se,'PFNLost'))
      if pfnMetadata['Unavailable']:
        unavailableReplicas.append((pfnLfns[pfn],pfn,se,'PFNUnavailable'))
    if lostReplicas:
      gLogger.info('The following %s physical files are Lost at %s' % (len(lostReplicas),se))
      for lfn,pfn,se,reason in sortList(lostReplicas):
        gLogger.info(lfn)
      res = self.setReplicaProblematic(lostReplicas,sourceComponent='DataIntegrityClient')
      if not res['OK']:
        gLogger.info('Failed to update integrity DB with replicas',res['Message'])
      else:
        gLogger.info('Successfully updated integrity DB with replicas')
    if unavailableReplicas:
      gLogger.info('The following %s physical files are Unavailable at %s' % (len(unavailableReplicas),se))
      for lfn,pfn,se,reason in sortList(unavailableReplicas):
        gLogger.info(lfn)
      res = self.setReplicaProblematic(unavailableReplicas,sourceComponent='DataIntegrityClient')
      if not res['OK']:
        gLogger.info('Failed to update integrity DB with replicas',res['Message'])
      else:
        gLogger.info('Successfully updated integrity DB with replicas')
    gLogger.info('Checking the integrity of physical files at %s complete' % se)
    return S_OK(pfnMetadataDict)

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
      gLogger.info('The following %s files were found with zero replicas in the catalog' % len(zeroReplicaFiles))
      for lfn in sortList(zeroReplicaFiles):
        gLogger.info(lfn)
      res = self.setFileProblematic(zeroReplicaFiles,'LFNZeroReplicas',sourceComponent='DataIntegrityClient')
      if not res['OK']:
        gLogger.info('Failed to update integrity DB with files',res['Message'])
      else:
        gLogger.info('Successfully updated integrity DB with files')
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
      gLogger.info('The following %s files were missing from the catalog' % len(missingCatalogFiles))
      for lfn in sortList(missingCatalogFiles):
        gLogger.info(lfn)
      res = self.setFileProblematic(missingCatalogFiles,'LFNCatalogMissing',sourceComponent='DataIntegrityClient')
      if not res['OK']:
        gLogger.info('Failed to update integrity DB with files',res['Message'])
      else:
        gLogger.info('Successfully updated integrity DB with files')
    gLogger.info('Checking the catalog existence of files complete')
    return S_OK(allMetadata)

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
