""" This is the Replica Manager which links the functionalities of StorageElement and FileCatalogue. """

__RCSID__ = "$Id: ReplicaManager.py,v 1.10 2007/12/11 17:49:19 acsmith Exp $"

import re, time, commands, random,os
import types

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Storage.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.File import getSize

class ReplicaManager:

  def __init__( self ):
    """ Constructor function.
    """

    self.fileCatalogue = LcgFileCatalogCombinedClient()
    self.accountingClient = None
    self.registrationProtocol = 'SRM2'
    self.thirdPartyProtocols = ['SRM2','SRM1']

  def setAccountingClient(self,client):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def putAndRegister(self,lfn,file,diracSE,guid=None,path=None):
    """ Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    # Check that the local file exists
    if not os.path.exists(file):
      errStr = "ReplicaManager.putAndRegister: Supplied file does not exist."
      gLogger.error(errStr, file)
      return S_ERROR(errStr)
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname(lfn)
    # Obtain the size of the local file
    size = getSize(file)
    if size == 0:
      errStr = "ReplicaManager.putAndRegister: Supplied file is zero size."
      gLogger.error(errStr,file)
      return S_ERROR(errStr)
    # If the GUID is not given, generate it here
    if not guid:
      guid = makeGuid(file)
    res = self.fileCatalogue.exists(lfn) #checkFileExistence(lfn,guid)
    if not res['OK']:
      return res
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename(lfn)
    localFileName = os.path.basename(file)
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Perform the put here
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.putAndRegister: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    res = storageElement.putFile(file,path,alternativeFileName=alternativeFile)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to put file to Storage Element."
      errMessage = res['Message']
      gLogger.error(errStr,"%s: %s" % (file,errMessage))
      return S_ERROR("%s %s" % (errStr,errMessage))
    destPfn = res['Value']
    destinationSE = storageElement.getStorageElementName()['Value']

    ###########################################################
    # Perform the registration here
    res = storageElement.getPfnForProtocol(destPfn,self.registrationProtocol,withPort=False)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to resolve desired PFN for registration."
      gLogger.error(errStr,destPfn)
      pfnForRegistration = destPfn
    else:
      pfnForRegistration = res['Value']
    fileTuple = (lfn,pfnForRegistration,size,destinationSE,guid)
    res = self.fileCatalogue.addFile(fileTuple)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to add file to catalogue."
      gLogger.error(errStr,"%s: %s" % (lfn,res['Message']))
      res['Message'] = "%s %s" % (errStr,res['Message'])
      resDict = {}
      resDict['Put'] = True
      resDict['Registration'] = False
      resDict['LFN'] = lfn
      resDict['PFN'] = pfnForRegistration
      resDict['Size'] = size
      resDict['SE'] = destinationSE
      resDict['GUID'] = guid
      res['FileInfo'] = resDict
      return res
    else:
      return res

  def getFile(self,lfn):
    """ Get a local copy of a LFN from Storage Elements.

        'lfn' is the logical file name for the desired file
    """
    ###########################################################
    # Get the LFN replicas from here
    res = self.fileCatalogue.getReplicas(lfn)
    if not res['OK']:
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.getFile: Failed to get replicas for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
    lfnReplicas = res['Value']['Successful'][lfn]
    res = self.fileCatalogue.getFileSize(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.getFile: Failed to get file size from FileCatalogue."
      gLogger.error(errStr,"%s: %s" % (lfn,res['Message']))
      return S_ERROR("%s %s" % (errStr,res['Message']))
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.getFile: Failed to get file size."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR("%s %s %s" % (errStr,lfn,res['Value']['Failed'][lfn]))
    catalogueSize = res['Value']['Successful'][lfn]

    ###########################################################
    # Determine the best replica
    replicaPreference = []
    for diracSE,pfn in lfnReplicas.items():
      storageElement = StorageElement(diracSE)
      if storageElement.isValid()['Value']:
        local = storageElement.isLocalSE()['Value']
        fileTuple = (diracSE,pfn)
        if local:
          replicaPreference.insert(0,fileTuple)
        else:
          replicaPreference.append(fileTuple)
      else:
        errStr = "ReplicaManager.getFile: Failed to determine whether SE is local."
        gLogger.error(errStr,diracSE)
    if not replicaPreference:
      errStr = "ReplicaManager.getFile: Failed to find any valid StorageElements."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)

    ###########################################################
    # Get a local copy depending on replica preference
    for diracSE,pfn in replicaPreference:
      storageElement = StorageElement(diracSE)
      res = storageElement.getFile(pfn,catalogueSize)
      if res['OK']:
        return res
    # If we get here then we failed to get any replicas
    errStr = "ReplicaManager.getFile: Failed to get local copy of file."
    gLogger.error(errStr,lfn)
    return S_ERROR(errStr)

  def replicateAndRegister(self,lfn,destSE,sourceSE='',destPath='',localCache=''):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    gLogger.info("ReplicaManager.replicateAndRegister: Attempting to replicate %s to %s." % (lfn,destSE))
    res = self.__replicate(lfn,destSE,sourceSE,destPath)
    if not res['OK']:
      errStr = "ReplicaManager.replicateAndRegister: Replication failed."
      gLogger.errStr(errStr,"%s %s" % (lfn,destSE))
      return res
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info("ReplicaManager.replicateAndRegister: %s already present at %s." % (lfn,destSE))
      return res
    destPfn = res['Value']['DestPfn']
    destSE = res['Value']['DestSE']
    gLogger.info("ReplicaManager.replicateAndRegister: Attempting to register %s at %s." % (destPfn,destSE))
    res = self.__registerReplica(lfn,destPfn,destSE)
    if not res['OK']:
      # Need to return to the client that the file was replicated but not registered
      errStr = "ReplicaManager.replicateAndRegister: Replica registration failed."
      gLogger.error(errStr,"%s %s %s" % (lfn,destPfn,destSE))
    else:
      gLogger.info("ReplicaManager.replicateAndRegister: Successfully registered replica.")
      return S_OK(lfn)

  def replicate(self,lfn,destSE,sourceSE='',destPath='',localCache=''):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    gLogger.info("ReplicaManager.replicate: Attempting to replicate %s to %s." % (lfn,destSE))
    res = self.__replicate(lfn,destSE,sourceSE,destPath)
    if not res['OK']:
      errStr = "ReplicaManager.replicate: Replication failed."
      gLogger.errStr(errStr,"%s %s" % (lfn,destSE))
      return res
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info("ReplicaManager.replicate: %s already present at %s." % (lfn,destSE))
      return res
    return S_OK(lfn)

  def __replicate(self,lfn,destSE,sourceSE='',destPath=''):
    """ Replicate a LFN to a destination SE.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
    """
    gLogger.info("ReplicaManager.__replicate: Performing replication initialization.")
    res = self.__initializeReplication(lfn,sourceSE,destSE)
    if not res['OK']:
      gLogger.error("ReplicaManager.__replicate: Replication initialisation failed.",lfn)
      return res
    destStorageElement = res['Value']['DestStorage']
    lfnReplicas = res['Value']['Replicas']
    destSE = res['Value']['DestSE']
    catalogueSize = res['Value']['CatalogueSize']
    ###########################################################
    # If the LFN already exists at the destination we have nothing to do
    if lfnReplicas.has_key(destSE):
      gLogger.info("ReplicaManager.__replicate: LFN is already registered at %s." % destSE)
      return S_OK()
    ###########################################################
    # Resolve the best source storage elements for replication
    gLogger.info("ReplicaManager.__replicate: Determining the best source replicas.")
    res = self.__resolveBestReplicas(sourceSE,lfnReplicas,catalogueSize)
    if not res['OK']:
      gLogger.error("ReplicaManager.__replicate: Best replica resolution failed." % lfn)
      return res
    replicaPreference = res['Value']
    ###########################################################
    # Now perform the replication for the file
    if not destPath:
      destPath = os.path.dirname(lfn)
    for sourceSE,sourcePfn in replicaPreference:
      gLogger.info("ReplicaManager.__replicate: Attempting replication from %s to %s." % (sourceSE,destSE))
      res = destStorageElement.replicateFile(sourcePfn,catalogueSize,destPath)
      if res['OK']:
        gLogger.info("ReplicaManager.__replicate: Replication successful.")
        resDict = {'DestSE':destSE,'DestPfn':res['Value']}
        return S_OK(resDict)
      else:
        errStr = "ReplicaManager.__replicate: Replication failed."
        gLogger.error(errStr,"%s from %s to %s." % (lfn,sourceSE,destSE))
    ##########################################################
    # If the replication failed for all sources give up
    errStr = "ReplicaManager.__replicate: Failed to replicate with all sources."
    gLogger.error(errStr,lfn)
    return S_ERROR(errStr)

  def __initializeReplication(self,lfn,sourceSE,destSE,):
    ###########################################################
    # Check that the destination storage element is sane and resolve its name
    gLogger.info("ReplicaManager.__initializeReplication: Verifying destination Storage Element validity (%s)." % destSE)
    destStorageElement = StorageElement(destSE)
    if not destStorageElement.isValid()['Value']:
      errStr = "ReplicaManager.__initializeReplication: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,destSE)
      return S_ERROR(errStr)
    destSE = destStorageElement.getStorageElementName()['Value']
    gLogger.info("ReplicaManager.__initializeReplication: Destination Storage Element verified.")
    ###########################################################
    # Get the LFN replicas from the file catalogue
    gLogger.info("ReplicaManager.__initializeReplication: Attempting to obtain replicas for %s." % lfn)
    res = self.fileCatalogue.getReplicas(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: Completely failed to get replicas for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.__initializeReplication: Failed to get replicas for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
    gLogger.info("ReplicaManager.__initializeReplication: Successfully obtained replicas for LFN.")
    lfnReplicas = res['Value']['Successful'][lfn]
    ###########################################################
    # If the file catalogue size is zero fail the transfer
    gLogger.info("ReplicaManager.__initializeReplication: Attempting to obtain size for %s." % lfn)
    res = self.fileCatalogue.getFileSize(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: Completely failed to get size for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.__initializeReplication: Failed to get size for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
    catalogueSize = res['Value']['Successful'][lfn]
    if catalogueSize == 0:
      errStr = "ReplicaManager.__initializeReplication: Registered file size is 0."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.__initializeReplication: File size determined to be %s." % catalogueSize)
    ###########################################################
    # Check whether the destination storage element is banned
    gLogger.info("ReplicaManager.__initializeReplication: Determining whether %s is banned." % destSE)
    configStr = '/Resources/StorageElements/BannedTarget'
    bannedTargets = gConfig.getValue(configStr,[])
    if destSE in bannedTargets:
      infoStr = "ReplicaManager.__initializeReplication: Destination Storage Element is currently banned."
      gLogger.info(infoStr,destSE)
      return S_ERROR(infoStr)
    gLogger.info("ReplicaManager.__initializeReplication: Destination site not banned.")
    ###########################################################
    # Check whether the supplied source SE is sane
    gLogger.info("ReplicaManager.__initializeReplication: Determining whether source Storage Element is sane.")
    configStr = '/Resources/StorageElements/BannedSource'
    bannedSources = gConfig.getValue(configStr,[])
    if sourceSE:
      if not lfnReplicas.has_key(sourceSE):
        errStr = "ReplicaManager.__initializeReplication: LFN does not exist at supplied source SE."
        gLogger.error(errStr,"%s %s" % (lfn,sourceSE))
        return S_ERROR(errStr)
      elif sourceSE in bannedSources:
        infoStr = "ReplicaManager.__initializeReplication: Supplied source Storage Element is currently banned."
        gLogger.info(infoStr,sourceSE)
        return S_ERROR(errStr)
    gLogger.info("ReplicaManager.__initializeReplication: Replication initialization successful.")
    resDict = {'DestStorage':destStorageElement,'DestSE':destSE,'Replicas':lfnReplicas,'CatalogueSize':catalogueSize}
    return S_OK(resDict)

  def __resolveBestReplicas(self,sourceSE,lfnReplicas,catalogueSize):
    ###########################################################
    # Determine the best replicas (remove banned sources, invalid storage elements and file with the wrong size)
    configStr = '/Resources/StorageElements/BannedSource'
    bannedSources = gConfig.getValue(configStr,[])
    gLogger.info("ReplicaManager.__resolveBestReplicas: Obtained current banned sources.")
    replicaPreference = []
    for diracSE,pfn in lfnReplicas.items():
      if sourceSE and diracSE != sourceSE:
        gLogger.info("ReplicaManager.__resolveBestReplicas: %s replica not requested." % diracSE)
      elif diracSE in bannedSources:
        gLogger.info("ReplicaManager.__resolveBestReplicas: %s is currently banned as a source." % diracSE)
      else:
        gLogger.info("ReplicaManager.__resolveBestReplicas: %s is available for use." % diracSE)
        storageElement = StorageElement(diracSE)
        if storageElement.isValid()['Value']:
          if storageElement.getRemoteProtocols()['Value']:
            gLogger.info("ReplicaManager.__resolveBestReplicas: Attempting to get source pfns for remote protocols.")
            res = storageElement.getPfnForProtocol(pfn,self.thirdPartyProtocols)
            if res['OK']:
              sourcePfn = res['Value']
              gLogger.info("ReplicaManager.__resolveBestReplicas: Attempting to get source file size.")
              res = storageElement.getFileSize(sourcePfn)
              if res['OK']:
                sourceFileSize = res['Value']
                gLogger.info("ReplicaManager.__resolveBestReplicas: Source file size determined to be %s." % sourceFileSize)
                if catalogueSize == sourceFileSize:
                  fileTuple = (diracSE,sourcePfn)
                  replicaPreference.append(fileTuple)
                else:
                  errStr = "ReplicaManager.__resolveBestReplicas: Catalogue size and physical file size mismatch."
                  gLogger.error(errStr,"%s %s" % (diracSE,sourcePfn))
              else:
                errStr = "ReplicaManager.__resolveBestReplicas: Failed to get physical file size."
                gLogger.error(errStr,"%s %s: %s" % (sourcePfn,diracSE,res['Message']))
            else:
              errStr = "ReplicaManager.__resolveBestReplicas: Failed to get PFN for replication for StorageElement."
              gLogger.error(errStr,"%s %s" % (diracSE,res['Message']))
          else:
            errStr = "ReplicaManager.__resolveBestReplicas: Source Storage Element has no remote protocols."
            gLogger.info(errStr,diracSE)
        else:
          errStr = "ReplicaManager.__resolveBestReplicas: Failed to get valid Storage Element."
          gLogger.error(errStr,diracSE)
    if not replicaPreference:
      errStr = "ReplicaManager.__resolveBestReplicas: Failed to find any valid source Storage Elements."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    else:
      return S_OK(replicaPreference)

  ###################################################################
  #
  # These are the file/replica registration methods
  #

  def registerFile(self,lfn,physicalFile,fileSize,storageElementName,fileGuid):
    """ Register a file.

        'lfn' is the LFN to be registered
        'physicalFile' is the PFN to be registered
        'fileSize' is the size of the file
        'storageElementName' is the location of the replica
        'fileGuid' is the unique GUID for the file
    """
    gLogger.info("ReplicaManager.registerFile: Attempting to register %s at %s." % (lfn,destSE))
    res = self.__registerFile(lfn,physicalFile,fileSize,storageElementName,fileGuid)
    if not res['OK']:
      errStr = "ReplicaManager.registerFile: File registration failed."
      gLogger.error(errStr,"%s %s %s %s %s" % (lfn,physicalFile,fileSize,storageElementName,fileGuid))
    else:
      gLogger.info("ReplicaManager.registerFile: Successfully registered file.")
      return S_OK(lfn)

  def __registerFile(self,lfn,physicalFile,fileSize,storageElementName,fileGuid):
    ##########################################################
    # Register the file at a given storage element
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.__registerFile: Failed to instantiate destination Storage Element."
      gLogger.error(errStr,storageElementName)
      return S_ERROR(errStr)
    resolvedSEName = storageElement.getStorageElementName()['Value']
    gLogger.info("ReplicaManager.__registerFile: Attempting to obtain pfn for registration.")
    res = storageElement.getPfnForProtocol(physicalFile,self.registrationProtocol,withPort=False)
    if not res['OK']:
      errStr = "ReplicaManager.__registerFile: Failed to resolve desired PFN for registration."
      gLogger.error(errStr,physicalFile)
      pfnForRegistration = physicalFile
    else:
      pfnForRegistration = res['Value']
    fileTuple = (lfn,pfnForRegistration,fileSize,resolvedSEName,fileGuid)
    gLogger.info("ReplicaManager.__registerFile: Adding file...")
    res = self.fileCatalogue.addFile(fileTuple)
    if res['OK']:
      if res['Value']['Successful'].has_key(lfn):
        gLogger.info("ReplicaManager.__registerFile: ...successful.")
        return S_OK(lfn)
      else:
        errStr = "ReplicaManager.__registerFile: Failed to register file."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
    else:
      errStr = "ReplicaManager.__registerFile: Completely failed to register file."
      gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
    return S_ERROR(errStr)

  def registerReplica(self,lfn,destPfn,destSE):
    """ Register a replica.

        'lfn' is the LFN to be registered
        'destPfn' is the PFN to be registered
        'destSE' is the Storage Element the file should be replicated to
    """
    gLogger.info("ReplicaManager.registerReplica: Attempting to register %s at %s." % (destPfn,destSE))
    res = self.__registerReplica(lfn,destPfn,destSE)
    if not res['OK']:
      errStr = "ReplicaManager.registerReplica: Replica registration failed."
      gLogger.error(errStr,"%s %s %s" % (lfn,destPfn,destSE))
    else:
      gLogger.info("ReplicaManager.registerReplica: Successfully registered replica.")
      return S_OK(lfn)

  def __registerReplica(self,lfn,destPfn,destSE):
    ##########################################################
    # Register the pfn at a given storage element
    destStorageElement = StorageElement(destSE)
    if not destStorageElement.isValid()['Value']:
      errStr = "ReplicaManager.__registerReplica: Failed to instantiate destination Storage Element."
      gLogger.error(errStr,destSE)
      return S_ERROR(errStr)
    destSE = destStorageElement.getStorageElementName()['Value']
    gLogger.info("ReplicaManager.__registerReplica: Attempting to obtain pfn for registration.")
    res = destStorageElement.getPfnForProtocol(destPfn,self.registrationProtocol,withPort=False)
    if not res['OK']:
      errStr = "ReplicaManager.__registerReplica: Failed to resolve desired PFN for registration."
      gLogger.error(errStr,destPfn)
      pfnForRegistration = destPfn
    else:
      pfnForRegistration = res['Value']
    replicaTuple = (lfn,pfnForRegistration,destSE,False)
    gLogger.info("ReplicaManager.__registerReplica: Adding replica...")
    res = self.fileCatalogue.addReplica(replicaTuple)
    if res['OK']:
      if res['Value']['Successful'].has_key(lfn):
        gLogger.info("ReplicaManager.__registerReplica: ...successful.")
        return S_OK(pfnForRegistration)
      else:
        errStr = "ReplicaManager.__registerReplica: Failed to register replica in file catalogue."
        gLogger.error(errStr,"%s %s" % (pfnForRegistration,res['Value']['Failed'][lfn]))
    else:
      errStr = "ReplicaManager.__registerReplica: Completely failed to register replica."
      gLogger.error(errStr,"%s %s" % (pfnForRegistration,res['Message']))
    return S_ERROR(errStr)

  ###################################################################
  #
  # These are the removal methods for physical and catalogue removal
  #

  def removeFile(self,lfn,replicas=False):
    """ Remove the file (all replicas) from Storage Elements and file catalogue

        'lfn' is the file to be removed
        'replicas' is a dictionary containing ALL the file replicas
    """
    gLogger.info("ReplicaManager.removeFile: Attempting to remove file from Storage and Catalogue.")
    if replicas:
      gLogger.info("ReplicaManager.removeFile: Using supplied replicas for %s." % lfn)
      lfnReplicas = replicas
    else:
      ###########################################################
      # Get the LFN replicas from the file catalogue
      gLogger.info("ReplicaManager.removeFile: Attempting to obtain replicas for %s." % lfn)
      res = self.fileCatalogue.getReplicas(lfn)
      if not res['OK']:
        errStr = "ReplicaManager.removeFile: Completely failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
        return res
      if not res['Value']['Successful'].has_key(lfn):
        errStr = "ReplicaManager.removeFile: Failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
        return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
      gLogger.info("ReplicaManager.removeFile: Successfully obtained replicas for LFN.")
      lfnReplicas = res['Value']['Successful'][lfn]
    res = self.__removeFile(lfn,lfnReplicas)
    return res

  def __removeFile(self,lfn,lfnReplicas):
    allRemoved = True
    for storageElementName,physicalFile in lfnReplicas.items():
      res = self.__removeReplica(lfn,storageElementName,physicalFile)
      if not res['OK']:
        allRemoved = False
    if not allRemoved:
      errStr = "ReplicaManager.__removeFile: Failed to remove all replicas."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)
    else:
      gLogger.info("ReplicaManager.__removeFile: Successfully removed all replicas.")
      res = self.fileCatalogue.removeFile(lfn)
      if not res['OK']:
        errStr = "ReplicaManager.__removeFile: Completely failed to remove file."
        gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
        return S_ERROR(errStr)
      else:
        if not res['Value']['Successful'].has_key(lfn):
          errStr = "ReplicaManager.__removeFile: Failed to remove file."
          gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
          return S_ERROR(errStr)
        else:
          gLogger.info("ReplicaManager.__removeFile: Successfully removed file.")
          return S_OK()

  def removeReplica(self,lfn,storageElementName,physicalFile=None):
    """ Remove replica from Storage Element and file catalogue

       'lfn' is the file to be removed
       'storageElementName' is the storage where the file is to be removed
       'physicalFile' is optionally the physical file to be removed
    """
    gLogger.info("ReplicaManager.removeReplica: Attempting to remove %s at %s." % (lfn,storageElementName))
    if physicalFile:
      gLogger.info("ReplicaManager.removeReplica: Using provided PFN: %s." % physicalFile)
      pfnToRemove = physicalFile
    else:
      gLogger.info("ReplicaManager.removeReplica: Attempting to resolve replicas.")
      res = self.fileCatalogue.getReplicas(lfn)
      if not res['OK']:
        errStr = "ReplicaManager.removeReplica: Completely failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
        return res
      if not res['Value']['Successful'].has_key(lfn):
        errStr = "ReplicaManager.removeReplica: Failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
        return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
      gLogger.info("ReplicaManager.removeReplica: Successfully obtained replicas for LFN.")
      lfnReplicas = res['Value']['Successful'][lfn]
      if not lfnReplicas.has_key(storageElementName):
        # The file doesn't exist so therefore don't have to remove it
        return S_OK()
      else:
        pfnToRemove = lfnReplicas[storageElementName]
    res = self.__removeReplica(lfn,storageElementName,pfnToRemove)
    return res

  def __removeReplica(self,lfn,storageElementName,pfnToRemove):
    gLogger.info("ReplicaManager.__removeReplica: Attmepting to remove %s from %s." % (lfn,storageElementName))
    res = self.__removePhysicalReplica(storageElementName,pfnToRemove)
    if res['OK']:
      gLogger.info("ReplicaManager.__removeReplica: Successfully removed physical replica.")
      res = self.__removeCatalogReplica(lfn,storageElementName,pfnToRemove)
      if res['OK']:
        gLogger.info("ReplicaManager.__removeReplica: Successfully removed catalogue replica.")
        return S_OK()
      else:
        errStr = "ReplicaManager.__removeReplica: Failed to remove catalogue replica."
        gLogger.error(errStr, "%s %s" % (pfnToRemove,res['Message']))
        return S_ERROR(errStr)
    else:
      errStr = "ReplicaManager.__removeReplica: Failed to remove physical replica."
      gLogger.error(errStr, "%s %s" % (pfnToRemove,res['Message']))
      return S_ERROR(errStr)

  def removeCatalogReplica(self,lfn,storageElementName,physicalFile=None):
    """ Remove replica from the file catalog

       'lfn' is the file to be removed
       'storageElementName' is the storage where the file is to be removed
       'physicalFile' is optionally the physical file to be removed
    """
    gLogger.info("ReplicaManager.removeCatalogReplica: Attempting to remove catalogue entry for %s at %s." % (lfn,storageElementName))
    if physicalFile:
      gLogger.info("ReplicaManager.removeCatalogReplica: Using provided PFN: %s." % physicalFile)
      pfnToRemove = physicalFile
    else:
      gLogger.info("ReplicaManager.removeCatalogReplica: Attempting to resolve replicas.")
      res = self.fileCatalogue.getReplicas(lfn)
      if not res['OK']:
        errStr = "ReplicaManager.removeCatalogReplica: Completely failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
        return res
      if not res['Value']['Successful'].has_key(lfn):
        errStr = "ReplicaManager.removeCatalogReplica: Failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
        return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
      gLogger.info("ReplicaManager.removeCatalogReplica: Successfully obtained replicas for LFN.")
      lfnReplicas = res['Value']['Successful'][lfn]
      if not lfnReplicas.has_key(storageElementName):
        # The file doesn't exist so therefore don't have to remove it
        return S_OK()
      else:
        pfnToRemove = lfnReplicas[storageElementName]
    res = self.__removeCatalogReplica(lfn,storageElementName,pfnToRemove)
    return res

  def __removeCatalogReplica(self,lfn,storageElementName,pfnToRemove):
    #######################################################
    # This performs the removal of the catalogue replica
    replicaTuple = (lfn,pfnToRemove,storageElementName)
    res = self.fileCatalogue.removeReplica(replicaTuple)
    if not res['OK']:
      errStr = "ReplicaManager.__removeCatalogReplica: Completely failed to remove replica."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    elif not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.__removeCatalogReplica: Failed to remove replica."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR(errStr)
    else:
      infoStr = "ReplicaManager.__removeCatalogReplica: Successfully removed replica."
      gLogger.info(infoStr,lfn)
      return S_OK()

  def removePhysicalReplica(self,lfn,storageElementName,physicalFile=None):
    """ Remove replica from Storage Element.

       'lfn' is the file to be removed
       'storageElementName' is the storage where the file is to be removed
       'physicalFile' is optionally the physical file to be removed
    """
    gLogger.info("ReplicaManager.removePhysicalReplica: Attempting to remove %s at %s." % (lfn,storageElementName))
    if physicalFile:
      gLogger.info("ReplicaManager.removePhysicalReplica: Using provided PFN: %s." % physicalFile)
      pfnToRemove = physicalFile
    else:
      gLogger.info("ReplicaManager.removePhysicalReplica: Attempting to resolve replicas.")
      res = self.fileCatalogue.getReplicas(lfn)
      if not res['OK']:
        errStr = "ReplicaManager.removePhysicalReplica: Completely failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
        return res
      if not res['Value']['Successful'].has_key(lfn):
        errStr = "ReplicaManager.removePhysicalReplica: Failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
        return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
      gLogger.info("ReplicaManager.removePhysicalReplica: Successfully obtained replicas for LFN.")
      lfnReplicas = res['Value']['Successful'][lfn]
      if not lfnReplicas.has_key(storageElementName):
        # The file doesn't exist so therefore don't have to remove it
        return S_OK()
      else:
        pfnToRemove = lfnReplicas[storageElementName]
    res = self.__removePhysicalReplica(storageElementName,pfnToRemove)
    return res

  def __removePhysicalReplica(self,storageElementName,pfnToRemove):
    gLogger.info("ReplicaManager.__removePhysicalReplica: Attempting to remove %s at %s." % (pfnToRemove,storageElementName))
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to instantiate Storage Element for removal."
      gLogger.error(errStr,destSE)
      return S_ERROR(errStr)
    res = storageElement.removeFile(pfnToRemove)
    if not res['OK']:
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to remove replica."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    else:
      infoStr = "ReplicaManager.__removePhysicalReplica: Successfully removed replica."
      gLogger.info(infoStr,pfnToRemove)
      return S_OK()

  ###################################################################
  #
  # These are the methods for obtaining file metadata (these are bulk methods)
  #

  def getReplicaMetadata(self,lfn,storageElementName):
    """ Obtain the metadata for physical files

        'lfn' is the file(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getReplicaMetadata: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.getReplicaMetadata: Attempting to get metadata for %s replicas."  % len(lfns))
    gLogger.info("ReplicaManager.getReplicaMetadata: Resolving replicas for supplied LFNs.")
    res = self.fileCatalogue.getReplicas(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.getReplicaMetadata: Completely failed to get replicas for LFNs."
      gLogger.error(errStr,res['Message'])
      return res
    pfnDict = {}
    failed = res['Value']['Failed']
    for lfn in lfns:
      if not res['Value']['Successful'].has_key(lfn):
        errStr = "ReplicaManager.getReplicaMetadata: Failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      elif not res['Value']['Successful'][lfn].has_key(storageElementName):
        errStr = "ReplicaManager.getReplicaMetadata: File does not have replica at supplied Storage Element."
        gLogger.error(errStr, "%s %s" % (lfn,storageElementName))
        failed[lfn] = errStr
      else:
        pfnDict = {res['Value']['Successful'][lfn][storageElementName]:lfn}
    res = self.__getPhysicalFileMetadata(pfnDict.keys(),storageElementName)
    if not res['OK']:
      return res
    else:
      successful = {}
      failed = {}
      for pfn,metadataDict in res['Value']['Successful'].items():
        successful[pfnDict[pfn]] = metadataDict
      for pfn, errorMessage in res['Value']['Failed'].items():
        failed[pfnDict[pfn]] = errorMessage
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)

  def  getPhysicalFileMetadata(self,physicalFile,storageElementName):
    """ Obtain the metadata for physical files

        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if type(physicalFile) == types.ListType:
      pfns = physicalFile
    elif type(physicalFile) == types.StringType:
      pfns = [physicalFile]
    else:
      errStr = "ReplicaManager.getPhysicalFileMetadata: Supplied physical file must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.__getPhysicalFileMetadata(pfns,storageElementName)
    return res

  def __getPhysicalFileMetadata(self,pfns,storageElementName):
    if not len(pfns) > 0:
      errStr = "ReplicaManager.__getPhysicalFileMetadata: There were no replicas supplied."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.__getPhysicalFileMetadata: Attempting to get metadata for %s files." % len(pfns))
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.__getPhysicalFileMetadata: Failed to instantiate Storage Element for obtaining metadata."
      gLogger.error(errStr,storageElementName)
      return S_ERROR(errStr)
    res = storageElement.getFileMetadata(pfns)
    if not res['OK']:
      errStr = "ReplicaManager.__getPhysicalFileMetadata: Failed to get metadata for replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res

  ###################################################################
  #
  # These are the methods for obtaining access urls (these are bulk methods)
  #

  def getReplicaAccessUrl(self,lfn,storageElementName):
    """ Obtain the metadata for physical files

        'lfn' is the file(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getReplicaAccessUrl: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.getReplicaAccessUrl: Attempting to get access urls for %s replicas."  % len(lfns))
    gLogger.info("ReplicaManager.getReplicaAccessUrl: Resolving replicas for supplied LFNs.")
    res = self.fileCatalogue.getReplicas(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.getReplicaAccessUrl: Completely failed to get replicas for LFNs."
      gLogger.error(errStr,res['Message'])
      return res
    pfnDict = {}
    failed = res['Value']['Failed']
    for lfn in lfns:
      if not res['Value']['Successful'].has_key(lfn):
        errStr = "ReplicaManager.getReplicaAccessUrl: Failed to get replicas for LFN."
        gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      elif not res['Value']['Successful'][lfn].has_key(storageElementName):
        errStr = "ReplicaManager.getReplicaAccessUrl: File does not have replica at supplied Storage Element."
        gLogger.error(errStr, "%s %s" % (lfn,storageElementName))
        failed[lfn] = errStr
      else:
        pfnDict = {res['Value']['Successful'][lfn][storageElementName]:lfn}
    res = self.__getPhysicalFileAccessUrl(pfnDict.keys(),storageElementName)
    if not res['OK']:
      return res
    else:
      successful = {}
      failed = {}
      for pfn,turl in res['Value']['Successful'].items():
        successful[pfnDict[pfn]] = turl
      for pfn, errorMessage in res['Value']['Failed'].items():
        failed[pfnDict[pfn]] = errorMessage
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)

  def getPhysicalFileAccessUrl(self,physicalFile,storageElementName):
    """ Obtain the access url for physical files

        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if type(physicalFile) == types.ListType:
      pfns = physicalFile
    elif type(lfn) == types.StringType:
      pfns = [physicalFile]
    else:
      errStr = "ReplicaManager.getPhysicalFileAccessUrl: Supplied physical file must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.__getPhysicalFileAccessUrl(pfns,storageElementName)
    return res

  def __getPhysicalFileAccessUrl(self,pfns,storageElementName):
    if not len(pfns) > 0:
      errStr = "ReplicaManager.__getPhysicalFileAccessUrl: There were no replicas supplied."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.__getPhysicalFileAccessUrl: Attempting to get access urls for %s files." % len(pfns))
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.__getPhysicalFileAccessUrl: Failed to instantiate Storage Element for obtaining metadata."
      gLogger.error(errStr,storageElementName)
      return S_ERROR(errStr)
    res = storageElement.getAccessUrl(pfns)
    if not res['OK']:
      errStr = "ReplicaManager.__getPhysicalFileAccessUrl: Failed to get access urls for replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res
