""" This is the Replica Manager which links the functionalities of StorageElement and FileCatalogue. """

__RCSID__ = "$Id: ReplicaManager.py,v 1.34 2008/08/01 08:04:25 rgracian Exp $"

import re, time, commands, random,os
import types

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.File import makeGuid,fileAdler
from DIRAC.Core.Utilities.File import getSize

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

class ReplicaManager:

  def __init__( self ):
    """ Constructor function.
    """

    self.fileCatalogue = FileCatalog()
    self.accountingClient = None
    self.registrationProtocol = 'SRM2'
    self.thirdPartyProtocols = ['SRM2','SRM1']

  def setAccountingClient(self,client):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def put(self,lfn,file,diracSE,path=None):
    """ Put a local file to a Storage Element

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    # Check that the local file exists
    if not os.path.exists(file):
      errStr = "ReplicaManager.put: Supplied file does not exist."
      gLogger.error(errStr, file)
      return S_ERROR(errStr)
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname(lfn)
    # Obtain the size of the local file
    size = getSize(file)
    if size == 0:
      errStr = "ReplicaManager.put: Supplied file is zero size."
      gLogger.error(errStr,file)
      return S_ERROR(errStr)
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename(lfn)
    localFileName = os.path.basename(file)
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.put: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    destinationSE = storageElement.getStorageElementName()['Value']

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = storageElement.putFile(file,path,alternativeFileName=alternativeFile)
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to put file to Storage Element."
      failed[lfn] = res['Message']
      gLogger.error(errStr,"%s: %s" % (file,res['Message']))
    else:
      gLogger.info("ReplicaManager.put: Put file to storage in %s seconds." % putTime)
      successful[lfn] = putTime
    resDict = {'Successful': successful,'Failed':failed}
    return S_OK(resDict)

  def putDirectory(self,storagePath,localDirectory,diracSE):
    """ Put a local file to a Storage Element

        'lfn' is the path on the storage
        'localDirectory' is the full path to local directory
        'diracSE' is the Storage Element to which to put the file
    """
    # Check that the local directory exists
    if not os.path.exists(localDirectory):
      errStr = "ReplicaManager.putDirectory: Supplied directory does not exist."
      gLogger.error(errStr,localDirectory)
      return S_ERROR(errStr)
    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.put: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    destinationSE = storageElement.getStorageElementName()['Value']

    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = storageElement.putDirectory(localDirectory,storagePath)
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to put file to Storage Element."
      gLogger.error(errStr,"%s: %s" % (localDirectory,res['Message']))
    else:
      gLogger.info("ReplicaManager.put: Put directory to storage in %s seconds." % putTime)
    return res


  def putAndRegister(self,lfn,file,diracSE,guid=None,path=None,checksum=None,catalog=None):
    """ Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    # Instantiate the desired file catalog
    if catalog:
      self.fileCatalogue = FileCatalog(catalog)
    else:
      self.fileCatalogue = FileCatalog()
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
    if not checksum:
      checksum = fileAdler(file)
    res = self.fileCatalogue.exists(lfn) #checkFileExistence(lfn,guid)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Completey failed to determine existence of destination LFN."
      gLogger.error(errStr,lfn)
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.putAndRegister: Failed to determine existence of destination LFN."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)
    if res['Value']['Successful'][lfn]:
      errStr = "ReplicaManager.putAndRegister: The supplied LFN already exists in the File Catalog."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename(lfn)
    localFileName = os.path.basename(file)
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.putAndRegister: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    destinationSE = storageElement.getStorageElementName()['Value']


    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = storageElement.putFile(file,path,alternativeFileName=alternativeFile)
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to put file to Storage Element."
      gLogger.error(errStr,"%s: %s" % (file,res['Message']))
      return S_ERROR("%s %s" % (errStr,res['Message']))
    destPfn = res['Value']
    successful[lfn] = {'put': putTime}

    ###########################################################
    # Perform the registration here
    fileTuple = (lfn,destPfn,size,destinationSE,guid,checksum)
    registerDict = {'LFN':lfn,'PFN':destPfn,'Size':size,'TargetSE':destinationSE,'GUID':guid,'Addler':checksum}
    startTime = time.time()
    res = self.registerFile(fileTuple)
    registerTime = time.time() - startTime

    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Completely failed to register file."
      gLogger.error(errStr,res['Message'])
      failed[lfn] = {'register':registerDict}
    elif not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.putAndRegister: Failed to register file."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      failed[lfn] = {'register':registerDict}
    else:
      successful[lfn]['register'] = registerTime
    resDict = {'Successful': successful,'Failed':failed}
    return S_OK(resDict)

  def getReplicas(self,lfn):
    """ Get the replicas registered in the catalog for supplied file.

        'lfn' is the files to check (can be a single lfn or list of lfns)
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getReplicas: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.getReplicas: Attempting to get replicas for %s files." % len(lfns))
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.__getReplicas: Completely failed to get replicas for %s lfns." % len(lfns)
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res

  def getFileSize(self,lfn):
    """ Get the size registered in the catalog for supplied file.

        'lfn' is the files to check (can be a single lfn or list of lfns)
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getFileSize: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.getFileSize: Attempting to get sizes for %s files." % len(lfns))
    res = self.fileCatalogue.getFileSize(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.getFile: Completely failed to get file size for %s lfns." % len(lfns)
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res

  def getPfn(self,pfn,diracSE):
    """ Get a local copy of the PFN from the given Storage Element.

        'pfn' is the pfn
        'storageElement' is the DIRAC storage element
    """
    if type(pfn) == types.ListType:
      pfns = pfn
    elif type(pfn) == types.StringType:
      pfns = [pfn]
    else:
      errStr = "ReplicaManager.getPfn: Supplied pfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    ###########################################################
    # Get a local copy depending on replica preference
    storageElement = StorageElement(diracSE)
    res = storageElement.getFileSize(pfns)
    if not res['OK']:
      errStr = "ReplicaManager.getPfn: Failed to get file sizes for pfns."
      gLogger.error(errStr,res['Message'])
      return res
    successful = {}
    failed = res['Value']['Failed']
    for pfn,size in res['Value']['Successful'].items():
      res = storageElement.getFile(pfn,size)
      if res['OK']:
        successful[pfn] = True
      else:
        failed[pfn] = res['Message']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def getFile(self,lfn):
    """ Get a local copy of a LFN from Storage Elements.

        'lfn' is the logical file name for the desired file
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getFile: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.getFile: Attempting to get %s files." % len(lfns))
    res = self.getReplicas(lfns)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    lfnReplicas = res['Value']['Successful']
    res = self.getFileSize(lfnReplicas.keys())
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    fileSizes = res['Value']['Successful']
    ###########################################################
    # Determine the best replicas
    replicaPreference = {}
    for lfn,size in fileSizes.items():
      replicas = []
      for diracSE,pfn in lfnReplicas[lfn].items():
        storageElement = StorageElement(diracSE)
        if storageElement.isValid()['Value']:
          local = storageElement.isLocalSE()['Value']
          fileTuple = (diracSE,pfn)
          if local:
            replicas.insert(0,fileTuple)
          else:
            replicas.append(fileTuple)
        else:
          errStr = "ReplicaManager.getFile: Failed to determine whether SE is local."
          gLogger.error(errStr,diracSE)
      if not replicas:
        errStr = "ReplicaManager.getFile: Failed to find any valid StorageElements."
        gLogger.error(errStr,lfn)
        failed[lfn] = errStr
      else:
        replicaPreference[lfn] = replicas
    ###########################################################
    # Get a local copy depending on replica preference
    successful = {}
    for lfn,replicas in replicaPreference.items():
      gotFile = False
      for diracSE,pfn in replicas:
        if not gotFile:
          storageElement = StorageElement(diracSE)
          res = storageElement.getFile(pfn,fileSizes[lfn])
          if res['OK']:
            gotFile = True
            successful[lfn] = res['Value']
      if not gotFile:
        # If we get here then we failed to get any replicas
        errStr = "ReplicaManager.getFile: Failed to get local copy from any replicas."
        gLogger.error(errStr,lfn)
        failed[lfn] = errStr
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def replicateAndRegister(self,lfn,destSE,sourceSE='',destPath='',localCache=''):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    successful = {}
    failed = {}
    gLogger.info("ReplicaManager.replicateAndRegister: Attempting to replicate %s to %s." % (lfn,destSE))
    startReplication = time.time()
    res = self.__replicate(lfn,destSE,sourceSE,destPath)
    replicationTime = time.time()-startReplication
    if not res['OK']:
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to replicate file."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info("ReplicaManager.replicateAndRegister: %s already present at %s." % (lfn,destSE))
      successful[lfn] = {'replicate':0,'register':0}
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)
    successful[lfn] = {'replicate':replicationTime}

    destPfn = res['Value']['DestPfn']
    destSE = res['Value']['DestSE']
    gLogger.info("ReplicaManager.replicateAndRegister: Attempting to register %s at %s." % (destPfn,destSE))
    replicaTuple = (lfn,destPfn,destSE)
    startRegistration = time.time()
    res = self.registerReplica(replicaTuple)
    registrationTime = time.time()-startRegistration
    if not res['OK']:
      # Need to return to the client that the file was replicated but not registered
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to register replica."
      gLogger.error(errStr,res['Message'])
      failed[lfn] = {'Registration':{'LFN':lfn,'TargetSE':destSE,'PFN':destPfn}}
    else:
      if res['Value']['Successful'].has_key(lfn):
        gLogger.info("ReplicaManager.replicateAndRegister: Successfully registered replica.")
        successful[lfn]['register'] = registrationTime
      else:
        errStr = "ReplicaManager.replicateAndRegister: Failed to register replica."
        gLogger.info(errStr,res['Value']['Failed'][lfn])
        failed[lfn] = {'Registration':{'LFN':lfn,'TargetSE':destSE,'PFN':destPfn}}
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

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
      gLogger.error(errStr,"%s %s" % (lfn,destSE))
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
      gLogger.error("ReplicaManager.__replicate: Best replica resolution failed.", lfn)
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
                if res['Value']['Successful'].has_key(sourcePfn):
                  sourceFileSize = res['Value']['Successful'][sourcePfn]
                  gLogger.info("ReplicaManager.__resolveBestReplicas: Source file size determined to be %s." % sourceFileSize)
                  if catalogueSize == sourceFileSize:
                    fileTuple = (diracSE,sourcePfn)
                    replicaPreference.append(fileTuple)
                  else:
                    errStr = "ReplicaManager.__resolveBestReplicas: Catalogue size and physical file size mismatch."
                    gLogger.error(errStr,"%s %s" % (diracSE,sourcePfn))
                else:
                  errStr = "ReplicaManager.__resolveBestReplicas: Failed to get physical file size."
                  gLogger.error(errStr,"%s %s: %s" % (sourcePfn,diracSE,res['Value']['Failed'][sourcePfn]))
              else:
                errStr = "ReplicaManager.__resolveBestReplicas: Completely failed to get physical file size."
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

  def registerFile(self,fileTuple):
    """ Register a file.

        'fileTuple' is the file tuple to be registered of the form (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    """
    if type(fileTuple) == types.ListType:
      fileTuples = fileTuple
    elif type(fileTuple) == types.TupleType:
      fileTuples = [fileTuple]
    else:
      errStr = "ReplicaManager.registerFile: Supplied file info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.registerFile: Attempting to register %s files." % len(fileTuples))
    res = self.__registerFile(fileTuples)
    if not res['OK']:
      errStr = "ReplicaManager.registerFile: Completely failed to register files."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res

  def __registerFile(self,fileTuples):
    seDict = {}
    for lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum in fileTuples:
      if not seDict.has_key(storageElementName):
        seDict[storageElementName] = []
      seDict[storageElementName].append((lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum))
    successful = {}
    failed = {}
    fileTuples = []
    for storageElementName,fileTuple in seDict.items():
      destStorageElement = StorageElement(storageElementName)
      if not destStorageElement.isValid()['Value']:
        errStr = "ReplicaManager.__registerFile: Failed to instantiate destination Storage Element."
        gLogger.error(errStr,storageElementName)
        for lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum in fileTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum in fileTuple:
          res = destStorageElement.getPfnForProtocol(physicalFile,self.registrationProtocol,withPort=False)
          if not res['OK']:
            pfn = physicalFile
          else:
            pfn = res['Value']
          tuple = (lfn,pfn,fileSize,storageElementName,fileGuid,checksum)
          fileTuples.append(tuple)
    gLogger.info("ReplicaManager.__registerFile: Resolved %s files for registration." % len(fileTuples))
    res = self.fileCatalogue.addFile(fileTuples)
    if not res['OK']:
      errStr = "ReplicaManager.__registerFile: Completely failed to register files."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def registerReplica(self,replicaTuple):
    """ Register a replica supplied in the replicaTuples.

        'replicaTuple' is a tuple or list of tuples of the form (lfn,pfn,se)
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(replicaTuple) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.registerReplica: Supplied file info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.registerReplica: Attempting to register %s replicas." % len(replicaTuples))
    res = self.__registerReplica(replicaTuples)
    if not res['OK']:
      errStr = "ReplicaManager.registerReplica: Completely failed to register replicas."
      gLogger.error(errStr,res['Message'])
    return res

  def __registerReplica(self,replicaTuples):
    seDict = {}
    for lfn,pfn,storageElementName in replicaTuples:
      if not seDict.has_key(storageElementName):
        seDict[storageElementName] = []
      seDict[storageElementName].append((lfn,pfn))
    successful = {}
    failed = {}
    replicaTuples = []
    for storageElementName,replicaTuple in seDict.items():
      destStorageElement = StorageElement(storageElementName)
      if not destStorageElement.isValid()['Value']:
        errStr = "ReplicaManager.__registerReplica: Failed to instantiate destination Storage Element."
        gLogger.error(errStr,storageElementName)
        for lfn,pfn in replicaTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn,pfn in replicaTuple:
          res = destStorageElement.getPfnForProtocol(pfn,self.registrationProtocol,withPort=False)
          if not res['OK']:
            failed[lfn] = res['Message']
          else:
            replicaTuple = (lfn,res['Value'],storageElementName,False)
            replicaTuples.append(replicaTuple)
    gLogger.info("ReplicaManager.__registerReplica: Successfully resolved %s replicas for registration." % len(replicaTuples))
    res = self.fileCatalogue.addReplica(replicaTuples)
    if not res['OK']:
      errStr = "ReplicaManager.__registerReplica: Completely failed to register replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setReplicaProblematic(self,replicaTuple,sourceComponent=''):
    """ This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaTuple should be of the form (lfn,pfn,se,prognosis)

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(replicaTuple) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.setReplicaProblematic: Supplied replica info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.registerReplica: Attempting to update %s replicas." % len(replicaTuples))
    statusTuples = []
    successful = {}
    failed = {}
    integrityDB = RPCClient('DataManagement/DataIntegrity')
    for lfn,pfn,se,reason in replicaTuples:
      fileMetadata = {'Prognosis':reason,'LFN':lfn,'PFN':pfn,'StorageElement':se}
      res = integrityDB.insertProblematic(sourceComponent,fileMetadata)
      if res['OK']:
        statusTuples.append((lfn,pfn,se,'Problematic'))
      else:
        failed[lfn] = res['Message']
    res = self.fileCatalog.setReplicaStatus(statusTuples)
    if not res['OK']:
      errStr = "ReplicaManager.setReplicaProblematic: Completely failed to update replicas."
      gLogger.error(errStr,res['Message'])
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  ###################################################################
  #
  # These are the removal methods for physical and catalogue removal
  #

  def removeFile(self,lfn):
    """ Remove the file (all replicas) from Storage Elements and file catalogue

        'lfn' is the file to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeFile: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.removeFile: Attempting to remove %s files from Storage and Catalogue." % len(lfns))
    gLogger.info("ReplicaManager.removeFile: Attempting to obtain replicas for %s lfns." % len(lfns))
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    lfnDict = res['Value']['Successful']
    res = self.__removeFile(lfnDict)
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to remove files."
      gLogger.error(errStr,res['Message'])
      return res
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    gDataStoreClient.commit()
    return S_OK(resDict)

  def __removeFile(self,lfnDict):
    storageElementDict = {}
    for lfn,repDict in lfnDict.items():
      for se,pfn in repDict.items():
        if not storageElementDict.has_key(se):
          storageElementDict[se] = []
        storageElementDict[se].append((lfn,pfn))
    failed = {}
    for storageElementName,fileTuple in storageElementDict.items():
      res = self.__removeReplica(storageElementName,fileTuple)
      if not res['OK']:
        errStr = res['Message']
        for lfn,pfn in fileTuple:
          if not failed.has_key(lfn):
            failed[lfn] = ''
          failed[lfn] = "%s %s" % (failed[lfn],errStr)
      else:
        for lfn,error in res['Value']['Failed'].items():
          if not failed.has_key(lfn):
            failed[lfn] = ''
          failed[lfn] = "%s %s" % (failed[lfn],error)
    completelyRemovedFiles = []
    for lfn in lfnDict.keys():
      if not failed.has_key(lfn):
        completelyRemovedFiles.append(lfn)
    res = self.fileCatalogue.removeFile(completelyRemovedFiles)
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeReplica(self,storageElementName,lfn):
    """ Remove replica at the supplied Storage Element from Storage Element then file catalogue

       'storageElementName' is the storage where the file is to be removed
       'lfn' is the file to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeReplica: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.removeReplica: Attempting to remove catalogue entry for %s lfns at %s." % (len(lfns),storageElementName))
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removeReplica: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn,repDict in res['Value']['Successful'].items():
      if not repDict.has_key(storageElementName):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        replicaTuple = (lfn,sePfn)
        replicaTuples.append(replicaTuple)
    res = self.__removeReplica(storageElementName,replicaTuples)
    failed.update(res['Value']['Failed'])
    successful.update(res['Value']['Successful'])
    resDict = {'Successful':successful,'Failed':failed}
    gDataStoreClient.commit()
    return S_OK(resDict)

  def __removeReplica(self,storageElementName,fileTuple):
    pfnDict = {}
    for lfn,pfn in fileTuple:
      pfnDict[pfn] = lfn
    failed = {}
    res = self.__removePhysicalReplica(storageElementName,pfnDict.keys())
    if not res['OK']:
      errStr = "ReplicaManager.__removeReplica: Failed to remove catalog replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    for pfn,error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    replicaTuples = []
    for pfn in res['Value']['Successful'].keys():
      replicaTuple = (pfnDict[pfn],pfn,storageElementName)
      replicaTuples.append(replicaTuple)
    successful = {}
    res = self.__removeCatalogReplica(replicaTuples)
    if not res['OK']:
      errStr = "ReplicaManager.__removeReplica: Completely failed to remove physical files."
      gLogger.error(errStr,res['Message'])
      for lfn in pfnDict.values():
        if not failed.has_key(lfn):
          failed[lfn] = errStr
    else:
      failed.update(res['Value']['Failed'])
      successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeCatalogReplica(self,storageElementName,lfn):
    """ Remove replica from the file catalog

       'lfn' are the file to be removed
       'storageElementName' is the storage where the file is to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeCatalogReplica: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.removeCatalogReplica: Attempting to remove catalogue entry for %s lfns at %s." % (len(lfns),storageElementName))
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removeCatalogReplica: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn,repDict in res['Value']['Successful'].items():
      if not repDict.has_key(storageElementName):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        replicaTuple = (lfn,sePfn,storageElementName)
        replicaTuples.append(replicaTuple)
    gLogger.info("ReplicaManager.removeCatalogReplica: Resolved %s pfns for catalog removal at %s." % (len(replicaTuples), storageElementName))
    res = self.__removeCatalogReplica(replicaTuples)
    failed.update(res['Value']['Failed'])
    successful.update(res['Value']['Successful'])
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeCatalogPhysicalFileNames(self,replicaTuple):
    """ Remove replicas from the file catalog specified by replica tuple

       'replicaTuple' is a tuple containing the replica to be removed and is of the form (lfn,pfn,se)
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(lfn) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.removeCatalogPhysicalFileNames: Supplied info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.__removeCatalogReplica(replicaTuples)
    return res

  def __removeCatalogReplica(self,replicaTuple):
    oDataOperation = self.__initialiseAccountingObject('removeCatalogReplica','',len(replicaTuple))
    oDataOperation.setStartTime()
    start= time.time()
    res = self.fileCatalogue.removeReplica(replicaTuple)
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey('RegistrationTime',time.time()-start)
    if not res['OK']:
      oDataOperation.setValueByKey('RegistrationOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      gDataStoreClient.addRegister(oDataOperation)
      errStr = "ReplicaManager.__removeCatalogReplica: Completely failed to remove replica."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    for lfn in res['Value']['Successful'].keys():
      infoStr = "ReplicaManager.__removeCatalogReplica: Successfully removed replica."
      gLogger.info(infoStr,lfn)
    for lfn,error in res['Value']['Failed'].items():
      errStr = "ReplicaManager.__removeCatalogReplica: Failed to remove replica."
      gLogger.error(errStr,"%s %s" % (lfn,error))
    oDataOperation.setValueByKey('RegistrationOK',len(res['Value']['Successful'].keys()))
    gDataStoreClient.addRegister(oDataOperation)
    return res

  def removePhysicalReplica(self,storageElementName,lfn):
    """ Remove replica from Storage Element.

       'lfn' are the files to be removed
       'storageElementName' is the storage where the file is to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removePhysicalReplica: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.removePhysicalReplica: Attempting to remove %s lfns at %s." % (len(lfns),storageElementName))
    gLogger.info("ReplicaManager.removePhysicalReplica: Attempting to resolve replicas.")
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removePhysicalReplica: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = {}
    pfnDict = {}
    for lfn,repDict in res['Value']['Successful'].items():
      if not lfnReplicas.has_key(storageElementName):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        pfnDict[sePfn] = lfn
    gLogger.info("ReplicaManager.removePhysicalReplica: Resolved %s pfns for removal at %s." % (len(pfnDict.keys()), storageElementName))
    res = self.__removePhysicalReplica(storageElementName,pfnDict.keys())
    for pfn,error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    for pfn in res['Value']['Successful'].keys():
      successful[pfnDict[pfn]]
    resDict = {'Successful':successful,'Failed':failed}
    return res

  def removePhysicalFile(self,storageElementName,pfnToRemove):
    """ This removes physical files given by a the physical file names

        'storageElementName' is the storage element where the file should be removed from
        'pfnsToRemove' is the physical files
    """
    if type(pfnToRemove) == types.ListType:
      pfns = pfnToRemove
    elif type(pfnToRemove) == types.StringType:
      pfns = [pfnToRemove]
    else:
      errStr = "ReplicaManager.removePhysicalFile: Supplied pfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.__removePhysicalReplica(storageElementName, pfns)
    return res

  def __removePhysicalReplica(self,storageElementName,pfnsToRemove):
    gLogger.info("ReplicaManager.__removePhysicalReplica: Attempting to remove %s pfns at %s." % (len(pfnsToRemove),storageElementName))
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to instantiate Storage Element for removal."
      gLogger.error(errStr,storageElement)
      return S_ERROR(errStr)
    oDataOperation = self.__initialiseAccountingObject('removePhysicalReplica',storageElementName,len(pfnsToRemove))
    oDataOperation.setStartTime()
    start= time.time()
    res = storageElement.removeFile(pfnsToRemove)
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey('TransferTime',time.time()-start)
    if not res['OK']:
      oDataOperation.setValueByKey('TransferOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      gDataStoreClient.addRegister(oDataOperation)
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to remove replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    else:
      oDataOperation.setValueByKey('TransferOK',len(res['Value']['Successful'].keys()))
      gDataStoreClient.addRegister(oDataOperation)
      infoStr = "ReplicaManager.__removePhysicalReplica: Successfully issued removal request."
      gLogger.info(infoStr)
      return res

  def  onlineRetransfer(self,diracSE,pfnToRemove):
    """ Requests the online system to re-transfer files

        'diracSE' is the storage element where the file should be removed from
        'pfnsToRemove' is the physical files
    """
    if type(pfnToRemove) == types.ListType:
      pfns = pfnToRemove
    elif type(pfnToRemove) == types.StringType:
      pfns = [pfnToRemove]
    else:
      errStr = "ReplicaManager.onlineRetransfer: Supplied pfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.onlineRetransfer: Failed to instantiate Storage Element for retransfer."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    res = storageElement.retransferOnlineFile(pfns)
    if not res['OK']:
      errStr = "ReplicaManager.onlineRetransfer: Failed to request retransfers."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    else:
      infoStr = "ReplicaManager.onlineRetransfer: Successfully issued retransfer request."
      gLogger.info(infoStr)
      return res

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
    elif type(physicalFile) == types.StringType:
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
    try:
      res = storageElement.getAccessUrl(pfns)
    except Exception, x:
      gLogger.exception(lException=x)
      raise x
    if not res['OK']:
      errStr = "ReplicaManager.__getPhysicalFileAccessUrl: Failed to get access urls for replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res

  def __initialiseAccountingObject(self,operation,se,files):
    accountingDict = {}
    accountingDict['OperationType'] = operation
    accountingDict['User'] = 'acsmith'
    accountingDict['Protocol'] = 'ReplicaManager'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    accountingDict['Destination'] = se
    accountingDict['TransferTotal'] = files
    accountingDict['TransferOK'] = files
    accountingDict['TransferSize'] = files
    accountingDict['TransferTime'] = 0.0
    accountingDict['FinalStatus'] = 'Successful'
    accountingDict['Source'] = gConfig.getValue('/LocalSite/Site','Unknown')
    oDataOperation = DataOperation()
    oDataOperation.setValuesFromDict(accountingDict)
    return oDataOperation
