""" This is the Replica Manager which links the functionalities of StorageElement and FileCatalogue. """

__RCSID__ = "$Id: ReplicaManager.py,v 1.6 2007/12/09 19:54:49 acsmith Exp $"

import re, time, commands, random,os
from types import *

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
    destPfn = res['Value']
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

  def registerReplica(self,lfn,destPfn,destSE):
    """ Replicate a LFN to a destination SE and register the replica.

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
        return S_OK(res['Value'])
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



  def __check_third_party(self,se1,se2):
    """Check the availability of the third party transfer

       Check that the third party transfer is possible between
       the two Storage Elements and return the protocols with
       which the transfer can be done
    """

    protocols = ['gsiftp','gridftp','srm']

    selem1 = StorageElement(se1)
    selem2 = StorageElement(se2)
    #print se1,selem1.getProtocols()
    #print se2,selem2.getProtocols()

    source_protocols = []
    # Check the source SE
    for p in protocols:
      if p in selem1.getProtocols():
        source_protocols.append(p)
    target_protocols = []
    # Check the target SE
    for p in protocols:
      if p in selem2.getProtocols():
        target_protocols.append(p)

    if source_protocols:
      return source_protocols,target_protocols
    else:
      return []

  def __replicate_third_party(self,lfn,source,target,replicas,path):
    """Replicate file by a thrid party transfer

       Replicate the file specified by its LFN from the source
       SE to the target SE by a third party transfer
    """

    result = S_ERROR("Third party transfer is not possible")

    for se,pfn,flag in replicas:
      if se == source:
        sprotocols,tprotocols = self.__check_third_party(source,target)
        for p in tprotocols:
          selement = StorageElement(source)
          for sp in sprotocols:
            spfn = selement.getPfnForProtocol(pfn,sp)
            spfn = spfn.replace("gridftp:","gsiftp:")
            if path:
              tpath = path
            else:
              tpath = os.path.dirname(selement.getPfnPath(pfn))
            result = self.copy(spfn,target,tpath,p,cr_operation=True)
            if result['Status'] == 'OK':
              result['Protocol'] = p.upper()
              break
          if result['Status'] == 'OK':
            break

    return result


  def __replicate_local_cache(self,lfn,se_target,path,localcache=''):
    """ Replicate a given LFN by copying to the intermediate local
        disk cache
    """

    cwd = os.getcwd()
    if localcache:
      try:
        os.chdir(localcache)
      except:
        return S_ERROR( 'Failed chdir to local cache %s' % localcache )

    fname = os.path.basename(lfn)
    if path:
      fpath = path
    else:
      fpath = os.path.dirname(lfn)
    result = self.get(lfn)
    if result['Status'] != 'OK':
      if os.path.exists(fname):
        os.remove(fname)
      os.chdir(cwd)
      return result

    result = self.copy(fname,se_target,fpath)

    if os.path.exists(fname):
      os.remove(fname)
    os.chdir(cwd)
    return result

  def removeReplica(self,lfn,se,pfn = None):
    """Remove physical replica

       Remove a physical replica of the file specified by lfn from
       the storage specified by the se argument
    """

    if pfn:
      # We know the pfn, do not ask for replicas
      selement = StorageElement(se)
      result = selement.removeFile(pfn)
      if result['Status'] == 'OK':
        t = result['RemoveOperationTime']
        print "Replica of",lfn,"at",se,"removed in",t,'sec'
        resultCatalog = self.removeReplicaFromCatalog(lfn,se)
        result.update(resultCatalog)
        return result
    else:
      # We do not know the pfn, should ask for replicas
      replicas = self.__get_SE_PFN_Names(lfn)
      for sel,pfn,flag in replicas:
        if sel == se:
          selement = StorageElement(se)
          result = selement.removeFile(pfn)
          if result['Status'] == 'OK':
            t = result['RemoveOperationTime']
            protocol = result['RemoveProtocol']
            print "Replica of",lfn,"at",se,"removed in",t,'sec via',protocol,'protocol'
            resultCatalog = self.removeReplicaFromCatalog(lfn,se)
            result.update(resultCatalog)
            return result

    result = S_ERROR('Failed to remove replica of '+lfn+" at "+se)
    return result

  def removeReplicaFromCatalog(self,lfn,se,catalog=None):
    """Remove replica from catalog

       Removes the replica specified by its lfn and se from all the existing
       catalogs.If catalog argument is given, only the record in the given
       catalog is deleted.
    """

    result = S_OK()
    start = time.time()
    log = []

    replicas = self.__get_SE_PFN_Names(lfn)

    if catalog:
      if self.fcs.has_key(catalog):
        fc = self.fcs[catalog]
        for sel,pfn,flag in replicas:
          if se == sel:
            res = fc.removePfn(lfn,pfn)
            if res['Status'] != 'OK':
              result = S_ERROR("Failed to remove replica from "+catalog)
              result['Message'] = "Failed to remove replica from "+catalog
              print result['Message']
            else:
              result = S_OK()
              log.append((fc.getName(),'OK'))
              result['RemoveReplicaLog'] = log
      else:
        print "Unknown catalog",catalog
        result = S_ERROR()
        result['Message'] = "Unknown catalog "+catalog
        log.append((fc.getName(),'Error'))
        result['RemoveReplicaLog'] = log
    else:
      for fcname,fc in self.fcs.items():
        for sel,pfn,flag in replicas:
          if se == sel:
            res = fc.removePfn(lfn,pfn)
            if res['Status'] != 'OK':
              result = S_ERROR()
              result['Message'] = "Failed to remove replica from "+fcname
              print result['Message']
              log.append((fc.getName(),'Error'))
              result['RemoveReplicaLog'] = log
            else:
              log.append((fc.getName(),'OK'))
              result['RemoveReplicaLog'] = log

    #print "Registration in",fc.name,time.time()-startF
    end = time.time()
    result['RemoveReplicaOperationTime'] = end - start
    return result

  def removeFileFromCatalog(self,lfn,catalog=None):
    """Remove file from catalog

       Removes the file specified by its lfn from all the existing catalogs.
       If catalog argument is given, only the record in the given catalog
       is deleted.
    """

    result = S_OK()
    start = time.time()
    log = []

    if catalog:
      if self.fcs.has_key(catalog):
        fc = self.fcs[catalog]
        res = fc.rmFile(lfn)
        if res['Status'] != 'OK':
          result = S_ERROR()
          result['Message'] = "Failed to remove replica from catalog "+catalog
        else:
          result = S_OK()
          log.append((fc.getName(),'OK'))
          result['RemoveFileLog'] = log
      else:
        print "Unknown catalog",catalog
        result = S_ERROR()
        result['Message'] = "Unknown catalog "+catalog
        log.append((fc.getName(),'Error'))
        result['RemoveFileLog'] = log
    else:
      for fcname,fc in self.fcs.items():
        res = fc.rmFile(lfn)
        if res['Status'] != 'OK':
          result = S_ERROR()
          result['Message'] = "Failed to remove in catalog "+fcname
          log.append((fc.getName(),'Error'))
          result['RemoveFileLog'] = log
        else:
          log.append((fc.getName(),'OK'))
          result['RemoveFileLog'] = log

    #print "Registration in",fc.name,time.time()-startF
    end = time.time()
    result['RemoveFileFromCatalogTime'] = end - start
    return result

  def removeFile(self,lfn):
    """Remove file from the grid

       Removes all the physical replicas of the given file together
       with all the associated records in all the File Catalogs
    """

    failed_se = []
    log = []

    replicas = self.__get_SE_PFN_Names(lfn)
    for se,pfn,flag in replicas:
      result = self.removeReplica(lfn,se,pfn)
      if result['Status'] != "OK":
        failed_se.append(se)
        log.append((se,'Error'))
      else:
        log.append((se,'OK'))

    if not failed_se:
      result = self.removeFileFromCatalog(lfn)
    else:
      print "Failed to remove replicas at",string.join(failed_se,",")
      result = S_ERROR("Failed to remove all replicas for "+lfn)
      result['RemoveLog'] = log

    return result

  def getFileMetaData(self,lfn,site):
    """
       This method is used to get the meta data for files stored on SRM storages

       INPUT: type(lfn) = string/list  if string then it will be converted
              type(site)= string This is the DIRAC SE definition

       OUTPUT: type(return) = dict
               If successful will be S_OK() with extra dict keys for the LFNs found with metadata
               The values of these keys will be the metadata
    """
    #attempting to instatiate srm storage for site
    try:
      selement = StorageElement(site)
      storages = selement.getStorage(['srm'])
      storage = storages[0]
    except Exception,x:
      errStr = "Failed to create SRM storage for "+site+" "+str(x)
      print errStr
      return S_ERROR(errStr)

    #create list of lfns from the input (could be single file)
    lfns = []
    if type(lfn) != list:
      lfns.append(lfn)
    else:
      lfns = lfn

    #now get the replicas for the lfns
    result = self.getPFNsForLFNs(lfns)
    #find the lfns with replicas at the given site
    files = {} #dictionary containing the LFN as key and PFN as value
    if result['OK']:
      replicasDict = result['Replicas']
      for lfn in replicasDict.keys():
        if site in replicasDict[lfn].keys():
          files[lfn] = replicasDict[lfn][site]
      surls = files.values()
      result = storage.getMetaData(surls)
      if not result['OK']:
        return result
      else:
        returnDict = S_OK()
        for surl in result.keys():
          for lfn in lfns:
            if re.search(lfn,surl):
              returnDict[lfn] = result[surl]
        return returnDict
    else:
      errorStr = 'Failed to get replicas from the LFC'
      return S_ERROR(errorStr)

  def getPFNMetaData(self,pfn,site):
    """
       This method is used to get the meta data for pfns stored on SRM storages

       INPUT: type(pfns) = string/list  if string then it will be converted

       OUTPUT: type(return) = dict
               If successful will be S_OK() with extra dict keys for the PFNs found with metadata
               The values of these keys will be the metadata
    """
    #attempting to instatiate srm storage for site
    try:
      selement = StorageElement(site)
      storages = selement.getStorage(['srm'])
      storage = storages[0]
    except Exception,x:
      errStr = "Failed to create SRM storage for "+site+" "+str(x)
      print errStr
      return S_ERROR(errStr)

    #create list of lfns from the input (could be single file)
    pfns = []
    if type(pfn) != list:
      pfns.append(pfn)
    else:
      pfns = pfn

    result = storage.getMetaData(pfns)
    if not result['OK']:
      errorStr = 'Failed to get replicas from the LFC'
      result = S_ERROR(errorStr)
    return result
