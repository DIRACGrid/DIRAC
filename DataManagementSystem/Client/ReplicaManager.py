""" This is the Replica Manager which links the functionalities of StorageElement and FileCatalogue. """

__RCSID__ = "$Id: ReplicaManager.py,v 1.1 2007/12/07 14:45:24 acsmith Exp $"

import re, time, commands, random
from types import *

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.File import getSize

class ReplicaManager:

  def __init__( self ):
    """ Constructor function.
    """

    self.fileCatalogue = FileCatalogue()
    self.accountingClient = None
    self.registrationProtocol = 'SRM2'

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
    res = self.fileCatalogue.checkFileExistence(lfn,guid)
    if not res['OK']:
      return res

    ##########################################################
    #  Perform the put here
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.putAndRegister: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
    res = storageElement.putFile(file,path)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to put file to Storage Element."
      errMessage = res['Message']
      gLogger.error(errStr,"%s: %s" % (file,errMessage))
      return S_ERROR("%s %s" % errStr,errMessage)
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
    res = self.fileCatalogue.addFile(lfn,pfnForRegistration,size,destinationSE,guid)
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
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr,"%s: %s" % (lfn,res['Message']))
    lfnReplicas = replicas['Value']['Successful'][lfn]
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
    for diracSE,pfn in lfnReplicas.keys():
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

  def __isLocalSE(self,se):
    """  Check if the SE is local in the current environment
    """

    lse = cfgSvc.get('Site','LocalSE')
    localse = map( lambda x : x.strip(), lse.split(','))
    if se in localse:
      return 1
    else:
      return 0

#    site = cfgSvc.get('Site','Site')
#
#    siteShort = site.split('.')[1]
#
#    sesite = se.split('_')[0]
#
#    if siteShort == sesite:
#      return 1
#    else:
#      return 0

  def __get_SE_PFN_Names(self,lfn):
    """Get SE's having the given lfn

    Get all the names of all the SE's posseding the given lfn.
    The returned list of SE's is ordered to contain local SE's
    first.
    """

    replicas = {}
    for fcname,fc in self.fcs.items():
      result = fc.getPfnsByLfn (lfn)
      if result['Status'] == 'OK':
        for se,pfn in result['Replicas'].items():
          replicas[se] = pfn

    ses = replicas.keys()
    if not ses:
      return []

    result = []
    bannedSource = cfgSvc.get('ReplicaManager','BannedSource',[])
    for se in ses:
      if se in bannedSource:
        continue
      if self.__isLocalSE(se):
        result.append((se,replicas[se],'local'))
    for se in ses:
      if se in bannedSource:
        continue
      if not self.__isLocalSE(se):
        result.append((se,replicas[se],'remote'))

    # Take the replicas in the arbitrary order until a better criteria
    random.shuffle(result)
    return result

  def replicateAndRegister(self,lfn,se_target,path='',localcache='',se_source='',tr_request={}):
    """Replicate and register file

       Replicates a file specified by its lfn to the se_target SE and register
       the newly created replica to the existing file catalogs
    """

    se_target = self._getStorageName(se_target)

    # Get the replicas of the given lfn
    start = time.time()
    replicas = self.__get_SE_PFN_Names(lfn)
    if not replicas:
      return S_ERROR( 'No replica available' )
    #print "get_SE_PFN_Names time:",time.time()-start
    for serep,pfn,flag in replicas:
      if se_target == serep:
        print "ReplicateAndRegister: replica of",lfn,"already exists in",se_target
        return S_OK( "LFN "+lfn+" already exists in "+se_target )

    log = {}
    # Result cumulates the outcome of both replicate and registerReplica
    # operations
    result = S_OK()
    failed_SEs = {}
    resRep = self.replicate(lfn,se_target,path,localcache,se_source,replicas,rr_operation=True)

    if resRep['Status'] == 'OK':
      result.update(resRep)
      result['TargetSE'] = se_target
      log['TransferStatus'] = 'OK'
      result['Log'] = log
      pfn = resRep['PFN']
      #print lfn,pfn,se_target
      resReg = self.registerReplica(lfn,pfn,se_target)
      result.update(resReg)
      result['Log']['RegisterLog'] = resReg['RegisterLog']
    else:
      if resRep.has_key('FailedSEs'):
        # Result is not OK, choose the first failed source SE if any
        # as the result
        failed_se_names = resRep['FailedSEs'].keys()
        se = failed_se_names[0]
        result.update(resRep['FailedSEs'][se])
      else:
        # Just in case - result is not OK but no failed SEs
        result['Status'] = "Error"
        result['Message'] = resRep['Message']

      log['TransferStatus'] = 'Error'
      result['Log'] = log
      result['Size'] = 0
      resReg = S_ERROR()

    if resRep.has_key('FailedSEs'):
      failed_SEs = resRep['FailedSEs']

    if self.accountingClient:

      request = {}
      if os.environ.has_key('TransferID'):
        request['TransferID'] = os.environ['TransferID']
      else:
        request['TransferID'] = "0"
      request['TargetSE'] = se_target
      request.update(tr_request)
      if resRep['Status'] == 'OK':
        result['TransferSuccessRate'] = "1:1"
        if resReg['Status'] == 'OK':
          result['RegistrationSuccessRate'] = "1:1"
        else:
          result['RegistrationSuccessRate'] = "0:1"
      else:
        result['TransferSuccessRate'] = "0:1"
        result['RegistrationSuccessRate'] = "0:0"
      self.accountingClient.sendAccountingInfo(request,result)

      source_se = None
      if result.has_key('SourceSE'):
        source_se = result['SourceSE']

      if failed_SEs:
        for se,fresult in failed_SEs.items():
          if se != source_se:
            fresult['Size'] = 0
            fresult['TransferSuccessRate'] = "0:1"
            self.accountingClient.sendAccountingInfo(request,fresult)

    return result

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

  def replicate(self,lfn,se_target,path='',localcache='',se_source='',replicas=None,rr_operation=False):
    """Replicate file

       Replicate a file specified by its lfn to the SE sepcified by
       the se_target argument. Optional argument localcache defines
       the path of the local disk space used as a cache before sending
       file to the destination. Optional se_source argument specifies
       the prefered source SE.
    """

    timing = {}
    # Make use of UTC time
    timing['TransferStartDate'] = time.strftime('%Y-%m-%d',time.gmtime())
    timing['TransferStartTime'] = time.strftime('%H:%M:%S',time.gmtime())
    startOp = time.time()

    result = S_ERROR()
    third_party_tried = False
    if not replicas:
      replicas = self.__get_SE_PFN_Names(lfn)
    failed_SEs = {}

    if se_source:
      # If the source is indicated, try the 3d party transfer first
      if self.__check_third_party(source,target):
        third_party_tried = True
        result = self.__replicate_third_party(lfn,se_source,se_target,replicas,path)
        if result['Status'] == "OK":
          message = "File %s replicated \n   from %s to %s via %s protocol" % \
                      (lfn,se_source,se_target,result['Protocol'])
          result['Message'] = message
          result['SourceSE'] = se_source
        else:
          resultFailed = {}
          resultFailed['SourceSE'] = se
          resultFailed.update(result)
          failed_SEs[se_source] = resultFailed

    if result['Status'] != 'OK':
      # Try other possible 3d party transfers
      for se,pfn,flag in replicas:
        #print se,pfn,flag
        if self.__check_third_party(se,se_target) and se != se_source:
          third_party_tried = True
          #print "Trying",se,"source"
          result = self.__replicate_third_party(lfn,se,se_target,replicas,path)
          #print result
          if result['Status'] == "OK":
            message = "File %s replicated \n   from %s to %s via %s protocol" % \
                      (lfn,se,se_target,result['Protocol'])
            result['Message'] = message
            result['SourceSE'] = se
            break
          else:
            resultFailed = {}
            resultFailed['SourceSE'] = se
            resultFailed.update(result)
            failed_SEs[se] = resultFailed

    if result['Status'] != 'OK' and not third_party_tried and replicas:
      # Third party transfers not possible, try through the local cache
      result = self.__replicate_local_cache(lfn,se_target,path,localcache)

    if failed_SEs:
      result['FailedSEs'] = failed_SEs

    # Send accounting information if requested
    result.update(timing)
    if self.accountingClient and not rr_operation:

      print "Sending accounting info in replicate()"
      request = {}
      if os.environ.has_key('TransferID'):
        request['TransferID'] = os.environ['TransferID']
      else:
        request['TransferID'] = "0"
      request['TargetSE'] = se_target
      if result['Status'] == 'OK':
        result['TransferSuccessRate'] = "1:1"
      else:
        result['Size'] = 0
        result['TransferSuccessRate'] = "0:1"
      self.accountingClient.sendAccountingInfo(request,result)

      source_se = None
      if result.has_key('SourceSE'):
        source_se = result['SourceSE']

      for se,fresult in failed_SEs.items():
        if se != source_se:
          fresult['Size'] = 0
          fresult['TransferSuccessRate'] = "0:1"
          self.accountingClient.sendAccountingInfo(request,fresult)

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


  def registerReplica(self,lfn,pfname,se_register,catalog=None):
    """Registers file replica in catalog(s)

    Registers a file replica to one or many catalogs. The specific catalog
    name can be given. Otherwise registers in all the existing
    catalogs
    """

    result = S_OK()
    start = time.time()
    log = []

    se = self._getStorageName(se_register)

    if catalog:
      if self.fcs.has_key(catalog):
        fc = self.fcs[catalog]
        res = fc.addPfn(lfn,pfname,se)
        if res['Status'] != 'OK':
          result = S_ERROR( "Failed to register replica in catalog "+catalog )
        else:
          result = S_OK()
          log.append((fc.getName(),'OK'))
          result['RegisterLog'] = log
      else:
        print "Unknown catalog",catalog
        result = S_ERROR( "Unknown catalog "+catalog )
        log.append((fc.getName(),'Error'))
        result['RegisterLog'] = log
    else:
      for fcname,fc in self.fcs.items():
        res = fc.addPfn(lfn,pfname,se)
        if res['Status'] != 'OK':
          result = S_ERROR( "Failed to register in catalog "+fcname )
          print result
          log.append((fc.getName(),'Error'))
          result['RegisterLog'] = log
        else:
          log.append((fc.getName(),'OK'))
          result['RegisterLog'] = log

    #print "Registration in",fc.name,time.time()-startF
    end = time.time()
    result['RegisterOperationTime'] = end - start
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

  def getPFNsForLFNs(self,lfnlist):
    for fcname,fc in self.fcs.items():
      if fcname == 'LFC':
        replicadict = fc.getPfnsByLfnList(lfnlist)
    if replicadict['Status']== 'OK':
      return replicadict
    else:
      return S_ERROR('Failed to obtain PFNs from LFC')

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
