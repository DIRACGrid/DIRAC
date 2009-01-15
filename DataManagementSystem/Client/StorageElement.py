""" This is the StorageElement class.

    self.name is the resolved name of the StorageElement i.e CERN-tape
    self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
    self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
    self.localProtocols is a list of the local protocols that were created by StorageFactory
    self.remoteProtocols is a list of the remote protocols that were created by StorageFactory
    self.protocolOptions is a list of dictionaries containing the options found in the CS. (should be removed)
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import sortList
from DIRAC.Core.Utilities.File import getSize
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
import re, time,os,types

class StorageElement:

  def __init__(self,name):
    self.valid = True
    res = StorageFactory().getStorages(name)
    if not res['OK']:
      self.valid = False
      self.name = name
    else:
      factoryDict = res['Value']
      self.name = factoryDict['StorageName']
      self.options = factoryDict['StorageOptions']
      self.localProtocols = factoryDict['LocalProtocols']
      self.remoteProtocols = factoryDict['RemoteProtocols']
      self.storages = factoryDict['StorageObjects']
      self.protocolOptions = factoryDict['ProtocolOptions']
      self.turlProtocols = factoryDict['TurlProtocols']

  def dump(self):
    """
      Dump to the logger a sumary of the StorageElement items
    """
    gLogger.info("StorageElement.dump: Perparing dump for StorageElement %s." % self.name)
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sortList(self.options.keys()):
      outStr = "%s%s: %s\n" % (outStr,key.ljust(15),self.options[key])

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % (outStr,i)
      res = storage.getParameters()
      storageParameters = res['Value']
      for key in sortList(storageParameters.keys()):
        outStr = "%s%s: %s\n" % (outStr,key.ljust(15),storageParameters[key])
      i = i + 1
    gLogger.info(outStr)

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementName(self):
    gLogger.debug("StorageElement.getStorageElementName: The Storage Element name is %s." % self.name)
    return S_OK(self.name)

  def isValid(self):
    gLogger.debug("StorageElement.isValid: Determining whether the StorageElement %s is valid for use." % self.name)
    return S_OK(self.valid)

  def getProtocols(self):
    """ Get the list of all the protocols defined for this Storage Element
    """
    gLogger.debug("StorageElement.getProtocols: Obtaining all protocols for %s." % self.name)
    allProtocols = self.localProtocols+self.remoteProtocols
    return S_OK(allProtocols)

  def getRemoteProtocols(self):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    gLogger.debug("StorageElement.getRemoteProtocols: Obtaining remote protocols for %s." % self.name)
    return S_OK(self.remoteProtocols)

  def getLocalProtocols(self):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    gLogger.debug("StorageElement.getLocalProtocols: Obtaining local protocols for %s." % self.name)
    return S_OK(self.localProtocols)

  def getStorageElementOption(self,option):
    """ Get the value for the option supplied from self.options
    """
    gLogger.info("StorageElement.getStorageElementOption: Obtaining %s option for Storage Element %s." % (option,self.name))
    if self.options.has_key(option):
      optionValue = self.options[option]
      return S_OK(optionValue)
    else:
      errStr = "StorageElement.getStorageElementOption: Option not defined for SE."
      gLogger.error(errStr,"%s for %s" % (option,self.name))
      return S_ERROR(errStr)

  def getStorageParameters(self,protocol):
    """ Get protocol specific options
    """
    gLogger.info("StorageElement.getStorageParameters: Obtaining storage parameters for %s protocol %s." % (self.name,protocol))
    res = self.getProtocols()
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getStorageParameters: Requested protocol not available for SE."
      gLogger.error(errStr,'%s for %s' % (protocol,self.name))
      return S_ERROR(errStr)
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK(storageParameters)
    errStr = "StorageElement.getStorageParameters: Requested protocol supported but no object found."
    gLogger.error(errStr,"%s for %s" % (protocol,self.name))
    return S_ERROR(errStr)

  def isLocalSE(self):
    """ Test if the Storage Element is local in the current context
    """
    gLogger.debug("StorageElement.isLocalSE: Determining whether %s is a local SE." % self.name)
    configStr = '/LocalSite/Site'
    localSite = gConfig.getValue(configStr)
    localSEs = getSEsForSite(localSite)['Value']
    if self.name in localSEs:
      return S_OK(True)
    else:
      return S_OK(False)

  #################################################################################################
  #
  # These are the basic get functions for pfn manipulation
  #

  def getPfnForProtocol(self,pfn,protocol,withPort=True):
    """ Transform the input pfn into another with the given protocol for the Storage Element.
    """
    res = self.getProtocols()
    if type(protocol) == types.StringType:
      protocols = [protocol]
    elif type(protocol) == types.ListType:
      protocols = protocol
    else:
      errStr = "StorageElement.getPfnForProtocol: Supplied protocol must be string or list of strings."
      gLogger.error(errStr,"%s %s" % (protocol,self.name))
      return S_ERROR(errStr)
    availableProtocols = res['Value']
    protocolsToTry = []
    for protocol in protocols:
      if protocol in availableProtocols:
        protocolsToTry.append(protocol)
      else:
        errStr = "StorageElement.getPfnForProtocol: Requested protocol not available for SE."
        gLogger.error(errStr,'%s for %s' % (protocol,self.name))
    if not protocolsToTry:
      errStr = "StorageElement.getPfnForProtocol: None of the requested protocols were available for SE."
      gLogger.error(errStr,'%s for %s' % (protocol,self.name))
      return S_ERROR(errStr)
    # Check all available storages for required protocol then contruct the PFN
    for storage in self.storages:
      res = storage.getParameters()
      if res['Value']['ProtocolName'] in protocolsToTry:
        res = pfnparse(pfn)
        if res['OK']:
          res = storage.getProtocolPfn(res['Value'],withPort)
          if res['OK']:
            return res
    errStr = "StorageElement.getPfnForProtocol: Failed to get PFN for requested protocols."
    gLogger.error(errStr,"%s for %s" % (protocols,self.name))
    return S_ERROR(errStr)

  def getPfnPath(self,pfn):
    """  Get the part of the PFN path below the basic storage path.
         This path must coinside with the LFN of the file in order to be compliant with the LHCb conventions.
    """
    res = pfnparse(pfn)
    if not res['OK']:
      return res
    fullPfnPath = '%s/%s' % (res['Value']['Path'],res['Value']['FileName'])

    # Check all available storages and check whether the pfn is for that protocol
    pfnPath = ''
    for storage in self.storages:
      res = storage.isPfnForProtocol(pfn)
      if res['OK']:
        if res['Value']:
          res = storage.getParameters()
          saPath = res['Value']['Path']
          if not saPath:
            # If the sa path doesn't exist then the pfn path is the entire string
            pfnPath = fullPfnPath
          else:
            if re.search(saPath,fullPfnPath):
              # Remove the sa path from the fullPfnPath
              pfnPath = fullPfnPath.replace(saPath,'')
      if pfnPath:
        return S_OK(pfnPath)
    # This should never happen. DANGER!!
    errStr = "StorageElement.getPfnPath: Failed to get the pfn path for any of the protocols!!"
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def getPfnForLfn(self,lfn):
    """ Get the full PFN constructed from the LFN.
    """
    for storage in self.storages:
      res = storage.getPFNBase()
      if res['OK']:
        fullPath = "%s%s" % (res['Value'],lfn)
        return S_OK(fullPath)
    # This should never happen. DANGER!!
    errStr = "StorageElement.getPfnForLfn: Failed to get the full pfn for any of the protocols!!"
    gLogger.error(errStr)
    return S_ERROR(errStr)

  #################################################################################################
  #
  # These are the file transfer methods
  #

  def __ToMoveToReplicaManagerForGetFile():
          ###############################################################################################
          # Pre-transfer check. Check if file exists, get the size and check against the catalogue
          res = storage.exists(protocolPfn)
          if res['OK']:
            if res['Value']['Successful'].has_key(protocolPfn):
              fileExists = res['Value']['Successful'][protocolPfn]
              if not fileExists:
                errStr = "StorageElement.getFile: Source file does not exist."
                gLogger.error(errStr,'%s for protocol %s' % (protocolPfn,protocolName))
                return S_ERROR(errStr)
              else:
                infoStr = "StorageElement.getFile: File exists, checking size."
                gLogger.info(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
                res = storage.getFileSize(protocolPfn)
                if res['OK']:
                  if res['Value']['Successful'].has_key(protocolPfn):
                    protocolSize = res['Value']['Successful'][protocolPfn]
                    if not catalogueFileSize == protocolSize:
                      infoStr ="StorageElement.getFile: Physical file size and catalogue file size mismatch."
                      gLogger.error(infoStr,'%s : Physical %s, Catalogue %s' % (protocolPfn,protocolSize,catalogueFileSize))
                      return S_ERROR('StorageElement.getFile: Physical file zero size')
                    if protocolSize == 0:
                      infoStr ="StorageElement.getFile: Physical file found with zero size."
                      gLogger.error(infoStr,'%s at %s' % (protocolPfn,self.name))
                      res = storage.removeFile(protocolPfn)
                      if res['OK']:
                        if res['Value']['Successful'].has_key(protocolPfn):
                          infoStr ="StorageElement.getFile: Removed zero size file from storage."
                          gLogger.info(infoStr,'%s with protocol %s' % (protocolPfn,protocolName))
                        else:
                          infoStr ="StorageElement.getFile: Failed to removed zero size file from storage."
                          gLogger.error(infoStr,'%s with protocol %s' % (protocolPfn,protocolName))
                      else:
                        infoStr ="StorageElement.getFile: Failed to removed zero size file from storage."
                        gLogger.error(infoStr,'%s with protocol %s' % (protocolPfn,protocolName))
                      return S_ERROR('StorageElement.getFile: Physical file zero size')
                    else:
                      infoStr ="StorageElement.getFile: %s file size: %s." %  (protocolPfn,protocolSize)
                      gLogger.info(infoStr)
                else:
                  infoStr = "StorageElement.getFile: Failed to get remote file size."
                  gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
            else:
              infoStr = "StorageElement.getFile: Failed to determine whether file exists."
              gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))
          else:
            infoStr = "StorageElement.getFile: Failed to determine whether file exists."
            gLogger.error(infoStr,'%s for protocol %s' % (protocolPfn,protocolName))

  #################################################################################################
  #
  # These are the directory manipulation methods
  #

  def getDirectory():
    """ Missing
    """
    pass

  def putDirectory(self,localDirectory,directoryPath):
    """ This will recursively put the directory on the storage

        'localDirectory' is the local directory
        'directoryPath' is a string containing the destination directory
    """
    successful = {}
    failed = {}
    localSE = self.isLocalSE()['Value']
    # Try all of the storages one by one
    for storage in self.storages:
      pfnDict = {}
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocols:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.putDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.putDirectory: Generating protocol PFNs for %s." % protocolName)
        res =  storage.getCurrentURL(directoryPath)
        if res['OK']:
          storageDirectory = res['Value']
          res  = pfnparse(storageDirectory)
          if not res['OK']:
            errStr = "StorageElement.putDirectory: Failed to parse supplied PFN."
            gLogger.error(errStr,"%s: %s" % (storageDirectory,res['Message']))
          else:
            res = storage.getProtocolPfn(res['Value'],True)
            if not res['OK']:
              infoStr = "StorageElement.putDirectory%s." % res['Message']
              gLogger.error(infoStr,'%s for protocol %s' % (storageDirectory,protocolName))
            else:
              remoteDirectory = res['Value']
              res = storage.putDirectory((localDirectory,remoteDirectory))
              if not res['OK']:
                infoStr = "StorageElement.putDirectory: Completely failed to put directory."
                gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
              else:
                if res['Value']['Successful'].has_key(remoteDirectory):
                  return S_OK(res['Value']['Successful'][remoteDirectory])
    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.putDirectory: Failed to put directory with all protocols."
    gLogger.error(errStr,localDirectory)
    return S_ERROR(errStr)

  ###########################################################################################
  #
  # This is the generic wrapper for simple operations
  #

  def retransferOnlineFile(self,pfn):
    return self.__executeFunction(pfn,'retransferOnlineFile')

  def exists(self,pfn):
    return self.__executeFunction(pfn,'exists')

  def isFile(self,pfn):
    return self.__executeFunction(pfn,'isFile')

  def getFile(self,pfn,localPath=False):
    return self.__executeFunction(pfn,'getFile',{'localPath':localPath})
  
  def putFile(self,pfn):
    return self.__executeFunction(pfn,'putFile')

  def replicateFile(self,pfn,sourceSize):
    return self.__executeFunction(pfn,'putFile',{'sourceSize':sourceSize}) 

  def getFileMetadata(self,pfn):
    return self.__executeFunction(pfn,'getFileMetadata')

  def getFileSize(self,pfn):
    return self.__executeFunction(pfn,'getFileSize')

  def getAccessUrl(self,pfn,protocol=False):
    if not protocol:
      return self.__executeFunction(pfn,'getTransportURL',{'protocols':self.turlProtocols})
    else:
      return self.__executeFunction(pfn,'getTransportURL',{'protocols':[protocol]})

  def removeFile(self,pfn):
    return self.__executeFunction(pfn,'removeFile')

  def prestageFile(self,pfn):
    return self.__executeFunction(pfn,'prestageFile')
   
  def prestageFileStatus(self,pfn):
    return self.__executeFunction(pfn,'prestageFileStatus')

  def pinFile(self,pfn,lifetime=60*60*24):
    return self.__executeFunction(pfn,'pinFile',{'lifetime':lifetime})

  def releaseFile(self,pfn):
    return self.__executeFunction(pfn,'releaseFile')

  def isDirectory(self,pfn):
    return self.__executeFunction(pfn,'isDirectory')

  def getDirectoryMetadata(self,pfn):
    return self.__executeFunction(pfn,'getDirectoryMetadata')

  def getDirectorySize(self,pfn):
    return self.__executeFunction(pfn,'getDirectorySize')

  def listDirectory(self,pfn):
    return self.__executeFunction(pfn,'listDirectory')

  def removeDirectory(self,pfn,recursive=False):
    return self.__executeFunction(pfn,'removeDirectory',{'recursive':recursive})

  def createDirectory(self,pfn):
    return self.__executeFunction(pfn,'createDirectory')

  def __executeFunction(self,pfn,method,argsDict={}):
    """
        'pfn' is the physical file name (as registered in the LFC)
        'method' is the functionality to be executed
    """
    if type(pfn) in types.StringTypes:
      pfns = {pfn:False}
    elif type(pfn) == types.ListType:
      pfns = {}
      for url in pfn:
        pfns[url] = False
    elif type(pfn) == types.DictType:
      pfns = pfn
    else:
      errStr = "StorageElement.__executeFunction: Supplied pfns must be string or list of strings or a dictionary."
      gLogger.error(errStr)
      return S_ERROR(errStr)

    if not pfns:
      gLogger.debug("StorageElement.__executeFunction: No pfns supplied.")
      return S_OK({'Failed':{}, 'Successful':{}})
    gLogger.debug("StorageElement.__executeFunction: Attempinting to perform '%s' operation with %s pfns." % (method,len(pfns)))
 
    successful = {}
    failed = {}
    localSE = self.isLocalSE()['Value']
    # Try all of the storages one by one
    for storage in self.storages:

      # Determine whether to use this storage object
      protocolName = storage.getParameters()['Value']['ProtocolName']
      useProtocol = True
      if not pfns:
        useProtocol = False
        gLogger.debug("StorageElement.__executeFunction: No pfns to be attempted for %s protocol." % protocolName)
      elif not (protocolName in self.remoteProtocols) and not localSE:
        # If the SE is not local then we can't use local protocols
        useProtocol = False
        gLogger.debug("StorageElement.__executeFunction: Protocol not appropriate for use: %s." % protocolName)

      if useProtocol:
        gLogger.debug("StorageElement.__executeFunction: Generating %s protocol PFNs for %s." % (len(pfns),protocolName))
        res = self.__generatePfnDict(pfns.keys(),storage,failed)
        pfnDict = res['Value']
        failed = res['Failed']    
        if not len(pfnDict.keys()) > 0:
          gLogger.debug("StorageElement.__executeFunction No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.debug("StorageElement.__executeFunction: Attempting to perform '%s' for %s physical files." % (method,len(pfnDict.keys())))
          pfnsToUse = {}
          for pfn in pfnDict.keys():
            pfnsToUse[pfn] = pfns[pfnDict[pfn]]
          if argsDict:
            execString = "res = storage.%s(pfnsToUse" % method
            for argument,value in argsDict.items():
              if type(value) == types.StringType:
                execString = "%s, %s='%s'" % (execString,argument,value) 
              else:
                execString = "%s, %s=%s" % (execString,argument,value)
            execString = "%s)" % execString
          else:
            execString = "res = storage.%s(pfnsToUse)" % method
          try:
            exec(execString)
          except AttributeError,errMessage:
            exceptStr = "StorageElement.__executeFunction: Exception while perfoming %s." % method
            gLogger.exception(exceptStr,str(errMessage))
            res = S_ERROR(exceptStr)

          if not res['OK']:
            errStr = "StorageElement.__executeFunction: Completely failed to perform %s." % method
            gLogger.error(errStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
            for pfn in pfnDict.values():
              if not failed.has_key(pfn):
                  failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],res['Message'])
          else:
            for protocolPfn,pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(pfn):
                  failed[pfn] = ''
                if res['Value']['Failed'].has_key(protocolPfn):
                  failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])
                else:
                  failed[pfn] = "%s %s" % (failed[pfn],'No error returned from plug-in') 
              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(pfn):
                  failed.pop(pfn)
                pfns.pop(pfn)

    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def __generatePfnDict(self,pfns,storage,failed):
    pfnDict = {}
    for pfn in pfns:
      res  = pfnparse(pfn)
      if not res['OK']:
        errStr = "StorageElement.__generatePfnDict: Failed to parse supplied PFN."
        gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
        if not failed.has_key(pfn):
          failed[pfn] = ''
        failed[pfn] = "%s %s" % (failed[pfn],errStr)
      else:
        res = storage.getProtocolPfn(res['Value'],True)
        if not res['OK']:
          errStr = "StorageElement.__generatePfnDict %s." % res['Message']
          gLogger.error(errStr,'%s for protocol %s' % (pfn,protocolName))
          if not failed.has_key(pfn):
            failed[pfn] = ''
          failed[pfn] = "%s %s" % (failed[pfn],errStr)
        else:
          pfnDict[res['Value']] = pfn
    res = S_OK(pfnDict)
    res['Failed'] = failed
    return res
