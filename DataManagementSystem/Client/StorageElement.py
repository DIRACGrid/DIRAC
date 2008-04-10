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

  def isValid(self):
    gLogger.info("StorageElement.isValid: Determining whether the StorageElement %s is valid for use." % self.name)
    return S_OK(self.valid)

  def getStorageElementName(self):
    return S_OK(self.name)
  #################################################################################################
  #
  # These are the basic get functions to get information about the supported protocols
  #

  def getProtocols(self):
    """ Get the list of all the protocols defined for this Storage Element
    """
    gLogger.info("StorageElement.getProtocols: Obtaining all protocols for %s." % self.name)
    allProtocols = self.localProtocols+self.remoteProtocols
    return S_OK(allProtocols)

  def getRemoteProtocols(self):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    gLogger.info("StorageElement.getRemoteProtocols: Obtaining remote protocols for %s." % self.name)
    return S_OK(self.remoteProtocols)

  def getLocalProtocols(self):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    gLogger.info("StorageElement.getLocalProtocols: Obtaining local protocols for %s." % self.name)
    return S_OK(self.localProtocols)

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

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
    gLogger.info("StorageElement.isLocalSE: Determining whether %s is a local SE." % self.name)
    configStr = '/LocalSite/Site'
    localSite = gConfig.getValue(configStr)
    configStr = '/Resources/SiteLocalSEMapping/%s' % localSite
    localSEs = gConfig.getValue(configStr,[])
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
  # These are the directory manipulation methods
  #

  def createDirectory(self,directoryUrl):
    """ This will recursively create the directories on the storage until the desired path

        'directoryUrl' is a string containing the directory to be created
    """
    if type(directoryUrl) == types.StringType:
      directoryUrls = [directoryUrl]
    elif type(directoryUrl) == types.ListType:
      directoryUrls = directoryUrl
    else:
      return S_ERROR("StorageElement.createDirectory: Supplied directory must be string or list of strings")
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
        gLogger.info("StorageElement.createDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.createDirectory: Generating protocol PFNs for %s." % protocolName)
        for directoryUrl in directoryUrls:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(directoryUrl):
            res =  storage.getCurrentURL(directoryUrl)
            if res['OK']:
              storageDirectory = res['Value']
              res  = pfnparse(storageDirectory)
              if not res['OK']:
                errStr = "StorageElement.createDirectory: Failed to parse supplied PFN."
                gLogger.error(errStr,"%s: %s" % (storageDirectory,res['Message']))
                if not failed.has_key(directoryUrl):
                  failed[directoryUrl] = ''
                failed[directoryUrl] = "%s %s" % (failed[directoryUrl],errStr)
              else:
                res = storage.getProtocolPfn(res['Value'],True)
                if not res['OK']:
                  infoStr = "StorageElement.createDirectory%s." % res['Message']
                  gLogger.error(infoStr,'%s for protocol %s' % (directoryUrl,protocolName))
                else:
                  pfnDict[res['Value']] = directoryUrl
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.createDirectory: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.createDirectory: Attempting to create %s directories." % len(pfnDict.keys()))
          res = storage.createDirectory(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.createDirectory: Completely failed to create directories."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,directoryUrl in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(directoryUrl):
                  failed[directoryUrl] = ''
                failed[directoryUrl] = "%s %s" % (failed[directoryUrl],res['Value']['Failed'][protocolPfn])
              else:
                successful[directoryUrl] = protocolPfn
                if failed.has_key(directoryUrl):
                  failed.pop(directoryUrl)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

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

  def removeDirectory(self,directoryUrl):
    """ This method removes the contents of a directory on the storage including files and subdirectories.

        'directoryUrl' is the directory on the storage to be removed
    """
    if type(directoryUrl) == types.StringType:
      directoryUrls = [directoryUrl]
    elif type(directoryUrl) == types.ListType:
      directoryUrls = directoryUrl
    else:
      return S_ERROR("StorageElement.removeDirectory: Supplied directory must be string or list of strings")
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
        gLogger.info("StorageElement.removeDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.removeDirectory: Generating protocol PFNs for %s." % protocolName)
        for directoryUrl in directoryUrls:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(directoryUrl):
            res  = pfnparse(directoryUrl)
            if not res['OK']:
              errStr = "StorageElement.removeDirectory: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (directoryUrl,res['Message']))
              if not failed.has_key(directoryUrl):
                failed[directoryUrl] = ''
              failed[directoryUrl] = "%s %s" % (failed[directoryUrl],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.removeDirectory%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (directoryUrl,protocolName))
              else:
                pfnDict[res['Value']] = directoryUrl
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.removeDirectory: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.removeDirectory: Attempting to remove %s directories." % len(pfnDict.keys()))
          res = storage.removeDirectory(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.removeDirectory: Completely failed to remove directories."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,directoryUrl in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(directoryUrl):
                  failed[directoryUrl] = ''
                failed[directoryUrl] = "%s %s" % (failed[directoryUrl],res['Value']['Failed'][protocolPfn])
              else:
                successful[directoryUrl] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(directoryUrl):
                  failed.pop(directoryUrl)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def listDirectory(self,directoryUrl):
    """ This method lists the contents of the supplied directories

        'directoryUrl' is the directory on the storage to be listed
    """
    if type(directoryUrl) == types.StringType:
      directoryUrls = [directoryUrl]
    elif type(directoryUrl) == types.ListType:
      directoryUrls = directoryUrl
    else:
      return S_ERROR("StorageElement.listDirectory: Supplied directory must be string or list of strings")
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
        gLogger.info("StorageElement.listDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.listDirectory: Generating protocol PFNs for %s." % protocolName)
        for directoryUrl in directoryUrls:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(directoryUrl):
            res  = pfnparse(directoryUrl)
            if not res['OK']:
              errStr = "StorageElement.listDirectory: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (directoryUrl,res['Message']))
              if not failed.has_key(directoryUrl):
                failed[directoryUrl] = ''
              failed[directoryUrl] = "%s %s" % (failed[directoryUrl],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.listDirectory%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (directoryUrl,protocolName))
              else:
                pfnDict[res['Value']] = directoryUrl
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.listDirectory: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.listDirectory: Attempting to list %s directories." % len(pfnDict.keys()))
          res = storage.listDirectory(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.listDirectory: Completely failed to list directories."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,directoryUrl in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(directoryUrl):
                  failed[directoryUrl] = ''
                failed[directoryUrl] = "%s %s" % (failed[directoryUrl],res['Value']['Failed'][protocolPfn])
              else:
                successful[directoryUrl] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(directoryUrl):
                  failed.pop(directoryUrl)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectoryMetadata(self,directoryUrl):
    """ This method gets the metadata associated to directories

        'directoryUrl' is the directory on the storage to be removed
    """
    if type(directoryUrl) == types.StringType:
      directoryUrls = [directoryUrl]
    elif type(directoryUrl) == types.ListType:
      directoryUrls = directoryUrl
    else:
      return S_ERROR("StorageElement.getDirectoryMetadata: Supplied directory must be string or list of strings")
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
        gLogger.info("StorageElement.getDirectoryMetadata: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.getDirectoryMetadata: Generating protocol PFNs for %s." % protocolName)
        for directoryUrl in directoryUrls:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(directoryUrl):
            res  = pfnparse(directoryUrl)
            if not res['OK']:
              errStr = "StorageElement.getDirectoryMetadata: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (directoryUrl,res['Message']))
              if not failed.has_key(directoryUrl):
                failed[directoryUrl] = ''
              failed[directoryUrl] = "%s %s" % (failed[directoryUrl],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.getDirectoryMetadata%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (directoryUrl,protocolName))
              else:
                pfnDict[res['Value']] = directoryUrl
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.getDirectoryMetadata: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.getDirectoryMetadata: Attempting to get metadata %s directories." % len(pfnDict.keys()))
          res = storage.getDirectoryMetadata(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.getDirectoryMetadata: Completely failed to get metadata for directories."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,directoryUrl in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(directoryUrl):
                  failed[directoryUrl] = ''
                failed[directoryUrl] = "%s %s" % (failed[directoryUrl],res['Value']['Failed'][protocolPfn])
              else:
                successful[directoryUrl] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(directoryUrl):
                  failed.pop(directoryUrl)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getDirectorySize(self,directoryUrl):
    """ This method gets the size of the contents of a directory

        'directoryUrl' is the directory on the storage to be removed
    """
    if type(directoryUrl) == types.StringType:
      directoryUrls = [directoryUrl]
    elif type(directoryUrl) == types.ListType:
      directoryUrls = directoryUrl
    else:
      return S_ERROR("StorageElement.getDirectorySize: Supplied directory must be string or list of strings")
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
        gLogger.info("StorageElement.getDirectorySize: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.getDirectorySize: Generating protocol PFNs for %s." % protocolName)
        for directoryUrl in directoryUrls:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(directoryUrl):
            res  = pfnparse(directoryUrl)
            if not res['OK']:
              errStr = "StorageElement.getDirectorySize: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (directoryUrl,res['Message']))
              if not failed.has_key(directoryUrl):
                failed[directoryUrl] = ''
              failed[directoryUrl] = "%s %s" % (failed[directoryUrl],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.getDirectorySize%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (directoryUrl,protocolName))
              else:
                pfnDict[res['Value']] = directoryUrl
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.getDirectorySize: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.getDirectorySize: Attempting to get size of %s directories." % len(pfnDict.keys()))
          res = storage.getDirectorySize(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.getDirectorySize: Completely failed to get size of directories."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,directoryUrl in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(directoryUrl):
                  failed[directoryUrl] = ''
                failed[directoryUrl] = "%s %s" % (failed[directoryUrl],res['Value']['Failed'][protocolPfn])
              else:
                successful[directoryUrl] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(directoryUrl):
                  failed.pop(directoryUrl)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #################################################################################################
  #
  # These are the file manipulation methods
  #


  def getFile(self,pfn,catalogueFileSize,localPath = ''):
    """ This method will obtain a local copy of a file from the SE

        'pfn' is the physical file name (as registered in the LFC)
        'catalogueFileSize' is the size from the catalogue
    """
    localSE = self.isLocalSE()['Value']

    res  = pfnparse(pfn)
    if not res['OK']:
      errStr = "StorageElement.getFile: Failed to parse supplied PFN."
      gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
      return S_ERROR(errStr)
    pfnDict = res['Value']
    fileName = os.path.basename(pfn)

    # Try all of the storages one by one until success
    for storage in self.storages:
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocols:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.makeDirectory: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        res = storage.getProtocolPfn(pfnDict,True)
        if not res['OK']:
          infoStr = "StorageElement.getFile%s." % res['Message']
          gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
        else:
          protocolPfn = res['Value']
          protocolSize = None
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

          ###########################################################################################
          # If the check was successful (i.e. we obtained correctly the file size) perform the transfer
          if protocolSize:
            if not localPath:
              localPath = '%s/%s' % (os.getcwd(),fileName)
            fileTuple = (protocolPfn,localPath,protocolSize)
            res = storage.getFile(fileTuple)
            if res['OK']:
              if res['Value']['Successful'].has_key(protocolPfn):
                infoStr ="StorageElement.getFile: Got local copy of file. Checking size..."
                gLogger.info(infoStr)
                ##############################################################
                # Post-transfer check. Check the local file size and remove it if not correct
                localSize = getSize(localPath)
                if localSize == protocolSize:
                  infoStr ="StorageElement.getFile: Local file size correct."
                  gLogger.info(infoStr,'%s with protocol %s' % (localPath,protocolName))
                  # woohoo, good work!!!
                  return S_OK(localPath)
              ##############################################################
              # If the transfer failed or the sizes don't match clean up the mess
              if os.path.exists(localPath):
                os.remove(localPath)
            else:
              infoStr = "StorageElement.getFile%s" % res['Message']
              gLogger.error(infoStr)

    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.getFile: Failed to get file for all protocols."
    gLogger.error(errStr,pfn)
    return S_ERROR(errStr)

  def putFile(self,file,directoryPath,alternativeFileName=None):
    """ This method will upload a local file to the SE

        'file' is the full local path to the file e.g. /opt/dirac/file/for/upload.file
        'directoryPath' is the path on the storage the file will be put
        'alternativeFileName' is the target file name
    """
    if alternativeFileName:
      gLogger.info("StorageElement.putFile: Attempting to put %s to %s with file name %s." % (file,directoryPath,alternativeFileName))
    else:
      gLogger.info("StorageElement.putFile: Attempting to put %s to %s." % (file,directoryPath))

    size = getSize(file)
    fileName = os.path.basename(file)
    if size == -1:
      infoStr = "StorageElement.putFile: Failed to get file size"
      gLogger.error(infoStr,file)
      return S_ERROR(infoStr)
    elif size == 0:
      infoStr ="StorageElement.putFile: File is zero size"
      gLogger.error(infoStr,file)
      return S_ERROR(infoStr)

    localSE = self.isLocalSE()['Value']

    gLogger.info("StorageElement.putFile: Determined file size of %s to be %s." % (file,size))
    localSE = self.isLocalSE()['Value']
    # The method will try all storages
    for storage in self.storages:
      # Get the parameters for the current storage
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      gLogger.info("StorageElement.putFile: Attempting to put file with %s." % protocolName)
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocols:
        useProtocol = True
      elif localSE:
        useProtocol = True
      else:
        useProtocol = False
        gLogger.info("StorageElement.putFile: %s not appropriate for use." % protocolName)
      if useProtocol:
        res =  storage.getCurrentURL(directoryPath)
        if res['OK']:
          destinationDirectory = res['Value']
          res = storage.createDirectory(destinationDirectory)
          if not res['OK']:
            infoStr ="StorageElement.putFile: Failed to create directory."
            gLogger.error(infoStr,'%s with protocol %s' % (directoryPath,protocolName))
          else:
            storage.changeDirectory(directoryPath)
        if res['OK']:
          # Obtain the full URL for the file from the file name and the cwd on the storage
          if alternativeFileName:
            res = storage.getCurrentURL(alternativeFileName)
          else:
            res = storage.getCurrentURL(fileName)
          if not res['OK']:
            infoStr ="StorageElement.putFile: Failed to get the file URL."
            gLogger.error(infoStr,' With protocol %s' % protocolName)
          else:
            destUrl = res['Value']
            ##############################################################
            # Pre-transfer check. Check if file already exists and remove it
            res = storage.exists(destUrl)
            if res['OK']:
              if res['Value']['Successful'].has_key(destUrl):
                fileExists = res['Value']['Successful'][destUrl]
                if not fileExists:
                  infoStr = "StorageElement.putFile: pre-transfer check completed successfully"
                  gLogger.info(infoStr)
                else:
                  infoStr = "StorageElement.putFile: pre-transfer check failed. File already exists. Removing..."
                  gLogger.info(infoStr)
                  res = storage.removeFile(destUrl)
                  if res['OK']:
                    if res['Value']['Successful'].has_key(destUrl):
                      infoStr ="StorageElement.putFile: Removed pre-existing file from storage."
                      gLogger.info(infoStr,'%s with protocol %s' % (destUrl,protocolName))
                    else:
                      infoStr ="StorageElement.putFile: Failed to remove pre-existing file from storage."
                      gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
                  else:
                    infoStr ="StorageElement.putFile: Failed to remove pre-existing file from storage."
                    gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
              else:
                infoStr ="StorageElement.putFile: Failed to find pre-existance of file."
                gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
            else:
              infoStr ="StorageElement.putFile: Failed to find pre-existance of file."
              gLogger.error(infoStr,'%s with protocol %s' % (destUrl,protocolName))
            ##############################################################
            # Perform the transfer here....
            fileTuple = (file,destUrl,size)
            res = storage.putFile(fileTuple)
            if res['OK']:
              if res['Value']['Successful'].has_key(destUrl):
                infoStr = "StorageElement.putFile: Successfully put %s with protocol %s." % (destUrl,protocolName)
                gLogger.info(infoStr)
                return S_OK(destUrl)
              else:
                errStr ="StorageElement.putFile: Failed to put file."
                errMessage = res['Value']['Failed'][destUrl]
                gLogger.error(errStr,'%s with protocol %s: %s' % (destUrl,protocolName,errMessage))
            # If the transfer completely failed
            else:
              errStr = "StorageElement.putFile: Completely failed to put file: %s" % res['Message']
              gLogger.error(errStr,"%s with protocol %s" % (destUrl,protocolName))
    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.putFile: Failed to put file for all protocols."
    gLogger.error(errStr,file)
    return S_ERROR(errStr)

  def removeFile(self,pfn):
    """ This method removes the physical file.

        'pfn' is a pfn for the storage
    """
    if type(pfn) == types.StringType:
      pfns = [pfn]
    elif type(pfn) == types.ListType:
      pfns = pfn
    else:
      return S_ERROR("StorageElement.removeFile: Supplied pfns must be string or list of strings")
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
        gLogger.info("StorageElement.removeFile: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.removeFile: Generating protocol PFNs for %s." % protocolName)
        for pfn in pfns:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(pfn):
            res  = pfnparse(pfn)
            if not res['OK']:
              errStr = "StorageElement.removeFile: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
              if not failed.has_key(pfn):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.removeFile%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
              else:
                pfnDict[res['Value']] = pfn
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.removeFile: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.removeFile: Attempting to remove %s physical files." % len(pfnDict.keys()))
          res = storage.removeFile(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.removeFile: Completely failed to remove files."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(pfn):
                  failed[pfn] = ''
                failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])
              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(pfn):
                  failed.pop(pfn)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileMetadata(self,pfn):
    """ This method obtains the metadata for the pfns given

        'pfn' is the physical file name
    """
    if type(pfn) == types.StringType:
      pfns = [pfn]
    elif type(pfn) == types.ListType:
      pfns = pfn
    else:
      return S_ERROR("StorageElement.getFileMetadata: Supplied pfns must be string or list of strings")
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
        gLogger.info("StorageElement.getFileMetadata: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        for pfn in pfns:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(pfn):
            res  = pfnparse(pfn)
            if not res['OK']:
              errStr = "StorageElement.getFileMetadata: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
              if not failed.has_key(pfn):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.getFileMetadata%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
              else:
                pfnDict[res['Value']] = pfn
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.getFileMetadata: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.getFileMetadata: Attempting to get metadata for %s physical files." % len(pfnDict.keys()))
          res = storage.getFileMetadata(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.getFileMetadata: Completely failed to get file metadata."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(pfn):
                  failed[pfn] = ''
                if not res['Value']['Failed'].has_key(protocolPfn):
                  res['Value']['Failed'][protocolPfn]='Another temporary hack unfortunately, apologies'
                failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])

              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(pfn):
                  failed.pop(pfn)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getFileSize(self,pfn):
    """ This method obtains the size for the pfn given

        'pfn' is the physical file name
    """
    if type(pfn) == types.StringType:
      pfns = [pfn]
    elif type(pfn) == types.ListType:
      pfns = pfn
    else:
      return S_ERROR("StorageElement.getFileSize: Supplied pfns must be string or list of strings")
    successful = {}
    failed = {}
    localSE = self.isLocalSE()['Value']
    # Try all of the storages one by one until success
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
        gLogger.info("StorageElement.getFileSize: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.getFileSize: Generating protocol PFNs for %s." % protocolName)
        for pfn in pfns:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(pfn):
            res  = pfnparse(pfn)
            if not res['OK']:
              errStr = "StorageElement.getFileSize: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
              if not failed.has_key(pfn):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.getFileSize%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
              else:
                pfnDict[res['Value']] = pfn
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.getFileSize: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.getFileSize: Attempting to get size for %s physical files." % len(pfnDict.keys()))
          res = storage.getFileSize(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.getFileSize: Completely failed to get file sizes."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(pfn):
                  failed[pfn] = ''
                failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])
              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(pfn):
                  failed.pop(pfn)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def prestageFile(self,pfn):
    """ This method issues prestage requests for pfn (or list of pfns)

        'pfn' is the physical file name (as registered in the LFC)
    """
    if type(pfn) == types.StringType:
      pfns = [pfn]
    elif type(pfn) == types.ListType:
      pfns = pfn
    else:
      return S_ERROR("StorageElement.prestageFile: Supplied pfns must be string or list of strings")
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
        gLogger.info("StorageElement.prestageFile: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.prestageFile: Generating protocol PFNs for %s." % protocolName)
        for pfn in pfns:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(pfn):
            res  = pfnparse(pfn)
            if not res['OK']:
              errStr = "StorageElement.prestageFile: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
              if not failed.has_key(pfn):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.prestageFile%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
              else:
                pfnDict[res['Value']] = pfn
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.prestageFile: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.prestageFile: Attempting to issue prestage requests for %s physical files." % len(pfnDict.keys()))
          res = storage.prestageFile(pfnDict.keys())
          if not res['OK']:
            infoStr = "StorageElement.prestageFile: Completely failed to issue prestage requests."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          else:
            for protocolPfn,pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(pfn):
                  failed[pfn] = ''
                failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])
              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if failed.has_key(pfn):
                  failed.pop(pfn)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def getAccessUrl(self,pfn):
    """ This method obtains a tURL for a pfn (or list of pfns)

        'pfn' is the physical file name (as registered in the LFC)
    """
    if type(pfn) == types.StringType:
      pfns = [pfn]
    elif type(pfn) == types.ListType:
      pfns = pfn
    else:
      return S_ERROR("StorageElement.getAccessUrl: Supplied pfns must be string or list of strings")
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
        gLogger.info("StorageElement.getAccessUrl: Protocol not appropriate for use: %s." % protocolName)
      if useProtocol:
        gLogger.info("StorageElement.getAccessUrl: Generating protocol PFNs for %s." % protocolName)
        for pfn in pfns:
          # If we have not already obtained metadata for the supplied pfn
          if not successful.has_key(pfn):
            res  = pfnparse(pfn)
            if not res['OK']:
              errStr = "StorageElement.getAccessUrl: Failed to parse supplied PFN."
              gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
              if not failed.has_key(pfn):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],errStr)
            else:
              res = storage.getProtocolPfn(res['Value'],True)
              if not res['OK']:
                infoStr = "StorageElement.getAccessUrl%s." % res['Message']
                gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
              else:
                pfnDict[res['Value']] = pfn
        if not len(pfnDict.keys()) > 0:
          gLogger.info("StorageElement.getAccessUrl: No pfns generated for protocol %s." % protocolName)
        else:
          gLogger.info("StorageElement.getAccessUrl: Attempting to get access urls for %s physical files." % len(pfnDict.keys()))
          res = storage.getTransportURL(pfnDict.keys(),protocols=self.turlProtocols)
          if not res['OK']:
            infoStr = "StorageElement.getAccessUrl: Completely failed to get access urls."
            gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
            for protocolPfn,pfn in pfnDict.items():
              failed[pfn] = res['Message']
          else:
            for protocolPfn,pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key(protocolPfn):
                if not failed.has_key(pfn):
                  failed[pfn] = ''
                failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])
              else:
                successful[pfn] = {protocolName:res['Value']['Successful'][protocolPfn]}
                if failed.has_key(pfn):
                  failed.pop(pfn)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)


  def retransferOnlineFile(self,pfn):
    """ This method requests that online system attempts to retransfer files

        'pfn' is the physical file name (as registered in the LFC)
    """
    if type(pfn) == types.StringType:
      pfns = [pfn]
    elif type(pfn) == types.ListType:
      pfns = pfn
    else:
      return S_ERROR("StorageElement.retransferOnlineFile: Supplied pfns must be string or list of strings")
    successful = {}
    failed = {}
    # Try all of the storages one by one
    for storage in self.storages:
      pfnDict = {}
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      for pfn in pfns:
        # If we have not already obtained metadata for the supplied pfn
        if not successful.has_key(pfn):
          res  = pfnparse(pfn)
          if not res['OK']:
            errStr = "StorageElement.retransferOnlineFile: Failed to parse supplied PFN."
            gLogger.error(errStr,"%s: %s" % (pfn,res['Message']))
            if not failed.has_key(pfn):
              failed[pfn] = ''
            failed[pfn] = "%s %s" % (failed[pfn],errStr)
          else:
            res = storage.getProtocolPfn(res['Value'],True)
            if not res['OK']:
              infoStr = "StorageElement.retransferOnlineFile%s." % res['Message']
              gLogger.error(infoStr,'%s for protocol %s' % (pfn,protocolName))
            else:
              pfnDict[res['Value']] = pfn
      if not len(pfnDict.keys()) > 0:
        gLogger.info("StorageElement.retransferOnlineFile: No pfns generated for protocol %s." % protocolName)
      else:
        gLogger.info("StorageElement.retransferOnlineFile: Attempting to get access urls for %s physical files." % len(pfnDict.keys()))
        res = storage.requestRetransfer(pfnDict.keys())
        if not res['OK']:
          infoStr = "StorageElement.retransferOnlineFile: Completely failed to get access urls."
          gLogger.error(infoStr,'%s for protocol %s: %s' % (self.name,protocolName,res['Message']))
          for protocolPfn,pfn in pfnDict.items():
            failed[pfn] = res['Message']
        else:
          for protocolPfn,pfn in pfnDict.items():
            if not res['Value']['Successful'].has_key(protocolPfn):
              if not failed.has_key(pfn):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % (failed[pfn],res['Value']['Failed'][protocolPfn])
            else:
              successful[pfn] = {protocolName:res['Value']['Successful'][protocolPfn]}
              if failed.has_key(pfn):
                failed.pop(pfn)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  #################################################################################################
  #
  # These are the methods for file replication (not supported directly in the storage plug-ins
  #

  def replicateFile(self,sourcePfn,sourceFileSize,directoryPath,alternativeFileName=None):
    """ This method will replicate a file to the Storage Element

       'sourcePfn' is the source pfn supporting remote protocols
       'sourceFileSize' is the size of the source file
       'directoryPath' is the path of the directory to place the file
       'alternativeFileName' is the target file name
    """
    fileName = os.path.basename(sourcePfn)
    if alternativeFileName:
      gLogger.info("StorageElement.replicateFile: Attempting to replicate %s to %s with file name %s." % (sourcePfn,directoryPath,alternativeFileName))
      fileName = alternativeFileName
    else:
      gLogger.info("StorageElement.replicateFile: Attempting to replicate %s to %s." % (sourcePfn,directoryPath))
    # Try all remote storages
    for storage in self.storages:
      # Get the parameters for the current storage
      res = storage.getParameters()
      protocolName = res['Value']['ProtocolName']
      # If the SE is not local then we can't use local protocols
      if protocolName in self.remoteProtocols:
        gLogger.info("StorageElement.replicateFile: Attempting to replicate file with %s." % protocolName)
        res =  storage.getCurrentURL(directoryPath)
        if res['OK']:
          destinationDirectory = res['Value']
          res = storage.createDirectory(destinationDirectory)
          if not res['OK']:
            infoStr ="StorageElement.replicateFile: Failed to create directory."
            gLogger.error(infoStr,'%s with protocol %s' % (directoryPath,protocolName))
          else:
            storage.changeDirectory(directoryPath)
        if res['OK']:
          # Obtain the full URL for the file from the file name and the cwd on the storage
          res = storage.getCurrentURL(fileName)
          if not res['OK']:
            infoStr ="StorageElement.replicateFile: Failed to get the file URL."
            gLogger.error(infoStr,'With protocol %s' % protocolName)
          else:
            destPfn = res['Value']
            fileTuple = (sourcePfn,destPfn,sourceFileSize)
            res = storage.putFile(fileTuple)
            if res['OK']:
              if res['Value']['Successful'].has_key(destPfn):
                infoStr = "StorageElement.replicateFile: Successfully replicated %s with protocol %s." % (destPfn,protocolName)
                gLogger.info(infoStr)
                return S_OK(destPfn)
              else:
                errStr ="StorageElement.replicateFile: Failed to replicate file."
                errMessage = res['Value']['Failed'][destPfn]
                gLogger.error(errStr,'%s with protocol %s: %s' % (destPfn,protocolName,errMessage))
            # If the transfer completely failed
            else:
              errStr = "StorageElement.replicateFile: Completely failed to replicate file: %s" % res['Message']
              gLogger.error(errStr,"%s with protocol %s" % (destPfn,protocolName))
    # If we get here we tried all the protocols and failed with all of them
    errStr = "StorageElement.putFile: Failed to replicate file for all protocols."
    gLogger.error(errStr,sourcePfn)
    return S_ERROR(errStr)

