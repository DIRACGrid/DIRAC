from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import sortList
import re, time

#from DIRAC.DataMgmt.Storage.StorageFactory                   import StorageFactory

class StorageElement:

  def __init__(self,name):

    self.valid = True
    # Resolve the supplied name to its real StorageName
    res =  self._getStorageName(name)
    print res
    if not res['OK']:
      self.valid = False
    self.name = res['Value']

    # Create the storages for the obtained storage name
    self.localProtocols = []
    self.remoteProtocols = []
    self.storages = []
    try:
      res = self.__analyze()
      if not res['OK']:
        errStr = "Failed to create StorageElement %s." % self.name
        gLogger.error(errStr)
        self.valid = False
    except:
      errStr = "Failed to __analyse('%s')."  % self.name
      gLogger.error(errStr)
      self.valid = False

  def _getStorageName(self,storage):
    """
      This gets the name of the storage the configuration service.
      This is used when links are made in the SEs.
      e.g. Tier0-disk -> CERN-disk
    """
    configPath = '/DataManagement/StorageElements/%s/StorageName' % storage
    resolvedName = gConfig.getValue(configPath)
    if not resolvedName:
      errStr = "StorageElement definition not complete for %s: StorageName option not defined." % storage
      gLogger.error(errStr)
      return S_ERROR(errStr)
    while resolvedName != storage:
      storage = resolvedName
      configPath = '/DataManagement/StorageElements/%s/StorageName' % storage
      resolvedName = gConfig.getValue(configPath)
    return S_OK(resolvedName)

  def isValid(self):
    return S_OK(self.valid)

  def __analyze(self):
    """
      Parse the contents of the CS for the supplied storage element name.
    """
    rootConfigPath = '/DataManagement/StorageElements/%s' % self.name
    # First get the options that are supplied at the base level of the storage.
    # These are defined for all the protocols
    res = gConfig.getOptions(rootConfigPath)
    if not res['OK']:
      errStr = 'StorageElement.__analyze: Failed to get options for %s: %s' % (self.name,res['Message'])
      gLogger.error(errStr)
      return S_ERROR(errStr)
    options = res['Value']
    self.options = {}
    for option in options:
      configPath = '%s/%s' % (rootConfigPath,option)
      self.options[option] = gConfig.getValue(configPath)

    # Next we get the sections for the storage element
    # These contain information for the specific protocols
    res = gConfig.getSections(rootConfigPath)
    if not res['OK']:
      errStr = 'StorageElement.__analyze: Failed to get sections for %s: %s' % (self.name,res['Message'])
      gLogger.error(errStr)
      return S_ERROR(errStr)
    protocolSections = res['Value']
    sortedProtocols = sortList(protocolSections)

    for protocol in sortedProtocols:
      res = self.__getProtocolDetails(protocol,rootConfigPath)
      if not res['OK']:
        return res
      self.storages.append(res['Value'])
    return S_OK()

  def __getProtocolDetails(self,protocol,baseConfigPath):
    """
      Parse the contents of the protocol block
    """
    protocolConfigPath = '%s/%s' % (baseConfigPath,protocol)
    res = gConfig.getOptions(protocolConfigPath)
    if not res['OK']:
      errStr = 'StorageElement.__getProtocolDetails: Failed for %s:%s' % (self.name,protocol)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    options = res['Value']
    protocolDict = {}
    for option in options:
      configPath = '%s/%s' % (protocolConfigPath,option)
      optionValue = gConfig.getValue(configPath)
      protocolDict[option] = optionValue
    # Now update the local and remote protocol lists
    if protocolDict['Access'] == 'remote':
      self.remoteProtocols.append(protocolDict['ProtocolName'])
    else:
      self.localProtocols.append(protocolDict['ProtocolName'])
    return S_OK(protocolDict)

  def dump(self):
    """
      Dump to the logger a sumary of the StorageElement items
    """
    outStr = "\nStorage Element %s:\n" % (self.name)
    i = 1
    outStr = "%s============ Options ============\n" % (outStr)
    for key in sortList(self.options.keys()):
      outStr = "%s%s: %s\n" % (outStr,key.ljust(15),self.options[key])

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % (outStr,i)
      for key in sortList(storage.keys()):
        outStr = "%s%s: %s\n" % (outStr,key.ljust(15),storage[key])
      i = i + 1
    gLogger.info(outStr)

  def getPrimaryProtocol(self):
    """ Get the primary protocol option
    """
    res = self.getStorageElementOption('PrimaryProtocol')
    return res

  def getLocalProtocol(self):
    """ Get the local protocol option
    """
    res = self.getStorageElementOption('LocalProtocol')
    return res

  def getStorageElementOption(self,option):
    """ Get the value for the option supplied from self.options
    """
    if self.options.has_key(option):
      optionValue = self.options[option]
      return S_OK(optionValue)
    else:
      errStr = "StorageElement.getStorageElementOption: %s not defined for %s." % (option,self.name)
      gLogger.error(errStr)
      return S_ERROR(errStr)

  def getProtocols(self):
    """ Get the list of all the protocols defined for this Storage Element
    """
    allProtocols = self.localProtocols+self.remoteProtocols
    return S_OK(allProtocols)

  def getRemoteProtocols(self):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    return S_OK(self.remoteProtocols)

  def getLocalProtocols(self):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    return S_OK(self.localProtocols)

  def isLocalSE(self):
    """ Test if the Storage Element is local in the current context
    """
    localSE = gConfig.getValue('/LocalSite/LocalSE',[])
    if self.name in localSE:
      return S_OK(True)
    else:
      return S_OK(False)

#################################################################################################
# Below this line things aren't implemented

  def getStorage(self,protocolList=[]):
    """ Get a list of storage access points for the specified list of protocols.
    """
    errStr = "StorageElement.getStorage: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def __getPfnDict(self,pfn,protocol):
    """ Gets the dictionary describing the PFN in terms of protocol
    """
    errStr = "StorageElement.__getPfnDict: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def getPfnForProtocol(self,pfn,protocol):
    """ Transform the input pfn into another with the given protocol for the Storage Element.
        The new PFN is built on the fly based on the information provided in the Configuration Service
    """
    errStr = "StorageElement.getPfnForProtocol: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)

  def getPfnPath(self,pfn):
    """  Get the part of the PFN path below the basic storage path.
         This path must coinside with the LFN of the file in order to be
         compliant with the LHCb conventions.
    """
    errStr = "StorageElement.getPfnPath: Implement me."
    gLogger.error(errStr)
    return S_ERROR(errStr)
