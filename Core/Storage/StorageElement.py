""" This is the StorageElement class.

    self.name is the resolved name of the StorageElement i.e CERN-tape
    self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
    self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
    self.localProtocols is a list of the local protocols that were created by StorageFactory
    self.remoteProtocols is a list of the remote protocols that were created by StorageFactory
    self.protocolOptions is a list of dictionaries containing the options found in the CS. (should be removed)
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import sortList
import re, time

class StorageElement:

  def __init__(self,name):
    self.valid = True
    res = StorageFactory().getStorages(name)
    if not res['OK']:
      self.valid = False
    factoryDict = res['Value']
    self.name = factoryDict['StorageName']
    self.options = factoryDict['StorageOptions']
    self.localProtocols = factoryDict['LocalProtocols']
    self.remoteProtocols = factoryDict['RemoteProtocols']
    self.storages = factoryDict['StorageObjects']
    self.protocolOptions = factoryDict['ProtocolOptions']

  def testStorageFactory(self,dict):
    return StorageFactory().getStorage(dict)

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
      res = storage.getParameters()
      storageParameters = res['Value']
      for key in sortList(storageParameters.keys()):
        outStr = "%s%s: %s\n" % (outStr,key.ljust(15),storageParameters[key])
      i = i + 1
    gLogger.info(outStr)

  def isValid(self):
    return S_OK(self.valid)

  #################################################################################################
  #
  # These are the basic get functions to get information about the supported protocols
  #

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

  def getStorageParameters(self,protocol):
    """ Get protocol specific options
    """
    res = self.getProtocols()
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getStorageParameters: %s protocol is not supported for %s." % (protocol,self.name)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK(storageParameters)
    errStr = "StorageElement.getStorageParameters: %s found in %s protocols list but no object found." % (protocol,self.name)
    gLogger.error(errStr)
    return S_ERROR(errStr)

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
  #
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
