""" Storage Factory Class

    This Class has three public methods:

    getStorageName():  Resolves links in the CS to the target SE name.

    getStorage():      This creates a single storage stub based on the parameters passed in a dictionary.
                      This dictionary must have the following keys: 'StorageName','ProtocolName','Protocol'
                      Other optional keys are 'Port','Host','Path','SpaceToken'

    getStorages()      This takes a DIRAC SE definition and creates storage stubs for the protocols found in the CS.
                      By providing an optional list of protocols it is possible to limit the created stubs.
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse
from DIRAC.Core.Utilities.List import sortList
import re, time

class StorageFactory:

  def __init__(self):
    self.rootConfigPath = '/DataManagement/StorageElements/'
    self.valid = True

  ###########################################################################################
  #
  # Below are public methods for obtaining storage objects
  #

  def getStorageName(self,initialName):
    return self._getConfigStorageName(initialName)

  def getStorage(self,parameterDict):
    """ This instanciates a single storage for the details provided and doesn't check the CS.
    """
    # The storage name must be supplied.
    if parameterDict.has_key('StorageName'):
      storageName = parameterDict['StorageName']
    else:
      errStr = "StorageFactory.getStorage: StorageName must be supplied"
      gLogger.error(errStr)
      return S_ERROR(errStr)

    # ProtocolName must be supplied otherwise nothing with work.
    if parameterDict.has_key('ProtocolName'):
      protocolName = parameterDict['ProtocolName']
    else:
      errStr = "StorageFactory.getStorage: ProtocolName must be supplied"
      gLogger.error(errStr)
      return S_ERROR(errStr)

    # Protocol must be supplied otherwise nothing with work.
    if parameterDict.has_key('Protocol'):
      protocol = parameterDict['Protocol']
    else:
      errStr = "StorageFactory.getStorage: Protocol must be supplied"
      gLogger.error(errStr)
      return S_ERROR(errStr)

    # The other options need not always be specified
    if parameterDict.has_key('Port'):
      port = parameterDict['Port']
    else:
      port = ''

    if parameterDict.has_key('Host'):
      host = parameterDict['Host']
    else:
      host = ''

    if parameterDict.has_key('Path'):
      path = parameterDict['Path']
    else:
      path = ''

    if parameterDict.has_key('SpaceToken'):
      spaceToken = parameterDict['SpaceToken']
    else:
      spaceToken = ''

    return self._generateStorageObject(storageName,protocolName,protocol,path,host,port,spaceToken)


  def getStorages(self,storageName,protocolList=[]):
    """ Get an instance of a Storage based on a description either from CS or
        directly from the parameters provided as a tuple or dictionary.
    """
    self.remoteProtocols = []
    self.localProtocols = []
    self.name = ''
    self.options = {}
    self.protocolDetails = []
    self.storages = []

    # Get the name of the storage provided
    res =  self._getConfigStorageName(storageName)
    if not res['OK']:
      self.valid = False
      return res
    storageName = res['Value']
    self.name = storageName

    # Get the options defined in the CS for this storage
    res = self._getConfigStorageOptions(storageName)
    if not res['OK']:
      self.valid = False
      return res
    self.options = res['Value']

    # Get the protocol specific details
    res = self._getConfigStorageProtocols(storageName)
    if not res['OK']:
      self.valid = False
      return res
    self.protocolDetails = res['Value']

    requestedLocalProtocols = []
    requestedRemoteProtocols = []
    requestedProcotlDetails = []
    # Generate the protocol specific plug-ins
    self.storages = []
    for protocolDict in self.protocolDetails:
      protocolName = protocolDict['ProtocolName']
      protocolRequested = True
      if protocolList:
        if protocolName not in protocolList:
          protocolRequested = False
      if protocolRequested:
        protocol = protocolDict['Protocol']
        host = protocolDict['Host']
        path = protocolDict['Path']
        port = protocolDict['Port']
        spaceToken = protocolDict['SpaceToken']
        res = self._generateStorageObject(storageName,protocolName,protocol,path=path,host=host,port=port,spaceToken=spaceToken)
        if res['OK']:
          self.storages.append(res['Value'])
          if protocolName in self.localProtocols:
            requestedLocalProtocols.append(protocolName)
          if protocolName in self.remoteProtocols:
            requestedRemoteProtocols.append(protocolName)
          requestedProcotlDetails.append(protocolDict)
        else:
          gLogger.warn(res['Message'])

    resDict = {}
    resDict['StorageName'] = self.name
    resDict['StorageOptions'] = self.options
    resDict['StorageObjects'] = self.storages
    resDict['LocalProtocols'] = requestedLocalProtocols
    resDict['RemoteProtocols'] = requestedRemoteProtocols
    resDict['ProtocolOptions'] = requestedProcotlDetails
    return S_OK(resDict)

  ###########################################################################################
  #
  # Below are internal methods for obtaining section/option/value configuration
  #

  def _getConfigStorageName(self,initialName):
    """
      This gets the name of the storage the configuration service.
      This is used when links are made in the SEs.
      e.g. Tier0-disk -> CERN-disk
    """
    storage = ''
    resolvedName = initialName
    while resolvedName != storage:
      storage = resolvedName
      configPath = '%s%s/StorageName' % (self.rootConfigPath,storage)
      resolvedName = gConfig.getValue(configPath)
      if not resolvedName:
        errStr = "StorageElement definition not complete for %s: StorageName option not defined." % storage
        gLogger.error(errStr)
        return S_ERROR(errStr)
    return S_OK(resolvedName)

  def _getConfigStorageOptions(self,storageName):
    """ Get the options associated to the StorageElement as defined in the CS
    """
    storageConfigPath = '%s%s' % (self.rootConfigPath,storageName)
    res = gConfig.getOptions(storageConfigPath)
    if not res['OK']:
      errStr = 'StorageFactory._getStorageOptions: Failed to get options for %s: %s' % (storageName,res['Message'])
      gLogger.error(errStr)
      return S_ERROR(errStr)
    options = res['Value']
    optionsDict = {}
    for option in options:
      optionConfigPath = '%s/%s' % (storageConfigPath,option)
      optionsDict[option] = gConfig.getValue(optionConfigPath)
    return S_OK(optionsDict)

  def _getConfigStorageProtocols(self,storageName):
    """ Protocol specific information is present as sections in the Storage configuration
    """
    storageConfigPath = '%s%s' % (self.rootConfigPath,storageName)
    res = gConfig.getSections(storageConfigPath)
    if not res['OK']:
      errStr = 'StorageFactory._getConfigStorageProtocols: Failed to get sections for %s: %s' % (storageName,res['Message'])
      gLogger.error(errStr)
      return S_ERROR(errStr)
    protocolSections = res['Value']
    sortedProtocols = sortList(protocolSections)
    protocolDetails = []
    for protocol in sortedProtocols:
      res = self._getConfigStorageProtocolDetails(storageName,protocol)
      if not res['OK']:
        return res
      protocolDetails.append(res['Value'])
    self.protocols = self.localProtocols + self.remoteProtocols
    return S_OK(protocolDetails)

  def _getConfigStorageProtocolDetails(self,storageName,protocol):
    """
      Parse the contents of the protocol block
    """
    # First obtain the options that are available
    protocolConfigPath = '%s%s/%s' % (self.rootConfigPath,storageName,protocol)
    res = gConfig.getOptions(protocolConfigPath)
    if not res['OK']:
      errStr = 'StorageElement.__getProtocolDetails: Failed for %s:%s' % (storageName,protocol)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    options = res['Value']

    # We must have certain values internally even if not supplied in CS
    protocolDict = {'Access':'','Host':'','Path':'','Port':'','Protocol':'','ProtocolName':'','SpaceToken':''}
    for option in options:
      configPath = '%s/%s' % (protocolConfigPath,option)
      optionValue = gConfig.getValue(configPath)
      protocolDict[option] = optionValue

    # Now update the local and remote protocol lists.
    # A warning will be given if the Access option is not set.
    if protocolDict['Access'] == 'remote':
      self.remoteProtocols.append(protocolDict['ProtocolName'])
    elif protocolDict['Access'] == 'local':
      self.localProtocols.append(protocolDict['ProtocolName'])
    else:
      errStr = "StorageElement.__getProtocolDetails: The 'Access' option for %s:%s is neither 'local' or 'remote'." % (storageName,protocol)
      gLogger.warn(errStr)

    # The Protocol and ProtocolName option must be defined
    if not protocolDict['Protocol']:
      errStr = "StorageElement.__getProtocolDetails: The 'Protocol' option for %s:%s is not set." % (storageName,protocol)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    if not protocolDict['ProtocolName']:
      errStr = "StorageElement.__getProtocolDetails: The 'ProtocolName' option for %s:%s is not set." % (storageName,protocol)
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return S_OK(protocolDict)

  ###########################################################################################
  #
  # Below is the method for obtaining the object instantiated for a provided storage configuration
  #

  def _generateStorageObject(self,storageName,protocolName,protocol,path=None,host=None,port=None,spaceToken=None):
    try:
      moduleName = "%sStorage" % (protocolName)
      storageModule = __import__('DIRAC.Core.Storage.%s' % moduleName,globals(),locals(),[moduleName])
    except Exception, x:
      errStr = "StorageFactory._storage: Failed to import %s: %s" % (storageName, x)
      gLogger.exception(errStr)
      return S_ERROR(errStr)

    try:
      evalString = "storageModule.%s(storageName,protocol,path,host,port,spaceToken)" % moduleName
      storage = eval(evalString)
    except Exception, x:
      errStr = "StorageFactory._storage: Failed to instatiate %s(): %s" % (moduleName, x)
      gLogger.exception(errStr)
      return S_ERROR(errStr)
    return S_OK(storage)

