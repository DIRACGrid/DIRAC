""" Storage Factory Class - creates instances of various Storage plugins from the Core DIRAC or extensions

    This Class has three public methods:

    getStorageName():  Resolves links in the CS to the target SE name.

    getStorage():      This creates a single storage stub based on the parameters passed in a dictionary.
                      This dictionary must have the following keys: 'StorageName','ProtocolName','Protocol'
                      Other optional keys are 'Port','Host','Path','SpaceToken'

    getStorages()      This takes a DIRAC SE definition and creates storage stubs for the protocols found in the CS.
                      By providing an optional list of protocols it is possible to limit the created stubs.
"""

__RCSID__ = "$Id$"

from DIRAC                                              import gLogger, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers           import getInstalledExtensions
from DIRAC.ResourceStatusSystem.Client.ResourceStatus   import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSiteForResource
import os

class StorageFactory:

  def __init__( self, useProxy=False, vo = None ):

    self.valid = True
    self.proxy = False
    self.proxy = useProxy
    self.resourceStatus = ResourceStatus()
    self.resourcesHelper = Resources( vo = vo )

  ###########################################################################################
  #
  # Below are public methods for obtaining storage objects
  #

  def getStorageName( self, initialName ):
    return self._getConfigStorageName( initialName )

  def getStorage( self, parameterDict ):
    """ This instantiates a single storage for the details provided and doesn't check the CS.
    """
    # The storage name must be supplied.
    if parameterDict.has_key( 'StorageName' ):
      storageName = parameterDict['StorageName']
    else:
      errStr = "StorageFactory.getStorage: StorageName must be supplied"
      gLogger.error( errStr )
      return S_ERROR( errStr )

    # ProtocolName must be supplied otherwise nothing with work.
    if parameterDict.has_key( 'ProtocolName' ):
      protocolName = parameterDict['ProtocolName']
    else:
      errStr = "StorageFactory.getStorage: ProtocolName must be supplied"
      gLogger.error( errStr )
      return S_ERROR( errStr )

    # The other options need not always be specified
    if parameterDict.has_key( 'Protocol' ):
      protocol = parameterDict['Protocol']
    else:
      protocol = ''

    if parameterDict.has_key( 'Port' ):
      port = parameterDict['Port']
    else:
      port = ''

    if parameterDict.has_key( 'Host' ):
      host = parameterDict['Host']
    else:
      host = ''

    if parameterDict.has_key( 'Path' ):
      path = parameterDict['Path']
    else:
      path = ''

    if parameterDict.has_key( 'SpaceToken' ):
      spaceToken = parameterDict['SpaceToken']
    else:
      spaceToken = ''

    if parameterDict.has_key( 'WSUrl' ):
      wsPath = parameterDict['WSUrl']
    else:
      wsPath = ''

    return self.__generateStorageObject( storageName, protocolName, protocol, path, host, port, spaceToken, wsPath, parameterDict )


  def getStorages( self, storageName, protocolList = [] ):
    """ Get an instance of a Storage based on the DIRAC SE name based on the CS entries CS

        'storageName' is the DIRAC SE name i.e. 'CERN-RAW'
        'protocolList' is an optional list of protocols if a sub-set is desired i.e ['SRM2','SRM1']
    """
    self.remoteProtocols = []
    self.localProtocols = []
    self.name = ''
    self.options = {}
    self.protocolDetails = []
    self.storages = []

    # Get the name of the storage provided
    res = self._getConfigStorageName( storageName )
    if not res['OK']:
      self.valid = False
      return res
    storageName = res['Value']
    self.name = storageName

    # Get the options defined in the CS for this storage
    res = self._getConfigStorageOptions( storageName )
    if not res['OK']:
      self.valid = False
      return res
    self.options = res['Value']

    # Get the protocol specific details
    res = self._getConfigStorageProtocols( storageName )
    if not res['OK']:
      self.valid = False
      return res
    self.protocolDetails = res['Value']

    requestedLocalProtocols = []
    requestedRemoteProtocols = []
    requestedProtocolDetails = []
    turlProtocols = []
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
        wsUrl = protocolDict['WSUrl']
        res = self.__generateStorageObject( storageName, protocolName, protocol,
                                            path = path, host = host, port = port,
                                            spaceToken = spaceToken, wsUrl = wsUrl,
                                            parameters = protocolDict )
        if res['OK']:
          self.storages.append( res['Value'] )
          if protocolName in self.localProtocols:
            turlProtocols.append( protocol )
            requestedLocalProtocols.append( protocolName )
          if protocolName in self.remoteProtocols:
            requestedRemoteProtocols.append( protocolName )
          requestedProtocolDetails.append( protocolDict )
        else:
          gLogger.info( res['Message'] )

    if len( self.storages ) > 0:
      resDict = {}
      resDict['StorageName'] = self.name
      resDict['StorageOptions'] = self.options
      resDict['StorageObjects'] = self.storages
      resDict['LocalProtocols'] = requestedLocalProtocols
      resDict['RemoteProtocols'] = requestedRemoteProtocols
      resDict['ProtocolOptions'] = requestedProtocolDetails
      resDict['TurlProtocols'] = turlProtocols
      return S_OK( resDict )
    else:
      errStr = "StorageFactory.getStorages: Failed to instantiate any storage protocols."
      gLogger.error( errStr, self.name )
      return S_ERROR( errStr )
  ###########################################################################################
  #
  # Below are internal methods for obtaining section/option/value configuration
  #

  def _getConfigStorageName( self, storageName ):
    """
      This gets the name of the storage the configuration service.
      If the storage is an alias for another the resolution is performed.

      'storageName' is the storage section to check in the CS
    """
    result = self.resourcesHelper.getStorageElementOptionsDict( storageName )
    if not result['OK']:
      errStr = "StorageFactory._getConfigStorageName: Failed to get storage options"
      gLogger.error( errStr, result['Message'] )
      return S_ERROR( errStr )
    if not result['Value']:
      errStr = "StorageFactory._getConfigStorageName: Supplied storage doesn't exist."
      gLogger.error( errStr, storageName )
      return S_ERROR( errStr )
    if 'Alias' in result['Value']:
      #FIXME This cannot work as self.rootConfigPath is undefined
      configPath = '%s/%s/Alias' % ( self.rootConfigPath, storageName )
      aliasName = gConfig.getValue( configPath )
      result = self._getConfigStorageName( aliasName )
      if not result['OK']:
        errStr = "StorageFactory._getConfigStorageName: Supplied storage doesn't exist."
        gLogger.error( errStr, configPath )
        return S_ERROR( errStr )
      resolvedName = result['Value']
    else:
      resolvedName = storageName
    return S_OK( resolvedName )

  def _getConfigStorageOptions( self, storageName ):
    """ Get the options associated to the StorageElement as defined in the CS
    """
    
    result = self.resourcesHelper.getStorageElementOptionsDict( storageName ) 
    if not result['OK']:
      errStr = "StorageFactory._getStorageOptions: Failed to get storage options."
      gLogger.error( errStr, "%s: %s" % ( storageName, result['Message'] ) )
      return S_ERROR( errStr )    
    optionsDict = result['Value']
    
    result = self.resourceStatus.getStorageStatus( storageName, 'ReadAccess' )    
    if not result[ 'OK' ]:
      errStr = "StorageFactory._getStorageOptions: Failed to get storage status"
      gLogger.error( errStr, "%s: %s" % ( storageName, result['Message'] ) )
      return S_ERROR( errStr )
    #optionsDict.update( result[ 'Value' ][ storageName ] )

    return S_OK( optionsDict )

  def _getConfigStorageProtocols( self, storageName ):
    """ Protocol specific information is present as sections in the Storage configuration
    """
    result = getSiteForResource( storageName )
    if not result['OK']:
      return result
    site = result['Value']
    result = self.resourcesHelper.getEligibleNodes( 'AccessProtocol', {'Site': site, 'Resource': storageName } )
    if not result['OK']:
      return result
    nodesList = result['Value']
    protocols = []
    for node in nodesList:
      protocols.append( node )
    protocolDetails = []
    for protocol in protocols:
      result = self._getConfigStorageProtocolDetails( protocol )
      if not result['OK']:
        return result
      protocolDetails.append( result['Value'] )
    self.protocols = self.localProtocols + self.remoteProtocols
    return S_OK( protocolDetails )

  def _getConfigStorageProtocolDetails( self, protocol ):
    """
      Parse the contents of the protocol block
    """
    
    result = self.resourcesHelper.getAccessProtocolOptionsDict( protocol )
    if not result['OK']:
      return result
    optionsDict = result['Value']

    # We must have certain values internally even if not supplied in CS
    protocolDict = {'Access':'', 'Host':'', 'Path':'', 'Port':'', 'Protocol':'', 'ProtocolName':'', 'SpaceToken':'', 'WSUrl':''}
    for option in optionsDict:
      protocolDict[option] = optionsDict[option]

    # Now update the local and remote protocol lists.
    # A warning will be given if the Access option is not set.
    if protocolDict['Access'] == 'remote':
      self.remoteProtocols.append( protocolDict['ProtocolName'] )
    elif protocolDict['Access'] == 'local':
      self.localProtocols.append( protocolDict['ProtocolName'] )
    else:
      errStr = "StorageFactory.__getProtocolDetails: The 'Access' option for %s is neither 'local' or 'remote'." % protocol
      gLogger.warn( errStr )

    # The ProtocolName option must be defined
    if not protocolDict['ProtocolName']:
      errStr = "StorageFactory.__getProtocolDetails: 'ProtocolName' option is not defined."
      gLogger.error( errStr, "%s" % protocol )
      return S_ERROR( errStr )
    return S_OK( protocolDict )

  ###########################################################################################
  #
  # Below is the method for obtaining the object instantiated for a provided storage configuration
  #

  def __generateStorageObject( self, storageName, protocolName, protocol, path = None,
                              host = None, port = None, spaceToken = None, wsUrl = None, parameters={} ):
    
    storageType = protocolName
    if self.proxy:
      storageType = 'Proxy'
    
    moduleRootPaths = getInstalledExtensions()
    moduleLoaded = False
    path = path.rstrip( '/' )
    if not path:
      path = '/'
    for moduleRootPath in moduleRootPaths:
      if moduleLoaded:
        break
      gLogger.verbose( "Trying to load from root path %s" % moduleRootPath )
      moduleFile = os.path.join( rootPath, moduleRootPath, "Resources", "Storage", "%sStorage.py" % storageType )
      gLogger.verbose( "Looking for file %s" % moduleFile )
      if not os.path.isfile( moduleFile ):
        continue
      try:
        # This inforces the convention that the plug in must be named after the protocol
        moduleName = "%sStorage" % ( storageType )
        storageModule = __import__( '%s.Resources.Storage.%s' % ( moduleRootPath, moduleName ),
                                    globals(), locals(), [moduleName] )
      except Exception, x:
        errStr = "StorageFactory._generateStorageObject: Failed to import %s: %s" % ( storageName, x )
        gLogger.exception( errStr )
        return S_ERROR( errStr )

      try:
        evalString = "storageModule.%s(storageName,protocol,path,host,port,spaceToken,wsUrl)" % moduleName
        storage = eval( evalString )
        if not storage.isOK():
          errStr = "StorageFactory._generateStorageObject: Failed to instantiate storage plug in."
          gLogger.error( errStr, "%s" % ( moduleName ) )
          return S_ERROR( errStr )
      except Exception, x:
        errStr = "StorageFactory._generateStorageObject: Failed to instantiate %s(): %s" % ( moduleName, x )
        gLogger.exception( errStr )
        return S_ERROR( errStr )
      
      # Set extra parameters if any
      if parameters:
        result = storage.setParameters( parameters )
        if not result['OK']:
          return result
      
      # If use proxy, keep the original protocol name
      if self.proxy:
        storage.protocolName = protocolName
      return S_OK( storage )

    if not moduleLoaded:
      return S_ERROR( 'Failed to find storage plugin %s' % protocolName )
