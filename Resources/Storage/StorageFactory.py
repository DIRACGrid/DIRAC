""" Storage Factory Class - creates instances of various Storage plugins from the Core DIRAC or extensions

    This Class has three public methods:

    getStorageName():  Resolves links in the CS to the target SE name.

    getStorage():      This creates a single storage stub based on the parameters passed in a dictionary.
                      This dictionary must have the following keys: 'StorageName','PluginName','Protocol'
                      Other optional keys are 'Port','Host','Path','SpaceToken'

    getStorages()      This takes a DIRAC SE definition and creates storage stubs for the protocols found in the CS.
                      By providing an optional list of protocols it is possible to limit the created stubs.
"""

__RCSID__ = "$Id$"

from DIRAC                                            import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List                        import sortList
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Path    import cfgPath
from DIRAC.Core.Utilities.ObjectLoader                import ObjectLoader

class StorageFactory:

  def __init__( self, useProxy = False, vo = None ):
    self.rootConfigPath = '/Resources/StorageElements'
    self.proxy = False
    self.proxy = useProxy
    self.resourceStatus = ResourceStatus()
    self.vo = vo

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

    # PluginName must be supplied otherwise nothing with work.
    if parameterDict.has_key( 'PluginName' ):
      pluginName = parameterDict['PluginName']
    # Temporary fix for backward compatibility
    elif parameterDict.has_key( 'ProtocolName' ):
      pluginName = parameterDict['ProtocolName']  
    else:
      errStr = "StorageFactory.getStorage: PluginName must be supplied"
      gLogger.error( errStr )
      return S_ERROR( errStr )

    return self.__generateStorageObject( storageName, pluginName, parameterDict )

  def getStorages( self, storageName, pluginList = [] ):
    """ Get an instance of a Storage based on the DIRAC SE name based on the CS entries CS

        'storageName' is the DIRAC SE name i.e. 'CERN-RAW'
        'pluginList' is an optional list of protocols if a sub-set is desired i.e ['SRM2','SRM1']
    """
    self.remotePlugins = []
    self.localPlugins = []
    self.name = ''
    self.options = {}
    self.protocolDetails = []
    self.storages = []
    if not self.vo:
      return S_ERROR( 'Mandatory vo parameter is not defined' )

    # Get the name of the storage provided
    res = self._getConfigStorageName( storageName )
    if not res['OK']:
      return res
    storageName = res['Value']
    self.name = storageName

    # Get the options defined in the CS for this storage
    res = self._getConfigStorageOptions( storageName )
    if not res['OK']:
      return res
    self.options = res['Value']

    # Get the protocol specific details
    res = self._getConfigStorageProtocols( storageName )
    if not res['OK']:
      return res
    self.protocolDetails = res['Value']

    requestedLocalPlugins = []
    requestedRemotePlugins = []
    requestedProtocolDetails = []
    turlProtocols = []
    # Generate the protocol specific plug-ins
    self.storages = []
    for protocolDict in self.protocolDetails:
      pluginName = protocolDict.get( 'PluginName' ) 
      if pluginList and pluginName not in pluginList:
        continue
      protocol = protocolDict['Protocol']
      result = self.__generateStorageObject( storageName, pluginName, protocolDict )
      if result['OK']:
        self.storages.append( result['Value'] )
        if pluginName in self.localPlugins:
          turlProtocols.append( protocol )
          requestedLocalPlugins.append( pluginName )
        if pluginName in self.remotePlugins:
          requestedRemotePlugins.append( pluginName )
        requestedProtocolDetails.append( protocolDict )
      else:
        gLogger.info( result['Message'] )

    if len( self.storages ) > 0:
      resDict = {}
      resDict['StorageName'] = self.name
      resDict['StorageOptions'] = self.options
      resDict['StorageObjects'] = self.storages
      resDict['LocalPlugins'] = requestedLocalPlugins
      resDict['RemotePlugins'] = requestedRemotePlugins
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
    configPath = '%s/%s' % ( self.rootConfigPath, storageName )
    res = gConfig.getOptions( configPath )
    if not res['OK']:
      errStr = "StorageFactory._getConfigStorageName: Failed to get storage options"
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    if not res['Value']:
      errStr = "StorageFactory._getConfigStorageName: Supplied storage doesn't exist."
      gLogger.error( errStr, configPath )
      return S_ERROR( errStr )
    if 'Alias' in res['Value']:
      configPath = cfgPath( self.rootConfigPath, storageName, 'Alias' )
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
    storageConfigPath = cfgPath( self.rootConfigPath, storageName )
    res = gConfig.getOptions( storageConfigPath )
    if not res['OK']:
      errStr = "StorageFactory._getStorageOptions: Failed to get storage options."
      gLogger.error( errStr, "%s: %s" % ( storageName, res['Message'] ) )
      return S_ERROR( errStr )
    options = res['Value']
    optionsDict = {}
    for option in options:

      if option in [ 'ReadAccess', 'WriteAccess', 'CheckAccess', 'RemoveAccess']:
        continue
      optionConfigPath = cfgPath( storageConfigPath, option )
      if option in [ 'VO' ]:
        optionsDict[option] = gConfig.getValue( optionConfigPath, [] )
      else:
        optionsDict[option] = gConfig.getValue( optionConfigPath, '' )

    res = self.resourceStatus.getStorageElementStatus( storageName )
    if not res[ 'OK' ]:
      errStr = "StorageFactory._getStorageOptions: Failed to get storage status"
      gLogger.error( errStr, "%s: %s" % ( storageName, res['Message'] ) )
      return S_ERROR( errStr )

    # For safety, we did not add the ${statusType}Access keys
    # this requires modifications in the StorageElement class

    # We add the dictionary with the statusTypes and values
    # { 'statusType1' : 'status1', 'statusType2' : 'status2' ... }
    optionsDict.update( res[ 'Value' ][ storageName ] )

    return S_OK( optionsDict )

  def _getConfigStorageProtocols( self, storageName ):
    """ Protocol specific information is present as sections in the Storage configuration
    """
    storageConfigPath = cfgPath( self.rootConfigPath, storageName )
    res = gConfig.getSections( storageConfigPath )
    if not res['OK']:
      errStr = "StorageFactory._getConfigStorageProtocols: Failed to get storage sections"
      gLogger.error( errStr, "%s: %s" % ( storageName, res['Message'] ) )
      return S_ERROR( errStr )
    protocolSections = res['Value']
    sortedProtocolSections = sortList( protocolSections )
    protocolDetails = []
    for protocolSection in sortedProtocolSections:
      res = self._getConfigStorageProtocolDetails( storageName, protocolSection )
      if not res['OK']:
        return res
      protocolDetails.append( res['Value'] )
    return S_OK( protocolDetails )

  def _getConfigStorageProtocolDetails( self, storageName, protocolSection ):
    """
      Parse the contents of the protocol block
    """
    # First obtain the options that are available
    protocolConfigPath = cfgPath( self.rootConfigPath, storageName, protocolSection )
    res = gConfig.getOptions( protocolConfigPath )
    if not res['OK']:
      errStr = "StorageFactory.__getProtocolDetails: Failed to get protocol options."
      gLogger.error( errStr, "%s: %s" % ( storageName, protocolSection ) )
      return S_ERROR( errStr )
    options = res['Value']

    # We must have certain values internally even if not supplied in CS
    protocolDict = {'Access':'', 'Host':'', 'Path':'', 'Port':'', 'Protocol':'', 'PluginName':'', 'SpaceToken':'', 'WSUrl':''}
    for option in options:
      configPath = cfgPath( protocolConfigPath, option )
      optionValue = gConfig.getValue( configPath, '' )
      protocolDict[option] = optionValue
        
    # This is a temporary for backward compatibility
    if "ProtocolName" in protocolDict and not protocolDict['PluginName']:  
      protocolDict['PluginName'] = protocolDict['ProtocolName']
    protocolDict.pop( 'ProtocolName' )  
        
    # Evaluate the base path taking into account possible VO specific setting 
    if self.vo:
      result = gConfig.getOptionsDict( cfgPath( protocolConfigPath, 'VOPath' ) )
      voPath = ''
      if result['OK']:
        voPath = result['Value'].get( self.vo, '' )
      if voPath:
        protocolDict['Path'] = voPath  

    # Now update the local and remote protocol lists.
    # A warning will be given if the Access option is not set.
    if protocolDict['Access'].lower() == 'remote':
      self.remotePlugins.append( protocolDict['PluginName'] )
    elif protocolDict['Access'].lower() == 'local':
      self.localPlugins.append( protocolDict['PluginName'] )
    else:
      errStr = "StorageFactory.__getProtocolDetails: The 'Access' option for %s:%s is neither 'local' or 'remote'." % ( storageName, protocolSection )
      gLogger.warn( errStr )

    # The PluginName option must be defined
    if not protocolDict['PluginName']:
      errStr = "StorageFactory.__getProtocolDetails: 'PluginName' option is not defined."
      gLogger.error( errStr, "%s: %s" % ( storageName, protocolSection ) )
      return S_ERROR( errStr )
    
    return S_OK( protocolDict )

  ###########################################################################################
  #
  # Below is the method for obtaining the object instantiated for a provided storage configuration
  #

  def __generateStorageObject( self, storageName, pluginName, parameters ):

    storageType = pluginName
    if self.proxy:
      storageType = 'Proxy'

    objectLoader = ObjectLoader()
    result = objectLoader.loadObject( 'Resources.Storage.%sStorage' % storageType, storageType + 'Storage' )
    if not result['OK']:
      gLogger.error( 'Failed to load storage object: %s' % result['Message'] )
      return result

    storageClass = result['Value']
    try:
      storage = storageClass( storageName, parameters )
    except Exception, x:
      errStr = "StorageFactory._generateStorageObject: Failed to instantiate %s: %s" % ( storageName, x )
      gLogger.exception( errStr )
      return S_ERROR( errStr )

    return S_OK( storage )
