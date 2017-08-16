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
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Path    import cfgPath
from DIRAC.Core.Utilities.ObjectLoader                import ObjectLoader
from DIRAC.Core.Security.ProxyInfo                    import getVOfromProxyGroup

class StorageFactory( object ):

  def __init__( self, useProxy = False, vo = None ):
    self.rootConfigPathBase = '/Resources/StorageElementBases'
    self.rootConfigPath = '/Resources/StorageElements'
    self.proxy = False
    self.proxy = useProxy
    self.resourceStatus = ResourceStatus()
    self.vo = vo
    if self.vo is None:
      result = getVOfromProxyGroup()
      if result['OK']:
        self.vo = result['Value']
      else:
        RuntimeError( "Can not get the current VO context" )
    self.remotePlugins = []
    self.localPlugins = []
    self.name = ''
    self.options = {}
    self.protocols = {}
    self.storages = []

  ###########################################################################################
  #
  # Below are public methods for obtaining storage objects
  #

  def getStorageName( self, initialName ):
    return self._getConfigStorageName( initialName, 'Alias' )

  def getStorage( self, parameterDict, hideExceptions = False ):
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

    return self.__generateStorageObject( storageName, pluginName, parameterDict, hideExceptions = hideExceptions )

  def getStorages( self, storageName, pluginList = None, hideExceptions = False ):
    """ Get an instance of a Storage based on the DIRAC SE name based on the CS entries CS

        :params storageName: is the DIRAC SE name i.e. 'CERN-RAW'
        :params pluginList: is an optional list of protocols if a sub-set is desired i.e ['SRM2','SRM1']

        :return: dictionary containing storage elements and information about them
    """
    self.remotePlugins = []
    self.localPlugins = []
    self.name = ''
    self.options = {}
    self.protocols = {}
    self.storages = []
    if pluginList is None:
      pluginList = []
    elif isinstance( pluginList, basestring ):
      pluginList = [pluginList]
    if not self.vo:
      gLogger.warn( 'No VO information available' )

    # Get the name of the storage provided
    res = self._getConfigStorageName( storageName, 'Alias', self.rootConfigPath )
    if not res['OK']:
      return res
    storageName = res['Value']
    self.name = storageName

    # In case the storage is made from a base SE, get this information
    res = self._getConfigStorageName( storageName, 'BaseSE', self.rootConfigPath )
    if not res['OK']:
      return res
    # If the storage is derived frmo another one, keep the information
    if res['Value'] != storageName:
      derivedStorageName = storageName
      storageName = res['Value']
      rootConfigPath = self.rootConfigPathBase
    else:
      derivedStorageName = None
      rootConfigPath = self.rootConfigPath

    # Get the options defined in the CS for this storage
    res = self._getConfigStorageOptions( storageName, rootConfigPath, derivedStorageName = derivedStorageName )
    if not res['OK']:
      return res
    self.options = res['Value']

    # Get the protocol specific details
    res = self._getConfigStorageProtocols( storageName, rootConfigPath, derivedStorageName = derivedStorageName )
    if not res['OK']:
      return res
    self.protocols = res['Value']
    requestedLocalPlugins = []
    requestedRemotePlugins = []
    requestedProtocolDetails = []
    turlProtocols = []
    # Generate the protocol specific plug-ins
    for protocolSection, protocolDetails in self.protocols.iteritems():
      pluginName = protocolDetails.get('PluginName', protocolSection) 
      if pluginList and pluginName not in pluginList:
        continue
      protocol = protocolDetails['Protocol']
      result = self.__generateStorageObject( storageName, pluginName, protocolDetails, hideExceptions = hideExceptions )
      if result['OK']:
        self.storages.append( result['Value'] )
        if pluginName in self.localPlugins:
          turlProtocols.append( protocol )
          requestedLocalPlugins.append( pluginName )
        if pluginName in self.remotePlugins:
          requestedRemotePlugins.append( pluginName )
        requestedProtocolDetails.append( protocolDetails )
      else:
        gLogger.info( result['Message'] )

    if self.storages:
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

  def _getConfigStorageName( self, storageName, referenceType, rootConfigPath ):
    """
      This gets the name of the storage the configuration service.
      If the storage is a reference to another SE the resolution is performed.

      :params storageName: is the storage section to check in the CS
      :params referenceType: corresponds to an option inside the storage section
      :params rootConfigPath: the path of the storage section. 
                              It can be /Resources/StorageElements or StorageElementBases

      :return: the name of the storage
    """
    configPath = '%s/%s' % ( rootConfigPath, storageName )
    res = gConfig.getOptions( configPath )
    if not res['OK']:
      errStr = "StorageFactory._getConfigStorageName: Failed to get storage options"
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    if not res['Value']:
      errStr = "StorageFactory._getConfigStorageName: Supplied storage doesn't exist."
      gLogger.error( errStr, configPath )
      return S_ERROR( errStr )
    if referenceType in res['Value']:
      configPath = cfgPath( rootConfigPath, storageName, referenceType )
      referenceName = gConfig.getValue( configPath )

      # This is a backward compatibility test to check if base SE is in the correct section
      # or not.
      configPath = cfgPath( self.rootConfigPathBase, referenceName)
      if not gConfig.getValue(configPath):
        self.rootConfigPathBase = self.rootConfigPath      

      result = self._getConfigStorageName( referenceName, 'Alias', self.rootConfigPathBase )
      if not result['OK']:
        return result
      resolvedName = result['Value']
    else:
      resolvedName = storageName
    return S_OK( resolvedName )

  def _getConfigStorageOptions( self, storageName, rootConfigPath, derivedStorageName = None ):
    """ 
      Get the options associated to the StorageElement as defined in the CS

      :params storageName: is the storage section to check in the CS
      :params rootConfigPath: the path of the storage section. 
                              It can be /Resources/StorageElements or StorageElementBases
      :params derivedStorageName: is the storage section of a derived storage if it inherits from a base

      :return: options associated to the StorageElement as defined in the CS
    """
    optionsDict = {}

    # We first get the options of the baseSE, and then overwrite with the derivedSE
    for seName in ( storageName, derivedStorageName ) if derivedStorageName else ( storageName, ):
      storageConfigPath = cfgPath( rootConfigPath, seName )
      res = gConfig.getOptions( storageConfigPath )
      if not res['OK']:
        errStr = "StorageFactory._getStorageOptions: Failed to get storage options."
        gLogger.error( errStr, "%s: %s" % ( seName, res['Message'] ) )
        return S_ERROR( errStr )
      for option in set( res['Value'] ) - set( ( 'ReadAccess', 'WriteAccess', 'CheckAccess', 'RemoveAccess' ) ):
        optionConfigPath = cfgPath( storageConfigPath, option )
        default = [] if option in [ 'VO', 'AccessProtocols', 'WriteProtocols' ] else ''
        optionsDict[option] = gConfig.getValue( optionConfigPath, default )
      # We update the rootConfigPath. Used only if we have a baseSE, to update information in the derivedSE
      rootConfigPath = self.rootConfigPath

    # The status is that of the derived SE only
    seName = derivedStorageName if derivedStorageName else storageName
    res = self.resourceStatus.getElementStatus( seName, "StorageElement" )
    if not res[ 'OK' ]:
      errStr = "StorageFactory._getStorageOptions: Failed to get storage status"
      gLogger.error( errStr, "%s: %s" % ( seName, res['Message'] ) )
      return S_ERROR( errStr )

    # For safety, we did not add the ${statusType}Access keys
    # this requires modifications in the StorageElement class

    # We add the dictionary with the statusTypes and values
    # { 'statusType1' : 'status1', 'statusType2' : 'status2' ... }
    optionsDict.update( res[ 'Value' ][ seName ] )

    return S_OK( optionsDict )

  def __getProtocolsSections( self, storageName, rootConfigPath ):
    """
      Get the protocols of a specific storage section

      :params storageName: is the storage section to check in the CS
      :params rootConfigPath: the path of the storage section. 
                              It can be /Resources/StorageElements or StorageElementBases

      :return: list of protocol section names
    """
    storageConfigPath = cfgPath( rootConfigPath, storageName )
    res = gConfig.getSections( storageConfigPath )
    if not res['OK']:
      errStr = "StorageFactory._getConfigStorageProtocols: Failed to get storage sections"
      gLogger.error( errStr, "%s: %s" % ( storageName, res['Message'] ) )
      return S_ERROR( errStr )
    protocolSections = res['Value']
    return S_OK( protocolSections )

  def _getConfigStorageProtocols( self, storageName, rootConfigPath, derivedStorageName = None ):
    """      
      Make a dictionary of protocols with the information associated. Merge with a base SE if it exists
      
      :params storageName: is the storage section to check in the CS
      :params rootConfigPath: the path of the storage section. 
                              It can be /Resources/StorageElements or StorageElementBases
      :params derivedStorageName: is the storage section of a derived storage if it inherits from a base

      :return: dictionary of protocols like {protocolSection: {protocolOptions}}
    """
    # Get the sections 
    res = self.__getProtocolsSections( storageName, rootConfigPath )
    if not res['OK']:
      return res
    protocolSections = res['Value']
    sortedProtocolSections = sorted( protocolSections )
    
    # Get the details for each section in a dictionary
    for protocolSection in sortedProtocolSections:
      res = self._getConfigStorageProtocolDetails( storageName, protocolSection, rootConfigPath )
      if not res['OK']:
        return res
      self.protocols[protocolSection]= res['Value']
    if derivedStorageName:
      # We may have parameters overwriting the baseSE protocols
      res = self.__getProtocolsSections( derivedStorageName, self.rootConfigPath )
      if not res['OK']:
        return res
      for protocolSection in res['Value']:
        res = self._getConfigStorageProtocolDetails( derivedStorageName, protocolSection, self.rootConfigPath )
        if not res['OK']:
          return res
        detail = res['Value']
        # If we found the plugin section from which we inherit
        inheritanceMatched = False
        for baseStorageProtocolSection in protocolSections:
          if protocolSection == baseStorageProtocolSection:
            inheritanceMatched = True
            for key, val in detail.iteritems():
              if val:
                self.protocols[protocolSection][key] = val
            break
        # If not matched, consider it a new protocol
        if not inheritanceMatched:
          self.protocols[protocolSection] = detail

    return S_OK(self.protocols)

  def _getConfigStorageProtocolDetails( self, storageName, protocolSection, rootConfigPath ):
    """
      Parse the contents of the protocol block

      :params storageName: is the storage section to check in the CS
      :params protocolSection: name of the protocol section to find information
      :params rootConfigPath: the path of the storage section. 
                              It can be /Resources/StorageElements or StorageElementBases
      :return: dictionary of the protocol options 
    """
    # First obtain the options that are available
    protocolConfigPath = cfgPath( rootConfigPath, storageName, protocolSection )
    res = gConfig.getOptions( protocolConfigPath )
    if not res['OK']:
      errStr = "StorageFactory.__getProtocolDetails: Failed to get protocol options."
      gLogger.error( errStr, "%s: %s" % ( storageName, protocolSection ) )
      return S_ERROR( errStr )
    options = res['Value']

    # We must have certain values internally even if not supplied in CS
    protocolDict = {'Access':'', 'Host':'', 'Path':'', 'Port':'', 'Protocol':'', 'SpaceToken':'', 'WSUrl':''}
    for option in options:
      configPath = cfgPath( protocolConfigPath, option )
      optionValue = gConfig.getValue( configPath, '' )
      protocolDict[option] = optionValue

    # Evaluate the base path taking into account possible VO specific setting
    if self.vo:
      result = gConfig.getOptionsDict( cfgPath( protocolConfigPath, 'VOPath' ) )
      voPath = ''
      if result['OK']:
        voPath = result['Value'].get( self.vo, '' )
      if voPath:
        protocolDict['Path'] = voPath

    # Now update the local and remote protocol lists.
    # A warning will be given if the Access option is not set and the plugin is not already in remote or local.
    plugin = protocolDict.get('PluginName', protocolSection)
    if protocolDict['Access'].lower() == 'remote':
      self.remotePlugins.append(plugin)
    elif protocolDict['Access'].lower() == 'local':
      self.localPlugins.append(plugin)
    elif protocolSection not in self.protocols.keys():
      errStr = "StorageFactory.__getProtocolDetails: The 'Access' option \
      for %s:%s is neither 'local' or 'remote'." % ( storageName, protocolSection )
      gLogger.warn( errStr )

    return S_OK( protocolDict )

  ###########################################################################################
  #
  # Below is the method for obtaining the object instantiated for a provided storage configuration
  #

  def __generateStorageObject( self, storageName, pluginName, parameters, hideExceptions = False ):

    storageType = pluginName
    if self.proxy:
      storageType = 'Proxy'

    objectLoader = ObjectLoader()
    result = objectLoader.loadObject( 'Resources.Storage.%sStorage' % storageType, storageType + 'Storage',
                                      hideExceptions = hideExceptions )
    if not result['OK']:
      gLogger.error( 'Failed to load storage object: %s' % result['Message'] )
      return result

    storageClass = result['Value']
    try:
      storage = storageClass( storageName, parameters )
    except Exception as x:
      errStr = "StorageFactory._generateStorageObject: Failed to instantiate %s: %s" % ( storageName, x )
      gLogger.exception( errStr )
      return S_ERROR( errStr )

    return S_OK( storage )
