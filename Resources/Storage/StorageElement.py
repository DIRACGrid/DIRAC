""" This is the StorageElement class.
"""
from types import ListType

__RCSID__ = "$Id$"
# # custom duty
import re
import errno
# # from DIRAC
from DIRAC import gLogger, gConfig, DError, DIRACError
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, returnSingleResult
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Resources.Utilities import checkArgumentFormat
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

class StorageElementCache( object ):

  def __init__( self ):
    self.seCache = DictCache()

  def __call__( self, name, protocols = None, vo = None ):
    self.seCache.purgeExpired( expiredInSeconds = 60 )
    argTuple = ( name, protocols, vo )
    seObj = self.seCache.get( argTuple )

    if not seObj:
      seObj = StorageElementItem( name, protocols, vo )
      # Add the StorageElement to the cache for 1/2 hour
      self.seCache.add( argTuple, 1800, seObj )

    return seObj

class StorageElementItem( object ):
  """
  .. class:: StorageElement

  common interface to the grid storage element



  self.name is the resolved name of the StorageElement i.e CERN-tape
  self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
  self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
  self.localPlugins is a list of the local protocols that were created by StorageFactory
  self.remotePlugins is a list of the remote protocols that were created by StorageFactory
  self.protocolOptions is a list of dictionaries containing the options found in the CS. (should be removed)



  dynamic method :
  retransferOnlineFile( lfn )
  exists( lfn )
  isFile( lfn )
  getFile( lfn, localPath = False )
  putFile( lfnLocal, sourceSize = 0 ) : {lfn:local}
  replicateFile( lfn, sourceSize = 0 )
  getFileMetadata( lfn )
  getFileSize( lfn )
  removeFile( lfn )
  prestageFile( lfn, lifetime = 86400 )
  prestageFileStatus( lfn )
  pinFile( lfn, lifetime = 60 * 60 * 24 )
  releaseFile( lfn )
  isDirectory( lfn )
  getDirectoryMetadata( lfn )
  getDirectorySize( lfn )
  listDirectory( lfn )
  removeDirectory( lfn, recursive = False )
  createDirectory( lfn )
  putDirectory( lfn )
  getDirectory( lfn, localPath = False )


  """

  __deprecatedArguments = ["singleFile", "singleDirectory"]  # Arguments that are now useless

  # Some methods have a different name in the StorageElement and the plugins...
  # We could avoid this static list in the __getattr__ by checking the storage plugin and so on
  # but fine... let's not be too smart, otherwise it becomes unreadable :-)
  __equivalentMethodNames = {"exists" : "exists",
                            "isFile" : "isFile",
                            "getFile" : "getFile",
                            "putFile" : "putFile",
                            "replicateFile" : "putFile",
                            "getFileMetadata" : "getFileMetadata",
                            "getFileSize" : "getFileSize",
                            "removeFile" : "removeFile",
                            "prestageFile" : "prestageFile",
                            "prestageFileStatus" : "prestageFileStatus",
                            "pinFile" : "pinFile",
                            "releaseFile" : "releaseFile",
                            "isDirectory" : "isDirectory",
                            "getDirectoryMetadata" : "getDirectoryMetadata",
                            "getDirectorySize" : "getDirectorySize",
                            "listDirectory" : "listDirectory",
                            "removeDirectory" : "removeDirectory",
                            "createDirectory" : "createDirectory",
                            "putDirectory" : "putDirectory",
                            "getDirectory" : "getDirectory",
                            }

  # We can set default argument in the __executeFunction which impacts all plugins
  __defaultsArguments = {"putFile" : {"sourceSize"  : 0 },
                         "getFile": { "localPath": False },
                         "prestageFile" : { "lifetime" : 86400 },
                         "pinFile" : { "lifetime" : 60 * 60 * 24 },
                         "removeDirectory" : { "recursive" : False },
                         "getDirectory" : { "localPath" : False },
                         }

  def __init__( self, name, plugins = None, vo = None ):
    """ c'tor

    :param str name: SE name
    :param list plugins: requested storage plugins
    :param vo
    """

    self.methodName = None

    if vo:
      self.vo = vo
    else:
      result = getVOfromProxyGroup()
      if not result['OK']:
        return
      self.vo = result['Value']
    self.opHelper = Operations( vo = self.vo )

    proxiedProtocols = gConfig.getValue( '/LocalSite/StorageElements/ProxyProtocols', "" ).split( ',' )
    useProxy = ( gConfig.getValue( "/Resources/StorageElements/%s/AccessProtocol.1/Protocol" % name, "UnknownProtocol" )
                in proxiedProtocols )

    if not useProxy:
      useProxy = gConfig.getValue( '/LocalSite/StorageElements/%s/UseProxy' % name, False )
    if not useProxy:
      useProxy = self.opHelper.getValue( '/Services/StorageElements/%s/UseProxy' % name, False )

    self.valid = True
    if plugins == None:
      res = StorageFactory( useProxy = useProxy, vo = self.vo ).getStorages( name, pluginList = [] )
    else:
      res = StorageFactory( useProxy = useProxy, vo = self.vo ).getStorages( name, pluginList = plugins )

    if not res['OK']:
      self.valid = False
      self.name = name
      self.errorReason = res['Message']
    else:
      factoryDict = res['Value']
      self.name = factoryDict['StorageName']
      self.options = factoryDict['StorageOptions']
      self.localPlugins = factoryDict['LocalPlugins']
      self.remotePlugins = factoryDict['RemotePlugins']
      self.storages = factoryDict['StorageObjects']
      self.protocolOptions = factoryDict['ProtocolOptions']
      self.turlProtocols = factoryDict['TurlProtocols']
      for storage in self.storages:
        storage.setStorageElement( self )

    self.log = gLogger.getSubLogger( "SE[%s]" % self.name )
    self.useCatalogURL = gConfig.getValue( '/Resources/StorageElements/%s/UseCatalogURL' % self.name, False )

    #                         'getTransportURL',
    self.readMethods = [ 'getFile',
                         'prestageFile',
                         'prestageFileStatus',
                         'getDirectory']

    self.writeMethods = [ 'retransferOnlineFile',
                          'putFile',
                          'replicateFile',
                          'pinFile',
                          'releaseFile',
                          'createDirectory',
                          'putDirectory' ]

    self.removeMethods = [ 'removeFile', 'removeDirectory' ]

    self.checkMethods = [ 'exists',
                          'getDirectoryMetadata',
                          'getDirectorySize',
                          'getFileSize',
                          'getFileMetadata',
                          'listDirectory',
                          'isDirectory',
                          'isFile',
                           ]

    self.okMethods = [ 'getLocalProtocols',
                       'getProtocols',
                       'getRemoteProtocols',
                       'getStorageElementName',
                       'getStorageParameters',
                       'getTransportURL',
                       'isLocalSE' ]

    self.__fileCatalog = None

  def dump( self ):
    """ Dump to the logger a summary of the StorageElement items. """
    self.log.verbose( "dump: Preparing dump for StorageElement %s." % self.name )
    if not self.valid:
      self.log.debug( "dump: Failed to create StorageElement plugins.", self.errorReason )
      return
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sorted( self.options ):
      outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), self.options[key] )

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % ( outStr, i )
      storageParameters = storage.getParameters()
      for key in sorted( storageParameters ):
        outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), storageParameters[key] )
      i = i + 1
    self.log.verbose( outStr )

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementName( self ):
    """ SE name getter """
    self.log.verbose( "StorageElement.getStorageElementName: The Storage Element name is %s." % self.name )
    return S_OK( self.name )

  def getChecksumType( self ):
    """ get local /Resources/StorageElements/SEName/ChecksumType option if defined, otherwise
        global /Resources/StorageElements/ChecksumType
    """
    self.log.verbose( "StorageElement.getChecksumType : get checksum type for %s." % self.name )
    return S_OK( str( gConfig.getValue( "/Resources/StorageElements/ChecksumType", "ADLER32" ) ).upper()
                 if "ChecksumType" not in self.options else str( self.options["ChecksumType"] ).upper() )

  def getStatus( self ):
    """
     Return Status of the SE, a dictionary with:
      - Read: True (is allowed), False (it is not allowed)
      - Write: True (is allowed), False (it is not allowed)
      - Remove: True (is allowed), False (it is not allowed)
      - Check: True (is allowed), False (it is not allowed).
      NB: Check always allowed IF Read is allowed (regardless of what set in the Check option of the configuration)
      - DiskSE: True if TXDY with Y > 0 (defaults to True)
      - TapeSE: True if TXDY with X > 0 (defaults to False)
      - TotalCapacityTB: float (-1 if not defined)
      - DiskCacheTB: float (-1 if not defined)
    """

    self.log.verbose( "StorageElement.getStatus : determining status of %s." % self.name )

    retDict = {}
    if not self.valid:
      retDict['Read'] = False
      retDict['Write'] = False
      retDict['Remove'] = False
      retDict['Check'] = False
      retDict['DiskSE'] = False
      retDict['TapeSE'] = False
      retDict['TotalCapacityTB'] = -1
      retDict['DiskCacheTB'] = -1
      return S_OK( retDict )

    # If nothing is defined in the CS Access is allowed
    # If something is defined, then it must be set to Active
    retDict['Read'] = not ( 'ReadAccess' in self.options and self.options['ReadAccess'] not in ( 'Active', 'Degraded' ) )
    retDict['Write'] = not ( 'WriteAccess' in self.options and self.options['WriteAccess'] not in ( 'Active', 'Degraded' ) )
    retDict['Remove'] = not ( 'RemoveAccess' in self.options and self.options['RemoveAccess'] not in ( 'Active', 'Degraded' ) )
    if retDict['Read']:
      retDict['Check'] = True
    else:
      retDict['Check'] = not ( 'CheckAccess' in self.options and self.options['CheckAccess'] not in ( 'Active', 'Degraded' ) )
    diskSE = True
    tapeSE = False
    if 'SEType' in self.options:
      # Type should follow the convention TXDY
      seType = self.options['SEType']
      diskSE = re.search( 'D[1-9]', seType ) != None
      tapeSE = re.search( 'T[1-9]', seType ) != None
    retDict['DiskSE'] = diskSE
    retDict['TapeSE'] = tapeSE
    try:
      retDict['TotalCapacityTB'] = float( self.options['TotalCapacityTB'] )
    except Exception:
      retDict['TotalCapacityTB'] = -1
    try:
      retDict['DiskCacheTB'] = float( self.options['DiskCacheTB'] )
    except Exception:
      retDict['DiskCacheTB'] = -1

    return S_OK( retDict )

  def isValid( self, operation = '' ):
    """ check CS/RSS statuses for :operation:

    :param str operation: operation name
    """
    self.log.verbose( "StorageElement.isValid: Determining if the StorageElement %s is valid for VO %s" % ( self.name,
                                                                                                            self.vo ) )

    if not self.valid:
      self.log.debug( "StorageElement.isValid: Failed to create StorageElement plugins.", self.errorReason )
      return S_ERROR( self.errorReason )

    # Check if the Storage Element is eligible for the user's VO
    if 'VO' in self.options and not self.vo in self.options['VO']:
      self.log.debug( "StorageElementisValid: StorageElement is not allowed for VO %s" % self.vo )
      return DError( errno.EACCES, "StorageElement.isValid: StorageElement is not allowed for VO" )
    self.log.verbose( "StorageElement.isValid: Determining if the StorageElement %s is valid for %s" % ( self.name,
                                                                                                         operation ) )
    if ( not operation ) or ( operation in self.okMethods ):
      return S_OK()

    if ( not operation ) or ( operation in self.okMethods ):
      return S_OK()
    # Determine whether the StorageElement is valid for checking, reading, writing
    res = self.getStatus()
    if not res[ 'OK' ]:
      self.log.debug( "Could not call getStatus" )
      return S_ERROR( "StorageElement.isValid could not call the getStatus method" )
    checking = res[ 'Value' ][ 'Check' ]
    reading = res[ 'Value' ][ 'Read' ]
    writing = res[ 'Value' ][ 'Write' ]
    removing = res[ 'Value' ][ 'Remove' ]

    # Determine whether the requested operation can be fulfilled
    if ( not operation ) and ( not reading ) and ( not writing ) and ( not checking ):
      self.log.debug( "StorageElement.isValid: Read, write and check access not permitted." )
      return DError( errno.EACCES, "StorageElement.isValid: Read, write and check access not permitted." )

    # The supplied operation can be 'Read','Write' or any of the possible StorageElement methods.
    if ( operation in self.readMethods ) or ( operation.lower() in ( 'read', 'readaccess' ) ):
      operation = 'ReadAccess'
    elif operation in self.writeMethods or ( operation.lower() in ( 'write', 'writeaccess' ) ):
      operation = 'WriteAccess'
    elif operation in self.removeMethods or ( operation.lower() in ( 'remove', 'removeaccess' ) ):
      operation = 'RemoveAccess'
    elif operation in self.checkMethods or ( operation.lower() in ( 'check', 'checkaccess' ) ):
      operation = 'CheckAccess'
    else:
      self.log.debug( "StorageElement.isValid: The supplied operation is not known.", operation )
      return DError( DIRACError.ENOMETH , "StorageElement.isValid: The supplied operation is not known." )
    self.log.debug( "in isValid check the operation: %s " % operation )
    # Check if the operation is valid
    if operation == 'CheckAccess':
      if not reading:
        if not checking:
          self.log.debug( "StorageElement.isValid: Check access not currently permitted." )
          return DError( errno.EACCES, "StorageElement.isValid: Check access not currently permitted." )
    if operation == 'ReadAccess':
      if not reading:
        self.log.debug( "StorageElement.isValid: Read access not currently permitted." )
        return DError( errno.EACCES, "StorageElement.isValid: Read access not currently permitted." )
    if operation == 'WriteAccess':
      if not writing:
        self.log.debug( "StorageElementisValid: Write access not currently permitted." )
        return DError( errno.EACCES, "StorageElement.isValid: Write access not currently permitted." )
    if operation == 'RemoveAccess':
      if not removing:
        self.log.debug( "StorageElement.isValid: Remove access not currently permitted." )
        return DError( errno.EACCES, "StorageElement.isValid: Remove access not currently permitted." )
    return S_OK()

  def getPlugins( self ):
    """ Get the list of all the plugins defined for this Storage Element
    """
    self.log.verbose( "StorageElement.getPlugins : Obtaining all plugins of %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    allPlugins = self.localPlugins + self.remotePlugins
    return S_OK( allPlugins )

  def getRemotePlugins( self ):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    self.log.verbose( "StorageElement.getRemotePlugins: Obtaining remote protocols for %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    return S_OK( self.remotePlugins )

  def getLocalPlugins( self ):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    self.log.verbose( "StorageElement.getLocalPlugins: Obtaining local protocols for %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    return S_OK( self.localPlugins )

  def getStorageParameters( self, plugin ):
    """ Get plugin specific options
      :param plugin : plugin we are interested in
    """
    self.log.verbose( "StorageElement.getStorageParameters: Obtaining storage parameters for %s plugin %s." % ( self.name,
                                                                                                                plugin ) )
    res = self.getPlugins()
    if not res['OK']:
      return res
    availablePlugins = res['Value']
    if not plugin in availablePlugins:
      errStr = "StorageElement.getStorageParameters: Requested plugin not available for SE."
      self.log.debug( errStr, '%s for %s' % ( plugin, self.name ) )
      return S_ERROR( errStr )
    for storage in self.storages:
      storageParameters = storage.getParameters()
      if storageParameters['PluginName'] == plugin:
        return S_OK( storageParameters )
    errStr = "StorageElement.getStorageParameters: Requested plugin supported but no object found."
    self.log.debug( errStr, "%s for %s" % ( plugin, self.name ) )
    return S_ERROR( errStr )

  def negociateProtocolWithOtherSE( self, sourceSE, protocols = None ):
    """ Negotiate what protocol could be used for a third party transfer
        between the sourceSE and ourselves. If protocols is given,
        the chosen protocol has to be among those

        :param sourceSE : storageElement instance of the sourceSE
        :param protocols: protocol restriction list

        :return a list protocols that fits the needs, or None

    """

    # We should actually separate source and destination protocols
    # For example, an SRM can get as a source an xroot or gsiftp url...
    # but with the current implementation, we get only srm

    destProtocols = set( [destStorage.protocolParameters['Protocol'] for destStorage in self.storages] )
    sourceProtocols = set( [sourceStorage.protocolParameters['Protocol'] for sourceStorage in sourceSE.storages] )

    commonProtocols = destProtocols & sourceProtocols

    if protocols:
      protocols = set( list( protocols ) ) if protocols else set()
      commonProtocols = commonProtocols & protocols

    return S_OK( list( commonProtocols ) )

  #################################################################################################
  #
  # These are the basic get functions for lfn manipulation
  #

  def __getURLPath( self, url ):
    """  Get the part of the URL path below the basic storage path.
         This path must coincide with the LFN of the file in order to be compliant with the DIRAC conventions.
    """
    self.log.verbose( "StorageElement.__getURLPath: Getting path from url in %s." % self.name )
    if not self.valid:
      return S_ERROR( self.errorReason )
    res = pfnparse( url )
    if not res['OK']:
      return res
    fullURLPath = '%s/%s' % ( res['Value']['Path'], res['Value']['FileName'] )

    # Check all available storages and check whether the url is for that protocol
    urlPath = ''
    for storage in self.storages:
      res = storage.isNativeURL( url )
      if res['OK']:
        if res['Value']:
          parameters = storage.getParameters()
          saPath = parameters['Path']
          if not saPath:
            # If the sa path doesn't exist then the url path is the entire string
            urlPath = fullURLPath
          else:
            if re.search( saPath, fullURLPath ):
              # Remove the sa path from the fullURLPath
              urlPath = fullURLPath.replace( saPath, '' )
      if urlPath:
        return S_OK( urlPath )
    # This should never happen. DANGER!!
    errStr = "StorageElement.__getURLPath: Failed to get the url path for any of the protocols!!"
    self.log.debug( errStr )
    return S_ERROR( errStr )

  def getLFNFromURL( self, urls ):
    """ Get the LFN from the PFNS .
        :param lfn : input lfn or lfns (list/dict)
    """
    result = checkArgumentFormat( urls )
    if result['OK']:
      urlDict = result['Value']
    else:
      errStr = "StorageElement.getLFNFromURL: Supplied urls must be string, list of strings or a dictionary."
      self.log.debug( errStr )
      return DError( errno.EINVAL, errStr )

    retDict = { "Successful" : {}, "Failed" : {} }
    for url in urlDict:
      res = self.__getURLPath( url )
      if res["OK"]:
        retDict["Successful"][url] = res["Value"]
      else:
        retDict["Failed"][url] = res["Message"]
    return S_OK( retDict )

  ###########################################################################################
  #
  # This is the generic wrapper for file operations
  #

  def getURL( self, lfn, protocol = False, replicaDict = None ):
    """ execute 'getTransportURL' operation.
      :param str lfn: string, list or dictionary of lfns
      :param protocol: if no protocol is specified, we will request self.turlProtocols
      :param replicaDict: optional results from the File Catalog replica query
    """

    self.log.verbose( "StorageElement.getURL: Getting accessUrl %s for lfn in %s." % ( "(%s)" % protocol if protocol else "", self.name ) )

    if not protocol:
      protocols = self.turlProtocols
    elif type( protocol ) is ListType:
      protocols = protocol
    elif type( protocol ) == type( '' ):
      protocols = [protocol]

    self.methodName = "getTransportURL"
    result = self.__executeMethod( lfn, protocols = protocols )
    return result

  def __isLocalSE( self ):
    """ Test if the Storage Element is local in the current context
    """
    self.log.verbose( "StorageElement.isLocalSE: Determining whether %s is a local SE." % self.name )

    import DIRAC
    localSEs = getSEsForSite( DIRAC.siteName() )['Value']
    if self.name in localSEs:
      return S_OK( True )
    else:
      return S_OK( False )

  def __getFileCatalog( self ):

    if not self.__fileCatalog:
      self.__fileCatalog = FileCatalog( vo = self.vo )
    return self.__fileCatalog

  def __generateURLDict( self, lfns, storage, replicaDict = {} ):
    """ Generates a dictionary (url : lfn ), where the url are constructed
        from the lfn using the constructURLFromLFN method of the storage plugins.
        :param: lfns : dictionary {lfn:whatever}
        :returns dictionary {constructed url : lfn}
    """
    self.log.verbose( "StorageElement.__generateURLDict: generating url dict for %s lfn in %s." % ( len( lfns ), self.name ) )

    urlDict = {}  # url : lfn
    failed = {}  # lfn : string with errors
    for lfn in lfns:
      if self.useCatalogURL:
        # Is this self.name alias proof?
        url = replicaDict.get( lfn, {} ).get( self.name, '' )
        if url:
          urlDict[url] = lfn
          continue
        else:
          fc = self.__getFileCatalog()
          result = fc.getReplicas()
          if not result['OK']:
            failed[lfn] = result['Message']
          url = result['Value']['Successful'].get( lfn, {} ).get( self.name, '' )

        if not url:
          failed[lfn] = 'Failed to get catalog replica'
        else:
          # Update the URL according to the current SE description
          result = returnSingleResult( storage.updateURL( url ) )
          if not result['OK']:
            failed[lfn] = result['Message']
          else:
            urlDict[result['Value']] = lfn
      else:
        result = storage.constructURLFromLFN( lfn, withWSUrl = True )
        if not result['OK']:
          errStr = "StorageElement.__generateURLDict %s." % result['Message']
          self.log.debug( errStr, 'for %s' % ( lfn ) )
          failed[lfn] = "%s %s" % ( failed[lfn], errStr ) if lfn in failed else errStr
        else:
          urlDict[result['Value']] = lfn

    res = S_OK( {'Successful': urlDict, 'Failed' : failed} )
#     res['Failed'] = failed
    return res

  def __executeMethod( self, lfn, *args, **kwargs ):
    """ Forward the call to each storage in turn until one works.
        The method to be executed is stored in self.methodName
        :param lfn : string, list or dictionnary
        :param *args : variable amount of non-keyword arguments. SHOULD BE EMPTY
        :param **kwargs : keyword arguments
        :returns S_OK( { 'Failed': {lfn : reason} , 'Successful': {lfn : value} } )
                The Failed dict contains the lfn only if the operation failed on all the storages
                The Successful dict contains the value returned by the successful storages.
    """


    removedArgs = {}
    self.log.verbose( "StorageElement.__executeMethod : preparing the execution of %s" % ( self.methodName ) )

    # args should normaly be empty to avoid problem...
    if len( args ):
      self.log.verbose( "StorageElement.__executeMethod: args should be empty!%s" % args )
      # because there is normally only one kw argument, I can move it from args to kwargs
      methDefaultArgs = StorageElementItem.__defaultsArguments.get( self.methodName, {} ).keys()
      if len( methDefaultArgs ):
        kwargs[methDefaultArgs[0] ] = args[0]
        args = args[1:]
      self.log.verbose( "StorageElement.__executeMethod: put it in kwargs, but dirty and might be dangerous!args %s kwargs %s" % ( args, kwargs ) )


    # We check the deprecated arguments
    for depArg in StorageElementItem.__deprecatedArguments:
      if depArg in kwargs:
        self.log.verbose( "StorageElement.__executeMethod: %s is not an allowed argument anymore. Please change your code!" % depArg )
        removedArgs[depArg] = kwargs[depArg]
        del kwargs[depArg]



    # Set default argument if any
    methDefaultArgs = StorageElementItem.__defaultsArguments.get( self.methodName, {} )
    for argName in methDefaultArgs:
      if argName not in kwargs:
        self.log.debug( "StorageElement.__executeMethod : default argument %s for %s not present.\
         Setting value %s." % ( argName, self.methodName, methDefaultArgs[argName] ) )
        kwargs[argName] = methDefaultArgs[argName]

    res = checkArgumentFormat( lfn )
    if not res['OK']:
      errStr = "StorageElement.__executeMethod: Supplied lfns must be string, list of strings or a dictionary."
      self.log.debug( errStr, res['Message'] )
      return res
    lfnDict = res['Value']

    self.log.verbose( "StorageElement.__executeMethod: Attempting to perform '%s' operation with %s lfns." % ( self.methodName,
                                                                                                               len( lfnDict ) ) )

    res = self.isValid( operation = self.methodName )
    if not res['OK']:
      return res
    else:
      if not self.valid:
        return S_ERROR( self.errorReason )

    successful = {}
    failed = {}
    localSE = self.__isLocalSE()['Value']
    # Try all of the storages one by one
    for storage in self.storages:
      # Determine whether to use this storage object
      storageParameters = storage.getParameters()
      if not storageParameters:
        self.log.debug( "StorageElement.__executeMethod: Failed to get storage parameters.", "%s %s" % ( self.name,
                                                                                                         res['Message'] ) )
        continue
      pluginName = storageParameters['PluginName']
      if not lfnDict:
        self.log.debug( "StorageElement.__executeMethod: No lfns to be attempted for %s protocol." % pluginName )
        continue
      if not ( pluginName in self.remotePlugins ) and not localSE and not storage.pluginName == "Proxy":
        # If the SE is not local then we can't use local protocols
        self.log.debug( "StorageElement.__executeMethod: Local protocol not appropriate for remote use: %s." % pluginName )
        continue

      self.log.verbose( "StorageElement.__executeMethod: Generating %s protocol URLs for %s." % ( len( lfnDict ),
                                                                                                  pluginName ) )
      replicaDict = kwargs.pop( 'replicaDict', {} )
      if storage.pluginName != "Proxy":
        res = self.__generateURLDict( lfnDict, storage, replicaDict = replicaDict )
        urlDict = res['Value']['Successful']  # url : lfn
        failed.update( res['Value']['Failed'] )
      else:
        urlDict = dict( [ ( lfn, lfn ) for lfn in lfnDict ] )
      if not len( urlDict ):
        self.log.verbose( "StorageElement.__executeMethod No urls generated for protocol %s." % pluginName )
      else:
        self.log.verbose( "StorageElement.__executeMethod: Attempting to perform '%s' for %s physical files" % ( self.methodName,
                                                                                                    len( urlDict ) ) )
        fcn = None
        if hasattr( storage, self.methodName ) and callable( getattr( storage, self.methodName ) ):
          fcn = getattr( storage, self.methodName )
        if not fcn:
          return DError( DIRACError.ENOMETH, "StorageElement.__executeMethod: unable to invoke %s, it isn't a member function of storage" )

        urlsToUse = {}  # url : the value of the lfn dictionary for the lfn of this url
        for url in urlDict:
          urlsToUse[url] = lfnDict[urlDict[url]]

        res = fcn( urlsToUse, *args, **kwargs )
        if not res['OK']:
          errStr = "StorageElement.__executeMethod: Completely failed to perform %s." % self.methodName
          self.log.debug( errStr, '%s with plugin %s: %s' % ( self.name, pluginName, res['Message'] ) )
          for lfn in urlDict.values():
            if lfn not in failed:
              failed[lfn] = ''
            failed[lfn] = "%s %s" % ( failed[lfn], res['Message'] ) if failed[lfn] else res['Message']
        else:
          for url, lfn in urlDict.items():
            if url not in res['Value']['Successful']:
              if lfn not in failed:
                failed[lfn] = ''
              if url in res['Value']['Failed']:
                self.log.debug( res['Value']['Failed'][url] )
                failed[lfn] = "%s %s" % ( failed[lfn], res['Value']['Failed'][url] ) if failed[lfn] else res['Value']['Failed'][url]
              else:
                errStr = 'No error returned from plug-in'
                failed[lfn] = "%s %s" % ( failed[lfn], errStr ) if failed[lfn] else errStr
            else:
              successful[lfn] = res['Value']['Successful'][url]
              if lfn in failed:
                failed.pop( lfn )
              lfnDict.pop( lfn )

    return S_OK( { 'Failed': failed, 'Successful': successful } )


  def __getattr__( self, name ):
    """ Forwards the equivalent Storage calls to StorageElement.__executeMethod"""
    # We take either the equivalent name, or the name itself
    self.methodName = StorageElementItem.__equivalentMethodNames.get( name, None )

    if self.methodName:
      return self.__executeMethod

    raise AttributeError( "StorageElement does not have a method '%s'" % name )



StorageElement = StorageElementCache()
