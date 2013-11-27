########################################################################
# $HeadURL$
########################################################################
""" This is the StorageElement class.

    self.name is the resolved name of the StorageElement i.e CERN-tape
    self.options is dictionary containing the general options defined in the CS e.g. self.options['Backend] = 'Castor2'
    self.storages is a list of the stub objects created by StorageFactory for the protocols found in the CS.
    self.localProtocols is a list of the local protocols that were created by StorageFactory
    self.remoteProtocols is a list of the remote protocols that were created by StorageFactory
    self.protocolOptions is a list of dictionaries containing the options found in the CS. (should be removed)
"""
__RCSID__ = "$Id$"
## custom duty
import re
from types import ListType, StringType, StringTypes, DictType
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

class StorageElement:
  """
  .. class:: StorageElement

  common interface to the grid storage element
  """

  def __init__( self, name, protocols = None, vo = None ):
    """ c'tor

    :param str name: SE name
    :param list protocols: requested protocols
    """

    self.vo = vo
    if not vo:
      result = getVOfromProxyGroup()
      if not result['OK']:
        return result
      self.vo = result['Value']
    self.opHelper = Operations( vo = self.vo )
    useProxy = gConfig.getValue( '/LocalSite/StorageElements/%s/UseProxy' % name, False )
    if not useProxy:
      useProxy = self.opHelper.getValue( '/Services/StorageElements/%s/UseProxy' % name, False )

    self.valid = True
    if protocols == None:
      res = StorageFactory( useProxy ).getStorages( name, protocolList = [] )
    else:
      res = StorageFactory( useProxy ).getStorages( name, protocolList = protocols )
    if not res['OK']:
      self.valid = False
      self.name = name
      self.errorReason = res['Message']
    else:
      factoryDict = res['Value']
      self.name = factoryDict['StorageName']
      self.options = factoryDict['StorageOptions']
      self.localProtocols = factoryDict['LocalProtocols']
      self.remoteProtocols = factoryDict['RemoteProtocols']
      self.storages = factoryDict['StorageObjects']
      self.protocolOptions = factoryDict['ProtocolOptions']
      self.turlProtocols = factoryDict['TurlProtocols']

    self.log = gLogger.getSubLogger( "SE[%s]" % self.name )

    self.readMethods = [ 'getFile',
                         'getAccessUrl',
                         'getTransportURL',
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
                       'getPfnForProtocol',
                       'getPfnForLfn',
                       'getPfnPath',
                       'getProtocols',
                       'getRemoteProtocols',
                       'getStorageElementName',
                       'getStorageElementOption',
                       'getStorageParameters',
                       'isLocalSE' ]

  def dump( self ):
    """ Dump to the logger a summary of the StorageElement items. """
    self.log.info( "dump: Preparing dump for StorageElement %s." % self.name )
    if not self.valid:
      self.log.error( "dump: Failed to create StorageElement plugins.", self.errorReason )
      return
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sorted( self.options ):
      outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), self.options[key] )

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % ( outStr, i )
      res = storage.getParameters()
      storageParameters = res['Value']
      for key in sorted( storageParameters ):
        outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), storageParameters[key] )
      i = i + 1
    self.log.info( outStr )

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementName( self ):
    """ SE name getter """
    self.log.verbose( "getStorageElementName: The Storage Element name is %s." % self.name )
    return S_OK( self.name )

  def getChecksumType( self ):
    """ get local /Resources/StorageElements/SEName/ChecksumType option if defined, otherwise
        global /Resources/StorageElements/ChecksumType
    """
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
    self.log.debug( "isValid: Determining whether the StorageElement %s is valid for %s" % ( self.name,
                                                                                             operation ) )

    if ( not operation ) or ( operation in self.okMethods ):
      return S_OK()

    if not self.valid:
      self.log.error( "isValid: Failed to create StorageElement plugins.", self.errorReason )
      return S_ERROR( self.errorReason )
    # Determine whether the StorageElement is valid for checking, reading, writing
    res = self.getStatus()
    if not res[ 'OK' ]:
      self.log.error( "Could not call getStatus" )
      return S_ERROR( "StorageElement.isValid could not call the getStatus method" )
    checking = res[ 'Value' ][ 'Check' ]
    reading = res[ 'Value' ][ 'Read' ]
    writing = res[ 'Value' ][ 'Write' ]
    removing = res[ 'Value' ][ 'Remove' ]

    # Determine whether the requested operation can be fulfilled
    if ( not operation ) and ( not reading ) and ( not writing ) and ( not checking ):
      self.log.error( "isValid: Read, write and check access not permitted." )
      return S_ERROR( "StorageElement.isValid: Read, write and check access not permitted." )

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
      self.log.error( "isValid: The supplied operation is not known.", operation )
      return S_ERROR( "StorageElement.isValid: The supplied operation is not known." )
    self.log.debug( "in isValid check the operation: %s " % operation )
    # Check if the operation is valid
    if operation == 'CheckAccess':
      if not reading:
        if not checking:
          self.log.error( "isValid: Check access not currently permitted." )
          return S_ERROR( "StorageElement.isValid: Check access not currently permitted." )
    if operation == 'ReadAccess':
      if not reading:
        self.log.error( "isValid: Read access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Read access not currently permitted." )
    if operation == 'WriteAccess':
      if not writing:
        self.log.error( "isValid: Write access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Write access not currently permitted." )
    if operation == 'RemoveAccess':
      if not removing:
        self.log.error( "isValid: Remove access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Remove access not currently permitted." )
    return S_OK()

  def getProtocols( self ):
    """ Get the list of all the protocols defined for this Storage Element
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    self.log.verbose( "getProtocols: Obtaining all protocols." )
    allProtocols = self.localProtocols + self.remoteProtocols
    return S_OK( allProtocols )

  def getRemoteProtocols( self ):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    self.log.verbose( "getRemoteProtocols: Obtaining remote protocols for %s." % self.name )
    return S_OK( self.remoteProtocols )

  def getLocalProtocols( self ):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    self.log.verbose( "getLocalProtocols: Obtaining local protocols for %s." % self.name )
    return S_OK( self.localProtocols )

  def getStorageElementOption( self, option ):
    """ Get the value for the option supplied from self.options
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    self.log.verbose( "getStorageElementOption: Obtaining %s option for Storage Element %s." % ( option,
                                                                                                 self.name ) )
    if option in self.options:
      optionValue = self.options[option]
      return S_OK( optionValue )
    else:
      errStr = "getStorageElementOption: Option not defined for SE."
      self.log.error( errStr, "%s for %s" % ( option, self.name ) )
      return S_ERROR( errStr )

  def getStorageParameters( self, protocol ):
    """ Get protocol specific options
    """
    self.log.verbose( "getStorageParameters: Obtaining storage parameters for %s protocol %s." % ( self.name,
                                                                                                   protocol ) )
    res = self.getProtocols()
    if not res['OK']:
      return res
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "getStorageParameters: Requested protocol not available for SE."
      self.log.warn( errStr, '%s for %s' % ( protocol, self.name ) )
      return S_ERROR( errStr )
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK( storageParameters )
    errStr = "getStorageParameters: Requested protocol supported but no object found."
    self.log.error( errStr, "%s for %s" % ( protocol, self.name ) )
    return S_ERROR( errStr )

  def isLocalSE( self ):
    """ Test if the Storage Element is local in the current context
    """
    import DIRAC
    self.log.verbose( "isLocalSE: Determining whether %s is a local SE." % self.name )
    localSEs = getSEsForSite( DIRAC.siteName() )['Value']
    if self.name in localSEs:
      return S_OK( True )
    else:
      return S_OK( False )

  #################################################################################################
  #
  # These are the basic get functions for pfn manipulation
  #

  def getPfnForProtocol( self, pfn, protocol, withPort = True ):
    """ Transform the input pfn into another with the given protocol for the Storage Element.
    """
    res = self.getProtocols()
    if not res['OK']:
      return res
    if type( protocol ) == StringType:
      protocols = [protocol]
    elif type( protocol ) == ListType:
      protocols = protocol
    else:
      errStr = "getPfnForProtocol: Supplied protocol must be string or list of strings."
      self.log.error( errStr, "%s %s" % ( protocol, self.name ) )
      return S_ERROR( errStr )
    availableProtocols = res['Value']
    protocolsToTry = []
    for protocol in protocols:
      if protocol in availableProtocols:
        protocolsToTry.append( protocol )
      else:
        errStr = "getPfnForProtocol: Requested protocol not available for SE."
        self.log.debug( errStr, '%s for %s' % ( protocol, self.name ) )
    if not protocolsToTry:
      errStr = "getPfnForProtocol: None of the requested protocols were available for SE."
      self.log.error( errStr, '%s for %s' % ( protocol, self.name ) )
      return S_ERROR( errStr )
    # Check all available storages for required protocol then contruct the PFN
    for storage in self.storages:
      res = storage.getParameters()
      if res['Value']['ProtocolName'] in protocolsToTry:
        res = pfnparse( pfn )
        if res['OK']:
          res = storage.getProtocolPfn( res['Value'], withPort )
          if res['OK']:
            return res
    errStr = "getPfnForProtocol: Failed to get PFN for requested protocols."
    self.log.error( errStr, "%s for %s" % ( protocols, self.name ) )
    return S_ERROR( errStr )

  def getPfnPath( self, pfn ):
    """  Get the part of the PFN path below the basic storage path.
         This path must coincide with the LFN of the file in order to be compliant with the LHCb conventions.
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    res = pfnparse( pfn )
    if not res['OK']:
      return res
    fullPfnPath = '%s/%s' % ( res['Value']['Path'], res['Value']['FileName'] )

    # Check all available storages and check whether the pfn is for that protocol
    pfnPath = ''
    for storage in self.storages:
      res = storage.isPfnForProtocol( pfn )
      if res['OK']:
        if res['Value']:
          res = storage.getParameters()
          saPath = res['Value']['Path']
          if not saPath:
            # If the sa path doesn't exist then the pfn path is the entire string
            pfnPath = fullPfnPath
          else:
            if re.search( saPath, fullPfnPath ):
              # Remove the sa path from the fullPfnPath
              pfnPath = fullPfnPath.replace( saPath, '' )
      if pfnPath:
        return S_OK( pfnPath )
    # This should never happen. DANGER!!
    errStr = "getPfnPath: Failed to get the pfn path for any of the protocols!!"
    self.log.error( errStr )
    return S_ERROR( errStr )

  def getPfnForLfn( self, lfn ):
    """ Get the full PFN constructed from the LFN.
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    for storage in self.storages:
      res = storage.getPFNBase()
      if res['OK']:
        fullPath = "%s%s" % ( res['Value'], lfn )
        return S_OK( fullPath )
    # This should never happen. DANGER!!
    errStr = "getPfnForLfn: Failed to get the full pfn for any of the protocols (%s)!!" % ( self.name )
    self.log.error( errStr )
    return S_ERROR( errStr )

  def getPFNBase( self ):
    """ Get the base to construct a PFN
    """
    if not self.storages:
      return S_ERROR( 'No storages defined' )
    for storage in self.storages:
      result = storage.getPFNBase()
      if result['OK']:
        return result

    return result

  ###########################################################################################
  #
  # This is the generic wrapper for file operations
  #

  def retransferOnlineFile( self, pfn, singleFile = False ):
    """ execcute 'retransferOnlineFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'retransferOnlineFile' )

  def exists( self, pfn, singleFile = False ):
    """ execute 'exists' operation  """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'exists' )


  def isFile( self, pfn, singleFile = False ):
    """ execute 'isFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'isFile' )

  def getFile( self, pfn, localPath = False, singleFile = False ):
    """ execute 'getFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'getFile', { 'localPath': localPath } )

  def putFile( self, pfn, sourceSize = 0, singleFile = False ):
    """ execute 'putFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'putFile', { 'sourceSize': sourceSize } )

  def replicateFile( self, pfn, sourceSize = 0, singleFile = False ):
    """ execute 'putFile' as replicate """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'putFile', { 'sourceSize': sourceSize } )

  def getFileMetadata( self, pfn, singleFile = False ):
    """ execute 'getFileMetadata' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'getFileMetadata' )

  def getFileSize( self, pfn, singleFile = False ):
    """ execute 'getFileSize' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'getFileSize' )

  def getAccessUrl( self, pfn, protocol = False, singleFile = False ):
    """ execute 'getTransportURL' operation """
    if not protocol:
      protocols = self.turlProtocols
    else:
      protocols = [protocol]
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'getTransportURL', {'protocols': protocols} )

  def removeFile( self, pfn, singleFile = False ):
    """ execute 'removeFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'removeFile' )

  def prestageFile( self, pfn, lifetime = 86400, singleFile = False ):
    """ execute 'prestageFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'prestageFile', { 'lifetime': lifetime } )

  def prestageFileStatus( self, pfn, singleFile = False ):
    """ execute 'prestageFileStatus' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'prestageFileStatus' )

  def pinFile( self, pfn, lifetime = 60 * 60 * 24, singleFile = False ):
    """ execute 'pinFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'pinFile', { 'lifetime': lifetime } )

  def releaseFile( self, pfn, singleFile = False ):
    """ execute 'releaseFile' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleFile )]( pfn, 'releaseFile' )

  def isDirectory( self, pfn, singleDirectory = False ):
    """ execute 'isDirectory' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'isDirectory' )

  def getDirectoryMetadata( self, pfn, singleDirectory = False ):
    """ execute 'getDirectoryMetadata' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'getDirectoryMetadata' )

  def getDirectorySize( self, pfn, singleDirectory = False ):
    """ execute 'getDirectorySize' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'getDirectorySize' )

  def listDirectory( self, pfn, singleDirectory = False ):
    """ execute 'listDirectory' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'listDirectory' )

  def removeDirectory( self, pfn, recursive = False, singleDirectory = False ):
    """ execute 'removeDirectory' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'removeDirectory', {'recursive':
                                                                                               recursive} )

  def createDirectory( self, pfn, singleDirectory = False ):
    """ execute 'createDirectory' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'createDirectory' )

  def putDirectory( self, pfn, singleDirectory = False ):
    """ execute 'putDirectory' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'putDirectory' )

  def getDirectory( self, pfn, localPath = False, singleDirectory = False ):
    """ execute 'getDirectory' operation """
    return { True : self.__executeSingleFile,
             False : self.__executeFunction }[bool( singleDirectory )]( pfn, 'getDirectory', { 'localPath':
                                                                                             localPath } )

  def __executeSingleFile( self, pfn, operation, arguments = None ):
    """ execute for single file """
    if arguments == None:
      res = self.__executeFunction( pfn, operation, {} )
    else:
      res = self.__executeFunction( pfn, operation, arguments )
    if type( pfn ) == ListType:
      pfn = pfn[0]
    elif type( pfn ) == DictType:
      pfn = pfn.keys()[0]
    if not res['OK']:
      return res
    elif pfn in res['Value']['Failed']:
      errorMessage = res['Value']['Failed'][pfn]
      return S_ERROR( errorMessage )
    else:
      return S_OK( res['Value']['Successful'][pfn] )

  def __executeFunction( self, pfn, method, argsDict = None ):
    """
        'pfn' is the physical file name (as registered in the LFC)
        'method' is the functionality to be executed
    """
    ## default args  = no args
    argsDict = argsDict if argsDict else {}
    if type( pfn ) in StringTypes:
      pfns = {pfn:False}
    elif type( pfn ) == ListType:
      pfns = {}
      for url in pfn:
        pfns[url] = False
    elif type( pfn ) == DictType:
      pfns = pfn.copy()
    else:
      errStr = "__executeFunction: Supplied pfns must be string or list of strings or a dictionary."
      self.log.error( errStr )
      return S_ERROR( errStr )

    if not pfns:
      self.log.verbose( "__executeFunction: No pfns supplied." )
      return S_OK( {'Failed':{}, 'Successful':{}} )
    self.log.verbose( "__executeFunction: Attempting to perform '%s' operation with %s pfns." % ( method,
                                                                                                  len( pfns ) ) )

    res = self.isValid( operation = method )
    if not res['OK']:
      return res
    else:
      if not self.valid:
        return S_ERROR( self.errorReason )

    successful = {}
    failed = {}
    localSE = self.isLocalSE()['Value']
    # Try all of the storages one by one
    for storage in self.storages:
      # Determine whether to use this storage object
      res = storage.getParameters()
      useProtocol = True
      if not res['OK']:
        self.log.error( "__executeFunction: Failed to get storage parameters.", "%s %s" % ( self.name,
                                                                                            res['Message'] ) )
        useProtocol = False
      else:
        protocolName = res['Value']['ProtocolName']
        if not pfns:
          useProtocol = False
          self.log.verbose( "__executeFunction: No pfns to be attempted for %s protocol." % protocolName )
        elif not ( protocolName in self.remoteProtocols ) and not localSE:
          # If the SE is not local then we can't use local protocols
          useProtocol = False
          self.log.verbose( "__executeFunction: Protocol not appropriate for use: %s." % protocolName )
      if useProtocol:
        self.log.verbose( "__executeFunction: Generating %s protocol PFNs for %s." % ( len( pfns ),
                                                                                       protocolName ) )
        res = self.__generatePfnDict( pfns, storage )
        pfnDict = res['Value']
        failed.update( res['Failed'] )
        if not len( pfnDict ) > 0:
          self.log.verbose( "__executeFunction No pfns generated for protocol %s." % protocolName )
        else:
          self.log.verbose( "__executeFunction: Attempting to perform '%s' for %s physical files" % ( method,
                                                                                                      len( pfnDict ) ) )
          fcn = None
          if hasattr( storage, method ) and callable( getattr( storage, method ) ):
            fcn = getattr( storage, method )
          if not fcn:
            return S_ERROR( "__executeFunction: unable to invoke %s, it isn't a member function of storage" )

          pfnsToUse = {}
          for pfn in pfnDict:
            pfnsToUse[pfn] = pfns[pfnDict[pfn]]

          res = fcn( pfnsToUse, **argsDict )

          if not res['OK']:
            errStr = "__executeFunction: Completely failed to perform %s." % method
            self.log.error( errStr, '%s for protocol %s: %s' % ( self.name, protocolName, res['Message'] ) )
            for pfn in pfnDict.values():
              if pfn not in failed:
                failed[pfn] = ''
              failed[pfn] = "%s %s" % ( failed[pfn], res['Message'] )
          else:
            for protocolPfn, pfn in pfnDict.items():
              if protocolPfn not in res['Value']['Successful']:
                if pfn not in failed:
                  failed[pfn] = ''
                if protocolPfn in res['Value']['Failed']:
                  failed[pfn] = "%s %s" % ( failed[pfn], res['Value']['Failed'][protocolPfn] )
                else:
                  failed[pfn] = "%s %s" % ( failed[pfn], 'No error returned from plug-in' )
              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if pfn in failed:
                  failed.pop( pfn )
                pfns.pop( pfn )

    return S_OK( { 'Failed': failed, 'Successful': successful } )

  def __generatePfnDict( self, pfns, storage ):
    """ whatever, it creates PFN dict  """
    pfnDict = {}
    failed = {}
    for pfn in pfns:
      res = pfnparse( pfn )
      if not res['OK']:
        errStr = "__generatePfnDict: Failed to parse supplied PFN."
        self.log.error( errStr, "%s: %s" % ( pfn, res['Message'] ) )
        if pfn not in failed:
          failed[pfn] = ''
        failed[pfn] = "%s %s" % ( failed[pfn], errStr )
      else:
        res = storage.getProtocolPfn( res['Value'], True )
        if not res['OK']:
          errStr = "__generatePfnDict %s." % res['Message']
          self.log.error( errStr, 'for %s' % ( pfn ) )
          if pfn not in failed:
            failed[pfn] = ''
          failed[pfn] = "%s %s" % ( failed[pfn], errStr )
        else:
          pfnDict[res['Value']] = pfn
    res = S_OK( pfnDict )
    res['Failed'] = failed
    return res
