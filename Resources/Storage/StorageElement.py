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


from DIRAC                                              import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Resources.Storage.StorageFactory             import StorageFactory
from DIRAC.Core.Utilities.Pfn                           import pfnparse
from DIRAC.Core.Utilities.List                          import sortList
from DIRAC.Core.Utilities.SiteSEMapping                 import getSEsForSite
import re, types

class StorageElement:

  def __init__( self, name, protocols = None, overwride = False ):
    self.overwride = overwride
    self.valid = True
    if protocols == None:
      res = StorageFactory().getStorages( name, protocolList = [] )
    else:
      res = StorageFactory().getStorages( name, protocolList = protocols )
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

    self.readMethods = [   'getFile',
                           'getAccessUrl',
                           'getTransportURL',
                           'prestageFile',
                           'prestageFileStatus',
                           'getDirectory']
    self.writeMethods = [  'retransferOnlineFile',
                           'putFile',
                           'replicateFile',
                           'pinFile',
                           'releaseFile',
                           'createDirectory',
                           'putDirectory']

    self.removeMethods = [
                           'removeFile',
                           'removeDirectory',
                          ]
    self.checkMethods = [
                         'exists',
                         'getDirectoryMetadata',
                         'getDirectorySize',
                         'getFileSize',
                         'getFileMetadata',
                         'getLocalProtocols',
                         'getPfnForProtocol',
                         'getPfnForLfn',
                         'getPfnPath',
                         'getProtocols',
                         'getRemoteProtocols',
                         'getStorageElementName',
                         'getStorageElementOption',
                         'getStorageParameters',
                         'listDirectory',
                         'isDirectory',
                         'isFile',
                         'isLocalSE'
                         ]

  def dump( self ):
    """
      Dump to the logger a summary of the StorageElement items
    """
    gLogger.info( "StorageElement.dump: Preparing dump for StorageElement %s." % self.name )
    if not self.valid:
      gLogger.error( "StorageElement.dump: Failed to create StorageElement plugins.", self.errorReason )
      return
    i = 1
    outStr = "\n\n============ Options ============\n"
    for key in sortList( self.options.keys() ):
      outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), self.options[key] )

    for storage in self.storages:
      outStr = "%s============Protocol %s ============\n" % ( outStr, i )
      res = storage.getParameters()
      storageParameters = res['Value']
      for key in sortList( storageParameters.keys() ):
        outStr = "%s%s: %s\n" % ( outStr, key.ljust( 15 ), storageParameters[key] )
      i = i + 1
    gLogger.info( outStr )

  #################################################################################################
  #
  # These are the basic get functions for storage configuration
  #

  def getStorageElementName( self ):
    gLogger.verbose( "StorageElement.getStorageElementName: The Storage Element name is %s." % self.name )
    return S_OK( self.name )

  def getChecksumType( self ):
    """ get local /Resources/StorageElements/SEName/ChecksumType option if defined, otherwise 
        global /Resources/StorageElements/ChecksumType
    """
    return S_OK( str(gConfig.getValue( "/Resources/StorageElements/ChecksumType", "ADLER32" )).upper() 
                 if "ChecksumType" not in self.options else str(self.options["ChecksumType"]).upper() )
     
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
    retDict['Read'] = not ( self.options.has_key( 'Read' ) and self.options['Read'] not in ( 'Active', 'Degraded' ) )
    #retDict['Read'] = not ( self.options.has_key( 'ReadAccess' ) and self.options['ReadAccess'] != 'Active' )
    retDict['Write'] = not ( self.options.has_key( 'Write' ) and self.options['Write'] not in ( 'Active', 'Degraded' ) )
    #retDict['Write'] = not ( self.options.has_key( 'WriteAccess' ) and self.options['WriteAccess'] != 'Active' )
    retDict['Remove'] = not ( self.options.has_key( 'Remove' ) and self.options['Remove'] not in ( 'Active', 'Degraded' ) )
    #retDict['Remove'] = not ( self.options.has_key( 'RemoveAccess' ) and self.options['RemoveAccess'] != 'Active' )
    if retDict['Read']:
      retDict['Check'] = True
    else:
      #retDict['Check'] = not ( self.options.has_key( 'CheckAccess' ) and self.options['CheckAccess'] != 'Active' )
      retDict['Check'] = not ( self.options.has_key( 'Check' ) and self.options['Check'] not in ( 'Active', 'Degraded' ) )
    diskSE = True
    tapeSE = False
    if self.options.has_key( 'SEType' ):
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
    gLogger.debug( "StorageElement.isValid: Determining whether the StorageElement %s is valid for %s" % ( self.name, operation ) )
    if self.overwride:
      return S_OK()
    gLogger.verbose( "StorageElement.isValid: Determining whether the StorageElement %s is valid for use." % self.name )
    if not self.valid:
      gLogger.error( "StorageElement.isValid: Failed to create StorageElement plugins.", self.errorReason )
      return S_ERROR( self.errorReason )
    # Determine whether the StorageElement is valid for checking, reading, writing
    res = self.getStatus()
    if not res[ 'OK' ]:
      gLogger.error( "Could not call getStatus" )
      return S_ERROR( "StorageElement.isValid could not call the getStatus method" )
    checking = res[ 'Value' ][ 'Check' ]
    reading = res[ 'Value' ][ 'Read' ]
    writing = res[ 'Value' ][ 'Write' ]
    removing = res[ 'Value' ][ 'Remove' ]

    # Determine whether the requested operation can be fulfilled    
    if ( not operation ) and ( not reading ) and ( not writing ) and ( not checking ):
      gLogger.error( "StorageElement.isValid: Read, write and check access not permitted." )
      return S_ERROR( "StorageElement.isValid: Read, write and check access not permitted." )
    if not operation:
      gLogger.warn( "StorageElement.isValid: The 'operation' argument is not supplied. It should be supplied in the future." )
      return S_OK()
    # The supplied operation can be 'Read','Write' or any of the possible StorageElement methods.
    if ( operation in self.readMethods ) or ( operation.lower() == 'read' ):
      operation = 'Read'
    elif operation in self.writeMethods or ( operation.lower() == 'write' ):
      operation = 'Write'
    elif operation in self.removeMethods or ( operation.lower() == 'remove' ):
      operation = 'Remove'
    elif operation in self.checkMethods or ( operation.lower() == 'check' ):
      operation = 'Check'
    else:
      gLogger.error( "StorageElement.isValid: The supplied operation is not known.", operation )
      return S_ERROR( "StorageElement.isValid: The supplied operation is not known." )
    gLogger.debug( "in isValid check the operation: %s " % operation )
    # Check if the operation is valid
    if operation == 'Check':
      if not reading:
        if not checking:
          gLogger.error( "StorageElement.isValid: Check access not currently permitted." )
          return S_ERROR( "StorageElement.isValid: Check access not currently permitted." )
    if operation == 'Read':
      if not reading:
        gLogger.error( "StorageElement.isValid: Read access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Read access not currently permitted." )
    if operation == 'Write':
      if not writing:
        gLogger.error( "StorageElement.isValid: Write access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Write access not currently permitted." )
    if operation == 'Remove':
      if not removing:
        gLogger.error( "StorageElement.isValid: Remove access not currently permitted." )
        return S_ERROR( "StorageElement.isValid: Remove access not currently permitted." )
    return S_OK()

  def getProtocols( self ):
    """ Get the list of all the protocols defined for this Storage Element
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    gLogger.verbose( "StorageElement.getProtocols: Obtaining all protocols for %s." % self.name )
    allProtocols = self.localProtocols + self.remoteProtocols
    return S_OK( allProtocols )

  def getRemoteProtocols( self ):
    """ Get the list of all the remote access protocols defined for this Storage Element
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    gLogger.verbose( "StorageElement.getRemoteProtocols: Obtaining remote protocols for %s." % self.name )
    return S_OK( self.remoteProtocols )

  def getLocalProtocols( self ):
    """ Get the list of all the local access protocols defined for this Storage Element
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    gLogger.verbose( "StorageElement.getLocalProtocols: Obtaining local protocols for %s." % self.name )
    return S_OK( self.localProtocols )

  def getStorageElementOption( self, option ):
    """ Get the value for the option supplied from self.options
    """
    if not self.valid:
      return S_ERROR( self.errorReason )
    gLogger.verbose( "StorageElement.getStorageElementOption: Obtaining %s option for Storage Element %s." % ( option, self.name ) )
    if self.options.has_key( option ):
      optionValue = self.options[option]
      return S_OK( optionValue )
    else:
      errStr = "StorageElement.getStorageElementOption: Option not defined for SE."
      gLogger.error( errStr, "%s for %s" % ( option, self.name ) )
      return S_ERROR( errStr )

  def getStorageParameters( self, protocol ):
    """ Get protocol specific options
    """
    gLogger.verbose( "StorageElement.getStorageParameters: Obtaining storage parameters for %s protocol %s." % ( self.name, protocol ) )
    res = self.getProtocols()
    if not res['OK']:
      return res
    availableProtocols = res['Value']
    if not protocol in availableProtocols:
      errStr = "StorageElement.getStorageParameters: Requested protocol not available for SE."
      gLogger.warn( errStr, '%s for %s' % ( protocol, self.name ) )
      return S_ERROR( errStr )
    for storage in self.storages:
      res = storage.getParameters()
      storageParameters = res['Value']
      if storageParameters['ProtocolName'] == protocol:
        return S_OK( storageParameters )
    errStr = "StorageElement.getStorageParameters: Requested protocol supported but no object found."
    gLogger.error( errStr, "%s for %s" % ( protocol, self.name ) )
    return S_ERROR( errStr )

  def isLocalSE( self ):
    """ Test if the Storage Element is local in the current context
    """
    import DIRAC
    gLogger.verbose( "StorageElement.isLocalSE: Determining whether %s is a local SE." % self.name )
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
    if type( protocol ) == types.StringType:
      protocols = [protocol]
    elif type( protocol ) == types.ListType:
      protocols = protocol
    else:
      errStr = "StorageElement.getPfnForProtocol: Supplied protocol must be string or list of strings."
      gLogger.error( errStr, "%s %s" % ( protocol, self.name ) )
      return S_ERROR( errStr )
    availableProtocols = res['Value']
    protocolsToTry = []
    for protocol in protocols:
      if protocol in availableProtocols:
        protocolsToTry.append( protocol )
      else:
        errStr = "StorageElement.getPfnForProtocol: Requested protocol not available for SE."
        gLogger.warn( errStr, '%s for %s' % ( protocol, self.name ) )
    if not protocolsToTry:
      errStr = "StorageElement.getPfnForProtocol: None of the requested protocols were available for SE."
      gLogger.error( errStr, '%s for %s' % ( protocol, self.name ) )
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
    errStr = "StorageElement.getPfnForProtocol: Failed to get PFN for requested protocols."
    gLogger.error( errStr, "%s for %s" % ( protocols, self.name ) )
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
    errStr = "StorageElement.getPfnPath: Failed to get the pfn path for any of the protocols!!"
    gLogger.error( errStr )
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
    errStr = "StorageElement.getPfnForLfn: Failed to get the full pfn for any of the protocols!!"
    gLogger.error( errStr )
    return S_ERROR( errStr )

  ###########################################################################################
  #
  # This is the generic wrapper for file operations
  #

  def retransferOnlineFile( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'retransferOnlineFile' )
    else:
      return self.__executeFunction( pfn, 'retransferOnlineFile' )

  def exists( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'exists' )
    else:
      return self.__executeFunction( pfn, 'exists' )

  def isFile( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'isFile' )
    else:
      return self.__executeFunction( pfn, 'isFile' )

  def getFile( self, pfn, localPath = False, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'getFile', {'localPath':localPath} )
    else:
      return self.__executeFunction( pfn, 'getFile', {'localPath':localPath} )

  def putFile( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'putFile' )
    else:
      return self.__executeFunction( pfn, 'putFile' )

  def replicateFile( self, pfn, sourceSize = 0, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'putFile', {'sourceSize':sourceSize} )
    else:
      return self.__executeFunction( pfn, 'putFile', {'sourceSize':sourceSize} )

  def getFileMetadata( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'getFileMetadata' )
    else:
      return self.__executeFunction( pfn, 'getFileMetadata' )

  def getFileSize( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'getFileSize' )
    else:
      return self.__executeFunction( pfn, 'getFileSize' )

  def getAccessUrl( self, pfn, protocol = False, singleFile = False ):
    if not protocol:
      protocols = self.turlProtocols
    else:
      protocols = [protocol]
    if singleFile:
      return self.__executeSingleFile( pfn, 'getTransportURL', {'protocols':protocols} )
    else:
      return self.__executeFunction( pfn, 'getTransportURL', {'protocols':protocols} )

  def removeFile( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'removeFile' )
    else:
      return self.__executeFunction( pfn, 'removeFile' )

  def prestageFile( self, pfn, lifetime = 60 * 60 * 24, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'prestageFile' )
    else:
      return self.__executeFunction( pfn, 'prestageFile' )

  def prestageFileStatus( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'prestageFileStatus' )
    else:
      return self.__executeFunction( pfn, 'prestageFileStatus' )

  def pinFile( self, pfn, lifetime = 60 * 60 * 24, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'pinFile', {'lifetime':lifetime} )
    else:
      return self.__executeFunction( pfn, 'pinFile', {'lifetime':lifetime} )

  def releaseFile( self, pfn, singleFile = False ):
    if singleFile:
      return self.__executeSingleFile( pfn, 'releaseFile' )
    else:
      return self.__executeFunction( pfn, 'releaseFile' )

  def isDirectory( self, pfn, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'isDirectory' )
    else:
      return self.__executeFunction( pfn, 'isDirectory' )

  def getDirectoryMetadata( self, pfn, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'getDirectoryMetadata' )
    else:
      return self.__executeFunction( pfn, 'getDirectoryMetadata' )

  def getDirectorySize( self, pfn, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'getDirectorySize' )
    else:
      return self.__executeFunction( pfn, 'getDirectorySize' )

  def listDirectory( self, pfn, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'listDirectory' )
    else:
      return self.__executeFunction( pfn, 'listDirectory' )

  def removeDirectory( self, pfn, recursive = False, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'removeDirectory', {'recursive':recursive} )
    else:
      return self.__executeFunction( pfn, 'removeDirectory', {'recursive':recursive} )

  def createDirectory( self, pfn, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'createDirectory' )
    else:
      return self.__executeFunction( pfn, 'createDirectory' )

  def putDirectory( self, pfn, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'putDirectory' )
    else:
      return self.__executeFunction( pfn, 'putDirectory' )

  def getDirectory( self, pfn, localPath = False, singleDirectory = False ):
    if singleDirectory:
      return self.__executeSingleFile( pfn, 'getDirectory', {'localPath':localPath} )
    else:
      return self.__executeFunction( pfn, 'getDirectory', {'localPath':localPath} )

  def __executeSingleFile( self, pfn, operation, arguments = None ):
    if arguments == None:
      res = self.__executeFunction( pfn, operation, {} )
    else:
      res = self.__executeFunction( pfn, operation, arguments )
    if type( pfn ) == types.ListType:
      pfn = pfn[0]
    elif type( pfn ) == types.DictType:
      pfn = pfn.keys()[0]
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key( pfn ):
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
    if type( pfn ) in types.StringTypes:
      pfns = {pfn:False}
    elif type( pfn ) == types.ListType:
      pfns = {}
      for url in pfn:
        pfns[url] = False
    elif type( pfn ) == types.DictType:
      pfns = pfn.copy()
    else:
      errStr = "StorageElement.__executeFunction: Supplied pfns must be string or list of strings or a dictionary."
      gLogger.error( errStr )
      return S_ERROR( errStr )

    if not pfns:
      gLogger.verbose( "StorageElement.__executeFunction: No pfns supplied." )
      return S_OK( {'Failed':{}, 'Successful':{}} )
    gLogger.verbose( "StorageElement.__executeFunction: Attempting to perform '%s' operation with %s pfns." % ( method, len( pfns ) ) )

    if not self.overwride:
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
        gLogger.error( "StorageElement.__executeFunction: Failed to get storage parameters.", "%s %s" % ( self.name, res['Message'] ) )
        useProtocol = False
      else:
        protocolName = res['Value']['ProtocolName']
        if not pfns:
          useProtocol = False
          gLogger.verbose( "StorageElement.__executeFunction: No pfns to be attempted for %s protocol." % protocolName )
        elif not ( protocolName in self.remoteProtocols ) and not localSE:
          # If the SE is not local then we can't use local protocols
          useProtocol = False
          gLogger.verbose( "StorageElement.__executeFunction: Protocol not appropriate for use: %s." % protocolName )
      if useProtocol:
        gLogger.verbose( "StorageElement.__executeFunction: Generating %s protocol PFNs for %s." % ( len( pfns ), protocolName ) )
        res = self.__generatePfnDict( pfns.keys(), storage, failed )
        pfnDict = res['Value']
        failed = res['Failed']
        if not len( pfnDict.keys() ) > 0:
          gLogger.verbose( "StorageElement.__executeFunction No pfns generated for protocol %s." % protocolName )
        else:
          gLogger.verbose( "StorageElement.__executeFunction: Attempting to perform '%s' for %s physical files." % ( method, len( pfnDict.keys() ) ) )
          fcn = None
          if hasattr( storage, method ) and callable( getattr( storage, method ) ):
            fcn = getattr( storage, method )
          if not fcn:
            return S_ERROR( "StorageElement.__executeFunction: unable to invoke %s, it isn't a member function of storage" )

          pfnsToUse = {}
          for pfn in pfnDict.keys():
            pfnsToUse[pfn] = pfns[pfnDict[pfn]]

          res = fcn( pfnsToUse, **argsDict )

          if not res['OK']:
            errStr = "StorageElement.__executeFunction: Completely failed to perform %s." % method
            gLogger.error( errStr, '%s for protocol %s: %s' % ( self.name, protocolName, res['Message'] ) )
            for pfn in pfnDict.values():
              if not failed.has_key( pfn ):
                failed[pfn] = ''
              failed[pfn] = "%s %s" % ( failed[pfn], res['Message'] )
          else:
            for protocolPfn, pfn in pfnDict.items():
              if not res['Value']['Successful'].has_key( protocolPfn ):
                if not failed.has_key( pfn ):
                  failed[pfn] = ''
                if res['Value']['Failed'].has_key( protocolPfn ):
                  failed[pfn] = "%s %s" % ( failed[pfn], res['Value']['Failed'][protocolPfn] )
                else:
                  failed[pfn] = "%s %s" % ( failed[pfn], 'No error returned from plug-in' )
              else:
                successful[pfn] = res['Value']['Successful'][protocolPfn]
                if failed.has_key( pfn ):
                  failed.pop( pfn )
                pfns.pop( pfn )

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __generatePfnDict( self, pfns, storage, failed ):
    pfnDict = {}
    for pfn in pfns:
      res = pfnparse( pfn )
      if not res['OK']:
        errStr = "StorageElement.__generatePfnDict: Failed to parse supplied PFN."
        gLogger.error( errStr, "%s: %s" % ( pfn, res['Message'] ) )
        if not failed.has_key( pfn ):
          failed[pfn] = ''
        failed[pfn] = "%s %s" % ( failed[pfn], errStr )
      else:
        res = storage.getProtocolPfn( res['Value'], True )
        if not res['OK']:
          errStr = "StorageElement.__generatePfnDict %s." % res['Message']
          gLogger.error( errStr, 'for %s' % ( pfn ) )
          if not failed.has_key( pfn ):
            failed[pfn] = ''
          failed[pfn] = "%s %s" % ( failed[pfn], errStr )
        else:
          pfnDict[res['Value']] = pfn
    res = S_OK( pfnDict )
    res['Failed'] = failed
    return res
