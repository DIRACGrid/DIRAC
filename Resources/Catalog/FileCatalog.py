########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
""" File catalog class. This is a simple dispatcher for the file catalogue plug-ins.
    It ensures that all operations are performed on the desired catalogs.
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR, rootPath
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.ConfigurationSystem.Client.Helpers import getInstalledExtensions
import types, re, os

class FileCatalog:

  ro_methods = ['exists', 'isLink', 'readLink', 'isFile', 'getFileMetadata', 'getReplicas',
                'getReplicaStatus', 'getFileSize', 'isDirectory', 'getDirectoryReplicas',
                'listDirectory', 'getDirectoryMetadata', 'getDirectorySize', 'getDirectoryContents',
                'resolveDataset', 'getPathPermissions', 'getLFNForPFN']

  write_methods = ['createLink', 'removeLink', 'addFile', 'setFileStatus', 'addReplica', 'removeReplica',
                   'removeFile', 'setReplicaStatus', 'setReplicaHost', 'createDirectory', 'setDirectoryStatus',
                   'removeDirectory', 'removeDataset', 'removeFileFromDataset', 'createDataset']

  def __init__( self, catalogs = [] ):
    """ Default constructor
    """
    self.valid = True
    self.timeout = 180
    self.readCatalogs = []
    self.writeCatalogs = []
    self.rootConfigPath = '/Resources/FileCatalogs'

    if type( catalogs ) in types.StringTypes:
      catalogs = [catalogs]
    if catalogs:
      res = self._getSelectedCatalogs( catalogs )
    else:
      res = self._getCatalogs()
    if not res['OK']:
      self.valid = False
    elif ( len( self.readCatalogs ) == 0 ) and ( len( self.writeCatalogs ) == 0 ):
      self.valid = False

  def isOK( self ):
    return self.valid

  def getReadCatalogs( self ):
    return self.readCatalogs

  def getWriteCatalogs( self ):
    return self.writeCatalogs

  def __getattr__( self, name ):
    self.call = name
    if name in FileCatalog.write_methods:
      return self.w_execute
    elif name in FileCatalog.ro_methods:
      return self.r_execute
    else:
      raise AttributeError

  def __checkArgumentFormat( self, path ):
    if type( path ) in types.StringTypes:
      urls = {path:False}
    elif type( path ) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type( path ) == types.DictType:
      urls = path
    else:
      return S_ERROR( "FileCatalog.__checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )

  def w_execute( self, *parms, **kws ):
    """ Write method executor.
    """
    successful = {}
    failed = {}
    failedCatalogs = []
    fileInfo = parms[0]
    res = self.__checkArgumentFormat( fileInfo )
    if not res['OK']:
      return res
    fileInfo = res['Value']
    allLfns = fileInfo.keys()
    for catalogName, oCatalog, master in self.writeCatalogs:
      method = getattr( oCatalog, self.call )
      res = method( fileInfo, **kws )
      if not res['OK']:
        if master:
          # If this is the master catalog and it fails we dont want to continue with the other catalogs
          gLogger.error( "FileCatalog.w_execute: Failed to execute %s on master catalog %s." % ( self.call, catalogName ), res['Message'] )
          return res
        else:
          # Otherwise we keep the failed catalogs so we can update their state later
          failedCatalogs.append( ( catalogName, res['Message'] ) )
      else:
        for lfn, message in res['Value']['Failed'].items():
          # Save the error message for the failed operations
          if not failed.has_key( lfn ):
            failed[lfn] = {}
          failed[lfn][catalogName] = message
          if master:
            # If this is the master catalog then we should not attempt the operation on other catalogs
            fileInfo.pop( lfn )
        for lfn, result in res['Value']['Successful'].items():
          # Save the result return for each file for the successful operations
          if not successful.has_key( lfn ):
            successful[lfn] = {}
          successful[lfn][catalogName] = result
    # This recovers the states of the files that completely failed i.e. when S_ERROR is returned by a catalog
    for catalogName, errorMessage in failedCatalogs:
      for file in allLfns:
        if not failed.has_key( file ):
          failed[file] = {}
        failed[file][catalogName] = errorMessage
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def r_execute( self, *parms, **kws ):
    """ Read method executor.
    """
    successful = {}
    failed = {}
    for catalogName, oCatalog, master in self.readCatalogs:
      method = getattr( oCatalog, self.call )
      res = method( *parms, **kws )
      if res['OK']:
        for key, item in res['Value']['Successful'].items():
          if not successful.has_key( key ):
            successful[key] = item
            if failed.has_key( key ):
              failed.pop( key )
        for key, item in res['Value']['Failed'].items():
          if not successful.has_key( key ):
            failed[key] = item
        if len( failed ) == 0:
          resDict = {'Failed':failed, 'Successful':successful}
          return S_OK( resDict )
    if ( len( successful ) == 0 ) and ( len( failed ) == 0 ):
      return S_ERROR( 'Failed to perform %s from any catalog' % self.call )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ###########################################################################################
  #
  # Below is the method for obtaining the objects instantiated for a provided catalogue configuration
  #

  def addCatalog( self, catalogName, mode = "Write", master = False ):
    """ Add a new catalog with catalogName to the pool of catalogs in mode:
        "Read","Write" or "ReadWrite"
    """

    result = self._generateCatalogObject( catalogName )
    if not result['OK']:
      return result

    oCatalog = result['Value']
    if mode.lower().find( "read" ) != -1:
      self.readCatalogs.append( ( catalogName, oCatalog, master ) )
    if mode.lower().find( "write" ) != -1:
      self.writeCatalogs.append( ( catalogName, oCatalog, master ) )

    return S_OK()

  def removeCatalog( self, catalogName ):
    """ Remove the specified catalog from the internal pool
    """

    catalog_removed = False

    for i in range( len( self.readCatalogs ) ):
      catalog, object, master = self.readCatalogs[i]
      if catalog == catalogName:
        del self.readCatalogs[i]
        catalog_removed = True
        break
    for i in range( len( self.writeCatalogs ) ):
      catalog, object, master = self.writeCatalogs[i]
      if catalog == catalogName:
        del self.writeCatalogs[i]
        catalog_removed = True
        break

    if catalog_removed:
      return S_OK()
    else:
      return S_OK( 'Catalog does not exist' )

  def _getSelectedCatalogs( self, desiredCatalogs ):
    for catalogName in desiredCatalogs:
      res = self._generateCatalogObject( catalogName )
      if not res['OK']:
        return res
      oCatalog = res['Value']
      self.readCatalogs.append( ( catalogName, oCatalog, True ) )
      self.writeCatalogs.append( ( catalogName, oCatalog, True ) )
    return S_OK()

  def _getCatalogs( self ):
    res = gConfig.getSections( self.rootConfigPath, listOrdered = True )
    if not res['OK']:
      errStr = "FileCatalog._getCatalogs: Failed to get file catalog configuration."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    fileCatalogs = res['Value']
    for catalogName in fileCatalogs:
      res = self._getCatalogConfigDetails( catalogName )
      if not res['OK']:
        return res
      catalogConfig = res['Value']
      if catalogConfig['Status'] == 'Active':
        res = self._generateCatalogObject( catalogName )
        if not res['OK']:
          return res
        oCatalog = res['Value']
        master = catalogConfig['Master']
        # If the catalog is read type
        if re.search( 'Read', catalogConfig['AccessType'] ):
          if master:
            self.readCatalogs.insert( 0, ( catalogName, oCatalog, master ) )
          else:
            self.readCatalogs.append( ( catalogName, oCatalog, master ) )
        # If the catalog is write type
        if re.search( 'Write', catalogConfig['AccessType'] ):
          if master:
            self.writeCatalogs.insert( 0, ( catalogName, oCatalog, master ) )
          else:
            self.writeCatalogs.append( ( catalogName, oCatalog, master ) )
    return S_OK()

  def _getCatalogConfigDetails( self, catalogName ):
    # First obtain the options that are available
    catalogConfigPath = '%s/%s' % ( self.rootConfigPath, catalogName )
    res = gConfig.getOptions( catalogConfigPath )
    if not res['OK']:
      errStr = "FileCatalog._getCatalogConfigDetails: Failed to get catalog options."
      gLogger.error( errStr, catalogName )
      return S_ERROR( errStr )
    catalogConfig = {}
    for option in res['Value']:
      configPath = '%s/%s' % ( catalogConfigPath, option )
      optionValue = gConfig.getValue( configPath )
      catalogConfig[option] = optionValue
    # The 'Status' option should be defined (default = 'Active')
    if not catalogConfig.has_key( 'Status' ):
      warnStr = "FileCatalog._getCatalogConfigDetails: 'Status' option not defined."
      gLogger.warn( warnStr, catalogName )
      catalogConfig['Status'] = 'Active'
    # The 'AccessType' option must be defined
    if not catalogConfig.has_key( 'AccessType' ):
      errStr = "FileCatalog._getCatalogConfigDetails: Required option 'AccessType' not defined."
      gLogger.error( errStr, catalogName )
      return S_ERROR( errStr )
    # Anything other than 'True' in the 'Master' option means it is not
    if not catalogConfig.has_key( 'Master' ):
      catalogConfig['Master'] = False
    elif catalogConfig['Master'] == 'True':
      catalogConfig['Master'] = True
    else:
      catalogConfig['Master'] = False
    return S_OK( catalogConfig )

  def _generateCatalogObject( self, catalogName ):
    moduleRootPaths = getInstalledExtensions()
    moduleLoaded = False
    for moduleRootPath in moduleRootPaths:
      if moduleLoaded:
        break
      gLogger.verbose( "Trying to load from root path %s" % moduleRootPath )
      moduleFile = os.path.join( rootPath, moduleRootPath, "Resources", "Catalog", "%sClient.py" % catalogName )
      gLogger.verbose( "Looking for file %s" % moduleFile )
      if not os.path.isfile( moduleFile ):
        continue
      try:
        # This inforces the convention that the plug in must be named after the file catalog
        moduleName = "%sClient" % ( catalogName )
        catalogModule = __import__( '%s.Resources.Catalog.%s' % ( moduleRootPath, moduleName ),
                                    globals(), locals(), [moduleName] )
      except Exception, x:
        errStr = "FileCatalog._generateCatalogObject: Failed to import %s: %s" % ( catalogName, x )
        gLogger.exception( errStr )
        return S_ERROR( errStr )
      try:
        evalString = "catalogModule.%s()" % moduleName
        catalog = eval( evalString )
        if not catalog.isOK():
          errStr = "FileCatalog._generateCatalogObject: Failed to instantiate catalog plug in."
          gLogger.error( errStr, moduleName )
          return S_ERROR( errStr )
        return S_OK( catalog )
      except Exception, x:
        errStr = "FileCatalog._generateCatalogObject: Failed to instantiate %s()" % ( moduleName )
        gLogger.exception( errStr, lException = x )
        return S_ERROR( errStr )

    if not moduleLoaded:
      return S_ERROR( 'Failed to find catalog client %s' % catalogName )
