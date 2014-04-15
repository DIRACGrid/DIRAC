""" File catalog class. This is a simple dispatcher for the file catalog plug-ins.
    It ensures that all operations are performed on the desired catalogs.
"""

import types, re

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.Core.Security.ProxyInfo                          import getVOfromProxyGroup
from DIRAC.Resources.Utilities.Utils                        import checkArgumentFormat
from DIRAC.Resources.Catalog.FileCatalogFactory             import FileCatalogFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources     import Resources

class FileCatalog( object ):

  ro_methods = ['exists', 'isLink', 'readLink', 'isFile', 'getFileMetadata', 'getReplicas',
                'getReplicaStatus', 'getFileSize', 'isDirectory', 'getDirectoryReplicas',
                'listDirectory', 'getDirectoryMetadata', 'getDirectorySize', 'getDirectoryContents',
                'resolveDataset', 'getPathPermissions', 'getLFNForPFN', 'getUsers', 'getGroups', 'getFileUserMetadata']

  write_methods = ['createLink', 'removeLink', 'addFile', 'setFileStatus', 'addReplica', 'removeReplica',
                   'removeFile', 'setReplicaStatus', 'setReplicaHost', 'createDirectory', 'setDirectoryStatus',
                   'removeDirectory', 'removeDataset', 'removeFileFromDataset', 'createDataset']

  def __init__( self, catalogs = [], vo = None ):
    """ Default constructor
    """
    self.valid = True
    self.timeout = 180
    self.readCatalogs = []
    self.writeCatalogs = []
    self.vo = vo if vo else getVOfromProxyGroup().get( 'Value', None )
    if self.vo:
      self.opHelper = Operations( vo = self.vo )
      self.reHelper = Resources( vo = self.vo ) 
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
    else:
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

  def w_execute( self, *parms, **kws ):
    """ Write method executor.
    """
    successful = {}
    failed = {}
    failedCatalogs = []
    fileInfo = parms[0]
    res = checkArgumentFormat( fileInfo )
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
          failed.setdefault( lfn, {} )[catalogName] = message
          if master:
            # If this is the master catalog then we should not attempt the operation on other catalogs
            fileInfo.pop( lfn )
        for lfn, result in res['Value']['Successful'].items():
          # Save the result return for each file for the successful operations
          successful.setdefault( lfn, {} )[catalogName] = result
    # This recovers the states of the files that completely failed i.e. when S_ERROR is returned by a catalog
    for catalogName, errorMessage in failedCatalogs:
      for lfn in allLfns:
        failed.setdefault( lfn, {} )[catalogName] = errorMessage
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def r_execute( self, *parms, **kws ):
    """ Read method executor.
    """
    successful = {}
    failed = {}
    for _catalogName, oCatalog, _master in self.readCatalogs:
      method = getattr( oCatalog, self.call )
      res = method( *parms, **kws )
      if res['OK']:
        if 'Successful' in res['Value']:
          for key, item in res['Value']['Successful'].items():
            successful.setdefault( key, item )
            failed.pop( key, None )
          for key, item in res['Value']['Failed'].items():
            if key not in successful:
              failed[key] = item
        else:
          return res
    if not successful and not failed:
      return S_ERROR( "Failed to perform %s from any catalog" % self.call )
    return S_OK( {'Failed':failed, 'Successful':successful} )

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
      catalog, _object, _master = self.readCatalogs[i]
      if catalog == catalogName:
        del self.readCatalogs[i]
        catalog_removed = True
        break
    for i in range( len( self.writeCatalogs ) ):
      catalog, _object, _master = self.writeCatalogs[i]
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

    # Get the eligible catalogs first
    # First, look in the Operations, if nothing defined look in /Resources 
    result = self.opHelper.getSections( '/Services/Catalogs' )
    fileCatalogs = []
    operationsFlag = False
    optCatalogDict = {}
    if result['OK']:
      fcs = result['Value']
      for fc in fcs:
        fName = self.opHelper.getValue( '/Services/Catalogs/%s/CatalogName' % fc, fc )
        fileCatalogs.append( fName )
        optCatalogDict[fName] = fc
      operationsFlag = True
    else:   
      res = self.reHelper.getEligibleResources( 'Catalog' )
      if not res['OK']:
        errStr = "FileCatalog._getCatalogs: Failed to get file catalog configuration."
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      fileCatalogs = res['Value']

    # Get the catalogs now
    for catalogName in fileCatalogs:
      res = self._getCatalogConfigDetails( catalogName )
      if not res['OK']:
        return res
      catalogConfig = res['Value']
      if operationsFlag:
        result = self.opHelper.getOptionsDict( '/Services/Catalogs/%s' % optCatalogDict[catalogName] )
        if not result['OK']:
          return result
        catalogConfig.update( result['Value'] )
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
    
    result = self.reHelper.getCatalogOptionsDict( catalogName )
    if not result['OK']:
      errStr = "FileCatalog._getCatalogConfigDetails: Failed to get catalog options"
      gLogger.error( errStr, catalogName )
      return S_ERROR( errStr )
    catalogConfig = result['Value']
    # The 'Status' option should be defined (default = 'Active')
    if 'Status' not in catalogConfig:
      warnStr = "FileCatalog._getCatalogConfigDetails: 'Status' option not defined."
      gLogger.warn( warnStr, catalogName )
      catalogConfig['Status'] = 'Active'
    # The 'AccessType' option must be defined
    if 'AccessType' not in catalogConfig:
      errStr = "FileCatalog._getCatalogConfigDetails: Required option 'AccessType' not defined."
      gLogger.error( errStr, catalogName )
      return S_ERROR( errStr )
    # Anything other than 'True' in the 'Master' option means it is not
    catalogConfig['Master'] = ( catalogConfig.setdefault( 'Master', False ) == 'True' )
    return S_OK( catalogConfig )

  def _generateCatalogObject( self, catalogName ):
    """ Create a file catalog object from its name and CS description
    """
    useProxy = gConfig.getValue( '/LocalSite/Catalogs/%s/UseProxy' % catalogName, False )
    if not useProxy:
      useProxy = self.opHelper.getValue( '/Services/Catalogs/%s/UseProxy' % catalogName, False )
    return FileCatalogFactory().createCatalog( catalogName, useProxy )

