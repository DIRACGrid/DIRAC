""" File catalog class. This is a simple dispatcher for the file catalog plug-ins.
    It ensures that all operations are performed on the desired catalogs.

    The File Catalog plug-ins are supposed to implement a certain number of methods of
    the File Catalog interface. The names of the implemented methods classified in
    "read", "write" and "no_lfn" categories are reported by each plug-in using
    getInterfaceMethods() call. The File Catalog collects and memorizes all the
    method names from all the plug-ins and in each category. Only calls to these
    methods will be attempted.

    One plug-in can be declared to be a Master in the CS. If the Master plug-in is
    declared it must implement all the methods collected in the "write" category.
    When the FileCatalog is called with a given "write" method name, the Master plugin
    is called first. If it fails, no other plug-in is called to preserve consistency
    in the states of different catalogs. If no Master plug-in is declared, all the
    plug-ins are called (in case they implement the method) for the "write" methods.

    For the "read" methods plug-ins are called one by one, starting with the Master
    plug-in if declared, until getting a successful result.

    Most of the catalog plug-in methods are taking the first argument which represents
    the required LFNS. The LFNs argument can have one of the following forms:

    - string: just a single LFN itself
    - list: a list of LFN strings
    - dictionary: the keys are LFN strings, the values are LFN specific parameters
                  needed by the method

    All the methods taking the first LFNs argument are returning a standard DIRAC
    bulk result structure. If the call is successful, the Successful and Failed
    dictionaries have LFNs as keys and specific results of operation as values.
    The LFNs argument before calling these methods is checked to conform to the
    convention above and modified to take the "dictionary" form. The original LFN names
    are memorized and restored in the final result.

    Some methods implemented by plug-ins do not have LFNs as the first argument.
    The names of those methods are reported by the plug-ins as "no_lfn" methods
    in the getInterfaceMethods() call. For those methods there is obviously no
    additional check of the structure of the LFNs argument and no corresponding
    processing of the results.

    For the actual methods that can be called vie the File Catalog object, see
    the documentation of the respective FileCatalog plug-ins ( client classes )

"""

import re

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo                       import getVOfromProxyGroup
from DIRAC.Resources.Catalog.Utilities                   import checkArgumentFormat
from DIRAC.Resources.Catalog.FileCatalogFactory          import FileCatalogFactory

class FileCatalog( object ):

  ro_methods = set()
  write_methods = set()
  no_lfn_methods = set()

  def __init__( self, catalogs = None, vo = None ):
    """ Default constructor
    """
    self.valid = True
    self.timeout = 180
    self.readCatalogs = []
    self.writeCatalogs = []
    self.rootConfigPath = '/Resources/FileCatalogs'
    self.vo = vo if vo else getVOfromProxyGroup().get( 'Value', None )

    self.opHelper = Operations( vo = self.vo )

    if catalogs is None:
      catalogList = []
    elif isinstance( catalogs, basestring ):
      catalogList = [catalogs]
    elif isinstance( catalogs, ( list, tuple ) ):
      catalogList = list( catalogs )

    if catalogList:
      res = self._getSelectedCatalogs( catalogList )
    else:
      res = self._getCatalogs()
    if not res['OK']:
      self.valid = False
    elif ( len( self.readCatalogs ) == 0 ) and ( len( self.writeCatalogs ) == 0 ):
      self.valid = False

    result = self.getMasterCatalogNames()
    masterCatalogs = result['Value']
    # There can not be more than one master catalog
    haveMaster = False
    if len( masterCatalogs ) > 1:
      self.valid = False
    elif len( masterCatalogs ) == 1:
      haveMaster = True

    # Get the list of write methods
    if haveMaster:
      # All the write methods must be present in the master
      catalogName, oCatalog, master = self.writeCatalogs[0]
      _roList, writeList, nolfnList = oCatalog.getInterfaceMethods()
      FileCatalog.write_methods.update( writeList )
      FileCatalog.no_lfn_methods.update( nolfnList )
    else:
      for catalogName, oCatalog, master in self.writeCatalogs:
        _roList, writeList, nolfnList = oCatalog.getInterfaceMethods()
        FileCatalog.write_methods.update( writeList )
        FileCatalog.no_lfn_methods.update( nolfnList )

    # Get the list of read methods
    for catalogName, oCatalog, master in self.readCatalogs:
      roList, _writeList, nolfnList = oCatalog.getInterfaceMethods()
      FileCatalog.ro_methods.update( roList )
      FileCatalog.no_lfn_methods.update( nolfnList )

  def isOK( self ):
    return self.valid

  def getReadCatalogs( self ):
    return self.readCatalogs

  def getWriteCatalogs( self ):
    return self.writeCatalogs

  def getMasterCatalogNames( self ):
    """ Returns the list of names of the Master catalogs """

    masterNames = [catalogName for catalogName, oCatalog, master in self.writeCatalogs if master]
    return S_OK( masterNames )


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
    failedCatalogs = {}
    successfulCatalogs = {}

    allLfns = []
    lfnMapDict = {}
    masterResult = {}
    if not self.call in FileCatalog.no_lfn_methods:
      fileInfo = parms[0]
      result = checkArgumentFormat( fileInfo, generateMap = True )
      if not result['OK']:
        return result
      fileInfo, lfnMapDict = result['Value']
      # No need to check the LFNs again in the clients
      kws['LFNChecking'] = False
      allLfns = fileInfo.keys()
      parms1 = parms[1:]

    for catalogName, oCatalog, master in self.writeCatalogs:

      # Skip if the method is not implemented in this catalog
      if not oCatalog.hasCatalogMethod( self.call ):
        if master:
          gLogger.error( "Master catalog does not implement the write method", self.call )
          return S_ERROR( "Master catalog does not implement the write method %s" % self.call )
        else:
          continue

      method = getattr( oCatalog, self.call )
      if self.call in FileCatalog.no_lfn_methods:
        result = method( *parms, **kws )
      else:
        result = method( fileInfo, *parms1, **kws )
      if master:
        masterResult = result
      if not result['OK']:
        if master:
          # If this is the master catalog and it fails we dont want to continue with the other catalogs
          gLogger.error( "FileCatalog.w_execute: Failed to execute call on master catalog",
                         "%s on %s: %s" % ( self.call, catalogName, result['Message'] ) )
          return result
        else:
          # Otherwise we keep the failed catalogs so we can update their state later
          failedCatalogs[catalogName] = result['Message']
      else:
        successfulCatalogs[catalogName] = result['Value']

      if allLfns:
        if result['OK']:
          for lfn, message in result['Value']['Failed'].items():
            # Save the error message for the failed operations
            failed.setdefault( lfn, {} )[catalogName] = message
            if master:
              # If this is the master catalog then we should not attempt the operation on other catalogs
              fileInfo.pop( lfn, None )
          for lfn, result in result['Value']['Successful'].items():
            # Save the result return for each file for the successful operations
            successful.setdefault( lfn, {} )[catalogName] = result

    if allLfns:
      # This recovers the states of the files that completely failed i.e. when S_ERROR is returned by a catalog
      for catalogName, errorMessage in failedCatalogs.items():
        for lfn in allLfns:
          failed.setdefault( lfn, {} )[catalogName] = errorMessage
      # Restore original lfns if they were changed by normalization
      if lfnMapDict:
        for lfn in failed:
          failed[lfnMapDict.get( lfn, lfn )] = failed[lfn]
        for lfn in successful:
          successful[lfnMapDict.get( lfn, lfn )] = successful[lfn]
      resDict = {'Failed':failed, 'Successful':successful}
      return S_OK( resDict )
    else:
      if failedCatalogs:
        result = S_ERROR( 'Failed to execute on some catalogs' )
        resDict = {'Failed':failedCatalogs, 'Successful':successfulCatalogs}
        result['Value'] = resDict
        return result
      else:
        return masterResult


  def r_execute( self, *parms, **kws ):
    """ Read method executor.
    """
    successful = {}
    failed = {}
    for catalogName, oCatalog, _master in self.readCatalogs:

      # Skip if the method is not implemented in this catalog
      if not oCatalog.hasCatalogMethod( self.call ):
        continue

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
      res = self._getCatalogConfigDetails( catalogName )
      if not res['OK']:
        return res
      catalogConfig = res['Value']
      res = self._generateCatalogObject( catalogName )
      if not res['OK']:
        return res
      oCatalog = res['Value']
      self.readCatalogs.append( ( catalogName, oCatalog, True ) )
      self.writeCatalogs.append( ( catalogName, oCatalog, True ) )
    return S_OK()

  def _getCatalogs( self ):

    # Get the eligible catalogs first
    # First, look in the Operations, if nothing defined look in /Resources for backward compatibility
    fileCatalogs = self.opHelper.getValue( '/Services/Catalogs/CatalogList', [] )
    if fileCatalogs:
      operationsFlag = True
    else:
      result = self.opHelper.getSections( '/Services/Catalogs' )
      operationsFlag = False
      if result['OK']:
        fileCatalogs = result['Value']
        operationsFlag = True
      else:
        res = gConfig.getSections( self.rootConfigPath, listOrdered = True )
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
        result = self.opHelper.getOptionsDict( '/Services/Catalogs/%s' % catalogName )
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

