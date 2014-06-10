""" File catalog client for the LFC service combined with multiple read-only mirrors """

__RCSID__ = "$Id$"

import time, os

import DIRAC
from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Catalog.LcgFileCatalogClient       import LcgFileCatalogClient
from DIRAC.Core.Utilities.Subprocess                    import pythonCall
from DIRAC.Core.Utilities.CountryMapping                import getCountryMappingTier1
from DIRAC.Core.Utilities.List                          import randomize

#######################################################################################
#
# Utility functions 
#
def getActiveCatalogs():
  res = gConfig.getSections( "/Resources/FileCatalogs/LcgFileCatalogCombined" )
  if not res['OK']:
    gLogger.error( "Failed to get Active Catalogs", "%s" % res['Message'] )
    return res
  readDict = {}
  for site in res['Value']:
    res = gConfig.getOptionsDict( "/Resources/FileCatalogs/LcgFileCatalogCombined/%s" % site )
    if not res['OK']:
      gLogger.error( "Failed to get Tier1 catalog options", "%s %s" % ( site, res['Message'] ) )
      continue
    siteDict = res['Value']
    if siteDict['Status'] == 'Active':
      readDict[site] = siteDict['ReadOnly']
  return S_OK( readDict )

def getLocationOrderedCatalogs( siteName = '' ):
  # First get a list of the active catalogs and their location
  res = getActiveCatalogs()
  if not res['OK']:
    gLogger.error( "Failed to get list of active catalogs", res['Message'] )
    return res
  catalogDict = res['Value']
  # Get the tier1 associated to the current location
  if not siteName:
    siteName = DIRAC.siteName()
  countryCode = siteName.split( '.' )[-1]
  res = getCountryMappingTier1( countryCode )
  if not res['OK']:
    gLogger.error( "Failed to resolve closest Tier1", res['Message'] )
    return res
  tier1 = res['Value']
  # Create a sorted list of the active readonly catalogs
  catalogList = []
  if catalogDict.has_key( tier1 ):
    catalogList.append( catalogDict[tier1] )
    catalogDict.pop( tier1 )
  for catalogURL in randomize( catalogDict.values() ):
    catalogList.append( catalogURL )
  return S_OK( catalogList )

##############################################################################################
#
#  Now the main class
#

class LcgFileCatalogCombinedClient( object ):

  ro_methods = ['exists', 'isLink', 'readLink', 'isFile', 'getFileMetadata', 'getReplicas',
                'getReplicaStatus', 'getFileSize', 'isDirectory', 'getDirectoryReplicas',
                'listDirectory', 'getDirectoryMetadata', 'getDirectorySize', 'getDirectoryContents',
                'resolveDataset', 'getPathPermissions', 'getLFNForPFN']

  write_methods = ['createLink', 'removeLink', 'addFile', 'addReplica', 'removeReplica',
                   'removeFile', 'setReplicaStatus', 'setReplicaHost', 'createDirectory',
                   'removeDirectory', 'removeDataset', 'removeFileFromDataset', 'createDataset']

  def __init__( self, infosys = None, master_host = None, mirrors = [] ):
    """ Default constructor
    """
    if not infosys:
      configPath = '/Resources/FileCatalogs/LcgFileCatalogCombined/LcgGfalInfosys'
      infosys = gConfig.getValue( configPath )

    self.valid = False
    if not master_host:
      configPath = '/Resources/FileCatalogs/LcgFileCatalogCombined/MasterHost'
      master_host = gConfig.getValue( configPath )
    if master_host:
      # Create the master LFC client first
      self.lfc = LcgFileCatalogClient( infosys, master_host )
      if self.lfc.isOK():
        self.valid = True

      if not mirrors:
        siteName = DIRAC.siteName()
        res = getLocationOrderedCatalogs( siteName = siteName )
        if not res['OK']:
          mirrors = []
        else:
          mirrors = res['Value']
      # Create the mirror LFC instances
      self.mirrors = []
      for mirror in mirrors:
        lfc = LcgFileCatalogClient( infosys, mirror )
        self.mirrors.append( lfc )
      self.nmirrors = len( self.mirrors )

      # Keep the environment for the master instance
      self.master_host = self.lfc.host
      os.environ['LFC_HOST'] = self.master_host
      os.environ['LCG_GFAL_INFOSYS'] = infosys
      self.name = 'LFC'
      self.timeout = 3000

  def isOK( self ):
    return self.valid

  def getName( self, DN = '' ):
    """ Get the file catalog type name
    """
    return self.name

  def __getattr__( self, name ):
    self.call = name
    if name in LcgFileCatalogCombinedClient.write_methods:
      return self.w_execute
    elif name in LcgFileCatalogCombinedClient.ro_methods:
      return self.r_execute
    else:
      raise AttributeError

  def w_execute( self, *parms, **kws ):
    """ Write method executor.
        Dispatches execution of the methods which need Read/Write
        access to the master LFC instance
    """

    # If the DN argument is given, this is an operation on behalf
    # of the user with this DN, prepare setAuthorizationId call
    userDN = ''
    if kws.has_key( 'DN' ):
      userDN = kws['DN']
      del kws['DN']

    # Try the method 3 times just in case of intermittent errors
    max_retry = 2
    count = 0
    result = S_ERROR()

    while ( not result['OK'] ) and ( count <= max_retry ):
      if count > 0:
        # If retrying, wait a bit
        time.sleep( 1 )
      try:
        result = S_OK()
        if userDN:
          resAuth = pythonCall( self.timeout, self.lfc.setAuthorizationId, userDN )
          if not resAuth['OK']:
            result = S_ERROR( 'Failed to set user authorization' )
        if result['OK']:
          method = getattr( self.lfc, self.call )
          resMeth = method( *parms, **kws )
          if not resMeth['OK']:
            return resMeth
          else:
            result = resMeth
      except Exception, x:
        gLogger.exception( 'Exception while calling LFC Master service', '', x )
        result = S_ERROR( 'Exception while calling LFC Master service ' + str( x ) )
      count += 1
    return result

  def r_execute( self, *parms, **kws ):
    """ Read-only method executor.
        Dispatches execution of the methods which need Read-only
        access to the mirror LFC instances
    """

    # If the DN argument is given, this is an operation on behalf
    # of the user with this DN, prepare setAuthorizationId call
    userDN = ''
    if kws.has_key( 'DN' ):
      userDN = kws['DN']
      del kws['DN']

    result = S_ERROR()
    # Try the method 3 times just in case of intermittent errors
    max_retry = 2
    count = 0

    while ( not result['OK'] ) and ( count <= max_retry ):
      i = 0
      while not result['OK'] and i < self.nmirrors:
        # Switch environment to the mirror instance
        os.environ['LFC_HOST'] = self.mirrors[i].host

        try:
          result = S_OK()
          if userDN:
            resAuth = pythonCall( self.timeout, self.mirrors[i].setAuthorizationId, userDN )
            if not resAuth['OK']:
              result = S_ERROR( 'Failed to set user authorization' )
          if result['OK']:
            method = getattr( self.mirrors[i], self.call )
            resMeth = method( *parms, **kws )
            if not resMeth['OK']:
              return resMeth
            else:
              result = resMeth
              if not result['Value'][ 'Successful' ]:
                for reason in result['Value'][ 'Failed' ].values():
                  # If the error is 'Timed out', we can not connect to the LFC server
                  if reason == 'Timed out':
                    result = S_ERROR( 'Time out' )
                    break
        except Exception, x:
          gLogger.exception( 'Exception while calling LFC Mirror service' )
          result = S_ERROR( 'Exception while calling LFC Mirror service ' + str( x ) )
        i += 1
      count += 1

    # Return environment to the master LFC instance
    os.environ['LFC_HOST'] = self.master_host

    # Call the master LFC if all the mirrors failed
    if not result['OK']:
      try:
        result = S_OK()
        if userDN:
          resAuth = pythonCall( self.timeout, self.lfc.setAuthorizationId, userDN )
          if not resAuth['OK']:
            result = S_ERROR( 'Failed to set user authorization' )
        if result['OK']:
          method = getattr( self.lfc, self.call )
          resMeth = method( *parms, **kws )
          if not resMeth['OK']:
            result = S_ERROR( 'Timout calling ' + self.call + " method" )
          else:
            result = resMeth
      except Exception, x:
        gLogger.exception( 'Exception while calling LFC Master service' )
        result = S_ERROR( 'Exception while calling LFC Master service ' + str( x ) )

    return result
