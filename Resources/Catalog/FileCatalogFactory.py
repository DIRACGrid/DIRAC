########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
'''
FileCatalogFactory class to create file catalog client objects according to the 
configuration description
'''

from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Catalog.FileCatalogProxyClient import FileCatalogProxyClient
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

class FileCatalogFactory:

  def __init__( self ):
    self.log = gLogger.getSubLogger( 'FileCatalogFactory' )

  def createCatalog( self, catalogName, useProxy = False ):
    """ Create a file catalog object from its name and CS description
    """    
    if useProxy:
      catalog = FileCatalogProxyClient( catalogName )
      return S_OK( catalog )

    # get the CS description first
    catConfig = catalogConfig
    if not catConfig:
      if not vo:
        result = getVOfromProxyGroup()
        if not result['OK']:
          return result
        vo = result['Value']
      reHelper = Resources( vo = vo )
      result = reHelper.getCatalogOptionsDict( catalogName )
      if not result['OK']:
        return result
      catConfig = result['Value']
    
    catalogType = catConfig.get('CatalogType', catalogName)
    catalogURL = catConfig.get('CatalogURL', "DataManagement/" + catalogType)
    
    self.log.verbose( 'Creating %s client' % catalogName )
    
    objectLoader = ObjectLoader()
    result = objectLoader.loadObject( 'Resources.Catalog.%sClient' % catalogType, catalogType+'Client' )
    if not result['OK']:
      gLogger.error( 'Failed to load catalog object: %s' % result['Message'] )
      return result

    catalogClass = result['Value']

    try:
      if catalogType in ['LcgFileCatalogCombined', 'LcgFileCatalog']:
        # The LFC special case
        infoSys = catConfig.get( 'LcgGfalInfosys', '' )
        host = catConfig.get( 'MasterHost', '' )
        catalog = catalogClass( infoSys, host )
      else:
        if catalogURL:
          catalog = catalogClass( url = catalogURL )  
        else:  
          catalog = catalogClass()
      self.log.debug( 'Loaded module %sClient' % catalogType )
      return S_OK( catalog )
    except Exception, x:
      errStr = "Failed to instantiate %s()" % ( catalogType )
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )

    # Catalog module was not loaded  
    return S_ERROR( 'No suitable client found for %s' % catalogName )

