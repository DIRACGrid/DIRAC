########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
""" FileCatalogFactory class to create file catalog client objects according to the 
    configuration description 
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCatalogPath
from DIRAC.Resources.Catalog.FileCatalogProxyClient import FileCatalogProxyClient
from DIRAC.Core.Utilities import ObjectLoader

class FileCatalogFactory(object):
  
  def __init__(self):
    self.log = gLogger.getSubLogger('FileCatalogFactory')
  
  def createCatalog( self, catalogName, useProxy=False ):
    """ Create a file catalog object from its name and CS description
    """
    if useProxy:
      catalog = FileCatalogProxyClient( catalogName )
      return S_OK( catalog )
    
    # get the CS description first
    catalogPath = getCatalogPath( catalogName )
    catalogType = gConfig.getValue( catalogPath+'/CatalogType', catalogName )
    catalogURL = gConfig.getValue( catalogPath+'/CatalogURL', "DataManagement/"+catalogName ) 
    
    self.log.verbose( 'Creating %s client' % catalogName )
    
    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject( 'Resources.Catalog.%sClient' % catalogType, catalogType+'Client' )
    if not result['OK']:
      gLogger.error( 'Failed to load catalog object: %s' % result['Message'] )
      return result
    
    catalogClass = result['Value']
     
    try:
      if catalogType in [ 'LcgFileCatalogCombined', 'LcgFileCatalog' ]:
        # The LFC special case
        infoSys = gConfig.getValue( catalogPath+'/LcgGfalInfosys', '' )
        host = gConfig.getValue( catalogPath+'/MasterHost', '' )
        catalog = catalogClass( infoSys, host )
      else:  
        if catalogURL:
          catalog = catalogClass( url = catalogURL )
        else:  
          catalog = catalogClass()
      self.log.debug('Loaded module %sClient' % catalogType )
      return S_OK( catalog )
    except Exception, x:
      errStr = "Failed to instantiate %s()" % ( catalogType )
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )
      
    # Catalog module was not loaded  
    return S_ERROR( 'No suitable client found for %s' % catalogName )  
 
