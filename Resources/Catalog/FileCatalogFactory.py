########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
""" FileCatalogFactory class to create file catalog client objects according to the 
    configuration description 
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCatalogPath
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getInstalledExtensions

class FileCatalogFactory:
  
  def __init__(self):
    self.log = gLogger.getSubLogger('FileCatalogFactory')
  
  def createCatalog( self, catalogName ):
    """ Create a file catalog object from its name and CS description
    """
    # get the CS description first
    catalogPath = getCatalogPath( catalogName )
    catalogType = gConfig.getValue(catalogPath+'/CatalogType',catalogName)
    catalogURL = gConfig.getValue(catalogPath+'/CatalogURL','')
    
    self.log.verbose('Creating %s client' % catalogName)
    moduleRootPaths = getInstalledExtensions()
    for moduleRootPath in moduleRootPaths:
      gLogger.verbose( "Trying to load from root path %s" % moduleRootPath )
      #moduleFile = os.path.join( rootPath, moduleRootPath, "Resources", "Catalog", "%sClient.py" % catalogType )
      #gLogger.verbose( "Looking for file %s" % moduleFile )
      #if not os.path.isfile( moduleFile ):
      #  continue
      try:
        # This enforces the convention that the plug in must be named after the file catalog
        moduleName = "%sClient" % ( catalogType )
        catalogModule = __import__( '%s.Resources.Catalog.%s' % ( moduleRootPath, moduleName ),
                                    globals(), locals(), [moduleName] )
      except ImportError, x:
        if "No module" in str(x):
          gLogger.debug('Catalog module %s not found in %s' % ( catalogType, moduleRootPath ) )
        else:
          errStr = "Failed attempt to import %s from the path %s: %s" % ( catalogType, moduleRootPath, x )
          gLogger.error( errStr )
        continue
      except Exception, x: 
        errStr = "Failed attempt to import %s from the path %s: %s" % ( catalogType, moduleRootPath, x )
        gLogger.error( errStr )
        continue
     
      try:
        if catalogType in ['LcgFileCatalogCombined','LcgFileCatalog']:
          # The LFC special case
          infoSys = gConfig.getValue(catalogPath+'/LcgGfalInfosys','')
          host = gConfig.getValue(catalogPath+'/MasterHost','')
          evalString = "catalogModule.%s('%s','%s')" % (moduleName,infoSys,host)
        else:  
          if catalogURL:
            evalString = "catalogModule.%s(url='%s')" % (moduleName,catalogURL)
          else:  
            evalString = "catalogModule.%s()" % moduleName
        catalog = eval( evalString )
        if not catalog.isOK():
          errStr = "Failed to instantiate catalog plug in"
          gLogger.error( errStr, moduleName )
          return S_ERROR( errStr )
        self.log.debug('Loaded module %sClient from %s' % ( catalogType, moduleRootPath ) )
        return S_OK( catalog )
      except Exception, x:
        errStr = "Failed to instantiate %s()" % ( moduleName )
        gLogger.exception( errStr, lException = x )
        return S_ERROR( errStr )
      
    # Catalog module was not loaded  
    return S_ERROR('No suitable client found for %s' % catalogName)  
 
