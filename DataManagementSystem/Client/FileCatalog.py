""" File catalog class. This is a simple dispatcher for the file catalogue plug-ins.
    It ensures that all operations are performed on the desired catalogs.
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import pythonCall

import types,re

class FileCatalog:

  ro_methods = ['exists','isLink','readLink','isFile','getFileMetadata','getReplicas',
                'getReplicaStatus','getFileSize','isDirectory','getDirectoryReplicas',
                'listDirectory','getDirectoryMetadata','getDirectorySize']

  write_methods = ['createLink','removeLink','addFile','addReplica','removeReplica',
                   'removeFile','setReplicaStatus','setReplicaHost','createDirectory',
                   'removeDirectory']

  def __init__(self,catalogs=[]):
    """ Default constructor
    """
    self.valid = True
    self.timeout = 180
    self.readCatalogs = {}
    self.writeCatalogs = {}
    self.rootConfigPath = '/Resources/FileCatalogs'

    if type(catalogs) in types.StringTypes:
      catalogs = [catalogs]
    if catalogs:
      res = self._getSelectedCatalogs(catalogs)
    else:
      res = self._getCatalogsConfigs()
    if not res['OK']:
      self.valid = False

  def isOK(self):
    return self.valid

  def getReadCatalogs(self):
    return self.readCatalogs

  def getWriteCatalogs(self):
    return self.writeCatalogs

  def __getattr__(self, name):
    self.call = name
    if name in FileCatalog.write_methods:
      return self.w_execute
    elif name in FileCatalog.ro_methods:
      return self.r_execute
    else:
      raise AttributeError

  def w_execute(self, *parms, **kws):
    """ Write method executor.
    """
    successful = {}
    failed = {}
    for catalogName,oCatalog in self.writeCatalogs.items():
      method = getattr(oCatalog,self.call)
      res = method(*parms,**kws)
      if not res['OK']:
        return res
      else:
        for key,item in res['Value']['Successful'].items():
          if not successful.has_key(key):
            successful[key] = {}
          successful[key][catalogName] = item
        for key,item in res['Value']['Failed'].items():
          if not failed.has_key(key):
            failed[key] = {}
          failed[key][catalogName] = item
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  def r_execute(self,*parms, **kws):
    """ Read method executor.
    """
    successful = {}
    failed = {}
    for catalogName,oCatalog in self.readCatalogs.items():
      method = getattr(oCatalog,self.call)
      res = method(*parms,**kws)
      if res['OK']:
        for key,item in res['Value']['Successful']:
          if not successful.has_key(key):
            successful[key] = item
            if failed.has_key(key):
              failed.pop(key)
        for key,item in res['Value']['Failed']:
          if not successful.has_key(key):
            failed[key] = item
        if len(failed) == 0:
          resDict = {'Failed':failed,'Successful':successful}
          return S_OK(resDict)
    if len(successful) == 0:
      return S_ERROR('FileCatalog.%s: Completely failed for all catalogs.' % self.call)
    resDict = {'Failed':failed,'Successful':successful}
    return S_OK(resDict)

  ###########################################################################################
  #
  # Below is the method for obtaining the objects instantiated for a provided catalogue configuration
  #

  def _getSelectedCatalogs(self,desiredCatalogs):
    for catalogName in desiredCatalogs:
      res = self._generateCatalogObject(catalogName)
      if not res['OK']:
        return res
      oCatalog = res['Value']
      self.readCatalogs[catalogName] = oCatalog
      self.writeCatalogs[catalogName] = oCatalog
    return S_OK()

  def _getCatalogs(self):
    res = gConfig.getSections(self.rootConfigPath)
    if not res['OK']:
      errStr = "FileCatalog._getCatalogs: Failed to get file catalog configuration."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    fileCatalogs = res['Value']
    for catalogName in fileCatalogs:
      res = self._getCatalogConfigDetails(catalogName)
      if not res['OK']:
        return res
      catalogConfig = res['Value']
      if catalogConfig['Status'] == 'Active':
        res = self._generateCatalogObject(catalogName)
        if not res['OK']:
          return res
        oCatalog = res['Value']
        # If the catalog is read type
        if re.search('Read',catalogConfig['AccessType']):
          self.readCatalogs[catalogName] = oCatalog
        # If the catalog is write type
        if re.search('Write',catalogConfig['AccessType']):
          self.writeCatalogs[catalogName] = oCatalog
    return S_OK()

  def _getCatalogConfigDetails(self,catalogName):
    # First obtain the options that are available
    catalogConfigPath = '%s/%s' % (self.rootConfigPath,catalogName)
    res = gConfig.getOptions(catalogConfigPath)
    if not res['OK']:
      errStr = "FileCatalog._getCatalogConfigDetails: Failed to get catalog options."
      gLogger.error(errStr,catalogName)
      return S_ERROR(errStr)
    catalogConfig = {}
    for option in res['Value']:
      configPath = '%s/%s' % (catalogConfigPath,option)
      optionValue = gConfig.getValue(configPath)
      catalogConfig[option] = optionValue
    # The 'Status' option should be defined (default = 'Active')
    if not catalogConfig.has_key('Status'):
      warnStr = "FileCatalog._getCatalogConfigDetails: 'Status' option not defined."
      gLogger.warn(warnStr,catalogName)
      catalogConfig['Status'] = 'Active'
    # The 'AccessType' option must be defined
    if not catalogConfig.has_key('AccessType'):
      errStr = "FileCatalog._getCatalogConfigDetails: Required option 'AccessType' not defined."
      gLogger.error(errStr,catalogName)
      return S_ERROR(errStr)
    return S_OK(catalogConfig)

  def _generateCatalogObject(self,catalogName):
    try:
      # This inforces the convention that the plug in must be named after the file catalog
      moduleName = "%sClient" % (catalogName)
      catalogModule = __import__('DIRAC.DataManagementSystem.Client.Catalog.%s' % moduleName,globals(),locals(),[moduleName])
    except Exception, x:
      errStr = "FileCatalog._generateCatalogObject: Failed to import %s: %s" % (catalogName, x)
      gLogger.exception(errStr)
      return S_ERROR(errStr)
    try:
      evalString = "catalogModule.%s()" % moduleName
      catalog = eval(evalString)
      if not catalog.isOK():
        errStr = "FileCatalog._generateCatalogObject: Failed to instatiate catalog plug in."
        gLogger.error(errStr,"%s" % (moduleName))
        return S_ERROR(errStr)
    except Exception, x:
      errStr = "FileCatalog._generateCatalogObject: Failed to instatiate %s()" % (moduleName)
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)
    return S_OK(catalog)
