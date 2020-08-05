""" DIRAC FileCatalog Security Manager mix-in class
"""

__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.Properties import FC_MANAGEMENT

_readMethods = ['exists', 'isFile', 'getFileSize', 'getFileMetadata',
                'getReplicas', 'getReplicaStatus', 'getFileAncestors',
                'getFileDescendents', 'listDirectory', 'isDirectory',
                'getDirectoryReplicas', 'getDirectorySize', 'getDirectoryMetadata']

_writeMethods = ['changePathOwner', 'changePathGroup', 'changePathMode',
                 'addFile', 'setFileStatus', 'removeFile', 'addReplica',
                 'removeReplica', 'setReplicaStatus', 'setReplicaHost',
                 'addFileAncestors', 'createDirectory', 'removeDirectory',
                 'setMetadata', '__removeMetadata']


class SecurityManagerBase(object):

  def __init__(self, database=None):
    self.db = database

  def setDatabase(self, database):
    self.db = database

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """
    return S_ERROR('The getPathPermissions method must be implemented in the inheriting class')

  def hasAccess(self, opType, paths, credDict):
    # Map the method name to Read/Write
    if opType in _readMethods:
      opType = 'Read'
    elif opType in _writeMethods:
      opType = 'Write'

    # Check if admin access is granted first
    result = self.hasAdminAccess(credDict)
    if not result['OK']:
      return result
    if result['Value']:
      # We are admins, allow everything
      permissions = {}
      for path in paths:
        permissions[path] = True
      return S_OK({'Successful': permissions, 'Failed': {}})

    successful = {}
    failed = {}
    if not opType.lower() in ['read', 'write', 'execute']:
      return S_ERROR("Operation type not known")
    if self.db.globalReadAccess and (opType.lower() == 'read'):
      for path in paths:
        successful[path] = True
      resDict = {'Successful': successful, 'Failed': {}}
      return S_OK(resDict)

    result = self.getPathPermissions(paths, credDict)
    if not result['OK']:
      return result

    permissions = result['Value']['Successful']
    for path, permDict in permissions.items():
      if permDict[opType]:
        successful[path] = True
      else:
        successful[path] = False

    failed.update(result['Value']['Failed'])

    resDict = {'Successful': successful, 'Failed': failed}
    return S_OK(resDict)

  def hasAdminAccess(self, credDict):
    if FC_MANAGEMENT in credDict['properties']:
      return S_OK(True)
    return S_OK(False)


class NoSecurityManager(SecurityManagerBase):

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """

    permissions = {}
    for path in paths:
      permissions[path] = {'Read': True, 'Write': True, 'Execute': True}

    return S_OK({'Successful': permissions, 'Failed': {}})

  def hasAccess(self, opType, paths, credDict):
    successful = {}
    for path in paths:
      successful[path] = True
    resDict = {'Successful': successful, 'Failed': {}}
    return S_OK(resDict)

  def hasAdminAccess(self, credDict):
    return S_OK(True)


class DirectorySecurityManager(SecurityManagerBase):

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """

    toGet = dict(zip(paths, [[path] for path in paths]))
    permissions = {}
    failed = {}
    while toGet:
      res = self.db.dtree.getPathPermissions(toGet.keys(), credDict)
      if not res['OK']:
        return res
      for path, mode in res['Value']['Successful'].items():
        for resolvedPath in toGet[path]:
          permissions[resolvedPath] = mode
        toGet.pop(path)
      for path, error in res['Value']['Failed'].items():
        if error != 'No such file or directory':
          for resolvedPath in toGet[path]:
            failed[resolvedPath] = error
          toGet.pop(path)
      for path, resolvedPaths in toGet.items():
        if path == '/':
          for resolvedPath in resolvedPaths:
            permissions[path] = {'Read': True, 'Write': True, 'Execute': True}
        if os.path.dirname(path) not in toGet:
          toGet[os.path.dirname(path)] = []
        toGet[os.path.dirname(path)] += resolvedPaths
        toGet.pop(path)

    if self.db.globalReadAccess:
      for path in permissions:
        permissions[path]['Read'] = True

    return S_OK({'Successful': permissions, 'Failed': failed})


class FullSecurityManager(SecurityManagerBase):

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """

    toGet = dict(zip(paths, [[path] for path in paths]))
    permissions = {}
    failed = {}
    res = self.db.fileManager.getPathPermissions(paths, credDict)
    if not res['OK']:
      return res
    for path, mode in res['Value']['Successful'].items():
      for resolvedPath in toGet[path]:
        permissions[resolvedPath] = mode
      toGet.pop(path)
    for path, resolvedPaths in toGet.items():
      if path == '/':
        for resolvedPath in resolvedPaths:
          permissions[path] = {'Read': True, 'Write': True, 'Execute': True}
      if os.path.dirname(path) not in toGet:
        toGet[os.path.dirname(path)] = []
      toGet[os.path.dirname(path)] += resolvedPaths
      toGet.pop(path)
    while toGet:
      paths = toGet.keys()
      res = self.db.dtree.getPathPermissions(paths, credDict)
      if not res['OK']:
        return res
      for path, mode in res['Value']['Successful'].items():
        for resolvedPath in toGet[path]:
          permissions[resolvedPath] = mode
        toGet.pop(path)
      for path, error in res['Value']['Failed'].items():
        if error != 'No such file or directory':
          for resolvedPath in toGet[path]:
            failed[resolvedPath] = error
          toGet.pop(path)
      for path, resolvedPaths in toGet.items():
        if path == '/':
          for resolvedPath in resolvedPaths:
            permissions[path] = {'Read': True, 'Write': True, 'Execute': True}
        if os.path.dirname(path) not in toGet:
          toGet[os.path.dirname(path)] = []
        toGet[os.path.dirname(path)] += resolvedPaths
        toGet.pop(path)

    if self.db.globalReadAccess:
      for path in permissions:
        permissions[path]['Read'] = True

    return S_OK({'Successful': permissions, 'Failed': failed})


class DirectorySecurityManagerWithDelete(DirectorySecurityManager):
  """ This security manager implements a Delete operation.
       For Read, Write, Execute, it's behavior is the one of DirectorySecurityManager.
       For Delete, if the directory does not exist, we return True.
       If the directory exists, then we test the Write permission

  """

  def hasAccess(self, opType, paths, credDict):
    # The other SecurityManager do not support the Delete operation,
    # and it is transformed in Write
    # so we keep the original one

    if opType in ['removeFile', 'removeReplica', 'removeDirectory']:
      self.opType = 'Delete'
    elif opType in _readMethods:
      self.opType = 'Read'
    elif opType in _writeMethods:
      self.opType = 'Write'

    res = super(DirectorySecurityManagerWithDelete, self).hasAccess(opType, paths, credDict)

    # We reinitialize self.opType in case someone would call getPathPermissions directly
    self.opType = ''

    return res

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """

    # If we are testing in anything else than a Delete, just return the parent methods
    if hasattr(self, 'opType') and self.opType.lower() != 'delete':
      return super(DirectorySecurityManagerWithDelete, self).getPathPermissions(paths, credDict)

    # If the object (file or dir) does not exist, we grant the permission
    res = self.db.dtree.exists(paths)
    if not res['OK']:
      return res

    nonExistingDirectories = set(path for path in res['Value']['Successful'] if not res['Value']['Successful'][path])

    res = self.db.fileManager.exists(paths)
    if not res['OK']:
      return res

    nonExistingFiles = set(path for path in res['Value']['Successful'] if not res['Value']['Successful'][path])

    nonExistingObjects = nonExistingDirectories & nonExistingFiles

    permissions = {}
    failed = {}

    for path in nonExistingObjects:
      permissions[path] = {'Read': True, 'Write': True, 'Execute': True}
      # The try catch is just to protect in case there are duplicate in the paths
      try:
        paths.remove(path)
      except Exception as _e:
        try:
          paths.pop(path)
        except Exception as _ee:
          pass

    # For all the paths that exist, check the write permission
    if paths:

      res = super(DirectorySecurityManagerWithDelete, self).getPathPermissions(paths, credDict)
      if not res['OK']:
        return res

      failed = res['Value']['Failed']
      permissions.update(res['Value']['Successful'])

    return S_OK({'Successful': permissions, 'Failed': failed})


class PolicyBasedSecurityManager(SecurityManagerBase):
  """ This security manager loads a python plugin and forwards the
    calls to it. The python plugin has to be defined in the CS under
    /Systems/DataManagement/YourSetup/FileCatalog/SecurityPolicy
  """

  def __init__(self, database=False):
    super(PolicyBasedSecurityManager, self).__init__(database)

    from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
    from DIRAC import gConfig
    from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath

    serviceSection = getServiceSection('DataManagement/FileCatalog')

    pluginPath = gConfig.getValue(cfgPath(serviceSection, 'SecurityPolicy'))

    if not pluginPath:
      raise Exception("SecurityPolicy not defined in service options")

    pluginCls = self.__loadPlugin(pluginPath)
    self.policyObj = pluginCls(database=database)

    # For the old clients to work with the new policy (since getPathPermissions is meant to disappear...)
    # we fetch the old SecurityManager, and we call it if needed in the plugin.
    oldSecurityManagerName = gConfig.getValue(cfgPath(serviceSection, 'OldSecurityManager'), '')
    self.policyObj.oldSecurityManager = None
    if oldSecurityManagerName:
      self.policyObj.oldSecurityManager = eval("%s(self.db)" % oldSecurityManagerName)

  @staticmethod
  def __loadPlugin(pluginPath):
    """ Create an instance of requested plugin class, loading and importing it when needed.
    This function could raise ImportError when plugin cannot be found or TypeError when
    loaded class object isn't inherited from SecurityManagerBase class.

    :param str pluginName: dotted path to plugin, specified as in import statement, i.e.
    "DIRAC.CheesShopSystem.private.Cheddar" or alternatively in 'normal' path format
    "DIRAC/CheesShopSystem/private/Cheddar"

    :return: object instance
    This function try to load and instantiate an object from given path. It is assumed that:

    - :pluginPath: is pointing to module directory "importable" by python interpreter, i.e.: it's
    package's top level directory is in $PYTHONPATH env variable,
    - the module should consist a class definition following module name,
    - the class itself is inherited from SecurityManagerBase
    If above conditions aren't meet, function is throwing exceptions:

    - ImportError when class cannot be imported
    - TypeError when class isn't inherited from SecurityManagerBase

    """
    if "/" in pluginPath:
      pluginPath = ".".join([chunk for chunk in pluginPath.split("/") if chunk])
    pluginName = pluginPath.split(".")[-1]
    if pluginName not in globals():
      mod = __import__(pluginPath, globals(), fromlist=[pluginName])
      pluginClassObj = getattr(mod, pluginName)
    else:
      pluginClassObj = globals()[pluginName]

    if not issubclass(pluginClassObj, SecurityManagerBase):
      raise TypeError("Security policy '%s' isn't inherited from SecurityManagerBase class" % pluginName)

    return pluginClassObj

  def hasAccess(self, opType, paths, credDict):
    return self.policyObj.hasAccess(opType, paths, credDict)

  def getPathPermissions(self, paths, credDict):
    return self.policyObj.getPathPermissions(paths, credDict)
