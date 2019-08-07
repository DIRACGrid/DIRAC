""" DIRAC FileCatalog Security Manager mix-in class based on security policies
"""

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.SecurityManagerBase import SecurityManagerBase


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
