import os
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.HTTP.Lib import Conf

_RCSID_ = "$Id$"


class SessionData(object):

  __disetConfig = ThreadConfig()
  __handlers = {}
  __groupMenu = {}
  __extensions = []
  __extVersion = "ext-6.2.0"

  @classmethod
  def setHandlers(cls, handlers):
    cls.__handlers = {}
    for k in handlers:
      handler = handlers[k]
      cls.__handlers[handler.LOCATION.strip("/")] = handler
    # Calculate extensions
    cls.__extensions = []
    for ext in CSGlobals.getInstalledExtensions():
      if ext in ("WebAppDIRAC", "DIRAC"):
        continue
      cls.__extensions.append(ext)
    cls.__extensions.append("DIRAC")
    cls.__extensions.append("WebAppDIRAC")

  def __init__(self, credDict, setup):
    self.__credDict = credDict
    self.__setup = setup

  def __isGroupAuthApp(self, appLoc):
    """
    The method checks if the application is authorized for a certain user group

    :param str appLoc It is the application name for example: DIRAC.JobMonitor
    :return bool if the handler is authorized to the user returns True otherwise False

    """

    handlerLoc = "/".join(List.fromChar(appLoc, ".")[1:])
    if not handlerLoc:
      gLogger.error("Application handler does not exists:", appLoc)
      return False
    if handlerLoc not in self.__handlers:
      gLogger.error("Handler %s required by %s does not exist!" % (handlerLoc, appLoc))
      return False
    handler = self.__handlers[handlerLoc]
    auth = AuthManager(Conf.getAuthSectionForHandler(handlerLoc))
    gLogger.info("Authorization: %s -> %s" % (dict(self.__credDict), handler.AUTH_PROPS))
    return auth.authQuery("", dict(self.__credDict), handler.AUTH_PROPS)

  def __generateSchema(self, base, path):
    """
    Generate a menu schema based on the user credentials
    """
    # Calculate schema
    schema = []
    fullName = "%s/%s" % (base, path)
    result = gConfig.getSections(fullName)
    if not result['OK']:
      return schema
    sectionsList = result['Value']
    for sName in sectionsList:
      subSchema = self.__generateSchema(base, "%s/%s" % (path, sName))
      if subSchema:
        schema.append((sName, subSchema))
    result = gConfig.getOptions(fullName)
    if not result['OK']:
      return schema
    optionsList = result['Value']
    for opName in optionsList:
      opVal = gConfig.getValue("%s/%s" % (fullName, opName))
      if opVal.find("link|") == 0:
        schema.append(("link", opName, opVal[5:]))
        continue
      if self.__isGroupAuthApp(opVal):
        schema.append(("app", opName, opVal))
    return schema

  def __getGroupMenu(self):
    """
    Load the schema from the CS and filter based on the group
    """
    # Somebody coming from HTTPS and not with a valid group
    group = self.__credDict.get("group", "")
    # Cache time!
    if group not in self.__groupMenu:
      base = "%s/Schema" % (Conf.BASECS)
      self.__groupMenu[group] = self.__generateSchema(base, "")
    return self.__groupMenu[group]

  @classmethod
  def getWebAppPath(cls):
    return os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "WebApp")

  @classmethod
  def getExtJSVersion(cls):
    if not cls.__extVersion:
      extPath = os.path.join(cls.getWebAppPath(), "static", "extjs")
      extVersionPath = []
      for entryName in os.listdir(extPath):
        if entryName.find("ext-") == 0:
          extVersionPath.append(entryName)

      cls.__extVersion = sorted(extVersionPath)[-1]
    return cls.__extVersion

  def getData(self):
    data = {'menu': self.__getGroupMenu(),
            'user': self.__credDict,
            'validGroups': [],
            'setup': self.__setup,
            'validSetups': gConfig.getSections("/DIRAC/Setups")['Value'],
            'extensions': self.__extensions,
            'extVersion': self.getExtJSVersion()}
    # Add valid groups if known
    DN = self.__credDict.get("DN", "")
    if DN:
      result = Registry.getGroupsForDN(DN)
      if result['OK']:
        data['validGroups'] = result['Value']
    # Calculate baseURL
    baseURL = [Conf.rootURL().strip("/"),
               "s:%s" % data['setup'],
               "g:%s" % self.__credDict.get('group', '')]
    data['baseURL'] = "/%s" % "/".join(baseURL)
    return data
