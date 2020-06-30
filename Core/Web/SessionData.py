from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from DIRAC import gConfig, gLogger
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

from DIRAC.Core.Web import Conf

__RCSID__ = "$Id$"


class SessionData(object):

  __disetConfig = ThreadConfig()
  __handlers = {}
  __groupMenu = {}
  __extensions = []
  __extVersion = "ext-6.2.0"
  __configuration = {}

  @classmethod
  def setHandlers(cls, handlers):
    """ Set handlers

        :param dict handlers: handlers
    """
    cls.__handlers = {}
    for k in handlers:
      handler = handlers[k]
      cls.__handlers[handler.LOCATION.strip("/")] = handler
    # Calculate extensions
    cls.__extensions = CSGlobals.getInstalledExtensions()
    for ext in ['DIRAC', 'WebAppDIRAC']:
      if ext in cls.__extensions:
        cls.__extensions.append(cls.__extensions.pop(cls.__extensions.index(ext)))

  def __init__(self, credDict, setup):
    self.__credDict = credDict
    self.__setup = setup

  def __isGroupAuthApp(self, appLoc):
    """ The method checks if the application is authorized for a certain user group

        :param str appLoc It is the application name for example: DIRAC.JobMonitor

        :return bool -- if the handler is authorized to the user returns True otherwise False
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
    """ Generate a menu schema based on the user credentials

        :param str base: base
        :param str path: path

        :return: list
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
    """ Load the schema from the CS and filter based on the group

        :param dict cfg: dictionary with current configuration

        :return: list
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
    """ Get WebApp path

        :return: str
    """
    return os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "WebApp")

  @classmethod
  def getExtJSVersion(cls):
    """ Get ExtJS version

        :return: str
    """
    if not cls.__extVersion:
      extPath = os.path.join(cls.getWebAppPath(), "static", "extjs")
      extVersionPath = []
      for entryName in os.listdir(extPath):
        if entryName.find("ext-") == 0:
          extVersionPath.append(entryName)

      cls.__extVersion = sorted(extVersionPath)[-1]
    return cls.__extVersion

  @classmethod
  def getWebConfiguration(cls):
    """ Get WebApp configuration

        :return: dict
    """
    result = gConfig.getOptionsDictRecursively("/WebApp")
    if not cls.__configuration and result['OK']:
      cls.__configuration = result['Value']
    return cls.__configuration

  def getData(self):
    """ Return session data

        :return: dict
    """
    data = {'configuration': self.getWebConfiguration(),
            'menu': self.__getGroupMenu(),
            'user': self.__credDict,
            'validGroups': [],
            'groupsStatuses': {},
            'setup': self.__setup,
            'validSetups': gConfig.getSections("/DIRAC/Setups")['Value'],
            'extensions': self.__extensions,
            'extVersion': self.getExtJSVersion()}
    # Add valid groups if known
    username = self.__credDict.get("username", "anonymous")
    if username != 'anonymous':
      result = Registry.getGroupsForUser(username)
      if not result['OK']:
        return result
      data['validGroups'] = result['Value']
      result = gProxyManager.getGroupsStatusByUsername(username)  # pylint: disable=no-member
      if result['OK']:
        data['groupsStatuses'] = result['Value']
    # Calculate baseURL
    baseURL = [Conf.rootURL().strip("/"),
               "s:%s" % data['setup'],
               "g:%s" % self.__credDict.get('group', '')]
    data['baseURL'] = "/%s" % "/".join(baseURL)
    return data
