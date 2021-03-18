""" Basic modules for loading handlers
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import six
import imp
import inspect
import collections

from DIRAC import S_OK, S_ERROR, rootPath, gLogger
from DIRAC.Core.Tornado.Web import Conf
from DIRAC.Core.Tornado.Web.WebHandler import WebHandler, WebSocketHandler
from DIRAC.Core.Tornado.Web.CoreHandler import CoreHandler
from DIRAC.Core.Tornado.Web.StaticHandler import StaticHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
# from DIRAC.FrameworkSystem.private.authorization import AuthServer

__RCSID__ = "$Id$"


@six.add_metaclass(DIRACSingleton)
class HandlerMgr(object):

  def __init__(self, sysService=[], baseURL="/"):
    """ Constructor

        :param list sysService: DIRAC system services
        :param str baseURL: base URL
    """
    self.__baseURL = baseURL.strip("/")
    if sysService and not isinstance(sysService, list):
      sysService = sysService.replace(' ', '').split(',')
    self.__sysServices = sysService
    self.__routes = []
    self.__handlers = []
    self.__setupGroupRE = r"(?:/s:([\w-]*)/g:([\w.-]*))?"
    self.__shySetupGroupRE = r"(?:/s:(?:[\w-]*)/g:(?:[\w.-]*))?"
    self.log = gLogger.getSubLogger("Routing")
    self.__isAuthServer = False
    self.__isPortal = True

  def getPaths(self, dirName):
    """ Get lists of paths for all installed and enabled extensions

        :param str dirName: path to handlers

        :return: list
    """
    pathList = []
    extNames = CSGlobals.getCSExtensions()
    if 'WebAppDIRAC' in extNames:
      # Move WebAppDirac to the end
      extNames.append(extNames.pop(extNames.index('WebAppDIRAC')))
    for extName in extNames:
      if not extName.endswith('DIRAC'):
        extName = "%sDIRAC" % extName
      try:
        modFile, modPath, desc = imp.find_module(extName)
        # to match in the real root path to enabling module web extensions (static, templates...)
        realModPath = os.path.realpath(modPath)
      except ImportError:
        continue
      staticPath = os.path.join(realModPath, "WebApp", dirName)
      if os.path.isdir(staticPath):
        pathList.append(staticPath)
    return pathList

  def __calculateRoutes(self):
    """ Load all handlers and generate the routes

        :return: S_OK()/S_ERROR()
    """
    ol = ObjectLoader()
    hendlerList = []
    self.log.debug("Added services: %s" % ','.join(self.__sysServices))
    for origin in self.__sysServices:
      result = ol.getObjects(origin, parentClass=WebHandler, recurse=True)
      if not result['OK']:
        return result
      hendlerList += list(result['Value'].items())
    self.__handlers = collections.OrderedDict(hendlerList)

    # ['/opt/dirac/pro/WebAppExt/WebApp/static', ...]
    staticPaths = self.getPaths("static")
    self.log.verbose("Static paths found:\n - %s" % "\n - ".join(staticPaths))
    self.__routes = []

    # Add some standard paths for static files
    statDirectories = Conf.getStaticDirs()
    self.log.info("The following static directories are used:%s" % str(statDirectories))
    for stdir in statDirectories:
      pattern = '/%s/(.*)' % stdir
      self.__routes.append((pattern, StaticHandler, dict(pathList=['%s/webRoot/www/%s' % (rootPath, stdir)])))
      self.log.debug(" - Static route: %s" % pattern)

    for pattern in ((r"/static/(.*)", r"/(favicon\.ico)", r"/(robots\.txt)")):
      pattern = r"%s%s" % (self.__shySetupGroupRE, pattern)
      if self.__baseURL:
        pattern = "/%s%s" % (self.__baseURL, pattern)
      self.__routes.append((pattern, StaticHandler, dict(pathList=staticPaths)))
      self.log.debug(" - Static route: %s" % pattern)
    for hn in self.__handlers:
      self.log.info("Found handler %s" % hn)
      handler = self.__handlers[hn]
      if handler.__name__ == 'AuthHandler':
        self.__isAuthServer = True
      # CHeck it has AUTH_PROPS
      if isinstance(handler.AUTH_PROPS, type(None)):
        return S_ERROR("Handler %s does not have AUTH_PROPS defined. Fix it!" % hn)
      # Get the root for the handler
      if handler.LOCATION:
        handlerRoute = handler.LOCATION.strip("/") and "/%s" % handler.LOCATION.strip("/") or ''
      else:
        handlerRoute = hn[len(re.sub(r".[A-z]+$", "", hn)):].replace(".", "/").replace("Handler", "")
      # Add the setup group RE before
      baseRoute = self.__setupGroupRE
      # IF theres a base url like /DIRAC add it
      baseRoute = "/%s%s" % (self.__baseURL or '', baseRoute)
      # Set properly the LOCATION after calculating where it is with helpers to add group and setup later
      handler.LOCATION = handlerRoute
      handler.PATH_RE = re.compile("%s(%s/[A-z]+|.)" % (baseRoute, handlerRoute))
      # if handler.OVERPATH:
      #   handler.PATH_RE = re.compile(handler.PATH_RE.pattern + '(/[A-z0-9=-_/|]+)?')
      handler.URLSCHEMA = "/%s%%(setup)s%%(group)s%%(location)s/%%(action)s" % (self.__baseURL)
      if issubclass(handler, WebSocketHandler):
        handler.PATH_RE = re.compile("%s(%s)" % (baseRoute, handlerRoute))
        route = "%s(%s)" % (baseRoute, handlerRoute)
        self.__routes.append((route, handler))
        self.log.verbose(" - WebSocket %s -> %s" % (handlerRoute, hn))
        self.log.debug("  * %s" % route)
        continue
      # Look for methods that are exported
      for mName, mObj in inspect.getmembers(handler):
        if inspect.ismethod(mObj) and mName.find(handler.METHOD_PREFIX) == 0:
          methodName = mName[len(handler.METHOD_PREFIX):]
          args = getattr(handler, 'path_%s' % methodName, [])
          if mName == "web_index" and handler.__name__ == 'RootHandler':
            # Index methods have the bare url
            self.log.verbose(" - Route %s -> %s.web_index" % (handlerRoute, hn))
            route = "%s(%s/)" % (baseRoute, handlerRoute)
            self.__routes.append((route, handler))
            self.__routes.append(("%s(%s)" % (baseRoute, handlerRoute), CoreHandler, dict(action='addSlash')))
          else:
            # Normal methods get the method appended without web_
            self.log.verbose(" - Route %s/%s ->  %s.%s" % (handlerRoute, mName[4:], hn, mName))
            route = "%s(%s%s)" % (baseRoute, handlerRoute, '' if methodName == 'index' else ('/%s' % methodName))
            # Use request path as options/values, for ex. ../method/<option>/<file path>?<option>=..
            if args:
              route += r'[\/]?%s' % '/'.join(args)
            self.__routes.append((route, handler))
          self.log.debug("  * %s" % route)
    # Send to root
    self.__routes.append(("%s(/?)" % self.__setupGroupRE, CoreHandler, dict(action="sendToRoot")))
    if self.__baseURL:
      self.__routes.append(("/%s%s()" % (self.__baseURL, self.__setupGroupRE),
                            CoreHandler, dict(action="sendToRoot")))
    return S_OK()

  def getHandlers(self):
    """ Get handlers

        :return: S_OK()/S_ERROR()
    """
    if not self.__handlers:
      result = self.__calculateRoutes()
      if not result['OK']:
        return result
    return S_OK(self.__handlers)

  def getRoutes(self):
    """ Get routes

        :return: S_OK()/S_ERROR()
    """
    if not self.__routes:
      result = self.__calculateRoutes()
      if not result['OK']:
        return result
    return S_OK(self.__routes)

  def isAuthServer(self):
    return self.__isAuthServer

  def isPortal(self):
    return self.__isPortal
