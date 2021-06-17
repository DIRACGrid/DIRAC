"""
  This module contains the necessary tools to discover and load
  the handlers for serving HTTPS
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import inspect
from six import string_types
from six.moves.urllib.parse import urlparse
from tornado.web import url as TornadoURL, RequestHandler

from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


def urlFinder(module):
  """
    Tries to guess the url from module name.
    The URL would be of the form ``/System/Component`` (e.g. ``DataManagement/FileCatalog``)
    We search something which looks like ``<...>.<component>System.<...>.<service>Handler``

    :param module: full module name (e.g. "DIRAC.something.something")

    :returns: the deduced URL or None
  """
  sections = module.split('.')
  for section in sections:
    # This condition is a bit long
    # We search something which look like <...>.<component>System.<...>.<service>Handler
    # If find we return /<component>/<service>
    if section.endswith("System") and sections[-1].endswith("Handler"):
      return "/".join(["", section[:-len("System")], sections[-1][:-len("Handler")]])


class HandlerManager(object):
  """
    This utility class allows to load the handlers, generate the appropriate route,
    and discover the handlers based on the CS.
    In order for a service to be considered as using HTTPS, it must have
    ``protocol = https`` as an option.

    Each of the Handler will have one associated route to it:

    * Directly specified as ``LOCATION`` in the handler module
    * automatically deduced from the module name, of the form
      ``System/Component`` (e.g. ``DataManagement/FileCatalog``)
  """

  def __init__(self, services, endpoints):
    """
      Initialization function, you can set False for both arguments to prevent automatic
      discovery of handlers and use `loadServicesHandlers()` to
      load your handlers or `loadEndpointsHandlers()`

      :param services: List of service handlers to load.
          If ``True``, loads all services from CS
      :type services: bool or list
      :param endpoints: List of endpoint handlers to load.
          If ``True``, loads all endpoints from CS
      :type endpoints: bool or list
    """
    self.loader = None
    self.__handlers = {}
    self.__services = services
    self.__endpoints = endpoints
    self.__objectLoader = ObjectLoader()

  def __addHandler(self, handlerPath, handler, urls=None, port=None):
    """
      Function which add handler to list of known handlers

      :param str handlerPath: module name, e.g.: `Framework/Auth`
      :param object handler: handler class
      :param list urls: request path
      :param int port: port

      :return: S_OK()/S_ERROR()
    """
    # First of all check if we can find route
    # If urls is not given, try to discover it
    if urls is None:
      # FIRST TRY: Url is hardcoded
      try:
        urls = handler.LOCATION
      # SECOND TRY: URL can be deduced from path
      except AttributeError:
        gLogger.debug("No location defined for %s try to get it from path" % handlerPath)
        urls = urlFinder(handlerPath)

    if not urls:
      gLogger.warn("URL not found for %s" % (handlerPath))
      return S_ERROR("URL not found for %s" % (handlerPath))

    for url in urls if isinstance(urls, (list, tuple)) else [urls]:
      # We add "/" if missing at begin, e.g. we found "Framework/Service"
      # URL can't be relative in Tornado
      if url and not url.startswith('/'):
        url = "/%s" % url

      # Some new handler
      if handlerPath not in self.__handlers:
        gLogger.debug("Add new handler %s with port %s" % (handlerPath, port))
        self.__handlers[handlerPath] = {'URLs': [], 'Port': port}

      # Check if URL already loaded
      if (url, handler) in self.__handlers[handlerPath]['URLs']:
        gLogger.debug("URL: %s already loaded for %s " % (url, handlerPath))
        continue

      # Finally add the URL to handlers
      gLogger.info("Add new URL %s to %s handler" % (url, handlerPath))
      self.__handlers[handlerPath]['URLs'].append((url, handler))

    return S_OK()

  def discoverHandlers(self, handlerInstance):
    """
      Force the discovery of URL, automatic call when we try to get handlers for the first time.
      You can disable the automatic call with autoDiscovery=False at initialization

      :param str handlerInstance: handler instance, the name of the section in some system section e.g.:: Services, APIs

      :return: list
    """
    urls = []
    gLogger.debug("Trying to auto-discover the %s handlers for Tornado" % handlerInstance)

    # Look in config
    diracSystems = gConfig.getSections('/Systems')
    if diracSystems['OK']:
      for system in diracSystems['Value']:
        try:
          sysInstance = PathFinder.getSystemInstance(system)
          result = gConfig.getSections('/Systems/%s/%s/%s' % (system, sysInstance, handlerInstance))
          if result['OK']:
            for instName in result['Value']:
              newInst = ("%s/%s" % (system, instName))
              port = gConfig.getValue('/Systems/%s/%s/%s/%s/Port' % (system, sysInstance,
                                                                     handlerInstance, instName))
              if port:
                newInst += ':%s' % port

              if handlerInstance == 'Services':
                # We search in the CS all handlers which used HTTPS as protocol
                isHTTPS = gConfig.getValue('/Systems/%s/%s/%s/%s/Protocol' % (system, sysInstance,
                                                                              handlerInstance, instName))
                if isHTTPS and isHTTPS.lower() == 'https':
                  urls.append(newInst)
              else:
                urls.append(newInst)
        # On systems sometime you have things not related to services...
        except RuntimeError:
          pass
    return urls

  def loadServicesHandlers(self, services=None):
    """
      Load a list of handler from list of service using DIRAC moduleLoader
      Use :py:class:`DIRAC.Core.Base.private.ModuleLoader`

      :param services: List of service handlers to load. Default value set at initialization
          If ``True``, loads all services from CS
      :type services: bool or list

      :return: S_OK()/S_ERROR()
    """
    # list of services, e.g. ['Framework/Hello', 'Configuration/Server']
    if isinstance(services, string_types):
      services = [services]
    # list of services
    self.__services = self.__services if services is None else services if services else []

    if self.__services is True:
      self.__services = self.discoverHandlers('Services')

    if self.__services:
      # Extract ports
      ports, self.__services = self.__extractPorts(self.__services)

      self.loader = ModuleLoader("Service", PathFinder.getServiceSection, RequestHandler, moduleSuffix="Handler")

      # Use DIRAC system to load: search in CS if path is given and if not defined
      # it search in place it should be (e.g. in DIRAC/FrameworkSystem/Service)
      load = self.loader.loadModules(self.__services)
      if not load['OK']:
        return load
      for module in self.loader.getModules().values():
        url = module['loadName']

        # URL can be like https://domain:port/service/name or just service/name
        # Here we just want the service name, for tornado
        serviceTuple = url.replace('https://', '').split('/')[-2:]
        url = "%s/%s" % (serviceTuple[0], serviceTuple[1])
        self.__addHandler(module['loadName'], module['classObj'], url, ports.get(module['modName']))
    return S_OK()

  def __extractPorts(self, serviceURIs):
    """ Extract ports from serviceURIs

        :param list serviceURIs: list of uri that can contain port, .e.g:: System/Service:port

        :return: (dict, list)
    """
    portMapping = {}
    newURLs = []
    for _url in serviceURIs:
      if ':' in _url:
        urlTuple = _url.split(':')
        if urlTuple[0] not in portMapping:
          portMapping[urlTuple[0]] = urlTuple[1]
        newURLs.append(urlTuple[0])
      else:
        newURLs.append(_url)
    return (portMapping, newURLs)

  def loadEndpointsHandlers(self, endpoints=None):
    """
      Load a list of handler from list of endpoints using DIRAC moduleLoader
      Use :py:class:`DIRAC.Core.Base.private.ModuleLoader`

      :param endpoints: List of endpoint handlers to load. Default value set at initialization
          If ``True``, loads all endpoints from CS
      :type endpoints: bool or list

      :return: S_OK()/S_ERROR()
    """
    # list of endpoints, e.g. ['Framework/Auth', ...]
    if isinstance(endpoints, string_types):
      endpoints = [endpoints]
    # list of endpoints. If __endpoints is ``True`` then list of endpoints will dicover from CS
    self.__endpoints = self.__endpoints if endpoints is None else endpoints if endpoints else []

    if self.__endpoints is True:
      self.__endpoints = self.discoverHandlers('APIs')

    if self.__endpoints:
      # Extract ports
      ports, self.__endpoints = self.__extractPorts(self.__endpoints)

      self.loader = ModuleLoader("API", PathFinder.getAPISection, RequestHandler, moduleSuffix="Handler")

      # Use DIRAC system to load: search in CS if path is given and if not defined
      # it search in place it should be (e.g. in DIRAC/FrameworkSystem/API)
      load = self.loader.loadModules(self.__endpoints)
      if not load['OK']:
        return load
      for module in self.loader.getModules().values():
        handler = module['classObj']
        if not handler.LOCATION:
          handler.LOCATION = urlFinder(module['loadName'])
        urls = []
        # Look for methods that are exported
        for mName, mObj in inspect.getmembers(handler):
          if inspect.ismethod(mObj) and mName.find(handler.METHOD_PREFIX) == 0:
            methodName = mName[len(handler.METHOD_PREFIX):]
            args = getattr(handler, 'path_%s' % methodName, [])
            gLogger.debug(" - Route %s/%s ->  %s %s" % (handler.LOCATION, methodName, module['loadName'], mName))
            url = "%s%s" % (handler.LOCATION, '' if methodName == 'index' else ('/%s' % methodName))
            if args:
              url += r'[\/]?%s' % '/'.join(args)
            urls.append(url)
            gLogger.debug("  * %s" % url)
        self.__addHandler(module['loadName'], handler, urls, ports.get(module['modName']))
    return S_OK()

  def getHandlersURLs(self):
    """
      Get all handler for usage in Tornado, as a list of tornado.web.url
      If there is no handler found before, it try to find them

      :returns: a list of URL (not the string with "https://..." but the tornado object)
                see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
    """
    urls = []
    for handlerData in self.__handlers.values():
      for url in handlerData['URLs']:
        urls.append(TornadoURL(*url))
    return urls

  def getHandlersDict(self):
    """
      Return all handler dictionary

      :returns: dictionary with service name as key ("System/Service")
                and tornado.web.url objects as value for 'URLs' key
                and port as value for 'Port' key
    """
    return self.__handlers
