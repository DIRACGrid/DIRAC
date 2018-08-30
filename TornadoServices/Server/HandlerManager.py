"""

  HandlerManager for tornado
  This class search in the CS all services who use HTTPS and load them.

  Must be used with the TornadoServer

"""

from tornado.web import url as TornadoURL, RequestHandler

from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC import gLogger, S_ERROR, S_OK, gConfig
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
from DIRAC.ConfigurationSystem.Client import PathFinder

def urlFinder(module):
  """
    Try to guess the url with module name

    :param module: path writed like import (e.g. "DIRAC.something.something")
  """
  sections = module.split('.')
  for section in sections:
    # This condition is a bit long
    # We search something which look like <...>.<component>System.<...>.<service>Handler
    # If find we return /<component>/<service>
    if(section.find("System") > 0) and (sections[-1].find('Handler') > 0):
      return "/%s/%s" % (section.replace("System", ""), sections[-1].replace("Handler", ""))
  return None

class HandlerManager(object):
  """
    This class is designed to work with Tornado

    It search and loads the handlers of services
  """

  def __init__(self, autoDiscovery=True, setup=None):
    """
      Initialization function, you can set autoDiscovery=False to prevent automatic
      discovery of handler. If disabled you can use loadHandlersByServiceName() to
      load your handlers or loadHandlerInHandlerManager()
    """
    self.__handlers = {}
    self.__objectLoader = ObjectLoader()
    self.setup = setup
    self.__autoDiscovery = autoDiscovery
    self.loader = ModuleLoader("Service", PathFinder.getServiceSection, RequestHandler, moduleSuffix="Handler")

  def __addHandler(self, handlerTuple, url=None):
    """
      Function who add handler to list of known handlers


      :param handlerTuple: (path, class)
    """
    # Check if handler not already loaded
    if not url or url not in self.__handlers:
      gLogger.debug("Find new handler %s" % (handlerTuple[0]))

      # If url is not given, try to discover it
      if url is None:
        # FIRST TRY: Url is hardcoded
        try:
          url = handlerTuple[1].LOCATION
        # SECOND TRY: URL can be deduced from path
        except AttributeError:
          gLogger.debug("No location defined for %s try to get it from path" % handlerTuple[0])
          url = urlFinder(handlerTuple[0])

      # We add "/" if missing at begin, e.g. we found "Framework/Service"
      # URL can't be relative in Tornado
      if url and not url.startswith('/'):
        url = "/%s" % url
      elif not url:
        gLogger.warn("URL not found for %s" % (handlerTuple[0]))
        return S_ERROR("URL not found for %s" % (handlerTuple[0]))

      # Finally add the URL to handlers
      if url not in self.__handlers:
        self.__handlers[url] = handlerTuple[1]
        gLogger.info("New handler: %s with URL %s" % (handlerTuple[0], url))
    else:
      gLogger.debug("Handler already loaded %s" % (handlerTuple[0]))
    return S_OK

  def discoverHandlers(self):
    """
      Force the discovery of URL, automatic call when we try to get handlers for the first time.
      You can disable the automatic call with autoDiscovery=False at initialization
    """
    gLogger.debug("Trying to auto-discover the handlers for Tornado")

    # Look in config
    diracSystems = gConfig.getSections('/Systems')
    serviceList = []
    if diracSystems['OK']:
      for system in diracSystems['Value']:
        try:
          instance = PathFinder.getSystemInstance(system)
          services = gConfig.getSections('/Systems/%s/%s/Services' % (system, instance))
          if services['OK']:
            for service in services['Value']:
              newservice = ("%s/%s" % (system, service))

              # We search in the CS all handlers who used HTTPS as protocol
              isHTTPS = gConfig.getValue('/Systems/%s/%s/Services/%s/Protocol' % (system, instance, service))
              if isHTTPS and isHTTPS.lower() == 'https':
                serviceList.append(newservice)
        # On systems sometime you have things not related to services...
        except RuntimeError as e:
          pass
    return self.loadHandlersByServiceName(serviceList)

  def loadHandlersByServiceName(self, servicesNames):
    """
      Load a list of handler from list of service using DIRAC moduleLoader
      Use :py:class:`DIRAC.Core.Base.private.ModuleLoader`

      :param servicesNames: list of service, e.g. ['Framework/Hello', 'Configuration/Server']
    """

    # Use DIRAC system to load: search in CS if path is given and if not defined
    # it search in place it should be (e.g. in DIRAC/FrameworkSystem/Service)
    if not isinstance(servicesNames, list):
      servicesNames = [servicesNames]

    load = self.loader.loadModules(servicesNames)
    if not load['OK']:
      return load
    for module in self.loader.getModules().values():
      url = module['loadName']

      # URL can be like https://domain:port/service/name or just service/name
      # Here we just want the service name, for tornado
      serviceTuple = url.replace('https://', '').split('/')[-2:]
      url = "%s/%s" % (serviceTuple[0], serviceTuple[1])
      self.__addHandler((module['loadName'], module['classObj']), url)
    return S_OK()


  def getHandlersURLs(self):
    """
      Get all handler for usage in Tornado, as a list of tornado.web.url
      If there is no handler found before, it try to find them

      :returns: a list of URL (not the string with "https://..." but the tornado object)
                see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
    """
    if not self.__handlers and self.__autoDiscovery:
      self.__autoDiscovery = False
      self.discoverHandlers()
    urls = []
    for key in self.__handlers:
      urls.append(TornadoURL(key, self.__handlers[key]))
    return urls

  def getHandlersDict(self):
    """
      Return all handler dictionnary

      :returns: dictionnary with absolute url as key ("/System/Service") 
                and tornado.web.url object as value
    """
    if not self.__handlers and self.__autoDiscovery:
      self.__autoDiscovery = False
      self.discoverHandlers()
    return self.__handlers
