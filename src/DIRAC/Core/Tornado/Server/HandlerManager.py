"""
  This module contains the necessary tools to discover and load
  the handlers for serving HTTPS
"""
from tornado.web import RequestHandler

from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader


class HandlerManager:
    """
    This utility class allows to load the handlers, generate the appropriate route,
    and discover the handlers based on the CS.
    In order for a service to be considered as using HTTPS, it must have
    ``protocol = https`` as an option.

    Each of the Handler will have one associated route to it:

    * Directly specified as ``DEFAULT_LOCATION`` in the handler module
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
        self.__handlers = {}
        self.instances = dict(Service=services, API=endpoints)

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
        diracSystems = gConfig.getSections("/Systems")
        if diracSystems["OK"]:
            for system in diracSystems["Value"]:
                try:
                    sysInstance = PathFinder.getSystemInstance(system)
                    result = gConfig.getSections(f"/Systems/{system}/{sysInstance}/{handlerInstance}")
                    if result["OK"]:
                        for instName in result["Value"]:
                            newInst = f"{system}/{instName}"
                            port = gConfig.getValue(
                                f"/Systems/{system}/{sysInstance}/{handlerInstance}/{instName}/Port"
                            )
                            if port:
                                newInst += ":%s" % port

                            if handlerInstance == "Services":
                                # We search in the CS all handlers which used HTTPS as protocol
                                isHTTPS = gConfig.getValue(
                                    f"/Systems/{system}/{sysInstance}/{handlerInstance}/{instName}/Protocol"
                                )
                                if isHTTPS and isHTTPS.lower() == "https":
                                    urls.append(newInst)
                            else:
                                urls.append(newInst)
                # On systems sometime you have things not related to services...
                except RuntimeError:
                    pass
        return urls

    def __load(self, instances, componentType, pathFinder):
        """Load a list of handler from list of given instances using DIRAC moduleLoader
        Use :py:class:`DIRAC.Core.Base.private.ModuleLoader`

        :return: S_OK()/S_ERROR()
        """
        # list of instances, e.g. ['Framework/Hello', 'Configuration/Server']
        if isinstance(instances, str):
            # make sure we have a list
            instances = [instances]

        instances = self.instances[componentType] if instances is None else instances if instances else []

        # `True` means automatically view the configuration
        if instances is True:
            instances = self.discoverHandlers(f"{componentType}s")
        if not instances:
            return S_OK()

        # Extract ports, e.g.: ['Framework/MyService', 'Framework/MyService2:9443]
        port, instances = self.__extractPorts(instances)

        loader = ModuleLoader(componentType, pathFinder, RequestHandler, moduleSuffix="Handler")

        # Use DIRAC system to load: search in CS if path is given and if not defined
        # it search in place it should be (e.g. in DIRAC/FrameworkSystem/< component type >)
        result = loader.loadModules(instances)
        if result["OK"]:
            for module in loader.getModules().values():
                handler = module["classObj"]
                fullComponentName = module["modName"]

                # Define the system and component name as the attributes of the handler that belongs to them
                handler.SYSTEM_NAME, handler.COMPONENT_NAME = fullComponentName.split("/")

                gLogger.info("Found new handler", f"{fullComponentName}: {handler}")

                # at this stage we run the basic handler initialization
                # see DIRAC.Core.Tornado.Server.private.BaseRequestHandler for more details
                # this method should return a list of routes associated with the handler, it is a regular expressions
                # see https://www.tornadoweb.org/en/stable/routing.html#tornado.routing.URLSpec, ``pattern`` argument.
                urls = handler._BaseRequestHandler__pre_initialize()

                # First of all check if we can find route
                if not urls:
                    gLogger.warn(f"URL not found for {fullComponentName}")
                    return S_ERROR(f"URL not found for {fullComponentName}")

                # Add new handler routes
                self.__handlers[fullComponentName] = dict(URLs=list(set(urls)), Port=port.get(fullComponentName))

        return result

    def loadServicesHandlers(self, services=None):
        """Load services

        :param services: List of service handlers to load. Default value set at initialization
            If ``True``, loads all services from CS
        :type services: bool or list

        :return: S_OK()/S_ERROR()
        """
        return self.__load(services, "Service", PathFinder.getServiceSection)

    def loadEndpointsHandlers(self, endpoints=None):
        """Load endpoints

        :param endpoints: List of endpoint handlers to load. Default value set at initialization
            If ``True``, loads all endpoints from CS
        :type endpoints: bool or list

        :return: S_OK()/S_ERROR()
        """
        return self.__load(endpoints, "API", PathFinder.getAPISection)

    def getHandlersURLs(self):
        """
        Get all handler for usage in Tornado, as a list of tornado.web.url

        :returns: a list of URL (not the string with "https://..." but the tornado object)
                  see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
        """
        urls = []
        for handler in self.__handlers:
            urls += self.__handlers[handler]["URLs"]
        return urls

    def getHandlersDict(self):
        """
        Return all handler dictionary

        :returns: dictionary with service name as key ("System/Service")
                  and tornado.web.url objects as value for 'URLs' key
                  and port as value for 'Port' key
        """
        return self.__handlers

    def __extractPorts(self, serviceURIs: list) -> tuple:
        """Extract ports from serviceURIs

        :param list serviceURIs: list of uri that can contain port, .e.g:: System/Service:port

        :return: (dict, list)
        """
        portMapping = {}
        newURLs = []
        for _url in serviceURIs:
            if ":" in _url:
                urlTuple = _url.split(":")
                if urlTuple[0] not in portMapping:
                    portMapping[urlTuple[0]] = urlTuple[1]
                newURLs.append(urlTuple[0])
            else:
                newURLs.append(_url)
        return (portMapping, newURLs)
