""" Collection of utilities for finding paths in the CS
"""
from urllib import parse

from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers import Path


def getDIRACSetup():
    """Get DIRAC default setup name

    :return: str
    """
    return gConfigurationData.extractOptionFromCFG("/DIRAC/Setup")


def divideFullName(entityName, componentName=None):
    """Convert component full name to tuple

    :param str entityName: component full name, e.g.: 'Framework/ProxyManager'
    :param str componentName: component name

    :return: tuple -- contain system and component name
    """
    entityName = entityName.strip("/")
    if entityName and "/" not in entityName and componentName:
        return (entityName, componentName)
    fields = [field.strip() for field in entityName.split("/") if field.strip()]
    if len(fields) == 2:
        return tuple(fields)
    raise RuntimeError("Service (%s) name must be with the form system/service" % entityName)


def getSystemInstance(system, setup=False):
    """Find system instance name

    :param str system: system name
    :param str setup: setup name

    :return: str
    """
    optionPath = Path.cfgPath("/DIRAC/Setups", setup or getDIRACSetup(), system)
    instance = gConfigurationData.extractOptionFromCFG(optionPath)
    if not instance:
        raise RuntimeError("Option %s is not defined" % optionPath)
    return instance


def getSystemSection(system, instance=False, setup=False):
    """Get system section

    :param str system: system name
    :param str instance: instance name
    :param str setup: setup name

    :return: str -- system section path
    """
    system, _ = divideFullName(system, "_")  # for backward compatibility
    return Path.cfgPath(
        "/Systems",
        system,
        instance or getSystemInstance(system, setup=setup),
    )


def getComponentSection(system, component=False, setup=False, componentCategory="Services"):
    """Function returns the path to the component.

    :param str system: system name or component name prefixed by the system in which it is placed.
                       e.g. 'WorkloadManagement/SandboxStoreHandler'
    :param str component: component name, e.g. 'SandboxStoreHandler'
    :param str setup: Name of the setup.
    :param str componentCategory: Category of the component, it can be:
                                  'Agents', 'Services', 'Executors' or 'Databases'.

    :return: Complete path to the component
    :rtype: str

    :raise RuntimeException: If in the system - the system part does not correspond to any known system in DIRAC.

    Examples:
      getComponentSection('WorkloadManagement/SandboxStoreHandler', setup='Production', componentCategory='Services')
      getComponentSection('WorkloadManagement', 'SandboxStoreHandler', 'Production')
    """
    system, component = divideFullName(system, component)
    return Path.cfgPath(getSystemSection(system, setup=setup), componentCategory, component)


def getAPISection(system, endpointName=False, setup=False):
    """Get API section in a system

    :param str system: system name
    :param str endpointName: endpoint name

    :return: str
    """
    return getComponentSection(system, component=endpointName, setup=setup, componentCategory="APIs")


def getServiceSection(system, serviceName=False, setup=False):
    """Get service section in a system

    :param str system: system name
    :param str serviceName: service name
    :param str setup: setup name

    :return: str
    """
    return getComponentSection(system, component=serviceName, setup=setup)


def getAgentSection(system, agentName=False, setup=False):
    """Get agent section in a system

    :param str system: system name
    :param str agentName: agent name
    :param str setup: setup name

    :return: str
    """
    return getComponentSection(system, component=agentName, setup=setup, componentCategory="Agents")


def getExecutorSection(system, executorName=None, component=False, setup=False):
    """Get executor section in a system

    :param str system: system name
    :param str executorName: executor name
    :param str setup: setup name

    :return: str
    """
    return getComponentSection(system, component=executorName, setup=setup, componentCategory="Executors")


def getDatabaseSection(system, dbName=False, setup=False):
    """Get DB section in a system

    :param str system: system name
    :param str dbName: DB name
    :param str setup: setup name

    :return: str
    """
    return getComponentSection(system, component=dbName, setup=setup, componentCategory="Databases")


def getSystemURLSection(system, setup=False):
    """Get URLs section in a system

    :param str system: system name
    :param str setup: setup name

    :return: str
    """
    return Path.cfgPath(getSystemSection(system, setup=setup), "URLs")


def checkComponentURL(componentURL, system=None, component=None, pathMandatory=False):
    """Check component URL port and path. Set default ports for http scheme and raise if no port can be found.
    Set path if its mandatory or raise if its empty in this case.

    :param str componentURL: full URL, e.g.: dips://some-domain:3424/Framework/Service
    :param str system: system name
    :param str component: component name
    :param bool pathMandatory: raise error if the path could not be generated

    :return: str
    """
    url = parse.urlparse(componentURL)
    # Check port
    if not url.port:
        if url.scheme == "dips":
            raise RuntimeError(f"No port found for {system}/{component} URL!")
        url = url._replace(netloc=url.netloc + ":" + str(80 if url.scheme == "http" else 443))
    # Check path
    if not url.path.strip("/"):
        if system and component:
            url = url._replace(path=f"/{system}/{component}")
        elif pathMandatory:
            raise RuntimeError(f"No path found for {system}/{component} URL!")
    return url.geturl()


def getSystemURLs(system, setup=False, failover=False):
    """Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str setup: DIRAC setup name, can be defined in dirac.cfg
    :param bool failover: to add failover URLs to end of result list

    :return: dict -- complete urls. e.g. [dips://some-domain:3424/Framework/Service]
    """
    urlDict = {}
    for service in gConfigurationData.getOptionsFromCFG("%s/URLs" % getSystemSection(system, setup=setup)) or []:
        urlDict[service] = getServiceURLs(system, service, setup=setup, failover=failover)
    return urlDict


def getServiceURLs(system, service=None, setup=False, failover=False):
    """Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str service: service name, like 'ProxyManager'.
    :param str setup: DIRAC setup name, can be defined in dirac.cfg
    :param bool failover: to add failover URLs to end of result list

    :return: list -- complete urls. e.g. [dips://some-domain:3424/Framework/Service]
    """
    system, service = divideFullName(system, service)
    resList = []
    mainServers = None
    systemSection = getSystemSection(system, setup=setup)

    # Add failover URLs at the end of the list
    failover = "Failover" if failover else ""
    for fURLs in ["", "Failover"] if failover else [""]:
        urlList = []
        urls = List.fromChar(gConfigurationData.extractOptionFromCFG(f"{systemSection}/{fURLs}URLs/{service}"))

        # Be sure that urls not None
        for url in urls or []:

            # Trying if we are refering to the list of main servers
            # which would be like dips://$MAINSERVERS$:1234/System/Component
            if "$MAINSERVERS$" in url:
                if not mainServers:
                    # Operations cannot be imported at the beginning because of a bootstrap problem
                    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

                    mainServers = Operations(setup=setup).getValue("MainServers", [])
                if not mainServers:
                    raise Exception("No Main servers defined")

                for srv in mainServers:
                    _url = checkComponentURL(url.replace("$MAINSERVERS$", srv), system, service, pathMandatory=True)
                    if _url not in urlList:
                        urlList.append(_url)
                continue

            _url = checkComponentURL(url, system, service, pathMandatory=True)
            if _url not in urlList:
                urlList.append(_url)

        # Randomize list if needed
        resList.extend(List.randomize(urlList))

    return resList


def getServiceURL(system, service=None, setup=False):
    """Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str service: service name, like 'ProxyManager'.
    :param str setup: DIRAC setup name, can be defined in dirac.cfg

    :return: str -- complete list of urls. e.g. dips://some-domain:3424/Framework/Service, dips://..
    """
    system, service = divideFullName(system, service)
    urls = getServiceURLs(system, service=service, setup=setup)
    return ",".join(urls) if urls else ""


def getServiceFailoverURL(system, service=None, setup=False):
    """Get failover URLs for service

    :param str system: system name or full name, like 'Framework/Service'.
    :param str service: service name, like 'ProxyManager'.
    :param str setup: DIRAC setup name, can be defined in dirac.cfg

    :return: str -- complete list of urls
    """
    system, service = divideFullName(system, service)
    systemSection = getSystemSection(system, setup=setup)
    failovers = gConfigurationData.extractOptionFromCFG(f"{systemSection}/FailoverURLs/{service}")
    if not failovers:
        return ""
    return ",".join([checkComponentURL(u, system, service) for u in List.fromChar(failovers, ",") if u])


def getGatewayURLs(system="", service=None):
    """Get gateway URLs for service

    :param str system: system name or full name, like 'Framework/Service'.
    :param str service: service name, like 'ProxyManager'.

    :return: list or False
    """
    if system:
        system, service = divideFullName(system, service)
    siteName = gConfigurationData.extractOptionFromCFG("/LocalSite/Site")
    if not siteName:
        return False
    gateways = gConfigurationData.extractOptionFromCFG("/DIRAC/Gateways/%s" % siteName)
    if not gateways:
        return False
    gateways = List.randomize(List.fromChar(gateways, ","))
    return [checkComponentURL(u, system, service) for u in gateways if u] if system and service else gateways
