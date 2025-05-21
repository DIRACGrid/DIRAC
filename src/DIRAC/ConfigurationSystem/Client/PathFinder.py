""" Collection of utilities for finding paths in the CS
"""
import re
from copy import deepcopy
from collections.abc import Iterable
from urllib import parse

from cachetools import cached, TTLCache

from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers import Path


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
    raise RuntimeError(f"Service ({entityName}) name must be with the form system/service")


def getComponentSection(system, component=False, componentCategory="Services"):
    """Function returns the path to the component.

    :param str system: system name or component name prefixed by the system in which it is placed.
                       e.g. 'WorkloadManagement/SandboxStoreHandler'
    :param str component: component name, e.g. 'SandboxStoreHandler'
    :param str componentCategory: Category of the component, it can be:
                                  'Agents', 'Services', 'Executors' or 'Databases'.

    :return: Complete path to the component
    :rtype: str

    :raise RuntimeException: If in the system - the system part does not correspond to any known system in DIRAC.

    Examples:
      getComponentSection('WorkloadManagement/SandboxStoreHandler', componentCategory='Services')
      getComponentSection('WorkloadManagement', 'SandboxStoreHandler')
    """
    system, component = divideFullName(system, component)
    return Path.cfgPath(f"/Systems/{system}", componentCategory, component)


def getAPISection(system, endpointName=False):
    """Get API section in a system

    :param str system: system name
    :param str endpointName: endpoint name

    :return: str
    """
    return getComponentSection(system, component=endpointName, componentCategory="APIs")


def getServiceSection(system, serviceName=False):
    """Get service section in a system

    :param str system: system name
    :param str serviceName: service name

    :return: str
    """
    return getComponentSection(system, component=serviceName)


def getAgentSection(system, agentName=False):
    """Get agent section in a system

    :param str system: system name
    :param str agentName: agent name

    :return: str
    """
    return getComponentSection(system, component=agentName, componentCategory="Agents")


def getExecutorSection(system, executorName=None):
    """Get executor section in a system

    :param str system: system name
    :param str executorName: executor name

    :return: str
    """
    return getComponentSection(system, component=executorName, componentCategory="Executors")


def getDatabaseSection(system, dbName=False):
    """Get DB section in a system

    :param str system: system name
    :param str dbName: DB name

    :return: str
    """
    return getComponentSection(system, component=dbName, componentCategory="Databases")


def getSystemURLSection(system):
    """Get URLs section in a system

    :param str system: system name

    :return: str
    """
    return Path.cfgPath(f"/Systems/{system}", "URLs")


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


def getSystemURLs(system, failover=False):
    """Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param bool failover: to add failover URLs to end of result list

    :return: dict -- complete urls. e.g. [dips://some-domain:3424/Framework/Service]
    """
    urlDict = {}
    for service in gConfigurationData.getOptionsFromCFG(f"/Systems/{system}/URLs") or []:
        urlDict[service] = getServiceURLs(system, service, failover=failover)
    return urlDict


def groupURLsByPriority(urls: Iterable[str]) -> list[set[str]]:
    """Group URLs by priority.

    :param Iterable[str] preferredURLPatterns: patterns to check in ranked order
    :param set[str] urls: URLs to check

    :return: list[set[str]] -- list of URL groups, ordered by priority
    """
    return deepcopy(_groupURLsByPriority(frozenset(urls)))


@cached(cache=TTLCache(maxsize=1024, ttl=300))
def _groupURLsByPriority(urls: frozenset[str]) -> list[set[str]]:
    preferredURLPatterns = []
    if patterns := gConfigurationData.extractOptionFromCFG("/DIRAC/PreferredURLPatterns"):
        preferredURLPatterns = [re.compile(pattern) for pattern in List.fromChar(patterns)]

    urlGroups = [set() for _ in range(len(preferredURLPatterns) + 1)]
    for url in urls:
        urlGroups[findURLPriority(preferredURLPatterns, url)].add(url)
    return urlGroups


def findURLPriority(preferredURLPatterns: list[re.Pattern[str]], url: str) -> int:
    """Find which preferred URL pattern the URL matches.

    :param str preferredURLPatterns: patterns to check in ranked order
    :param str url: URL to check

    :return: int -- index of the pattern that matched, smallest is the most preferred
    """
    for i, pattern in enumerate(preferredURLPatterns):
        if re.match(pattern, url):
            return i
    return len(preferredURLPatterns)


def getServiceURLs(system, service=None, failover=False):
    """Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str service: service name, like 'ProxyManager'.
    :param bool failover: to add failover URLs to end of result list

    :return: list -- complete urls. e.g. [dips://some-domain:3424/Framework/Service]
    """
    system, service = divideFullName(system, service)
    resList = []
    mainServers = None
    systemSection = f"/Systems/{system}"

    # Add failover URLs at the end of the list
    failover = "Failover" if failover else ""
    for fURLs in ["", "Failover"] if failover else [""]:
        urls = List.fromChar(gConfigurationData.extractOptionFromCFG(f"{systemSection}/{fURLs}URLs/{service}"))
        urlList = set()

        # Be sure that urls not None
        for url in urls or []:
            # Trying if we are refering to the list of main servers
            # which would be like dips://$MAINSERVERS$:1234/System/Component
            if "$MAINSERVERS$" in url:
                if not mainServers:
                    # Operations cannot be imported at the beginning because of a bootstrap problem
                    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

                    mainServers = Operations().getValue("MainServers", [])
                if not mainServers:
                    raise Exception("No Main servers defined")

                for srv in mainServers:
                    _url = checkComponentURL(url.replace("$MAINSERVERS$", srv), system, service, pathMandatory=True)
                    urlList.add(_url)
                continue

            urlList.add(checkComponentURL(url, system, service, pathMandatory=True))

        for urlGroup in groupURLsByPriority(urlList):
            resList.extend(List.randomize(urlGroup))

    return resList


def useLegacyAdapter(system, service=None) -> bool:
    """Should DiracX be used for this service via the legacy adapter mechanism

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str service: service name, like 'ProxyManager'.

    :return: bool -- True if DiracX should be used
    """
    system, service = divideFullName(system, service)
    value = gConfigurationData.extractOptionFromCFG(f"/DiracX/LegacyClientEnabled/{system}/{service}")
    return (value or "no").lower() in ("y", "yes", "true", "1")


def getServiceURL(system, service=None):
    """Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str service: service name, like 'ProxyManager'.

    :return: str -- complete list of urls. e.g. dips://some-domain:3424/Framework/Service, dips://..
    """
    system, service = divideFullName(system, service)
    urls = getServiceURLs(system, service=service)
    return ",".join(urls) if urls else ""


def getServiceFailoverURL(system, service=None):
    """Get failover URLs for service

    :param str system: system name or full name, like 'Framework/Service'.
    :param str service: service name, like 'ProxyManager'.

    :return: str -- complete list of urls
    """
    system, service = divideFullName(system, service)
    systemSection = f"/Systems/{system}"
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
    gateways = gConfigurationData.extractOptionFromCFG(f"/DIRAC/Gateways/{siteName}")
    if not gateways:
        return False
    gateways = List.randomize(List.fromChar(gateways, ","))
    return [checkComponentURL(u, system, service) for u in gateways if u] if system and service else gateways


def getDisabledDiracxVOs() -> list[str]:
    """Get the list of VOs for which DiracX is enabled"""
    vos = gConfigurationData.extractOptionFromCFG("/DiracX/DisabledVOs")
    return List.fromChar(vos or "", ",")
