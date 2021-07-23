""" Collection of utilities for finding paths in the CS
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from six.moves.urllib import parse as urlparse

from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData


def getDIRACSetup():
  """ Get DIRAC default setup name

      :return: str
  """
  return gConfigurationData.extractOptionFromCFG("/DIRAC/Setup")


def divideFullName(entityName):
  """ Convert component full name to tuple

      :param str entityName: component full name, e.g.: 'Framework/ProxyManager'

      :return: tuple -- contain system and service name
  """
  fields = [field.strip() for field in entityName.split("/")]
  if len(fields) < 2:
    raise RuntimeError("Service (%s) name must be with the form system/service" % entityName)
  return tuple(fields)


def getSystemInstance(systemName, setup=False):
  """ Find system instance name

      :param str systemName: system name
      :param str setup: setup name

      :return: str
  """
  if not setup:
    setup = gConfigurationData.extractOptionFromCFG("/DIRAC/Setup")
  optionPath = "/DIRAC/Setups/%s/%s" % (setup, systemName)
  instance = gConfigurationData.extractOptionFromCFG(optionPath)
  if instance:
    return instance
  else:
    raise RuntimeError("Option %s is not defined" % optionPath)


# TODO: serviceTuple here for backward compatibility and must be deleted in the next release(v7r4)
def getSystemSection(system, serviceTuple=False, instance=False, setup=False):
  """ Get system section

      :param str system: system name or full name e.g.: Framework/ProxyManager
      :param serviceTuple: unuse!
      :param str instance: instance name
      :param str setup: setup name

      :return: str -- system section path
  """
  if '/' in system:
    system, _ = divideFullName(system)
  if not instance:
    instance = getSystemInstance(system, setup=setup)
  return "/Systems/%s/%s" % (system, instance)


def getComponentSection(componentName, componentTuple=False, setup=False, componentCategory="Services"):
  """Function returns the path to the component.


  :param str componentName: Component name prefixed by the system in which it is placed.
                            e.g. 'WorkloadManagement/SandboxStoreHandler'
  :param tuple componentTuple: Path of the componenent already divided
                               e.g. ('WorkloadManagement', 'SandboxStoreHandler')
  :param str setup: Name of the setup.
  :param str componentCategory: Category of the component, it can be: 'Agents', 'Services', 'Executors'
                                or 'Databases'.

  :return str: Complete path to the component

  :raise RuntimeException: If in the componentName - the system part does not correspond to any known system in DIRAC.

  Example:
    getComponentSection('WorkloadManagement/SandboxStoreHandler', False,False,'Services')
  """
  system, service = componentTuple
  if not componentTuple:
    system, service = divideFullName(componentName)
  systemSection = getSystemSection(system, setup=setup)
  return "%s/%s/%s" % (systemSection, componentCategory, service)


def getServiceSection(serviceName, serviceTuple=False, setup=False):
  return getComponentSection(serviceName, serviceTuple, setup, "Services")


def getAgentSection(agentName, agentTuple=False, setup=False):
  return getComponentSection(agentName, agentTuple, setup, "Agents")


def getExecutorSection(executorName, executorTuple=False, setup=False):
  return getComponentSection(executorName, executorTuple, setup, "Executors")


def getDatabaseSection(dbName, dbTuple=False, setup=False):
  return getComponentSection(dbName, dbTuple, setup, "Databases")


def getSystemURLSection(serviceName, serviceTuple=False, setup=False):
  systemSection = getSystemSection(serviceName, setup=setup)
  return "%s/URLs" % systemSection


def checkServiceURL(url, system=None, service=None):
  """ Check service URL

      :param str url: full URL, e.g.: dips://some-domain:3424/Framework/Service
      :param str system: system name
      :param str service: service name

      :return: str
  """
  url = urlparse.urlparse(url)
  # Check port
  if not url.port:
    if url.scheme == 'disp':
      raise RuntimeError('No port found for %s/%s URL!' % (system, service))
    url._replace(netloc=url.netloc + ':' + (80 if url.scheme == 'http' else 443))
  # Check path
  if not url.path.strip('/'):
    if not system or not service:
      raise RuntimeError('No path found for %s/%s URL!' % (system, service))
    url._replace(path='/%s/%s' % (system, service))
  return url.geturl()


def getSystemURLs(system, setup=False):
  """
    Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str setup: DIRAC setup name, can be defined in dirac.cfg

    :return: dict -- complete urls. e.g. [dips://some-domain:3424/Framework/Service]
  """
  urlDict = {}
  systemSection = getSystemSection(system, setup=setup)
  for service in gConfigurationData.getOptionsFromCFG("%s/URLs" % systemSection, service):
    urlDict[service] = getServiceURLs(system, service, setup=setup)
  return urlDict


def getServiceURLs(system, service='', setup=False):
  """
    Generate url.

    :param str system: system name or full name e.g.: Framework/ProxyManager
    :param str service: service name, like 'ProxyManager'.
    :param str setup: DIRAC setup name, can be defined in dirac.cfg

    :return: list -- complete urls. e.g. [dips://some-domain:3424/Framework/Service]
  """
  if '/' in system:
    system, service = divideFullName(system)
  urlList = []
  mainServers = None
  systemSection = getSystemSection(system, setup=setup)
  urls = List.fromChar(gConfigurationData.extractOptionFromCFG("%s/URLs/%s" % (systemSection, service)))
  for url in urls:
    # Trying if we are refering to the list of main servers
    # which would be like dips://$MAINSERVERS$:1234/System/Component
    if '$MAINSERVERS$' in url:
      if not mainServers:
        # Operations cannot be imported at the beginning because of a bootstrap problem
        from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
        mainServers = Operations().getValue('MainServers', [])
        if not mainServers:
          raise Exception("No Main servers defined")

        for srv in mainServers:
          urlList.append(checkServiceURL(url.replace('$MAINSERVERS$', srv), system, service))
        continue
    urlList.append(checkServiceURL(url, system, service))

  return list(set(urlList))


def getServiceURL(serviceName, serviceTuple=False, setup=False):
  """
    Generate url.

    :param serviceName: Name of service, like 'Framework/Service'.
    :param serviceTuple: (optional) also name of service but look like ('Framework', 'Service').
    :param str setup: DIRAC setup name, can be defined in dirac.cfg

    :return: str -- complete list of urls. e.g. dips://some-domain:3424/Framework/Service, dips://..
  """
  urls = getServiceURLs(serviceName, setup=setup)
  return ','.join(urls) if urls else ""


def getServiceFailoverURL(serviceName, serviceTuple=False, setup=False):
  """ Get failover URLs for service

      :param str serviceName: Name of service, like 'Framework/Service'.
      :param str serviceTuple:(optional) also name of service but look like ('Framework', 'Service').
      :param str setup: DIRAC setup name, can be defined in dirac.cfg

      :return: str -- complete list of urls
  """
  system, service = serviceTuple
  if not serviceTuple:
    system, service = divideFullName(serviceName)
  systemSection = getSystemSection(system, setup=setup)
  url = gConfigurationData.extractOptionFromCFG("%s/FailoverURLs/%s" % (systemSection, service))
  if not url:
    return ""
  if len(url.split("/")) < 5:
    url = "%s/%s" % (url, serviceName)
  return url


def getGatewayURLs(serviceName=""):
  """ Get gateway URLs for service

      :param str serviceName: service name

      :return: list or False
  """
  siteName = gConfigurationData.extractOptionFromCFG("/LocalSite/Site")
  if not siteName:
    return False
  gatewayList = gConfigurationData.extractOptionFromCFG("/DIRAC/Gateways/%s" % siteName)
  if not gatewayList:
    return False
  gatewayList = List.fromChar(gatewayList, ",")
  if serviceName:
    gatewayList = ["%s/%s" % ("/".join(gw.split("/")[:3]), serviceName) for gw in gatewayList]
  return List.randomize(gatewayList)
