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
  return gConfigurationData.extractOptionFromCFG("/DIRAC/Setup")


def divideFullName(entityName):
  fields = [field.strip() for field in entityName.split("/")]
  if len(fields) < 2:
    raise RuntimeError("Service (%s) name must be with the form system/service" % entityName)
  return tuple(fields)


def getSystemInstance(systemName, setup=False):
  if not setup:
    setup = gConfigurationData.extractOptionFromCFG("/DIRAC/Setup")
  optionPath = "/DIRAC/Setups/%s/%s" % (setup, systemName)
  instance = gConfigurationData.extractOptionFromCFG(optionPath)
  if instance:
    return instance
  else:
    raise RuntimeError("Option %s is not defined" % optionPath)


def getSystemSection(serviceName, serviceTuple=False, instance=False, setup=False):
  if not serviceTuple:
    serviceTuple = divideFullName(serviceName)
  if not instance:
    instance = getSystemInstance(serviceTuple[0], setup=setup)
  return "/Systems/%s/%s" % (serviceTuple[0], instance)


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
  if not componentTuple:
    componentTuple = divideFullName(componentName)
  systemSection = getSystemSection(componentName, componentTuple, setup=setup)
  return "%s/%s/%s" % (systemSection, componentCategory, componentTuple[1])


def getServiceSection(serviceName, serviceTuple=False, setup=False):
  return getComponentSection(serviceName, serviceTuple, setup, "Services")


def getAgentSection(agentName, agentTuple=False, setup=False):
  return getComponentSection(agentName, agentTuple, setup, "Agents")


def getExecutorSection(executorName, executorTuple=False, setup=False):
  return getComponentSection(executorName, executorTuple, setup, "Executors")


def getDatabaseSection(dbName, dbTuple=False, setup=False):
  return getComponentSection(dbName, dbTuple, setup, "Databases")


def getSystemURLSection(serviceName, serviceTuple=False, setup=False):
  systemSection = getSystemSection(serviceName, serviceTuple, setup=setup)
  return "%s/URLs" % systemSection


def getServiceURL(serviceName, serviceTuple=False, setup=False):
  """
    Generate url.

    :param serviceName: Name of service, like 'Framework/Service'.
    :param serviceTuple: (optional) also name of service but look like ('Framework', 'Service').
    :param str setup: DIRAC setup name, can be defined in dirac.cfg

    :return: complete url. e.g. dips://some-domain:3424/Framework/Service
  """
  if not serviceTuple:
    serviceTuple = divideFullName(serviceName)
  systemSection = getSystemSection(serviceName, serviceTuple, setup=setup)
  url = gConfigurationData.extractOptionFromCFG("%s/URLs/%s" % (systemSection, serviceTuple[1]))
  if not url:
    return ""

  # Trying if we are refering to the list of main servers
  # which would be like dips://$MAINSERVERS$:1234/System/Component
  # This can only happen if there is only one server defined
  if ',' not in url:

    urlParse = urlparse.urlparse(url)
    server, port = urlParse.netloc.split(':')
    mainUrlsList = []

    if server == '$MAINSERVERS$':

      # Operations cannot be imported at the beginning because of a bootstrap problem
      from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
      mainServers = Operations().getValue('MainServers', [])
      if not mainServers:
        raise Exception("No Main servers defined")

      for srv in mainServers:
        mainUrlsList.append(urlparse.ParseResult(scheme=urlParse.scheme, netloc=':'.join([srv, port]),
                                                 path=urlParse.path, params='', query='', fragment='').geturl())
      return ','.join(mainUrlsList)

  if len(url.split("/")) < 5:
    url = "%s/%s" % (url, serviceName)
  return url


def getServiceFailoverURL(serviceName, serviceTuple=False, setup=False):
  if not serviceTuple:
    serviceTuple = divideFullName(serviceName)
  systemSection = getSystemSection(serviceName, serviceTuple, setup=setup)
  url = gConfigurationData.extractOptionFromCFG("%s/FailoverURLs/%s" % (systemSection, serviceTuple[1]))
  if not url:
    return ""
  if len(url.split("/")) < 5:
    url = "%s/%s" % (url, serviceName)
  return url


def getGatewayURLs(serviceName=""):
  siteName = gConfigurationData.extractOptionFromCFG("/LocalSite/Site")
  if not siteName:
    return False
  gatewayList = gConfigurationData.extractOptionFromCFG("/DIRAC/Gateways/%s" % siteName)
  if not gatewayList:
    return False
  if serviceName:
    gatewayList = ["%s/%s" % ("/".join(gw.split("/")[:3]), serviceName) for gw in List.fromChar(gatewayList, ",")]
  return List.randomize(gatewayList)
