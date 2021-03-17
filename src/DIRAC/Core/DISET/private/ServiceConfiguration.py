"""
It keeps the service configuration parameters like maximum running threads, number of processes, etc. ,
which can be configured in CS.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities import Network, List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.private.Protocols import gDefaultProtocol


class ServiceConfiguration:

  def __init__(self, nameList):
    self.serviceName = nameList[0]
    self.serviceURL = None
    self.nameList = nameList
    self.pathList = []
    for svcName in nameList:
      self.pathList.append(PathFinder.getServiceSection(svcName))

  def getOption(self, optionName):
    if optionName[0] == "/":
      return gConfigurationData.extractOptionFromCFG(optionName)
    for path in self.pathList:
      value = gConfigurationData.extractOptionFromCFG("%s/%s" % (path, optionName))
      if value:
        return value
    return None

  def getAddress(self):
    return ("", self.getPort())

  def getHandlerLocation(self):
    return self.getOption("HandlerPath")

  def getName(self):
    return self.serviceName

  def setURL(self, sURL):
    self.serviceURL = sURL

  def __getCSURL(self, URL=None):
    optionValue = self.getOption("URL")
    if optionValue:
      return optionValue
    return URL

  def registerAlsoAs(self):
    optionValue = self.getOption("RegisterAlsoAs")
    if optionValue:
      return List.fromChar(optionValue)
    else:
      return []

  def getMaxThreads(self):
    try:
      return int(self.getOption("MaxThreads"))
    except Exception:
      return 15

  def getMinThreads(self):
    try:
      return int(self.getOption("MinThreads"))
    except Exception:
      return 1

  def getMaxWaitingPetitions(self):
    try:
      return int(self.getOption("MaxWaitingPetitions"))
    except Exception:
      return 500

  def getMaxMessagingConnections(self):
    try:
      return int(self.getOption("MaxMessagingConnections"))
    except Exception:
      return 20

  def getMaxThreadsForMethod(self, actionType, method):
    try:
      return int(self.getOption("ThreadLimit/%s/%s" % (actionType, method)))
    except Exception:
      return 15

  def getCloneProcesses(self):
    try:
      return int(self.getOption("CloneProcesses"))
    except Exception:
      return 0

  def getPort(self):
    try:
      return int(self.getOption("Port"))
    except Exception:
      return 9876

  def getProtocol(self):
    optionValue = self.getOption("Protocol")
    if optionValue:
      return optionValue
    return gDefaultProtocol

  def getHostname(self):
    hostname = self.getOption("/DIRAC/Hostname")
    if not hostname:
      return Network.getFQDN()
    return hostname

  def getURL(self):
    """
    Build the service URL
    """
    if self.serviceURL:
      return self.serviceURL
    protocol = self.getProtocol()
    serviceURL = self.__getCSURL()
    if serviceURL:
      if serviceURL.find(protocol) != 0:
        urlFields = serviceURL.split(":")
        urlFields[0] = protocol
        serviceURL = ":".join(urlFields)
        self.setURL(serviceURL)
      return serviceURL
    hostName = self.getHostname()
    port = self.getPort()
    serviceURL = "%s://%s:%s/%s" % (protocol,
                                    hostName,
                                    port,
                                    self.getName())
    if serviceURL[-1] == "/":
      serviceURL = serviceURL[:-1]
    self.setURL(serviceURL)
    return serviceURL

  def getContextLifeTime(self):
    optionValue = self.getOption("ContextLifeTime")
    try:
      return int(optionValue)
    except Exception:
      return 21600
