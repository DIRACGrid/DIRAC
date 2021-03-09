""" Exposes singleton object gMonitor, which is instance of MonitoringClient class

    Uses RPC Framework/Monitoring service. Calls registerActivities exposed function
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id"

import time
import os
import six

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities import Time, Network, ThreadScheduler
from DIRAC.Core.DISET.RPCClient import RPCClient


class MonitoringClientActivityNotDefined(Exception):
  """ This class is used to raise an exception if an activity is not defined meaning
      not registered using the registerActivity method of the gMonitor object.
  """

  def __init__(self, message):
    self.message = str(message)

  def __str__(self):
    return self.message


class MonitoringClientActivityValueTypeError(Exception):
  """ This class is used to raise an exception if an activity type is mismatched while
      calling the addMark method of the gMonitor object.
  """

  def __init__(self, message):
    self.message = message

  def __str__(self):
    return self.message


class MonitoringClientUnknownParameter(Exception):
  """ This class is used to raise an exception when some unkown parameter is passed
      to the setComponentExtraParam method of the gMonitor object.
  """

  def __init__(self, message):
    self.message = message

  def __str__(self):
    return self.message


class MonitoringFlusher(object):
  """
  This class is used to flush all the instances of the MonitoringClient registered periodically.
  """

  def __init__(self):
    self.__mcList = []
    ThreadScheduler.gThreadScheduler.addPeriodicTask(300, self.flush)
    # HACK: Avoid exiting while the thread is starting
    time.sleep(0.1)

  def flush(self, allData=False):
    """ This method is used to periodically flush the data.

        :type allData: bool
        :param allData: This is used to indicate whether all the data is present or not.
    """
    for mc in self.__mcList:
      mc.flush(allData)

  def registerMonitoringClient(self, mc):
    """ This method is used to register instances of the MonitoringClient.

        :type mc: object of MonitoringClient
        :param mc: Its just an instance of the MonitoringClient that we want to register.
    """
    if mc not in self.__mcList:
      self.__mcList.append(mc)


# DIRAC_USE_TORNADO_IOLOOP is defined by starting scripts
if os.environ.get('DIRAC_USE_TORNADO_IOLOOP', 'false').lower() in ('yes', 'true'):
  from DIRAC.FrameworkSystem.Client.MonitoringClientIOLoop import MonitoringFlusherTornado
  gMonitoringFlusher = MonitoringFlusherTornado()
else:
  gMonitoringFlusher = MonitoringFlusher()


class MonitoringClient(object):
  """ This class is used to create the gMonitor object and acts as a client side for registering activities
      and committing the data.
  """

  # Different types of operations
  OP_MEAN = "mean"
  OP_ACUM = "acum"
  OP_SUM = "sum"
  OP_RATE = "rate"

  # Predefined components that can be registered
  COMPONENT_SERVICE = "service"
  COMPONENT_AGENT = "agent"
  COMPONENT_WEB = "web"
  COMPONENT_SCRIPT = "script"
  COMPONENT_TORNADO = "tornado"

  __validMonitoringValues = six.integer_types + (float,)

  def __init__(self):
    self.sourceId = 0
    self.sourceDict = {}
    self.sourceDict['componentType'] = "unknown"
    self.sourceDict['componentName'] = "unknown"
    self.sourceDict['componentLocation'] = "unknown"
    self.activitiesDefinitions = {}
    self.activitiesMarks = {}
    self.definitionsToSend = {}
    self.marksToSend = {}
    self.__compRegistrationExtraDict = {}
    self.__compCommitExtraDict = {}
    self.__activitiesLock = None  # threading.Lock()
    self.__flushingLock = None  # threading.Lock()
    self.timeStep = 60
    self.__initialized = False
    self.__enabled = True

  @property
  def activitiesLock(self):
    if not self.__activitiesLock:
      self.__activitiesLock = LockRing().getLock("activityLock")
    return self.__activitiesLock

  @property
  def flushingLock(self):
    if not self.__flushingLock:
      self.__flushingLock = LockRing().getLock("flushingLock")
    return self.__flushingLock

  def disable(self):
    self.__enabled = False

  def enable(self):
    self.__enabled = True

  def setComponentExtraParam(self, name, value):
    """
    Sets the extra parameters of the component reporting.

    :type  name: string
    :param name: It should belong to one of these 'version', 'DIRACVersion', 'description', 'startTime', 'platform',
                                                  'cycles', 'queries'.
    :type value: string
    :param value: The proper value corresponding to one of the selected names.
    """
    if name in ('version', 'DIRACVersion', 'description', 'startTime', 'platform'):
      self.__compRegistrationExtraDict[name] = str(value)
    elif name in ('cycles', 'queries', 'connections'):
      self.__compCommitExtraDict[name] = str(value)
    else:
      raise MonitoringClientUnknownParameter("Unknown parameter %s" % name)
      # raise Exception( "Unknown parameter %s" % name )

  def initialize(self):
    self.logger = gLogger.getSubLogger("Monitoring")
    self.logger.debug("Initializing Monitoring Client")
    self.sourceDict['setup'] = gConfig.getValue("/DIRAC/Setup")
    self.sourceDict['site'] = DIRAC.siteName()
    if self.sourceDict['componentType'] == self.COMPONENT_SERVICE:
      self.cfgSection = PathFinder.getSystemSection(self.sourceDict['componentName'])
    elif self.sourceDict['componentType'] == self.COMPONENT_AGENT:
      self.cfgSection = PathFinder.getAgentSection(self.sourceDict['componentName'])
      self.setComponentLocation(Network.getFQDN())
    elif self.sourceDict['componentType'] == self.COMPONENT_WEB:
      self.cfgSection = "/WebApp"
      self.setComponentLocation('http://%s' % Network.getFQDN())
      self.setComponentName('WebApp')
    elif self.sourceDict['componentType'] == self.COMPONENT_SCRIPT:
      self.cfgSection = "/Script"
    elif self.sourceDict['componentType'] == self.COMPONENT_TORNADO:
      self.cfgSection = "/Tornado"
    else:
      raise Exception("Component type has not been defined")
    gMonitoringFlusher.registerMonitoringClient(self)
    self.__initialized = True

  def setComponentLocation(self, componentLocation=False):
    """
    Sets the location of the component reporting.

    :type  componentLocation: string
    :param componentLocation: Location of the component reporting
    """
    if not componentLocation:
      self.sourceDict['componentLocation'] = gConfig.getValue("/Site")
    else:
      self.sourceDict['componentLocation'] = componentLocation

  def setComponentName(self, componentName):
    """
    Sets the name of the component reporting.

    :type  componentName: string
    :param componentName: Name of the component reporting
    """
    self.sourceDict['componentName'] = componentName

  def setComponentType(self, componentType):
    """
    Defines the type of component reporting data.

    :type  componentType: string
    :param componentType: Defines the grouping of the host by type. All the possibilities
                          are defined in the Constants.py file
    """
    self.sourceDict['componentType'] = componentType

  def registerActivity(self, name, description, category, unit, operation, bucketLength=60):
    """
    Registers new activity. Before reporting information to the server, the activity
    must be registered.

    :type  name: string
    :param name: Id of the activity to report
    :type description: string
    :param description: Description of the activity
    :type  category: string
    :param category: Grouping of the activity
    :type  unit: string
    :param unit: String representing the unit that will be printed in the plots
    :type  operation: string
    :param operation: Type of data operation to represent data. All the possibilities
                      are defined in the Constants.py file
    :type  bucketLength: int
    :param bucketLength: Bucket length in seconds
    """
    if not self.__initialized:
      return
    self.activitiesLock.acquire()
    try:
      self.logger.debug("Registering activity %s" % name)
      if name not in self.activitiesDefinitions:
        self.activitiesDefinitions[name] = {"category": category,
                                            "description": description,
                                            "unit": unit,
                                            "type": operation,
                                            "bucketLength": bucketLength
                                            }
        self.activitiesMarks[name] = {}
        self.definitionsToSend[name] = dict(self.activitiesDefinitions[name])
    finally:
      self.activitiesLock.release()

  def __UTCStepTime(self, acName):
    stepLength = self.activitiesDefinitions[acName]['bucketLength']
    nowEpoch = int(Time.toEpoch())
    return nowEpoch - nowEpoch % stepLength

  def addMark(self, name, value=1):
    """
    Adds a new mark to the specified activity

    :type  name: string
    :param name: Name of the activity to report
    :type  value: number
    :param value: Weight of the mark. By default it's one.
    """
    if not self.__initialized:
      return
    if not self.__enabled:
      return
    if name not in self.activitiesDefinitions:
      raise MonitoringClientActivityNotDefined("You must register activity %s before adding marks to it" % name)
      # raise Exception( "You must register activity %s before adding marks to it" % name )
    if not isinstance(value, self.__validMonitoringValues):
      raise MonitoringClientActivityValueTypeError("Activity '%s' value's type (%s) is not valid" % (name, type(value)))
      # raise Exception( "Value's type %s is not valid" % value )
    self.activitiesLock.acquire()
    try:
      self.logger.debug("Adding mark to %s" % name)
      markTime = self.__UTCStepTime(name)
      if markTime in self.activitiesMarks[name]:
        self.activitiesMarks[name][markTime].append(value)
      else:
        self.activitiesMarks[name][markTime] = [value]
    finally:
      self.activitiesLock.release()

  def __consolidateMarks(self, allData):
    """
      Copies all marks except last step ones and consolidates them.

      :type allData: bool
      :param allData: This is used to indicate whether all the data is present or not.
      :return: dictionary of consolidatedMarks.
    """
    consolidatedMarks = {}
    remainderMarks = {}
    for key in self.activitiesMarks:
      if allData:
        lastStepToSend = int(Time.toEpoch())
      else:
        lastStepToSend = self.__UTCStepTime(key)
      consolidatedMarks[key] = {}
      remainderMarks[key] = {}
      for markTime in self.activitiesMarks[key]:
        markValue = self.activitiesMarks[key][markTime]
        if markTime >= lastStepToSend:
          remainderMarks[key][markTime] = markValue
        else:
          consolidatedMarks[key][markTime] = markValue
          # Consolidate the copied ones
          totalValue = 0
          for mark in consolidatedMarks[key][markTime]:
            totalValue += mark
          if self.activitiesDefinitions[key]['type'] == self.OP_MEAN:
            totalValue /= len(consolidatedMarks[key][markTime])
          consolidatedMarks[key][markTime] = totalValue
      if len(consolidatedMarks[key]) == 0:
        del(consolidatedMarks[key])
    self.activitiesMarks = remainderMarks
    return consolidatedMarks

  def flush(self, allData=False):
    """ This method is used to periodically flush the data and send it to the server side.

        :type allData: bool
        :param allData: This is used to indicate whether all the data is present or not.
    """
    if not self.__enabled or not self.__initialized:
      return
    self.flushingLock.acquire()
    self.logger.debug("Sending information to server")
    try:
      self.activitiesLock.acquire()
      try:
        self.logger.debug("Consolidating data...")
        self.__appendMarksToSend(self.__consolidateMarks(allData))
      finally:
        self.activitiesLock.release()
      # Commit new activities
      if self.__dataToSend():
        if not self.__disabled():
          # this also creates a point of contact between te client and the server as it calls the __sendData method.
          self.__sendData()
      self.__pruneMarksData()
    finally:
      self.flushingLock.release()

  def __disabled(self):
    """ This method is basically used to check whether monitoring is disabled or not inside the
        configuration file.
    """
    return gConfig.getValue("%s/DisableMonitoring" % self.cfgSection, "false").lower() in \
        ("yes", "y", "true", "1")

  def __dataToSend(self):
    return len(self.definitionsToSend) or len(self.marksToSend)

  def __appendMarksToSend(self, acMarks):
    if self.__disabled():
      return
    for acName in acMarks:
      if acName in self.marksToSend:
        for timeMark in acMarks[acName]:
          self.marksToSend[acName][timeMark] = acMarks[acName][timeMark]
      else:
        self.marksToSend[acName] = acMarks[acName]

  # Decoding was fixed in https://github.com/DIRACGrid/DIRAC/pull/4462
  @ignoreEncodeWarning
  def __sendData(self, secsTimeout=False):
    """ This method is used to initialize the rpcClient from the server and also to initiate the task
        of registering the activities and committing the marks.

        :type secsTimeout: int
        :param secsTimeout: The timeout in seconds for the rpcClient.
    """
    from DIRAC.FrameworkSystem.private.monitoring.ServiceInterface import gServiceInterface
    if gServiceInterface.srvUp:
      self.logger.debug("Using internal interface to send data")
      rpcClient = gServiceInterface
    else:
      self.logger.debug("Creating RPC client")
      # Here is where the client is created from the running Framework/Monitoring service.
      rpcClient = RPCClient("Framework/Monitoring", timeout=secsTimeout)
    # Send registrations
    if not self.__sendRegistration(rpcClient):
      return False
    # Send marks
    maxIteration = 5
    if self.__sendMarks(rpcClient) and maxIteration:
      maxIteration -= 1
      if not self.__sendRegistration(rpcClient):
        return False

  def __pruneMarksData(self):
    """ This basically prunes the marks which exceed the bucket length.
    """
    for acName in self.marksToSend:
      maxBuckets = int(86400 / self.activitiesDefinitions[acName]['bucketLength'])
      if len(self.marksToSend[acName]) > maxBuckets:
        timeSlots = sorted(self.marksToSend[acName])
        while len(self.marksToSend[acName]) > maxBuckets:
          del(self.marksToSend[acName][timeSlots.pop(0)])

  def __sendRegistration(self, rpcClient):
    """ This method registers all the activities using the rpcClient.

        :type rpcClient: object of RPCClient
        :param rpcClient: This is used to access the methods within the service and register the activities.
        :return: bool
    """
    if not len(self.definitionsToSend):
      return True
    self.logger.debug("Registering activities")
    retDict = rpcClient.registerActivities(self.sourceDict,
                                           self.definitionsToSend,
                                           self.__compRegistrationExtraDict)
    if not retDict['OK']:
      self.logger.error("Can't register activities", retDict['Message'])
      return False
    self.sourceId = retDict['Value']
    self.definitionsToSend = {}
    return True

  def __sendMarks(self, rpcClient):
    """
    This method sends all the marks accumulated to the server using the rpcClient.

    :type rpcClient: object of RPCClient
    :param rpcClient: This is used to access the methods within the service and register the activities.
    :return: bool
    """
    assert self.sourceId
    self.logger.debug("Sending marks")
    retDict = rpcClient.commitMarks(self.sourceId,
                                    self.marksToSend,
                                    self.__compCommitExtraDict)
    if not retDict['OK']:
      self.logger.error("Can't send activities marks", retDict['Message'])
      return False
    acMissedMarks = {}
    if len(retDict['Value']) > 0:
      self.logger.debug("There are activities unregistered")
      for acName in retDict['Value']:
        if acName in self.activitiesDefinitions:
          self.definitionsToSend[acName] = dict(self.activitiesDefinitions[acName])
          acMissedMarks[acName] = self.marksToSend[acName]
        else:
          self.logger.debug("Server reported unregistered activity that does not exist")
    self.marksToSend = acMissedMarks
    return len(self.definitionsToSend)

  def forceFlush(self, exitCode):
    """ This method can be used to force flush all the data directly without flushing the
        data periodically.
    """
    self.sendingMode = "none"
    self.flush(allData=True)

  def getComponentsStatus(self, condDict):
    """ This method is used to get the status of the components.

        :type condDict: dictionary
        :param condDict: A condition dictionary.
        :return: S_OK with status and message about the component.
    """
    rpcClient = RPCClient("Framework/Monitoring", timeout=100)
    return rpcClient.getComponentsStatus(condDict)

  def __filterComponent(self, component, condDict):
    for key in condDict:
      if key not in component:
        return False
      condVal = condDict[key]
      componentVal = component[key]
      if isinstance(condVal, (list, tuple)):
        if componentVal not in condVal:
          return False
      else:
        if componentVal != condVal:
          return False
    return True

  def getComponentsStatusWebFormatted(self, condDict={}, sortingList=[], startItem=0, maxItems=0):
    result = self.getComponentsStatus(condDict)
    if not result['OK']:
      return result
    compDict, fields = result['Value']
    tabledData = []
    for setup in compDict:
      for cType in compDict[setup]:
        for name in compDict[setup][cType]:
          for component in compDict[setup][cType][name]:
            # How here we are. Now we need to filter the components
            if not self.__filterComponent(component, condDict):
              continue
            # Add to tabledData!
            row = []
            for field in fields:
              if field not in component:
                row.append("")
              else:
                row.append(component[field])
            tabledData.append(row)
    # We've got the data in table form
    # Now it's time to sort it
    if sortingList:
      sortingData = []
      sortField = sortingList[0][0]
      if sortField not in fields:
        return S_ERROR("Sorting field %s does not exist" % sortField)
      sortDirection = sortingList[0][1]
      fieldIndex = 0
      for i in range(len(fields)):
        if fields[i] == sortField:
          fieldIndex = i
          break
      for row in tabledData:
        sortingData.append((row[fieldIndex], row))
      sortingData.sort(key=lambda x: x[0])
      if sortDirection == "DESC":
        sortingData.reverse()
      tabledData = [row[1] for row in sortingData]
    # Now need to limit
    numRows = len(tabledData)
    tabledData = tabledData[startItem:]
    if maxItems:
      tabledData = tabledData[:maxItems]
    returnData = {'ParameterNames': fields,
                  'Records': tabledData,
                  'TotalRecords': numRows,
                  }
    return S_OK(returnData)


# Here the singleton gMonitor object is created which is used in all other use cases.
gMonitor = MonitoringClient()
