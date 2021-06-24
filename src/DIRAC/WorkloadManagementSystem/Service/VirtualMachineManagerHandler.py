""" VirtualMachineHandler provides remote access to VirtualMachineDB

    The following methods are available in the Service interface:

    - insertInstance
    - declareInstanceSubmitted
    - declareInstanceRunning
    - instanceIDHeartBeat
    - declareInstanceHalting
    - getInstancesByStatus
    - declareInstancesStopping
    - getUniqueID( instanceID ) return cloud manager uniqueID form VMDIRAC instanceID

"""

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
from subprocess import Popen, PIPE
import six

from DIRAC import gLogger, S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB
from DIRAC.Core.Security.Properties import VM_WEB_OPERATION, VM_RPC_OPERATION
from DIRAC.Resources.Cloud.Utilities import STATE_MAP
from DIRAC.Resources.Cloud.ConfigHelper import getVMTypeConfig, getVMTypes
from DIRAC.Resources.Cloud.EndpointFactory import EndpointFactory
from DIRAC.WorkloadManagementSystem.Utilities.Utils import getProxyFileForCloud

__RCSID__ = '$Id$'

# This is a global instance of the VirtualMachineDB class
gVirtualMachineDB = False


def initializeVirtualMachineManagerHandler(_serviceInfo):

  global gVirtualMachineDB

  gVirtualMachineDB = VirtualMachineDB()
  haltStalledInstances()
  checkStalledInstances()

  if gVirtualMachineDB._connected:
    gThreadScheduler.addPeriodicTask(60 * 15, checkStalledInstances)
    return S_OK()

  return S_ERROR()


def haltStalledInstances():

  result = gVirtualMachineDB.getInstancesByStatus('Stalled')
  if not result['OK']:
    return result

  uList = []
  for image in result['Value']:
    uList += result['Value'][image]

  stallingList = []
  for uID in uList:
    result = gVirtualMachineDB.getInstanceID(uID)
    if not result['OK']:
      continue
    stallingList.append(result['Value'])
  return haltInstances(stallingList)


def getCEInstances(siteList=None, ceList=None, vo=None):

  result = getVMTypes(siteList=siteList, ceList=ceList, vo=vo)
  if not result['OK']:
    return S_ERROR('Failed to get images from the CS')
  imageDict = result['Value']
  ceList = []
  for site in imageDict:
    for ce in imageDict[site]:
      result = EndpointFactory().getCE(site, ce)
      if not result['OK']:
        continue
      ceList.append((site, ce, result['Value']))

  nodeDict = {}
  for site, ceName, ce in ceList:
    result = ce.getVMNodes()
    if not result['OK']:
      continue
    for node in result['Value']:
      if not node.name.startswith('DIRAC'):
        continue
      ip = (node.public_ips[0] if node.public_ips else 'None')
      nodeState = node.state.upper() if not isinstance(node.state, six.integer_types) else STATE_MAP[node.state]
      nodeDict[node.id] = {"Site": site,
                           "CEName": ceName,
                           "NodeName": node.name,
                           "PublicIP": ip,
                           "State": nodeState}
  return S_OK(nodeDict)


def checkStalledInstances():
  """
   To avoid stalling instances consuming resources at cloud endpoint,
   attempts to halt the stalled list in the cloud endpoint
  """

  result = gVirtualMachineDB.declareStalledInstances()
  if not result['OK']:
    return result

  stallingList = result['Value']

  return haltInstances(stallingList)


def stopInstance(site, endpoint, nodeID):

  result = getVMTypeConfig(site, endpoint)
  if not result['OK']:
    return result
  ceParams = result['Value']
  ceFactory = EndpointFactory()
  result = ceFactory.getCEObject(parameters=ceParams)
  if not result['OK']:
    return result

  ce = result['Value']
  result = ce.stopVM(nodeID)
  return result


def createEndpoint(uniqueID):

  result = gVirtualMachineDB.getEndpointFromInstance(uniqueID)
  if not result['OK']:
    return result
  site, endpoint = result['Value'].split('::')

  result = getVMTypeConfig(site, endpoint)
  if not result['OK']:
    return result
  ceParams = result['Value']
  ceFactory = EndpointFactory()
  result = ceFactory.getCEObject(parameters=ceParams)
  return result


def haltInstances(vmList):
  """
   Common haltInstances for Running(from class VirtualMachineManagerHandler) and
   Stalled(from checkStalledInstances periodic task) to Halt
  """

  failed = {}
  successful = {}

  for instanceID in vmList:
    instanceID = int(instanceID)
    result = gVirtualMachineDB.getUniqueID(instanceID)
    if not result['OK']:
      gLogger.error('haltInstances: on getUniqueID call: %s' % result['Message'])
      continue
    uniqueID = result['Value']

    result = createEndpoint(uniqueID)
    if not result['OK']:
      gLogger.error('haltInstances: on createEndpoint call: %s' % result['Message'])
      continue

    endpoint = result['Value']

    # Get proxy to be used to connect to the cloud endpoint
    authType = endpoint.parameters.get('Auth')
    if authType and authType.lower() in ['x509', 'voms']:
      siteName = endpoint.parameters['Site']
      ceName = endpoint.parameters['CEName']
      gLogger.verbose("Getting cloud proxy for %s/%s" % (siteName, ceName))
      result = getProxyFileForCloud(endpoint)
      if not result['OK']:
        continue
      endpoint.setProxy(result['Value'])

    result = endpoint.stopVM(uniqueID)
    if result['OK']:
      gVirtualMachineDB.recordDBHalt(instanceID, 0)
      successful[instanceID] = True
    else:
      failed[instanceID] = result['Message']

  return S_OK({"Successful": successful, "Failed": failed})


def getPilotOutput(pilotRef):

  if not pilotRef.startswith('vm://'):
    return S_ERROR('Invalid pilot reference %s' % pilotRef)

  # Get the VM public IP
  diracID, nPilot = os.path.basename(pilotRef).split(':')
  result = gVirtualMachineDB.getUniqueIDByName(diracID)
  if not result['OK']:
    return result
  uniqueID = result['Value']
  result = gVirtualMachineDB.getInstanceID(uniqueID)
  if not result['OK']:
    return result
  instanceID = result['Value']
  result = gVirtualMachineDB.getInstanceParameter("PublicIP", instanceID)
  if not result['OK']:
    return result
  publicIP = result['Value']

  op = Operations()
  privateKeyFile = op.getValue('/Cloud/PrivateKey', '')
  diracUser = op.getValue('/Cloud/VMUser', '')

  ssh_str = '%s@%s' % (diracUser, publicIP)
  cmd = ['ssh', '-i', privateKeyFile, ssh_str,
         "cat /etc/joboutputs/vm-pilot.%s.log" % nPilot]
  inst = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
  output, stderr = inst.communicate()
  if inst.returncode:
    return S_ERROR('Failed to get pilot output: %s' % stderr)
  else:
    return S_OK(output)


class VirtualMachineManagerHandler(RequestHandler):

  def initialize(self):

    credDict = self.getRemoteCredentials()
    self.rpcProperties = credDict['properties']

  @staticmethod
  def __logResult(methodName, result):
    '''
    Method that writes to log error messages
    '''
    if not result['OK']:
      gLogger.error('%s: %s' % (methodName, result['Message']))

  types_getCEInstances = [(list, type(None)), (list, type(None)), six.string_types]

  def export_getCEInstances(self, siteList, ceList, vo):

    if not siteList:
      siteList = None
    return getCEInstances(siteList=siteList, ceList=ceList, vo=vo)

  types_stopInstance = [six.string_types, six.string_types, six.string_types]

  def export_stopInstance(self, site, endpoint, nodeID):

    return stopInstance(site, endpoint, nodeID)

  types_getPilotOutput = [six.string_types]

  def export_getPilotOutput(self, pilotReference):

    return getPilotOutput(pilotReference)

  types_checkVmWebOperation = [six.string_types]

  def export_checkVmWebOperation(self, operation):
    """
    return true if rpc has VM_WEB_OPERATION
    """
    if VM_WEB_OPERATION in self.rpcProperties:
      return S_OK('Auth')
    return S_OK('Unauth')

  types_insertInstance = [six.string_types, six.string_types, six.string_types, six.string_types, six.string_types]

  def export_insertInstance(self, uniqueID, imageName, instanceName, endpoint, runningPodName):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """
    res = gVirtualMachineDB.insertInstance(uniqueID, imageName, instanceName, endpoint, runningPodName)
    self.__logResult('insertInstance', res)

    return res

  types_getUniqueID = [six.string_types]

  def export_getUniqueID(self, instanceID):
    """
    return cloud manager uniqueID from VMDIRAC instanceID
    """
    res = gVirtualMachineDB.getUniqueID(instanceID)
    self.__logResult('getUniqueID', res)

    return res

  types_getUniqueIDByName = [six.string_types]

  def export_getUniqueIDByName(self, instanceName):
    """
    return cloud manager uniqueID from VMDIRAC name
    """
    result = gVirtualMachineDB.getUniqueIDByName(instanceName)
    self.__logResult('getUniqueIDByName', result)

    return result

  types_setInstanceUniqueID = [six.integer_types, six.string_types]

  def export_setInstanceUniqueID(self, instanceID, uniqueID):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """
    res = gVirtualMachineDB.setInstanceUniqueID(instanceID, uniqueID)
    self.__logResult('setInstanceUniqueID', res)

    return res

  types_declareInstanceSubmitted = [six.string_types]

  def export_declareInstanceSubmitted(self, uniqueID):
    """
    After submission of the instance the Director should declare the new Status
    """
    res = gVirtualMachineDB.declareInstanceSubmitted(uniqueID)
    self.__logResult('declareInstanceSubmitted', res)

    return res

  types_declareInstanceRunning = [six.string_types, six.string_types]

  def export_declareInstanceRunning(self, uniqueID, privateIP):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
    - instanceName does not have a "Submitted" entry
    - uniqueID is not unique
    """
    gLogger.info('Declare instance Running uniqueID: %s' % (uniqueID))
    if VM_RPC_OPERATION not in self.rpcProperties:
      return S_ERROR("Unauthorized declareInstanceRunning RPC")

    publicIP = self.getRemoteAddress()[0]
    gLogger.info('Declare instance Running publicIP: %s' % (publicIP))

    res = gVirtualMachineDB.declareInstanceRunning(uniqueID, publicIP, privateIP)
    self.__logResult('declareInstanceRunning', res)

    return res

  types_instanceIDHeartBeat = [six.string_types, float, six.integer_types,
                               six.integer_types, six.integer_types]

  def export_instanceIDHeartBeat(self, uniqueID, load, jobs,
                                 transferredFiles, transferredBytes, uptime=0):
    """
    Insert the heart beat info from a running instance
    It checks the status of the instance and the corresponding image
    Declares "Running" the instance and the image
    It returns S_ERROR if the status is not OK
    """
    if VM_RPC_OPERATION not in self.rpcProperties:
      return S_ERROR("Unauthorized declareInstanceIDHeartBeat RPC")

    try:
      uptime = int(uptime)
    except ValueError:
      uptime = 0

    res = gVirtualMachineDB.instanceIDHeartBeat(uniqueID, load, jobs,
                                                transferredFiles, transferredBytes, uptime)
    self.__logResult('instanceIDHeartBeat', res)

    return res

  types_declareInstancesStopping = [list]

  def export_declareInstancesStopping(self, instanceIdList):
    """
    Declares "Stopping" the instance because the Delete button of Browse Instances
    The instanceID is the VMDIRAC VM id
    When next instanceID heat beat with stopping status on the DB the VM will stop the job agent and terminates properly
    It returns S_ERROR if the status is not OK
    """
    if VM_WEB_OPERATION not in self.rpcProperties:
      return S_ERROR("Unauthorized VM Stopping")

    for instanceID in instanceIdList:
      gLogger.info('Stopping DIRAC instanceID: %s' % (instanceID))
      result = gVirtualMachineDB.getInstanceStatus(instanceID)
      if not result['OK']:
        self.__logResult('declareInstancesStopping on getInstanceStatus call: ', result)
        return result
      state = result['Value']
      gLogger.info('Stopping DIRAC instanceID: %s, current state %s' % (instanceID, state))

      if state == 'Stalled':
        result = gVirtualMachineDB.getUniqueID(instanceID)
        if not result['OK']:
          self.__logResult('declareInstancesStopping on getUniqueID call: ', result)
          return result
        uniqueID = result['Value']
        result = gVirtualMachineDB.getEndpointFromInstance(uniqueID)
        if not result['OK']:
          self.__logResult('declareInstancesStopping on getEndpointFromInstance call: ', result)
          return result
        endpoint = result['Value']

        result = self.export_declareInstanceHalting(uniqueID, 0)
      elif state == 'New':
        result = gVirtualMachineDB.recordDBHalt(instanceID, 0)
        self.__logResult('declareInstanceHalted', result)
      else:
        # this is only aplied to allowed trasitions
        result = gVirtualMachineDB.declareInstanceStopping(instanceID)
        self.__logResult('declareInstancesStopping: on declareInstanceStopping call: ', result)

    return result

  types_declareInstanceHalting = [six.string_types, float]

  def export_declareInstanceHalting(self, uniqueID, load):
    """
    Insert the heart beat info from a halting instance
    The VM has the uniqueID, which is the Cloud manager VM id
    Declares "Halted" the instance and the image
    It returns S_ERROR if the status is not OK
    """
    if VM_RPC_OPERATION not in self.rpcProperties:
      return S_ERROR("Unauthorized declareInstanceHalting RPC")

    endpoint = gVirtualMachineDB.getEndpointFromInstance(uniqueID)
    if not endpoint['OK']:
      self.__logResult('declareInstanceHalting', endpoint)
      return endpoint
    endpoint = endpoint['Value']

    result = gVirtualMachineDB.declareInstanceHalting(uniqueID, load)
    if not result['OK']:
      if "Halted ->" not in result["Message"]:
        self.__logResult('declareInstanceHalting on change status: ', result)
        return result
      else:
        gLogger.info("Bad transition from Halted to something, will assume Halted")

    haltingList = []
    instanceID = gVirtualMachineDB.getInstanceID(uniqueID)
    if not instanceID['OK']:
      self.__logResult('declareInstanceHalting', instanceID)
      return instanceID
    instanceID = instanceID['Value']
    haltingList.append(instanceID)

    return haltInstances(haltingList)

  types_getInstancesByStatus = [six.string_types]

  def export_getInstancesByStatus(self, status):
    """
    Get dictionary of Image Names with InstanceIDs in given status
    """

    res = gVirtualMachineDB.getInstancesByStatus(status)
    self.__logResult('getInstancesByStatus', res)
    return res

  types_getAllInfoForUniqueID = [six.string_types]

  def export_getAllInfoForUniqueID(self, uniqueID):
    """
    Get all the info for a UniqueID
    """
    res = gVirtualMachineDB.getAllInfoForUniqueID(uniqueID)
    self.__logResult('getAllInfoForUniqueID', res)

    return res

  types_getInstancesContent = [dict, (list, tuple),
                               six.integer_types, six.integer_types]

  def export_getInstancesContent(self, selDict, sortDict, start, limit):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getInstancesContent(selDict, sortDict, start, limit)
    self.__logResult('getInstancesContent', res)

    return res

  types_getHistoryForInstanceID = [six.integer_types]

  def export_getHistoryForInstanceID(self, instanceId):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getHistoryForInstanceID(instanceId)
    self.__logResult('getHistoryForInstanceID', res)

    return res

  types_getInstanceCounters = [six.string_types, dict]

  def export_getInstanceCounters(self, groupField, selDict):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getInstanceCounters(groupField, selDict)
    self.__logResult('getInstanceCounters', res)

    return res

  types_getHistoryValues = [int, dict]

  def export_getHistoryValues(self, averageBucket, selDict, fields2Get=None, timespan=0):
    """
    Retrieve the contents of the DB
    """
    if not fields2Get:
      fields2Get = []
    res = gVirtualMachineDB.getHistoryValues(averageBucket, selDict, fields2Get, timespan)
    self.__logResult('getHistoryValues', res)

    return res

  types_getRunningInstancesHistory = [int, int]

  def export_getRunningInstancesHistory(self, timespan, bucketSize):
    """
    Retrieve number of running instances in each bucket
    """
    res = gVirtualMachineDB.getRunningInstancesHistory(timespan, bucketSize)
    self.__logResult('getRunningInstancesHistory', res)

    return res

  types_getRunningInstancesBEPHistory = [int, int]

  def export_getRunningInstancesBEPHistory(self, timespan, bucketSize):
    """
    Retrieve number of running instances in each bucket by End-Point History
    """
    res = gVirtualMachineDB.getRunningInstancesBEPHistory(timespan, bucketSize)
    self.__logResult('getRunningInstancesBEPHistory', res)

    return res

  types_getRunningInstancesByRunningPodHistory = [int, int]

  def export_getRunningInstancesByRunningPodHistory(self, timespan, bucketSize):
    """
    Retrieve number of running instances in each bucket by Running Pod History
    """
    res = gVirtualMachineDB.getRunningInstancesByRunningPodHistory(timespan, bucketSize)
    self.__logResult('getRunningInstancesByRunningPodHistory', res)

    return res

  types_getRunningInstancesByImageHistory = [int, int]

  def export_getRunningInstancesByImageHistory(self, timespan, bucketSize):
    """
    Retrieve number of running instances in each bucket by Running Pod History
    """
    res = gVirtualMachineDB.getRunningInstancesByImageHistory(timespan, bucketSize)
    self.__logResult('getRunningInstancesByImageHistory', res)

    return res
