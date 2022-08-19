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
import os
from subprocess import Popen, PIPE

from DIRAC import gLogger, S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB
from DIRAC.Core.Security.Properties import OPERATOR, VM_RPC_OPERATION
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getVMTypeConfig, getVMTypes
from DIRAC.Resources.Cloud.Utilities import STATE_MAP
from DIRAC.Resources.Cloud.EndpointFactory import EndpointFactory
from DIRAC.WorkloadManagementSystem.Utilities.Utils import getProxyFileForCloud


class VirtualMachineManagerHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfo):
        cls.virtualMachineDB = VirtualMachineDB()
        cls.haltStalledInstances()
        cls.checkStalledInstances()

        if cls.virtualMachineDB._connected:
            gThreadScheduler.addPeriodicTask(60 * 15, cls.checkStalledInstances)
            return S_OK()
        return S_ERROR()

    @classmethod
    def haltStalledInstances(cls):
        result = cls.virtualMachineDB.getInstancesByStatus("Stalled")
        if not result["OK"]:
            return result

        uList = []
        for image in result["Value"]:
            uList += result["Value"][image]

        stallingList = []
        for uID in uList:
            result = cls.virtualMachineDB.getInstanceID(uID)
            if not result["OK"]:
                continue
            stallingList.append(result["Value"])
        return cls.haltInstances(stallingList)

    @classmethod
    def getCEInstances(cls, siteList=None, ceList=None, vo=None):

        result = getVMTypes(siteList=siteList, ceList=ceList, vo=vo)
        if not result["OK"]:
            return result
        imageDict = result["Value"]
        ceList = []
        for site in imageDict:
            for ce in imageDict[site]:
                result = EndpointFactory().getCE(site, ce)
                if not result["OK"]:
                    continue
                ceList.append((site, ce, result["Value"]))

        nodeDict = {}
        for site, ceName, ce in ceList:
            result = ce.getVMNodes()
            if not result["OK"]:
                continue
            for node in result["Value"]:
                if not node.name.startswith("DIRAC"):
                    continue
                ip = node.public_ips[0] if node.public_ips else "None"
                nodeState = node.state.upper() if not isinstance(node.state, int) else STATE_MAP[node.state]
                nodeDict[node.id] = {
                    "Site": site,
                    "CEName": ceName,
                    "NodeName": node.name,
                    "PublicIP": ip,
                    "State": nodeState,
                }
        return S_OK(nodeDict)

    @classmethod
    def checkStalledInstances(cls):
        """
        To avoid stalling instances consuming resources at cloud endpoint,
        attempts to halt the stalled list in the cloud endpoint
        """
        result = cls.virtualMachineDB.declareStalledInstances()
        if not result["OK"]:
            return result

        stallingList = result["Value"]
        return cls.haltInstances(stallingList)

    @classmethod
    def stopInstance(cls, site, endpoint, nodeID):

        result = getVMTypeConfig(site, endpoint)
        if not result["OK"]:
            return result
        ceParams = result["Value"]
        ceFactory = EndpointFactory()
        result = ceFactory.getCEObject(parameters=ceParams)
        if not result["OK"]:
            return result

        ce = result["Value"]
        return ce.stopVM(nodeID)

    @classmethod
    def createEndpoint(cls, uniqueID):

        result = cls.virtualMachineDB.getEndpointFromInstance(uniqueID)
        if not result["OK"]:
            return result
        site, endpoint = result["Value"].split("::")

        result = getVMTypeConfig(site, endpoint)
        if not result["OK"]:
            return result
        ceParams = result["Value"]
        ceFactory = EndpointFactory()
        return ceFactory.getCEObject(parameters=ceParams)

    @classmethod
    def haltInstances(cls, vmList):
        """
        Common haltInstances for Running(from class VirtualMachineManagerHandler) and
        Stalled(from checkStalledInstances periodic task) to Halt
        """
        failed = {}
        successful = {}

        for instanceID in vmList:
            instanceID = int(instanceID)
            result = cls.virtualMachineDB.getUniqueID(instanceID)
            if not result["OK"]:
                gLogger.error("haltInstances: on getUniqueID call: %s" % result["Message"])
                continue
            uniqueID = result["Value"]

            result = cls.createEndpoint(uniqueID)
            if not result["OK"]:
                gLogger.error("haltInstances: on createEndpoint call: %s" % result["Message"])
                continue

            endpoint = result["Value"]

            # Get proxy to be used to connect to the cloud endpoint
            authType = endpoint.parameters.get("Auth")
            if authType and authType.lower() in ["x509", "voms"]:
                siteName = endpoint.parameters["Site"]
                ceName = endpoint.parameters["CEName"]
                gLogger.verbose(f"Getting cloud proxy for {siteName}/{ceName}")
                result = getProxyFileForCloud(endpoint)
                if not result["OK"]:
                    continue
                endpoint.setProxy(result["Value"])

            result = endpoint.stopVM(uniqueID)
            if result["OK"]:
                cls.virtualMachineDB.recordDBHalt(instanceID, 0)
                successful[instanceID] = True
            else:
                failed[instanceID] = result["Message"]

        return S_OK({"Successful": successful, "Failed": failed})

    @classmethod
    def getPilotOutput(cls, pilotRef):
        if not pilotRef.startswith("vm://"):
            return S_ERROR("Invalid pilot reference %s" % pilotRef)

        # Get the VM public IP
        diracID, nPilot = os.path.basename(pilotRef).split(":")
        result = cls.virtualMachineDB.getUniqueIDByName(diracID)
        if not result["OK"]:
            return result
        uniqueID = result["Value"]
        result = cls.virtualMachineDB.getInstanceID(uniqueID)
        if not result["OK"]:
            return result
        instanceID = result["Value"]
        result = cls.virtualMachineDB.getInstanceParameter("PublicIP", instanceID)
        if not result["OK"]:
            return result
        publicIP = result["Value"]

        op = Operations()
        privateKeyFile = op.getValue("/Cloud/PrivateKey", "")
        diracUser = op.getValue("/Cloud/VMUser", "")

        ssh_str = f"{diracUser}@{publicIP}"
        cmd = ["ssh", "-i", privateKeyFile, ssh_str, "cat /etc/joboutputs/vm-pilot.%s.log" % nPilot]
        inst = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
        output, stderr = inst.communicate()
        if inst.returncode:
            return S_ERROR("Failed to get pilot output: %s" % stderr)
        else:
            return S_OK(output)

    def initialize(self):
        credDict = self.getRemoteCredentials()
        self.rpcProperties = credDict["properties"]

    types_getCEInstances = [(list, type(None)), (list, type(None)), str]

    def export_getCEInstances(self, siteList, ceList, vo):

        if not siteList:
            siteList = None
        return self.getCEInstances(siteList=siteList, ceList=ceList, vo=vo)

    types_stopInstance = [str, str, str]

    def export_stopInstance(self, site, endpoint, nodeID):

        return self.stopInstance(site, endpoint, nodeID)

    types_getPilotOutput = [str]

    def export_getPilotOutput(self, pilotReference):

        return self.getPilotOutput(pilotReference)

    types_checkVmWebOperation = [str]

    def export_checkVmWebOperation(self, operation):
        """
        return true if rpc has OPERATOR
        """
        if OPERATOR in self.rpcProperties:
            return S_OK("Auth")
        return S_OK("Unauth")

    types_insertInstance = [str, str, str, str, str]

    def export_insertInstance(self, uniqueID, imageName, instanceName, endpoint, runningPodName):
        """
        Check Status of a given image
        Will insert a new Instance in the DB
        """
        return self.virtualMachineDB.insertInstance(uniqueID, imageName, instanceName, endpoint, runningPodName)

    types_getUniqueID = [str]

    def export_getUniqueID(self, instanceID):
        """
        return cloud manager uniqueID from VMDIRAC instanceID
        """
        return self.virtualMachineDB.getUniqueID(instanceID)

    types_getUniqueIDByName = [str]

    def export_getUniqueIDByName(self, instanceName):
        """
        return cloud manager uniqueID from VMDIRAC name
        """
        return self.virtualMachineDB.getUniqueIDByName(instanceName)

    types_setInstanceUniqueID = [int, str]

    def export_setInstanceUniqueID(self, instanceID, uniqueID):
        """
        Check Status of a given image
        Will insert a new Instance in the DB
        """
        return self.virtualMachineDB.setInstanceUniqueID(instanceID, uniqueID)

    types_declareInstanceSubmitted = [str]

    def export_declareInstanceSubmitted(self, uniqueID):
        """
        After submission of the instance the Director should declare the new Status
        """
        return self.virtualMachineDB.declareInstanceSubmitted(uniqueID)

    types_declareInstanceRunning = [str, str]

    def export_declareInstanceRunning(self, uniqueID, privateIP):
        """
        Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
        Returns S_ERROR if:
        - instanceName does not have a "Submitted" entry
        - uniqueID is not unique
        """
        gLogger.info("Declare instance Running uniqueID: %s" % (uniqueID))
        publicIP = self.getRemoteAddress()[0]
        gLogger.info("Declare instance Running publicIP: %s" % (publicIP))

        return self.virtualMachineDB.declareInstanceRunning(uniqueID, publicIP, privateIP)

    types_instanceIDHeartBeat = [str, float, int, int, int]

    def export_instanceIDHeartBeat(self, uniqueID, load, jobs, transferredFiles, transferredBytes, uptime=0):
        """
        Insert the heart beat info from a running instance
        It checks the status of the instance and the corresponding image
        Declares "Running" the instance and the image
        It returns S_ERROR if the status is not OK
        """
        try:
            uptime = int(uptime)
        except ValueError:
            uptime = 0

        return self.virtualMachineDB.instanceIDHeartBeat(
            uniqueID, load, jobs, transferredFiles, transferredBytes, uptime
        )

    types_declareInstancesStopping = [list]

    def export_declareInstancesStopping(self, instanceIdList):
        """
        Declares "Stopping" the instance because the Delete button of Browse Instances
        The instanceID is the VMDIRAC VM id
        When next instanceID heat beat with stopping status on the DB the VM will stop the job agent and terminates properly
        It returns S_ERROR if the status is not OK
        """
        for instanceID in instanceIdList:
            gLogger.info("Stopping DIRAC instanceID: %s" % (instanceID))
            result = self.virtualMachineDB.getInstanceStatus(instanceID)
            if not result["OK"]:
                return result
            state = result["Value"]
            gLogger.info(f"Stopping DIRAC instanceID: {instanceID}, current state {state}")

            if state == "Stalled":
                result = self.virtualMachineDB.getUniqueID(instanceID)
                if not result["OK"]:
                    return result
                uniqueID = result["Value"]
                result = self.export_declareInstanceHalting(uniqueID, 0)
                if not result["OK"]:
                    return result
            elif state == "New":
                result = self.virtualMachineDB.recordDBHalt(instanceID, 0)
                if not result["OK"]:
                    return result
            else:
                # this is only applied to allowed transitions
                result = self.virtualMachineDB.declareInstanceStopping(instanceID)
                if not result["OK"]:
                    return result
        return S_OK()

    types_declareInstanceHalting = [str, float]

    def export_declareInstanceHalting(self, uniqueID, load):
        """
        Insert the heart beat info from a halting instance
        The VM has the uniqueID, which is the Cloud manager VM id
        Declares "Halted" the instance and the image
        It returns S_ERROR if the status is not OK
        """
        result = self.virtualMachineDB.declareInstanceHalting(uniqueID, load)
        if not result["OK"]:
            if "Halted ->" not in result["Message"]:
                return result
            else:
                gLogger.info("Bad transition from Halted to something, will assume Halted")

        haltingList = []
        instanceID = self.virtualMachineDB.getInstanceID(uniqueID)
        if not instanceID["OK"]:
            return instanceID
        instanceID = instanceID["Value"]
        haltingList.append(instanceID)

        return self.haltInstances(haltingList)

    types_getInstancesByStatus = [str]

    def export_getInstancesByStatus(self, status):
        """
        Get dictionary of Image Names with InstanceIDs in given status
        """
        return self.virtualMachineDB.getInstancesByStatus(status)

    types_getAllInfoForUniqueID = [str]

    def export_getAllInfoForUniqueID(self, uniqueID):
        """
        Get all the info for a UniqueID
        """
        return self.virtualMachineDB.getAllInfoForUniqueID(uniqueID)

    types_getInstancesContent = [dict, (list, tuple), int, int]

    def export_getInstancesContent(self, selDict, sortDict, start, limit):
        """
        Retrieve the contents of the DB
        """
        return self.virtualMachineDB.getInstancesContent(selDict, sortDict, start, limit)

    types_getHistoryForInstanceID = [int]

    def export_getHistoryForInstanceID(self, instanceId):
        """
        Retrieve the contents of the DB
        """
        return self.virtualMachineDB.getHistoryForInstanceID(instanceId)

    types_getInstanceCounters = [str, dict]

    def export_getInstanceCounters(self, groupField, selDict):
        """
        Retrieve the contents of the DB
        """
        return self.virtualMachineDB.getInstanceCounters(groupField, selDict)

    types_getHistoryValues = [int, dict]

    def export_getHistoryValues(self, averageBucket, selDict, fields2Get=None, timespan=0):
        """
        Retrieve the contents of the DB
        """
        if not fields2Get:
            fields2Get = []
        return self.virtualMachineDB.getHistoryValues(averageBucket, selDict, fields2Get, timespan)

    types_getRunningInstancesHistory = [int, int]

    def export_getRunningInstancesHistory(self, timespan, bucketSize):
        """
        Retrieve number of running instances in each bucket
        """
        return self.virtualMachineDB.getRunningInstancesHistory(timespan, bucketSize)

    types_getRunningInstancesBEPHistory = [int, int]

    def export_getRunningInstancesBEPHistory(self, timespan, bucketSize):
        """
        Retrieve number of running instances in each bucket by End-Point History
        """
        return self.virtualMachineDB.getRunningInstancesBEPHistory(timespan, bucketSize)

    types_getRunningInstancesByRunningPodHistory = [int, int]

    def export_getRunningInstancesByRunningPodHistory(self, timespan, bucketSize):
        """
        Retrieve number of running instances in each bucket by Running Pod History
        """
        return self.virtualMachineDB.getRunningInstancesByRunningPodHistory(timespan, bucketSize)

    types_getRunningInstancesByImageHistory = [int, int]

    def export_getRunningInstancesByImageHistory(self, timespan, bucketSize):
        """
        Retrieve number of running instances in each bucket by Running Pod History
        """
        return self.virtualMachineDB.getRunningInstancesByImageHistory(timespan, bucketSize)
