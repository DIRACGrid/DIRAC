"""
   OpenStackEndpoint is Endpoint base class implementation for the OpenStack cloud service.
"""

import requests
import json
import base64

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Cloud.Endpoint import Endpoint
from DIRAC.Resources.Cloud.KeystoneClient import KeystoneClient
from DIRAC.Core.Utilities.File import makeGuid

DEBUG = False


class OpenStackEndpoint(Endpoint):
    """OpenStack implementation of the Cloud Endpoint interface"""

    def __init__(self, parameters=None, bootstrapParameters=None):
        super().__init__(parameters=parameters, bootstrapParameters=bootstrapParameters)
        # logger
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.ks = None
        self.flavors = {}
        self.images = {}
        self.networks = {}
        self.computeURL = None
        self.imageURL = None
        self.networkURL = None
        self.network = None
        self.project = None
        self.projectID = None
        self.vmInfo = {}
        self.initialized = False

        result = self.initialize()
        if result["OK"]:
            self.log.debug("OpenStackEndpoint created and validated")
        else:
            self.log.error(result["Message"])

    def initialize(self):
        self.caPath = self.parameters.get("CAPath", True)
        self.network = self.parameters.get("Network")
        self.project = self.parameters.get("Project")
        keyStoneURL = self.parameters.get("AuthURL")
        result = self.getProxyFileLocation()
        if result["OK"]:
            self.parameters["Proxy"] = result["Value"]
        self.ks = KeystoneClient(keyStoneURL, self.parameters)
        result = self.ks.getToken()
        if not result["OK"]:
            return result
        self.valid = True
        self.token = result["Value"]
        self.computeURL = self.ks.computeURL
        self.imageURL = self.ks.imageURL
        self.networkURL = self.ks.networkURL
        self.projectID = self.ks.projectID

        self.log.verbose(
            "Service interfaces:\ncompute %s,\nimage %s,\nnetwork %s"
            % (self.computeURL, self.imageURL, self.networkURL)
        )

        result = self.getFlavors()
        if not result["OK"]:
            self.valid = False
        result = self.getImages()
        if not result["OK"]:
            self.valid = False
        self.getNetworks()
        return result

    def getFlavors(self):

        if not self.computeURL or not self.token:
            return S_ERROR("The endpoint object is not initialized")

        url = "%s/flavors/detail" % self.computeURL
        self.log.verbose("Getting flavors details on %s" % url)

        result = requests.get(url, headers={"X-Auth-Token": self.token}, verify=self.caPath)

        output = json.loads(result.text)
        for flavor in output["flavors"]:
            self.flavors[flavor["name"]] = {
                "FlavorID": flavor["id"],
                "RAM": flavor["ram"],
                "NumberOfProcessors": flavor["vcpus"],
            }

        return S_OK(self.flavors)

    def getImages(self):

        if not self.imageURL or not self.token:
            return S_ERROR("The endpoint object is not initialized")

        result = requests.get("%s/v2/images" % self.imageURL, headers={"X-Auth-Token": self.token}, verify=self.caPath)

        output = json.loads(result.text)
        for image in output["images"]:
            self.images[image["name"]] = {"id": image["id"]}

        return S_OK(self.images)

    def getNetworks(self):
        """Get a network object corresponding to the networkName

        :param str networkName: network name
        :return: S_OK|S_ERROR network object in case of S_OK
        """
        try:
            result = requests.get(
                "%s/v2.0/networks" % self.networkURL, headers={"X-Auth-Token": self.token}, verify=self.caPath
            )
            output = json.loads(result.text)
        except Exception as exc:
            return S_ERROR("Cannot get networks: %s" % str(exc))

        for network in output["networks"]:
            if network["project_id"] == self.projectID:
                self.networks[network["name"]] = {"NetworkID": network["id"]}
        return S_OK(self.networks)

    def createInstances(self, vmsToSubmit):
        outputDict = {}
        for nvm in range(vmsToSubmit):
            instanceID = makeGuid()[:8]
            result = self.createInstance(instanceID)
            if result["OK"]:
                nodeID = result["Value"]
                self.log.debug(f"Created VM instance {nodeID}/{instanceID}")
                nodeDict = {}
                nodeDict["InstanceID"] = instanceID
                nodeDict["NumberOfProcessors"] = self.parameters["NumberOfProcessors"]
                outputDict[nodeID] = nodeDict
            else:
                self.log.error(
                    "Failed to create OpenStack instance", "{} {} {}".format(nvm, instanceID, result["Message"])
                )
                break

        # We failed submission utterly
        if not outputDict:
            return S_ERROR("No VM submitted")

        return S_OK(outputDict)

    def createInstance(self, instanceID=""):
        """
        This creates a VM instance for the given boot image
        and creates a context script, taken the given parameters.
        Successful creation returns instance VM

        Boots a new node on the OpenStack server defined by self.endpointConfig. The
        'personality' of the node is done by self.imageConfig. Both variables are
        defined on initialization phase.

        The node name has the following format:
        <bootImageName><contextMethod><time>

        :return: S_OK( ( nodeID, publicIP ) ) | S_ERROR
        """

        if not self.initialized:
            self.initialize()

        imageID = self.parameters.get("ImageID")
        if not imageID:
            imageName = self.parameters.get("Image")
            if not imageName:
                return S_ERROR("No image name or ID is specified")
            if not self.images:
                result = self.getImages()
                if not result["OK"]:
                    return result
            imageID = self.images.get(imageName)["id"]
            if not imageID:
                return S_ERROR("Can not get ID for the image: %s" % imageName)
        self.parameters["ImageID"] = imageID
        if "Image" not in self.parameters:
            for image in self.images:
                if self.images[image]["id"] == imageID:
                    self.parameters["Image"] = image

        flavorID = self.parameters.get("FlavorID")
        if not flavorID:
            flavor = self.parameters.get("FlavorName")
            if not flavor:
                return S_ERROR("No flavor name or ID is specified")
            if not self.flavors:
                result = self.getFlavors()
                if not result["OK"]:
                    return result
            flavorID = self.flavors.get(flavor)["FlavorID"]
            if not flavorID:
                return S_ERROR("Can not get ID for the flavor: %s" % flavor)
            numberOfProcessors = self.flavors.get(flavor)["NumberOfProcessors"]
        self.parameters["FlavorID"] = flavorID
        if "NumberOfProcessors" not in self.parameters:
            self.parameters["NumberOfProcessors"] = numberOfProcessors

        networkID = self.parameters.get("NetworkID")
        if not networkID:
            network = self.parameters.get("Network")
            if not self.networks and self.networkURL:
                result = self.getNetworks()
                if not result["OK"]:
                    return result
            if network:
                if network in self.networks:
                    networkID = self.networks[network]["NetworkID"]
            elif self.networks:
                randomNW = self.networks.keys()[0]
                networkID = self.networks[randomNW]["NetworkID"]
            if not networkID:
                self.log.warn("Failed to get ID of the network interface")

        # Pass VMUUID to the instance
        self.parameters["VMUUID"] = instanceID

        result = self._createUserDataScript()
        if not result["OK"]:
            return result
        userDataCrude = str(result["Value"])
        userData = base64.b64encode(userDataCrude.encode()).decode()

        headers = {"X-Auth-Token": self.token}
        requestDict = {
            "server": {
                "user_data": userData,
                "name": "DIRAC_%s" % instanceID,
                "imageRef": imageID,
                "flavorRef": flavorID,
            }
        }
        # Some cloud sites do not expose network service interface, but some do
        if networkID:
            requestDict["server"]["networks"] = [{"uuid": networkID}]

        # Allow the use of pre-uploaded SSH keys
        osSSHKey = self.parameters.get("OSKeyName")
        if osSSHKey:
            requestDict["server"]["key_name"] = osSSHKey

        # print "AT >>> user data", userDataCrude
        # print "AT >>> requestDict", requestDict
        # return S_ERROR()

        try:
            result = requests.post(
                "%s/servers" % self.computeURL, json=requestDict, headers=headers, verify=self.caPath
            )
        except Exception as exc:
            self.log.exception("Exception creating VM")
            return S_ERROR("Exception creating VM: %s" % str(exc))

        if result.status_code in [200, 201, 202, 203, 204]:
            output = json.loads(result.text)
            nodeID = output["server"]["id"]
            return S_OK(nodeID)
        else:
            return S_ERROR("Error creating VM: %s" % result.text)

    def getVMIDs(self):
        """Get all the VM IDs on the endpoint

        :return: list of VM ids
        """

        if not self.initialized:
            result = self.initialize()
            if not result["OK"]:
                return result

        try:
            response = requests.get(
                "%s/servers" % self.computeURL, headers={"X-Auth-Token": self.token}, verify=self.caPath
            )
        except Exception as e:
            return S_ERROR("Cannot connect to " + str(self.computeURL) + " (" + str(e) + ")")

        output = json.loads(response.text)
        idList = []
        for server in output["servers"]:
            idList.append(server["id"])
        return S_OK(idList)

    def getVMStatus(self, vmID):
        """
        Get the status for a given node ID. libcloud translates the status into a digit
        from 0 to 4 using a many-to-one relation ( ACTIVE and RUNNING -> 0 ), which
        means we cannot undo that translation. It uses an intermediate states mapping
        dictionary, SITEMAP, which we use here inverted to return the status as a
        meaningful string. The five possible states are ( ordered from 0 to 4 ):
        RUNNING, REBOOTING, TERMINATED, PENDING & UNKNOWN.

        :Parameters:
          **uniqueId** - `string`
            openstack node id ( not uuid ! )

        :return: S_OK( status ) | S_ERROR
        """

        if not self.initialized:
            self.initialize()

        result = self.getVMInfo(vmID)
        if not result["OK"]:
            return result

        output = result["Value"]
        return S_OK(output["server"])

    def stopVM(self, nodeID):
        """
        Given the node ID it gets the node details, which are used to destroy the
        node

        :param str uniqueId:  openstack node id ( not uuid ! )

        :return: S_OK | S_ERROR
        """

        if not self.initialized:
            self.initialize()

        try:
            response = requests.delete(
                f"{self.computeURL}/servers/{nodeID}", headers={"X-Auth-Token": self.token}, verify=self.caPath
            )
        except Exception as e:
            return S_ERROR("Cannot get node details for %s (" % nodeID + str(e) + ")")

        if response.status_code == 204:
            # VM stopped successfully
            return S_OK(response.text)
        elif response.status_code == 404:
            # VM does not exist already
            return S_OK(response.text)
        else:
            return S_ERROR(response.text)

    def __getVMPortID(self, nodeID):
        """Get the port ID associated with the given VM

        :param str nodeID: VM ID
        :return: port ID
        """
        if nodeID in self.vmInfo and "portID" in self.vmInfo[nodeID]:
            return S_OK(self.vmInfo[nodeID]["portID"])

        # Get the port of my VM
        try:
            result = requests.get(
                "%s/v2.0/ports" % self.networkURL, headers={"X-Auth-Token": self.token}, verify=self.caPath
            )
            output = json.loads(result.text)
            portID = None
            for port in output["ports"]:
                if port["device_id"] == nodeID:
                    portID = port["id"]
                    self.vmInfo.setdefault(nodeID, {})
                    self.vmInfo[nodeID]["portID"] = portID
        except Exception as exc:
            return S_ERROR("Cannot get ports: %s" % str(exc))

        return S_OK(portID)

    def assignFloatingIP(self, nodeID):
        """
        Given a node, assign a floating IP from the ipPool defined on the imageConfiguration
        on the CS.

        :Parameters:
          **node** - `libcloud.compute.base.Node`
            node object with the vm details

        :return: S_OK( public_ip ) | S_ERROR
        """

        if not self.initialized:
            self.initialize()

        result = self.getVMFloatingIP(nodeID)
        if result["OK"]:
            ip = result["Value"]
            if ip:
                return S_OK(ip)

        # Get the port of my VM
        result = self.__getVMPortID(nodeID)
        if not result["OK"]:
            return result
        portID = result["Value"]

        # Get an available floating IP
        try:
            result = requests.get(
                "%s/v2.0/floatingips" % self.networkURL, headers={"X-Auth-Token": self.token}, verify=self.caPath
            )
            output = json.loads(result.text)
        except Exception as e:
            return S_ERROR("Cannot get floatingips")

        fipID = None
        for fip in output["floatingips"]:
            if fip["fixed_ip_address"] is None:
                fipID = fip["id"]
                break

        if fipID is None:
            return S_ERROR("No floating IP available")

        data = {"floatingip": {"port_id": portID}}
        dataJson = json.dumps(data)

        try:
            result = requests.put(
                f"{self.networkURL}/v2.0/floatingips/{fipID}",
                data=dataJson,
                headers={"X-Auth-Token": self.token},
                verify=self.caPath,
            )
        except Exception as e:
            return S_ERROR("Cannot assign floating IP")

        output = json.loads(result.text)

        self.vmInfo.setdefault(nodeID, {})
        self.vmInfo["floatingID"] = output["floatingip"]["id"]

        output = json.loads(result.text)
        self.vmInfo.setdefault(nodeID, {})
        self.vmInfo["floatingID"] = output["floatingip"]["id"]

        ip = output["floatingip"]["floating_ip_address"]
        return S_OK(ip)

    def getVMInfo(self, vmID):

        try:
            response = requests.get(
                f"{self.computeURL}/servers/{vmID}", headers={"X-Auth-Token": self.token}, verify=self.caPath
            )
        except Exception as e:
            return S_ERROR("Cannot get node details for %s (" % vmID + str(e) + ")")

        if response.status_code == 404:
            return S_ERROR("VM ID %s not found" % vmID)

        output = json.loads(response.text)
        if response.status_code == 403:
            if "forbidden" in output:
                return S_ERROR("Cannot get VM info: %s" % output["forbidden"].get("message"))
            else:
                return S_ERROR("Cannot get VM info: access forbidden")

        # Cache some info
        if response.status_code == 200:
            self.vmInfo.setdefault(vmID, {})
            self.vmInfo[vmID]["imageID"] = output["server"]["image"]["id"]
            self.vmInfo[vmID]["flavorID"] = output["server"]["flavor"]["id"]

        return S_OK(output)

    def getVMFloatingIP(self, nodeID):

        result = self.getVMInfo(nodeID)
        if not result["OK"]:
            return result

        floatingIP = None
        output = result["Value"]
        for network, addressList in output["server"]["addresses"].items():
            for address in addressList:
                if address["OS-EXT-IPS:type"] == "floating":
                    floatingIP = address["addr"]

        return S_OK(floatingIP)

    def deleteFloatingIP(self, nodeID, floatingIP=None):
        """
        Deletes a floating IP <public_ip> from the server.

        :param str publicIP: public IP to be deleted
        :param object node: node to which IP is attached
        :return: S_OK | S_ERROR
        """

        if nodeID in self.vmInfo and "floatingID" in self.vmInfo[nodeID]:
            fipID = self.vmInfo[nodeID]["floatingID"]
        else:
            result = self.getVMFloatingIP(nodeID)
            if not result["OK"]:
                return result
            ip = result["Value"]
            if ip is None:
                return S_OK()

            result = self.__getVMPortID(nodeID)
            if not result["OK"]:
                return result

            portID = result["Value"]
            # Get an available floating IP
            try:
                result = requests.get(
                    "%s/v2.0/floatingips" % self.networkURL, headers={"X-Auth-Token": self.token}, verify=self.caPath
                )
                output = json.loads(result.text)
            except Exception as e:
                return S_ERROR("Cannot get floatingips")

            fipID = None
            for fip in output["floatingips"]:
                if fip["port_id"] == portID:
                    fipID = fip["id"]
                    break

        if not fipID:
            return S_ERROR("Can not get the floating IP ID")

        data = {"floatingip": {"port_id": None}}
        dataJson = json.dumps(data)

        try:
            result = requests.put(
                f"{self.networkURL}/v2.0/floatingips/{fipID}",
                data=dataJson,
                headers={"X-Auth-Token": self.token},
                verify=self.caPath,
            )
        except Exception as exc:
            return S_ERROR("Cannot disassociate floating IP: %s" % str(exc))

        if result.status_code == 200:
            return S_OK(fipID)
        else:
            return S_ERROR("Cannot disassociate floating IP: %s" % result.text)
