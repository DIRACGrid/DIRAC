"""
   OcciEndpoint is Endpoint base class implementation for the Occi cloud service.
"""

import os
import requests
from requests.auth import HTTPBasicAuth
import uuid
import base64

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Cloud.Endpoint import Endpoint
from DIRAC.Resources.Cloud.KeystoneClient import KeystoneClient
from DIRAC.Core.Utilities.File import makeGuid

DEBUG = False


class OcciEndpoint(Endpoint):
    """OCCI implementation of the Cloud Endpoint interface"""

    def __init__(self, parameters=None):
        super().__init__(parameters=parameters)
        # logger
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.valid = False
        self.vmType = self.parameters.get("VMType")
        self.site = self.parameters.get("Site")

        # Prepare the authentication request parameters
        self.session = None
        self.authArgs = {}
        self.user = self.parameters.get("User")
        self.password = self.parameters.get("Password")
        self.loginMode = False
        if self.user and self.password:
            # we have the login/password case
            self.authArgs["auth"] = HTTPBasicAuth(self.user, self.password)
            self.authArgs["verify"] = False
            self.loginMode = True
        else:
            # we have the user proxy case
            self.userProxy = os.environ.get("X509_USER_PROXY")
            self.userProxy = self.parameters.get("Proxy", self.userProxy)
            if self.userProxy:
                self.parameters["Proxy"] = self.userProxy
            if self.userProxy is None:
                self.log.error("User proxy is not defined")
                self.valid = False
                return
            self.authArgs["cert"] = self.userProxy
            self.caPath = self.parameters.get("CAPath", "/etc/grid-security/certificates")
            self.parameters["CAPath"] = self.caPath
            self.authArgs["verify"] = self.caPath
            if self.parameters.get("Auth") == "voms":
                self.authArgs["data"] = '{"auth":{"voms": true}}'

        self.serviceUrl = self.parameters.get("EndpointUrl")
        self.computeUrl = None
        self.tenant = self.parameters.get("Tenant")
        self.token = None
        self.scheme = {}
        result = self.initialize()
        if result["OK"]:
            self.log.debug("OcciEndpoint created and validated")
            self.valid = True

        # import pprint
        # pprint.pprint( self.scheme )

    def initialize(self):

        try:
            result = requests.head(self.serviceUrl + "/-/", headers={"Content-Type": "text/plain"}, **self.authArgs)
        except Exception as exc:
            return S_ERROR(repr(exc))

        # for key,value in result.headers.items():
        #  print "AT >>> initialize", key,value
        # print "AT >>> initialize text", result.text

        return self.__checkConnection()

    def __getKeystoneUrl(self):

        # The URL can be specified in the configuration
        if self.parameters.get("KeystoneURL"):
            return S_OK(self.parameters.get("KeystoneURL"))

        # Make a trial service call
        try:
            result = requests.head(self.serviceUrl + "/-/", headers={"Content-Type": "text/plain"}, **self.authArgs)
        except Exception as e:
            return S_ERROR(str(e))

        # print "AT >>> __getKeystoneUrl", result, result.text
        # print "AT >>> __getKeystoneUrl", result.headers

        # This is not an authentication error
        if result.status_code != 401 or result.headers is None:
            return S_OK(None)
            # return S_ERROR('Do not recognise response when connecting to ' + self.serviceUrl)

        if "www-authenticate" not in result.headers:
            return S_OK(None)

        if not result.headers["www-authenticate"].startswith("Keystone uri="):
            return S_ERROR(
                'Only Keystone authentication is currently supported (instead got "%s")'
                % result.headers["www-authenticate"]
            )

        try:
            keystoneURL = result.headers["www-authenticate"][14:-1]
        except BaseException:
            return S_ERROR("Failed to find Keystone URL in %s" % result.headers["www-authenticate"])

        return S_OK(keystoneURL)

    def __getSchemaDefinitions(self):

        try:
            response = self.session.get("%s/-/" % self.serviceUrl, headers={"Accept": "text/plain,text/occi"})

        except Exception as exc:
            return S_ERROR("Failed to get schema definition: %s" % str(exc))

        if response.status_code != 200:
            return S_ERROR("Failed to get schema definition", response.text)

        self.scheme = {}

        categories = response.text.split("\n")[1:]
        for category in categories:
            if category:
                values = category.split(";")
                categoryName = values[0][values[0].find(":") + 1 :].strip()
                scheme = None
                className = None
                title = None
                location = None
                for prop in values:
                    if "scheme=" in prop:
                        scheme = prop.strip().replace("scheme=", "").replace('"', "")
                    if "class=" in prop:
                        className = prop.strip().replace("class=", "").replace('"', "")
                    if "title=" in prop:
                        title = prop.strip().replace("title=", "").replace('"', "")
                    if "location=" in prop:
                        tmp = prop.strip()
                        tmp = tmp.replace("https://", "").replace("http://", "").replace('"', "")
                        tmp = tmp[tmp.find("/") :]
                        location = tmp

                if className is None:
                    return S_ERROR("Failed to get schema definition:", "no class for category %s" % categoryName)
                self.scheme.setdefault(className, {})
                self.scheme[className][categoryName] = {}
                if scheme is not None:
                    self.scheme[className][categoryName]["scheme"] = scheme
                if title is not None:
                    self.scheme[className][categoryName]["title"] = title
                if location is not None:
                    self.scheme[className][categoryName]["location"] = location

        return S_OK()

    def __checkConnection(self):
        """
        Checks connection status by trying to list the images.

        :return: S_OK | S_ERROR
        """
        self.session = requests.Session()
        self.session.mount(self.serviceUrl, requests.adapters.HTTPAdapter(pool_connections=20))
        self.session.verify = self.authArgs["verify"]

        # Retrieve token
        result = self.__getKeystoneUrl()
        if not result["OK"]:
            return result

        self.keystoneUrl = result["Value"]
        if self.keystoneUrl is not None:
            keystoneClient = KeystoneClient(self.keystoneUrl, self.parameters)
            result = keystoneClient.getToken()
            if not result["OK"]:
                return result
            self.token = result["Value"]
            self.session.headers.clear()
            self.session.headers.update({"X-Auth-Token": self.token})
            self.session.verify = self.authArgs["verify"]
        else:
            if self.loginMode:
                self.session.auth = self.authArgs["auth"]
                self.session.verify = self.authArgs["verify"]
            else:
                self.session.cert = self.userProxy
                self.session.verify = self.caPath

        result = self.__getSchemaDefinitions()
        if not result["OK"]:
            return result
        self.computeUrl = "%s/compute/" % (self.serviceUrl)
        return S_OK()

    def createInstances(self, vmsToSubmit):
        outputDict = {}
        message = ""
        for nvm in range(vmsToSubmit):
            instanceID = makeGuid()[:8]
            createPublicIP = "ipPool" in self.parameters
            result = self.createInstance(instanceID, createPublicIP)
            if result["OK"]:
                nodeID, publicIP = result["Value"]
                self.log.debug(f"Created VM instance {nodeID}/{instanceID} with publicIP {publicIP}")
                nodeDict = {}
                nodeDict["PublicIP"] = publicIP
                nodeDict["InstanceID"] = instanceID
                nodeDict["NumberOfProcessors"] = self.parameters.get("NumberOfProcessors", 1)
                # nodeDict['RAM'] = self.flavor.ram
                # nodeDict['DiskSize'] = self.flavor.disk
                # nodeDict['Price'] = self.flavor.price
                outputDict[nodeID] = nodeDict
            else:
                message = result["Message"]
                break

        # We failed submission utterly
        if not outputDict:
            return S_ERROR("No VM submitted: %s" % message)

        return S_OK(outputDict)

    def __renderCategory(self, category, className):

        if className not in self.scheme:
            return None
        if category not in self.scheme[className]:
            return None

        output = 'Category: {}; scheme="{}"; class="{}"'.format(
            category,
            self.scheme[className][category]["scheme"],
            className,
        )
        for attribute in ["location", "title"]:
            if attribute in self.scheme[className][category]:
                output += f'; {attribute}="{self.scheme[className][category][attribute]}"'

        return output + "\n"

    def createInstance(self, instanceID="", createPublicIP=True):
        """
        This creates a VM instance for the given boot image
        and creates a context script, taken the given parameters.
        Successful creation returns instance VM

        Boots a new node on the OpenStack server defined by self.endpointConfig. The
        'personality' of the node is done by self.imageConfig. Both variables are
        defined on initialization phase.

        The node name has the following format:
        <bootImageName><contextMethod><time>

        It boots the node. If IPpool is defined on the imageConfiguration, a floating
        IP is created and assigned to the node.

        :return: S_OK( ( nodeID, publicIP ) ) | S_ERROR
        """

        imageID = self.parameters.get("ImageID")
        flavor = self.parameters.get("FlavorName")
        self.parameters["VMUUID"] = instanceID
        self.parameters["VMType"] = self.parameters.get("CEType", "Occi")

        result = self._createUserDataScript()
        if not result["OK"]:
            return result
        userData = str(result["Value"])

        headers = {"Accept": "text/plain,text/occi", "Content-Type": "text/plain,text/occi", "Connection": "close"}

        data = self.__renderCategory("compute", "kind")
        for cat in ["user_data", imageID, flavor]:
            item = self.__renderCategory(cat, "mixin")
            if item is None:
                return S_ERROR("Category %s not defined in the scheme" % cat)
            data += item
        data += 'X-OCCI-Attribute: occi.core.id="%s"\n' % str(uuid.uuid4())
        data += 'X-OCCI-Attribute: occi.core.title="%s"\n' % instanceID
        data += 'X-OCCI-Attribute: occi.compute.hostname="%s"\n' % instanceID
        data += 'X-OCCI-Attribute: org.openstack.compute.user_data="%s"' % base64.b64encode(userData)
        # data += 'X-OCCI-Attribute: org.openstack.credentials.publickey.data="ssh-rsa ' + sshPublicKey + ' vmdirac"'

        del self.authArgs["data"]

        result = self.session.post(self.computeUrl, data=data, headers=headers, **self.authArgs)

        # print "AT >>> createInstance", result, result.headers
        # print "AT >>> result.text", result.text

        if result.status_code == 201:
            nodeID = result.text.split()[-1]
            return S_OK((nodeID, None))

        return S_ERROR("Failed VM creation: %s" % result.text)

    def getVMIDs(self):
        """Get all the VM IDs on the endpoint

        :return: list of VM ids
        """

        try:
            response = self.session.get(self.computeUrl)
        except Exception as e:
            return S_ERROR("Cannot connect to " + self.computeUrl + " (" + str(e) + ")")

        vmIDs = [id.split()[1] for id in response.text.split("\n") if id.startswith("X-OCCI-Location:")]
        return S_OK(vmIDs)

    def getVMStatus(self, nodeID):
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
        url = f"{self.computeUrl}/{os.path.basename(nodeID)}"
        try:
            response = self.session.get(url)
        except Exception as e:
            return S_ERROR("Cannot get node details for %s (" % nodeID + str(e) + ")")

        status = "Unknown"
        for item in response.text.split("\n"):
            if "occi.compute.state" in item:
                status = item.split("=")[1].replace('"', "")
        return S_OK(status)

    def getVMNetworks(self):
        """Get a network object corresponding to the networkName

        :param str networkName: network name
        :return: S_OK|S_ERROR network object in case of S_OK
        """
        networkUrl = "%s/network/" % self.serviceUrl
        try:
            response = self.session.get(networkUrl)
        except Exception as e:
            return S_ERROR("Cannot get network details")

        if response.status_code != 200:
            return S_ERROR("Failed to get available networks")

        networks = []
        for line in response.text.split("\n"):
            networks.append(line.split()[1].split("/")[-1])

        return S_OK(networks)

    def getVMNetworkInterface(self, network):
        """Get a network object corresponding to the networkName

        :param str networkName: network name
        :return: S_OK|S_ERROR network object in case of S_OK
        """
        headers = {"Accept": "application/occi,application/json"}
        networkUrl = f"{self.serviceUrl}/network/{network}"
        try:
            response = self.session.get(networkUrl)
        except Exception as e:
            return S_ERROR("Cannot get network details")

        if response.status_code != 200:
            return S_ERROR("Failed to get available networks")

        return S_OK(response.text)

    def stopVM(self, nodeID, publicIP=""):
        """
        Given the node ID it gets the node details, which are used to destroy the
        node making use of the libcloud.openstack driver. If three is any public IP
        ( floating IP ) assigned, frees it as well.

        :Parameters:
          **uniqueId** - `string`
            openstack node id ( not uuid ! )
          **public_ip** - `string`
            public IP assigned to the node if any

        :return: S_OK | S_ERROR
        """

        url = f"{self.computeUrl}/{os.path.basename(nodeID)}"
        try:
            response = self.session.delete(url)
        except Exception as e:
            return S_ERROR("Cannot delete node %s (" % nodeID + str(e) + ")")

        if response.status_code == 200:
            return S_OK(response.text)
        else:
            return S_ERROR(response.text)

    def assignFloatingIP(self, nodeID):
        """
        Given a node, assign a floating IP from the ipPool defined on the imageConfiguration
        on the CS.

        :Parameters:
          **node** - `libcloud.compute.base.Node`
            node object with the vm details

        :return: S_OK( public_ip ) | S_ERROR
        """

        result = self.getVMNetworks()
        if not result["OK"]:
            return result

        network = result["Value"][1]
        networkInterfaceID = str(uuid.uuid4())[:8]

        headers = {"Accept": "text/plain,text/occi", "Content-Type": "text/plain,text/occi", "Connection": "close"}

        nodeRef = f"{self.computeUrl}/{os.path.basename(nodeID)}"
        nodeRef = nodeRef.replace("//", "/")

        data = (
            'Category: networkinterface;scheme="http://schemas.ogf.org/occi/infrastructure#";'
            'class="kind";location="/link/networkinterface/";title="networkinterface link"\n'
        )
        data += 'X-OCCI-Attribute: occi.core.source="%s"\n' % nodeRef
        data += f'X-OCCI-Attribute: occi.core.target="{self.serviceUrl}/network/{network}"\n'
        data += 'X-OCCI-Attribute: occi.core.id="%s"' % networkInterfaceID

        headers["Content-Length"] = str(len(data))
        result = self.session.post("%s/link/networkinterface/" % self.serviceUrl, headers=headers, data=data)

        if result.status_code != 201:
            return S_ERROR(result.text)
        else:
            return S_OK(result.text.split()[1])

    def getVMFloatingIP(self, publicIP):

        return S_ERROR("Not implemented")

    def deleteFloatingIP(self, nodeID):
        """
        Deletes a floating IP <public_ip> from the server.

        :param str publicIP: public IP to be deleted
        :param object node: node to which IP is attached
        :return: S_OK | S_ERROR
        """

        headers = {"Accept": "text/plain,text/occi", "Content-Type": "text/occi", "Connection": "close"}

        nodeURL = f"{self.serviceUrl}/link/networkinterface/{os.path.basename(nodeID)}"
        nodeURL = nodeURL.replace("//", "/")

        result = self.session.delete(nodeURL, headers=headers)

        if result.status_code != 200:
            return S_ERROR(result.text)
        else:
            return S_OK(result.text)
