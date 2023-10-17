""" OpenNebula XML-RPC (Version 6) Driver

To use this in DIRAC, on the CE Resource set:
 - CEType = Cloud
 - CloudType = DIRAC.Resources.Computing.CloudProviders.OpenNebula.OpenNebula_6_0_NodeDriver
 - Driver_host = <hostname of your cloud provider>
 - Driver_port = 2633
 - Driver_secure = True (for SSL)
 - Instance_Image = name:<name of image to use>
 - Instance_Flavor = name:<name of template to use>

(Key and Secret should be set to your username & password in cloud.auth)
"""

from base64 import b64encode
from libcloud.utils.py3 import ET
from libcloud.compute.drivers.opennebula import OpenNebulaNodeDriver, OpenNebulaNodeSize, OpenNebulaNetwork
from libcloud.compute.base import NodeDriver, NodeState, Node
from libcloud.compute.base import NodeImage, NodeSize, StorageVolume
from libcloud.common.base import ConnectionUserAndKey, XmlResponse
from libcloud.common.types import LibcloudError
from libcloud.common.xmlrpc import XMLRPCConnection, XMLRPCResponse


class OpenNebulaXMLRPCResponse(XMLRPCResponse):
    """
    Class for protocol responses in the OpenNebula XML-RPC Protocol.
    """

    def parse_body(self):
        """Decode the return body to extract the response status.
        In error cases raise a LibCloudError with the inner message.
        For successful requests, return either a base type or an XML
        ElementTree of the response data.
        """
        res = super().parse_body()
        success, value = res[0:2]
        if not success:
            # Non protocol error at server
            # Value contains the error string
            raise LibcloudError(value, driver=self)
        # Value is either an XML string of a base object
        # i.e. an int in the case of just an ID being returned
        if isinstance(value, str):
            return ET.fromstring(value)
        else:
            return value


class OpenNebulaXMLRPCConnection(XMLRPCConnection, ConnectionUserAndKey):
    """
    Connection class for new OpenNebula XML-RPC protocol with basic
    (username/password) auth.
    """

    responseCls = OpenNebulaXMLRPCResponse
    endpoint = "/RPC2"

    def request(self, method, *args, **kwargs):
        """Call XML-RPC method on OpenNebula server using the standard
        username/password authentication.
        The method is called with "username:password" as the first
        argument; other arguments are sent after this.
        """
        # First parmaeter is the username/password auth string
        auth_str = f"{self.user_id}:{self.key}"
        real_args = (method, auth_str) + args
        return super().request(*real_args, **kwargs)


class OpenNebula_6_0_NodeDriver(OpenNebulaNodeDriver):
    """
    OpenNebula.org node driver for OpenNebula.org v6.0.
    """

    name = "OpenNebula (v6.0)"
    connectionCls = OpenNebulaXMLRPCConnection

    # List function suppport filtering
    # These are the parameters to get all entries
    # (Filter -2 = ALL, Start = 0, End -1 = ALL
    REQ_FILTER_ALL = (-2, 0, -1)
    # Filter for VM state
    # Unstopped is all nodes in a non-terminated state
    REQ_FILTER_UNSTOPPED = -1
    # Numeric State ID value mappings, used for node states
    STATE_ID_MAP = {
        # 0: Init
        1: NodeState.PENDING,
        2: NodeState.PAUSED,  # Hold
        3: NodeState.RUNNING,  # Active
        4: NodeState.STOPPED,
        5: NodeState.SUSPENDED,
        6: NodeState.TERMINATED,  # Done
        7: NodeState.ERROR,  # Failed
        8: NodeState.STOPPED,  # Power Off
        # 9: Undeployed
        # 10: Cloning
        # 11: Cloning Failure
    }

    def __new__(cls, *args, **kwargs):
        return super(NodeDriver, cls).__new__(cls)

    def create_node(
        self, name, size, image=None, network=None, context=None, ex_onhold=False, ex_tmpl_network=True, **kwargs
    ):
        tmpl_id = None
        if isinstance(size, int):
            tmpl_id = size
        elif isinstance(size, NodeSize):
            tmpl_id = int(size.id)

        # Allow use of ex_userdata in place of context
        if context is None and "ex_userdata" in kwargs:
            context = kwargs["ex_userdata"]

        if tmpl_id is not None:
            return self._create_vm_template(name, tmpl_id, context, ex_onhold, ex_tmpl_network)
        else:
            return self._create_vm_direct(name, size, image, network, context, ex_onhold)

    def _create_vm_template(self, name, tmpl_id, context, ex_onhold, ex_tmpl_network):
        extra_str = self._gen_context(context, ex_tmpl_network)
        res = self.connection.request("one.template.instantiate", tmpl_id, name, ex_onhold, extra_str)
        return self.ex_get_node_details(res.object)

    def _create_vm_direct(self, name, size, image, network, context, ex_onhold):
        tmpl_str = self._gen_template(name, size, image, network, context)
        res = self.connection.request("one.vm.allocate", tmpl_str, ex_onhold)
        return self.ex_get_node_details(res.object)

    def destroy_node(self, node, ex_hard=False):
        action = "terminate"
        if ex_hard:
            action = "terminate-hard"
        return self.ex_node_action(action, node)

    def reboot_node(self, node, ex_hard=False):
        action = "reboot"
        if ex_hard:
            action = "reboot-hard"
        return self.ex_node_action(action, node)

    def list_images(self):
        res = self.connection.request("one.imagepool.info", *self.REQ_FILTER_ALL)
        return self._to_images(res.object)

    def list_nodes(self):
        res = self.connection.request("one.vmpool.info", *self.REQ_FILTER_ALL, self.REQ_FILTER_UNSTOPPED)
        return self._to_nodes(res.object)

    def list_sizes(self, location=None):
        res = self.connection.request("one.templatepool.info", *self.REQ_FILTER_ALL)
        return self._to_sizes(res.object)

    def list_networks(self):
        res = self.connection.request("one.vnpool.info", *self.REQ_FILTER_ALL)
        return self._to_networks(res.object)

    def ex_get_node_details(self, node_id):
        res = self.connection.request("one.vm.info", node_id)
        return self._to_node(res.object)

    def ex_node_action(self, action, node):
        node_id = None
        if isinstance(node, int):
            node_id = node
        else:
            node_id = node.id
        self.connection.request("one.vm.action", action, node_id)
        # Action only returns ID, exception thrown on error
        return None

    def _to_images(self, images_obj):
        images = []
        for element in images_obj.findall("IMAGE"):
            images.append(NodeImage(id=int(element.findtext("ID")), name=element.findtext("NAME"), driver=self))
        return images

    def _to_node(self, node_elem):
        # Work out state
        state_id = int(node_elem.findtext("STATE"))
        state = NodeState.UNKNOWN
        if state_id in self.STATE_ID_MAP:
            state = self.STATE_ID_MAP[state_id]
        # Find network IPs
        # We can't distinguish between public/private
        # So just store them all in private list
        private_ips = []
        template = node_elem.find("TEMPLATE")
        if template:
            for nic in template.findall("NIC"):
                ip_addr = nic.findtext("IP")
                if ip_addr:
                    private_ips.append(ip_addr)
        return Node(
            id=node_elem.findtext("ID"),
            name=node_elem.findtext("NAME"),
            state=state,
            public_ips=[],
            private_ips=private_ips,
            driver=self,
        )

    def _to_nodes(self, nodes_obj):
        nodes = []
        for element in nodes_obj.findall("VM"):
            nodes.append(self._to_node(element))
        return nodes

    def _to_sizes(self, sizes_obj):
        sizes = []
        for element in sizes_obj.findall("VMTEMPLATE"):
            template = element.find("TEMPLATE")
            size_ram = template.findtext("MEMORY")
            if size_ram is not None:
                size_ram = int(size_ram)
            size_cpu = template.findtext("CPU")
            if size_cpu is not None:
                size_cpu = int(size_cpu)
            obj = OpenNebulaNodeSize(
                id=int(element.findtext("ID")),
                name=element.findtext("NAME"),
                ram=size_ram,
                cpu=size_cpu,
                disk=None,
                bandwidth=None,
                price=None,
                driver=self,
            )
            sizes.append(obj)
        return sizes

    def _to_networks(self, networks_obj):
        networks = []
        for element in networks_obj.findall("VNET"):
            networks.append(
                OpenNebulaNetwork(
                    id=int(element.findtext("ID")), name=element.findtext("NAME"), size=None, address=None, driver=self
                )
            )
        return networks

    def _gen_context(self, context, en_network=True):
        extra = []
        if context is not None:
            userdata = b64encode(bytes(context, "utf-8")).decode("utf-8")
            extra.append(f'USERDATA = "{userdata}"')
            extra.append('USERDATA_ENCODING = "base64"')
        if en_network:
            extra.append('NETWORK = "YES"')
        extra_str = ""
        if extra:
            extra_str = "CONTEXT = [\n" + ",\n".join(extra) + "\n]\n"
        return extra_str

    def _gen_template(self, name, size, image, network=None, context=None):
        template = []
        template.append(f'NAME = "{name}"')
        template.append(f'CPU="{size.cpu}"')
        template.append(f'MEMORY="{size.ram}"')
        template.append(f'DISK = [ IMAGE_ID="{image.id}" ]')
        en_network = network is not None
        if en_network:
            template.append(f'NIC = [ NETWORK = "{network.name}" ]')
        if context is not None:
            template.append(self._gen_context(context, en_network))
        return "\n".join(template)
