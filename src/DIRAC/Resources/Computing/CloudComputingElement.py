r""" Cloud Computing Element

This allows submission to cloud sites using libcloud (via the standard
SiteDirector agent). The instances are contextualised using cloud-init.

Running cloud VM instances containing pilots is very analogous to classic cloud
jobs. There are however some things that work differently:

- File I/O: A small amount of input may be transferred through the
  instance metadata, but after that the VM is inaccessible.
- Authentication: Most cloud endpoints use a password or API style credentials
  rather than a grid style proxy based authentication. The pilot still requires
  a suitable proxy, but this cannot be renewed via the cloud interface due to
  the I/O limitations.
- Pilot (VM) Tidy-up: Cloud providers will not remove stopped instances by
  default.

To avoid the proxy renewal limitations, an alternate pilot proxy is used within
the instances. This can either be a longer version of the usual pilot proxy or
a pilot proxy generated from another dedicated cert/user. The proxy contains
the DIRAC group, but no VOMS (as this would likely expire too quickly).

By default it is assumed that a generic CentOS7 base image is being used. This
will be fully contextualised using cloud-init:

- CVMFS & Singularity will be installed.
- A dirac user will be created to run the jobs.
- Pilot proxy and start-up scripts will be installed in /mnt.
- The usual pilot script will be placed in the dirac home directory and
  the start-up scripts are run (as the dirac user).
- After the pilot terminates, the machine is stopped by calling halt.

A partially or fully pre-configured image may be used instead and the
cloud-init template can be customised as necessary for this or any use case.
This is recommended on production systems to cut-down on the overhead when
starting many new instances.

The majority of cloud providers identify instances with some form of unique
identifier (generally a UUID), this is used in the pilot references. Each
instance can generally also have a "friendly name" associated with it, which
may not be unique. We set the friendly name to match a string that can be
pattern matched; this allows any stopped instances to be found & removed
automatically without affecting other VMs potentially running as the same user.

Instances that match the "friendly name" prefix and have been running above a
maximum lifetime are assumed to be stuck or lost and will be removed. This is
to ensure that instances don't reserve/consume resources indefinitely.

Most cloud authentication systems require some form of static secret such as a
password or token. To store these securely we load them from an ini format
file, which should only be readable by the dirac service user on the host. The
values can be stored in the DEFAULT section of the ini file, or a more specific
section using the CE hostname can be used.

The special value PROXY will cause the secret to be replaced with the path to
the proxy that the site director would normally use to submit a job. This is
typically used for FedCloud sites using the libcloud OpenStack VOMS auth
plugin.
::

  [DEFAULT]
  key = "myusername"
  secret = "mypassword"

  [cloudprov.mysite.example]
  key = "cloudprovuser"
  secret = "01234567"

  [fedcloud.othersite.example]
  key = "fedclouduser"
  secret = "PROXY"

Configuration
-------------

The configuration is made up of a number of categories: These options are
loaded from the CE level, but can be overridden by the queue.

CloudType:
  (Required) This should match the libcloud driver name for the Cloud you're
  trying to access. e.g. For OpenStack this should be "OPENSTACK".

CloudAuth:
  (Optional) This sets the path to the authentication ini file as described
  above. Should be an absolute path but may use environment variables.
  Defaults to (DIRAC.rootPath)/etc/cloud.auth.

Driver\_\*:
  (Required) All options starting with Driver\_ will have the prefix stripped
  and be passed to the libcloud Driver object constructor. See the libcloud
  manual/examples for the options required for any given driver.

Instance_Image:
  (Required) The raw ID of the image to use or the name of the image prefixed
  by "name:".

Instance_Flavor:
  (Required) The raw ID of the flavor to use or the name of a flavor
  prefixed by "name:".

Instance_Networks:
  (Optional) A comma seperated list of either the raw IDs or the names
  prefixed by "name:" of the networks to use.

Instance_SSHKey:
  (Optional) The ID of an SSH key (on OpenStack this is just a plain name).
  If not specified the node will be booted without an extra key.

Context_Template:
  (Optional) The path to the cloudinit.template file to use for these
  instances. If unset the default template file will be used.

Context_ExtPackages:
  (Optional) Comma separated list of extra packages to install on the VM.
  Note: It is highly recommended to use SingularityCE with a container
  image with the required packages instead.

Context_ProxyLifetime:
  (Optional) When submitting an instance, it will be provisioned with a new
  proxy with the same properties as the one provided by the SiteDirector but
  with an extended lifetime. This option sets the lifetime of the new proxy
  in seconds: It must be greater than the maximum time jobs can run for in
  the instance. Defaults to two weeks.

Context_MaxLifetime:
  (Optional) The maximum lifetime of an instance in seconds. Any instances
  older than this will be removed regardless of state. Defaults to two weeks.

Example
-------

The following is an example set of settings for an OpenStack based cloud::

  CE = cloudprov.mysite.example
  CEType = Cloud
  CloudType = OPENSTACK
  Driver_ex_force_auth_url = https://cloudprov.mysite.example:5000
  Driver_ex_force_auth_version = 3.x_password
  Driver_ex_tenant_name = clouduser
  Instance_Image = name:CentOS-7-x86_64-GenericCloud-1905
  Instance_Flavor = name:m1.medium
  Instance_Networks = name:my_public_net,name:my_private_net
  Instance_SSHKey = mysshkey

"""

import uuid
import os
import sys
import yaml
import configparser
import datetime
from libcloud.compute.types import Provider, NodeState
from libcloud.compute.providers import get_driver
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from DIRAC import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

# Standard CE name
CE_NAME = "CloudCE"
# The string prefix used to construct & find instance friendly names
# A random 8-character hexadecimal string will be appended to this to
# construct the full name.
VM_NAME_PREFIX = "VMDIRAC3_"
VM_ID_PREFIX = "cloud://"
OPT_PROVIDER = "CloudType"
OPT_AUTHFILE = "CloudAuth"
DEF_AUTHFILE = os.path.join(rootPath, "etc/cloud.auth")
# default proxy lifetime (2 weeks)
DEF_PROXYLIFETIME = 1209600
DEF_PROXYGRACE = 86400
# default max instance lifetime in seconds
# all instances older than this will be removed
DEF_MAXLIFETIME = 1209600


class CloudComputingElement(ComputingElement):
    """Cloud computing element class
    Submits pilot jobs as VMs with libcloud.
    """

    def _getDriverOptions(self):
        """Extracts driver options from CE parameters.

        :return: dictionary of driver constructor parameters
        """
        # Slice Driver_ off start of names
        return {k[7:]: v for k, v in self.ceParameters.items() if k.startswith("Driver_")}

    def _getDriverAuth(self):
        """Gets driver authentication parameters
        from auth config file.

        :return: tuple containing key and secret strings
        """
        config = configparser.ConfigParser()
        configFile = self.ceParameters.get(OPT_AUTHFILE, DEF_AUTHFILE)
        if not os.path.exists(configFile):
            raise RuntimeError("cloud auth config file not found: %s" % configFile)
        config.read(configFile)
        sectionName = self.ceName
        if sectionName not in config:
            sectionName = "DEFAULT"
        try:
            key = config[sectionName]["key"]
            secret = config[sectionName]["secret"]
        except KeyError:
            raise RuntimeError("Invalid auth config for host %s" % self.ceName)
        # If the secret is set to the magic string "PROXY"
        # we instead return a path to a grid proxy file
        if secret == "PROXY":
            if self._origProxy:
                secret = self._origProxy
            else:
                self.log.warn("Proxy for %s not set!" % self.ceName)
                secret = ""
        return (key, secret)

    def _getDriver(self, refresh=False):
        """Return an instance of libcloud Driver for this CE.

        :param: refresh If set to true, force the driver instance
                        to be recreated.

        :return: libcloud Driver instance.
        """
        if self._cloudDriver and not refresh:
            return self._cloudDriver

        provName = self.ceParameters.get(OPT_PROVIDER, "").upper()
        # check if provider (type of cloud) exists
        if not provName or not hasattr(Provider, provName):
            self.log.error(f"Provider '{provName}' not found in libcloud for CE {self.ceName}.")
            raise RuntimeError(f"Provider '{provName}' not found in libcloud for CE {self.ceName}.")
        provIntName = getattr(Provider, provName)
        provCls = get_driver(provIntName)
        driverOpts = self._getDriverOptions()
        driverKey, driverOpts["secret"] = self._getDriverAuth()
        self._cloudDriver = provCls(driverKey, **driverOpts)
        return self._cloudDriver

    def _getImage(self):
        """Extracts image id from configuration system.

        :return: image object
        """
        rawID = self.ceParameters.get("Instance_Image", None)
        if not rawID:
            raise KeyError("Image not set in Configuration")
        imageName = None
        if rawID.startswith("name:"):
            imageName = rawID[5:]
        drv = self._getDriver()
        for image in drv.list_images():
            if not imageName and image.id == rawID:
                return image
            elif imageName and image.name == imageName:
                return image
        raise KeyError("No matching image found for %s" % rawID)

    def _getFlavor(self):
        """Extracts flavor from configuration system.

        :return: flavor object
        """
        rawID = self.ceParameters.get("Instance_Flavor", None)
        if not rawID:
            raise KeyError("Flavor not set in Configuration")
        flavorName = None
        if rawID.startswith("name:"):
            flavorName = rawID[5:]
        drv = self._getDriver()
        for flavor in drv.list_sizes():
            if not flavorName and flavor.id == rawID:
                return flavor
            elif flavorName and flavor.name == flavorName:
                return flavor
        raise KeyError("No matching flavor found for %s" % rawID)

    def _getNetworks(self):
        """Extracts network list from configuration system.

        :return: List of network objects or None if not set
        """
        rawIDs = self.ceParameters.get("Instance_Networks", None)
        if not rawIDs:
            return None
        drv = self._getDriver()
        avail_networks = drv.ex_list_networks()
        networks = []
        for netID in rawIDs.split(","):
            found = False
            for net in avail_networks:
                netName = ""
                if netID.startswith("name:"):
                    netName = netID[5:]
                if not netName and net.id == netID:
                    networks.append(net)
                    found = True
                    break
                elif netName and net.name == netName:
                    networks.append(net)
                    found = True
                    break
            if not found:
                raise KeyError("No matching network found for %s" % netID)
        return networks

    def _getSSHKeyID(self):
        """Extract ssh key id from configuration system.

        :return: ssh id string or None if not set
        """
        return self.ceParameters.get("Instance_SSHKey", None)

    def _getMetadata(self, executableFile):
        """Builds metadata from configuration system, cloudinit template
         and dirac pilot job wrapper

        :param str executableFile: path to pilot wrapper script to include in metadata.

        :return: instance specific metadata in mime string format
        """

        default_template = os.path.join(os.path.dirname(__file__), "cloudinit.template")
        template_file = self.ceParameters.get("Context_Template", default_template)

        exe_str = ""
        with open(executableFile) as efile:
            exe_str = efile.read().strip()
        template = ""
        with open(template_file) as template_fd:
            template = yaml.safe_load(template_fd)
        for filedef in template["write_files"]:
            if filedef["content"] == "PROXY_STR":
                filedef["content"] = self.proxy
            elif filedef["content"] == "EXECUTABLE_STR":
                filedef["content"] = exe_str
        ext_packages = self.ceParameters.get("Context_ExtPackages", None)
        if ext_packages:
            packages = [x.strip() for x in ext_packages.split(",")]
            if "packages" in template:
                template["packages"].extend(packages)
            else:
                template["packages"] = packages

        template_str = yaml.dump(template)
        userData = MIMEMultipart()
        mimeText = MIMEText(template_str, "cloud-config", sys.getdefaultencoding())
        mimeText.add_header("Content-Disposition", 'attachment; filename="pilotconfig"')
        userData.attach(mimeText)
        return str(userData)

    def _renewCloudProxy(self):
        """Takes short lived proxy from the site director and
        promotes it to a long lived proxy keeping the DIRAC group.

        :returns: True on success, false otherwise.
        :rtype: bool
        """
        if not self._cloudDN or not self._cloudGroup:
            self.log.error("Could not renew cloud proxy, DN and/or Group not set.")
            return False

        proxyLifetime = int(self.ceParameters.get("Context_ProxyLifetime", DEF_PROXYLIFETIME))
        # only renew proxy if lifetime is less than configured lifetime
        # self.valid is a datetime
        if self.valid - datetime.datetime.utcnow() > proxyLifetime * datetime.timedelta(seconds=1):
            return True
        proxyLifetime += DEF_PROXYGRACE
        proxyManager = ProxyManagerClient()
        self.log.info(f"Downloading proxy with cloudDN and cloudGroup: {self._cloudDN}, {self._cloudGroup}")
        res = proxyManager.downloadProxy(self._cloudDN, self._cloudGroup, limited=True, requiredTimeLeft=proxyLifetime)
        if not res["OK"]:
            self.log.error("Could not download proxy", res["Message"])
            return False
        resdump = res["Value"].dumpAllToString()
        if not resdump["OK"]:
            self.log.error("Failed to dump proxy to string", resdump["Message"])
            return False
        self.proxy = resdump["Value"]
        self.valid = datetime.datetime.utcnow() + proxyLifetime * datetime.timedelta(seconds=1)
        return True

    def __init__(self, *args, **kwargs):
        """Constructor
        Takes the standard CE parameters.
        See ComputeElement base class for details.
        """
        super().__init__(*args, **kwargs)
        self.ceType = CE_NAME
        self.proxy = ""
        # proxy expiry time (in date time)
        self.valid = datetime.datetime.utcnow()
        self._cloudDriver = None
        self._cloudDN = None
        self._cloudGroup = None
        self._origProxy = None

    def setProxy(self, proxy, valid=0):
        """Take existing proxy, and extract group name.
        Then create new proxy for the cloud pilot user
        bound to the same group with the lifetime set to
        the value specified in the CE config.

        :return: S_OK() or S_ERROR(error string)
        """
        # Store original proxy for FedCloud submission/auth
        # We write this to a file as that's the format we need
        ret = gProxyManager.dumpProxyToFile(proxy)
        if not ret["OK"]:
            self.log.error("Failed to write proxy file", "for {}: {}".format(self.ceName, ret["Message"]))
        self._origProxy = ret["Value"]
        # For a driver refresh to reload the proxy
        self._getDriver(refresh=True)
        # we deliberately log extra errors here,
        # as the return value is not always checked
        res = getProxyInfo(proxy, disableVOMS=True)
        if not res["OK"]:
            self.log.error("getProxyInfo failed", res["Message"])
            return S_ERROR("getProxyInfo did not return OK: %s" % str(res))
        info = res["Value"]
        if not "group" in info:
            self.log.error("No group found in proxy")
            return S_ERROR("No group found in proxy")
        if not "identity" in info:
            self.log.error("No user DN (identity) found in proxy")
            return S_ERROR("No user DN (identity) found in proxy")
        pilotGroup = info["group"]
        pilotDN = info["identity"]
        opsHelper = Operations(group=pilotGroup)
        self._cloudDN = opsHelper.getValue("Pilot/GenericCloudDN", pilotDN)
        self._cloudGroup = pilotGroup
        if not self._renewCloudProxy():
            self.log.error("Failed to renew proxy.")
            return S_ERROR("Failed to renew proxy.")
        return S_OK()

    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        """Creates VM instances

        :param str executableFile: Path to pilot job wrapper file to use
        :param str proxy: Unused, see setProxy()
        :param int numberOfJobs: Number of instances to start
        :return: S_OK/S_ERROR
        """
        if not self._renewCloudProxy():
            return S_ERROR("Failed to renew proxy during job submission.")

        instIDs = []

        # these parameters are identical for each job
        instParams = {}
        instParams["image"] = self._getImage()
        instParams["size"] = self._getFlavor()
        networks = self._getNetworks()
        if networks:
            instParams["networks"] = networks
        instParams["ex_keyname"] = self._getSSHKeyID()
        instParams["ex_userdata"] = self._getMetadata(executableFile)
        instParams["ex_config_drive"] = True

        driver = self._getDriver()

        for _ in range(numberOfJobs):
            # generates an 8 character hex string
            instRandom = str(uuid.uuid4()).upper()[:8]
            instName = VM_NAME_PREFIX + instRandom
            instParams["name"] = instName
            try:
                node = driver.create_node(**instParams)
            except Exception as err:
                self.log.error("Failed to create_node", str(err))
                continue
            instIDs.append(VM_ID_PREFIX + node.id)
        if not instIDs:
            return S_ERROR("Failed to submit any instances.")
        return S_OK(instIDs)

    def killJob(self, jobIDList):
        """Stops VM instances

        :param list jobIDList: Instance IDs to delete.
        :return: S_OK
        """
        driver = self._getDriver()
        for job in jobIDList:
            job = job.replace(VM_ID_PREFIX, "", 1)
            try:
                node = driver.ex_get_node_details(job)
                driver.destroy_node(node)
            except Exception as err:
                self.log.error("Failed to destroy_node", str(err))
                continue
        return S_OK()

    def getCEStatus(self):
        """Counts number of running jobs

        :return: S_OK
        :rtype: dict
        """
        driver = self._getDriver()
        count = 0
        for node in driver.list_nodes():
            if node.name.startswith(VM_NAME_PREFIX):
                count += 1
        result = S_OK()
        result["SubmittedJobs"] = 0
        result["RunningJobs"] = count
        result["WaitingJobs"] = 0
        return result

    def getJobStatus(self, jobIDList):
        """Lookup the status of the given pilot job IDs

        :return: S_OK(dict(jobID -> str(state))
        :rtype: dict
        """
        driver = self._getDriver()
        result = {}
        for jobRef in jobIDList:
            pilotUUID = jobRef.replace(VM_ID_PREFIX, "", 1)
            try:
                node = driver.ex_get_node_details(pilotUUID)
                if not node:
                    # instance cannot be found on the cloud
                    result[jobRef] = "Deleted"
                    continue
                if node.state == NodeState.STOPPED:
                    result[jobRef] = "Done"
                    continue
                else:
                    result[jobRef] = "Running"
                    continue
            except Exception as err:
                # general libcloud error, cloud probably inaccessible
                self.log.warn("Failed to get instance", f"{pilotUUID} state: {repr(err)}")
                result[jobRef] = "Unknown"
                continue
        return S_OK(result)

    def getJobOutput(self, *args, **kwargs):
        """Not implemented:
        There is no standard way of getting files back from an instance.
        We rely on remote pilot logging to collect logs for debugging (or
        an admin logging on to an instance manually).

        :return: S_ERROR, not implemented.
        """
        return S_ERROR("Not Implemented for CloudCE")

    def cleanupPilots(self):
        """Removes all stopped instances and
         removes all instances with a lifetime above threshold from config
         as a fallback to remove instances that have been lost.

        :return: S_OK
        """
        self.log.info("Starting cleanup for %s" % self.ceName)
        try:
            maxLifetime = int(self.ceParameters.get("Context_MaxLifetime", DEF_MAXLIFETIME))
            now = datetime.datetime.utcnow()
            driver = self._getDriver()
            for node in driver.list_nodes():
                if not node.name.startswith(VM_NAME_PREFIX):
                    continue
                # remove shutoff nodes
                if node.state == NodeState.STOPPED:
                    self.log.info(f"Deleting shutoff node: {node.name} ({node.id})")
                    driver.destroy_node(node)
                    continue
                # log error'd node seperately
                if node.state == NodeState.ERROR:
                    self.log.info(f"Deleting node in ERROR state: {node.name} ({node.id})")
                    driver.destroy_node(node)
                    continue

                # calculate lifetime (instance age in seconds) in a timezone agnostic way
                datetmp = node.created_at.utctimetuple()[0:6]
                created_at = datetime.datetime(*datetmp)
                lifetime = (now - created_at).total_seconds()
                # remove all nodes older than maxLifetime, independent of their state
                if lifetime > maxLifetime:
                    self.log.info(f"Deleting old node: {node.name} ({node.id}) with lifetime {int(lifetime)}")
                    driver.destroy_node(node)
        except Exception as err:
            self.log.error(f"Failed to clean up instances ({self.ceName}): {err}")
            return S_ERROR(f"Failed to clean up instances ({self.ceName}): {err}")
        return S_OK()
