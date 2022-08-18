#!/usr/bin/env python
""" Virtual Machine Command Line Interface. """

import pprint
import getpass

from DIRAC.Core.Base.CLI import CLI
from DIRAC.Core.Security.Locations import getProxyLocation
from DIRAC.Core.Utilities.PrettyPrint import printTable

from DIRAC.Resources.Cloud.EndpointFactory import EndpointFactory
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getPilotBootstrapParameters, getVMTypeConfig, getVMTypes
from DIRAC.Core.Utilities.File import makeGuid


class VirtualMachineCLI(CLI):
    """Virtual Machine management console"""

    def __init__(self, vo=None):
        CLI.__init__(self)
        self.site = None
        self.endpoint = None
        self.project = None
        self.vmType = None
        self.vo = vo
        self.proxyLocation = None
        self.user = None
        self.password = None

    def do_connect(self, args):
        """Choose the specified cloud endpoint for connection

        usage:
          connect <site> [<endpoint> [project]]
        """

        self.site = None
        self.endpoint = None
        self.project = None

        argss = args.split()
        self.site = argss.pop(0)
        if argss:
            self.endpoint = argss.pop(0)
        if argss:
            self.project = argss.pop(0)

        result = getVMTypeConfig(self.site, self.endpoint)
        if not result["OK"]:
            print("ERROR: can not get the cloud endpoint configuration \n%s" % result["Message"])
            return
        ceDict = result["Value"]
        if not self.project:
            self.project = ceDict.get("Project")
        if not self.endpoint:
            self.endpoint = ceDict["CEName"]
        # Check for authentication details
        authType = ceDict.get("Auth")
        if authType and authType in ["x509", "voms"]:
            # We need proxy to proceed
            self.proxyLocation = None
            proxy = getProxyLocation()
            if not proxy:
                print("ERROR: Requested endpoint requires proxy but it is not found")
                return
            self.proxyLocation = proxy
        else:
            # We need user/login to proceed
            if not ceDict.get("User") or not ceDict.get("Password"):
                print("Endpoint requires user/password")
                self.user = input(["Login:"])
                self.password = getpass.getpass("Password:")

        print(f"Connection: site={self.site}, endpoint={self.endpoint}, project={self.project}")
        self.prompt = f"{self.site}/{self.endpoint}/{self.project}> "

    def __getCE(self):
        """Get cloud Endpoint object"""

        result = EndpointFactory().getCE(self.site, self.endpoint, self.vmType)
        if not result["OK"]:
            print(result["Message"])
            return
        ce = result["Value"]

        # Add extra parameters if any
        extraParams = {}
        if self.project:
            extraParams["Project"] = self.project
        if self.user:
            extraParams["User"] = self.user
        if self.password:
            extraParams["Password"] = self.password

        if extraParams:
            ce.setParameters(extraParams)
            ce.initialize()

        return ce

    def __checkConnection(self):
        """Check that connection details are provided"""
        if not self.site:
            return False
        return True

    def do_list(self, args):
        """Get IDs of VM instances"""

        if not self.__checkConnection():
            print("No connection defined")
            return

        ce = self.__getCE()

        result = ce.getVMIDs()
        if not result["OK"]:
            print("ERROR: %s" % result["Message"])
        else:
            print("\n".join(result["Value"]))

    def do_info(self, args):
        """Get VM status"""

        argss = args.split()
        if not argss:
            print(self.do_status.__doc__)
            return

        longOutput = False
        arg = argss.pop(0)
        if arg == "-l":
            longOutput = True
            vmID = argss.pop(0)
        else:
            vmID = arg

        ce = self.__getCE()
        result = ce.getVMInfo(vmID)

        if not result["OK"]:
            print("ERROR: %s" % result["Message"])
        else:
            pprint.pprint(result["Value"])

    def do_sites(self, args):
        """List available cloud sites"""

        result = getVMTypes()

        print(result)

        siteDict = result["Value"]
        records = []
        for site in siteDict:
            ceStart = True
            for ce in siteDict[site]:
                vmStart = True
                for vmType in siteDict[site][ce]["VMTypes"]:
                    flavor = siteDict[site][ce]["VMTypes"][vmType].get("FlavorName", "Unknown")
                    image = siteDict[site][ce]["VMTypes"][vmType].get(
                        "Image", siteDict[site][ce]["VMTypes"][vmType].get("ImageID", "Unknown")
                    )
                    if ceStart and vmStart:
                        records.append([site, ce, vmType, flavor, image])
                    elif ceStart:
                        records.append(["", "", vmType, flavor, image])
                    else:
                        records.append(["", ce, vmType, flavor, image])
                    vmStart = False
                ceStart = False

        fields = ["Site", "Endpoint", "VM Type", "Flavor", "Image"]
        printTable(fields, records)

    def do_status(self, args):
        """Get VM status"""

        argss = args.split()
        if not argss:
            print(self.do_status.__doc__)
            return
        vmID = argss[0]
        del argss[0]
        longOutput = False
        if argss and args[0] == "-l":
            longOutput = True

        ce = self.__getCE()
        result = ce.getVMStatus(vmID)

        if not result["OK"]:
            print("ERROR: %s" % result["Message"])
        else:
            print(result["Value"]["status"])

    def do_ip(self, args):
        """Assign IP"""

        argss = args.split()
        if not argss:
            return
        vmID = argss[0]

        ce = self.__getCE()
        result = ce.assignFloatingIP(vmID)

        if not result["OK"]:
            print("ERROR: %s" % result["Message"])
        else:
            print(result["Value"])

    def do_token(self, args):
        """Display the current Keystone token if any"""

        ce = self.__getCE()
        if getattr(ce, "token"):
            print(ce.token)
        else:
            print("No token available")

    def do_create(self, args):
        """Create VM at the connected site

        usage:
          create <VMType> [ExtraParam1=Value1 [ExtraParam2=Value2]]
        """

        argss = args.split()
        if not argss:
            print(self.do_create.__doc__)
            return
        self.vmType = argss.pop(0)
        extraParameters = {}
        while argss:
            key, value = argss.pop(0).split("=")
            extraParameters[key] = value

        result = getPilotBootstrapParameters(vo=self.vo)
        bootParameters = result["Value"]
        bootParameters.update(extraParameters)

        ce = self.__getCE()
        ce.setBootstrapParameters(bootParameters)

        diracVMID = makeGuid()[:8]
        result = ce.createInstance(diracVMID)

        if not result["OK"]:
            print("ERROR: %s" % result["Message"])
        else:
            print(result["Value"])

    def do_stop(self, args):
        """Stop VM"""

        argss = args.split()
        if not argss:
            print(self.do_stop.__doc__)
            return
        vmID = argss[0]

        ce = self.__getCE()
        result = ce.stopVM(vmID)

        if not result["OK"]:
            print("ERROR: %s" % result["Message"])
        else:
            print("VM stopped")
