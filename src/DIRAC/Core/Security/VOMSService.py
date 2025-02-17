""" VOMSService class encapsulates connection to the VOMS service for a given VO
"""

import requests

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.Locations import getProxyLocation, getCAsLocation
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO


class VOMSService:
    def __init__(self, vo=None, compareWithIAM=False, useIAM=False):
        """c'tor

        :param str vo: name of the virtual organization (community)
        :param compareWithIAM: if true, also dump the list of users from IAM and compare
        :param useIAM: if True, use Iam instead of VOMS
        """

        if vo is None:
            vo = getVO()
        if not vo:
            raise Exception("No VO name given")

        self.vo = vo

        self.vomsVO = getVOOption(vo, "VOMSName")
        if not self.vomsVO:
            raise Exception(f"Can not get VOMS name for VO {vo}")

        self.urls = []
        result = gConfig.getSections(f"/Registry/VO/{self.vo}/VOMSServers")
        if result["OK"]:
            for server in result["Value"]:
                gLogger.verbose(f"Adding 'https://{server}:8443/voms/{self.vomsVO}/apiv2/users'")
                self.urls.append(f"https://{server}:8443/voms/{self.vomsVO}/apiv2/users")
        else:
            gLogger.error(f"Section '/Registry/VO/{self.vo}/VOMSServers' not found")

        self.iam_url = None
        self.compareWithIAM = compareWithIAM
        self.useIAM = useIAM
        if compareWithIAM or useIAM:
            id_provider = gConfig.getValue(f"/Registry/VO/{self.vo}/IdProvider")
            if not id_provider:
                raise ValueError(f"/Registry/VO/{self.vo}/IdProvider not found")
            result = gConfig.getOptionsDict(f"/Resources/IdProviders/{id_provider}")
            if result["OK"]:
                self.iam_url = result["Value"]["issuer"]
                gLogger.verbose("Using IAM server", self.iam_url)
            else:
                raise ValueError(f"/Resources/IdProviders/{id_provider}")

        self.userDict = None

    def getUsers(self):
        """Get all the users of the VOMS VO with their detailed information

        :return: dictionary of: "Users": user dictionary keyed by the user DN, "Errors": empty list
        """
        if not self.urls:
            return S_ERROR(DErrno.ENOAUTH, "No VOMS server defined")

        rawUserList = []
        result = None
        for url in self.urls:
            rawUserList = []
            startIndex = 0
            result = None
            error = None
            urlDone = False
            while not urlDone:
                try:
                    result = requests.get(
                        url,
                        headers={"X-VOMS-CSRF-GUARD": "y"},
                        cert=getProxyLocation(),
                        verify=getCAsLocation(),
                        params={"startIndex": str(startIndex), "pageSize": "100"},
                    )
                except requests.ConnectionError as exc:
                    error = f"{url}:{repr(exc)}"
                    urlDone = True
                    continue

                if result.status_code != 200:
                    error = f"Failed to contact the VOMS server: {result.text}"
                    urlDone = True
                    continue

                userList = result.json()["result"]
                rawUserList.extend(userList)
                if len(userList) < 100:
                    urlDone = True
                startIndex += 100

            # This URL did not work, try another one
            if error:
                continue
            else:
                break

        if error:
            return S_ERROR(DErrno.ENOAUTH, f"Failed to contact the VOMS server: {error}")

        # We have got the user info, reformat it
        resultDict = {}
        for user in rawUserList:
            for cert in user["certificates"]:
                dn = cert["subjectString"]
                resultDict[dn] = user
                resultDict[dn]["CA"] = cert["issuerString"]
                resultDict[dn]["certSuspended"] = cert.get("suspended")
                resultDict[dn]["suspended"] = user.get("suspended")
                resultDict[dn]["mail"] = user.get("emailAddress")
                resultDict[dn]["Roles"] = user.get("fqans")
                attributes = user.get("attributes")
                if attributes:
                    for attribute in user.get("attributes", []):
                        if attribute.get("name") == "nickname":
                            resultDict[dn]["nickname"] = attribute.get("value")

        self.userDict = dict(resultDict)
        # for consistency with IAM interface, we add Errors
        return S_OK({"Users": resultDict, "Errors": []})
