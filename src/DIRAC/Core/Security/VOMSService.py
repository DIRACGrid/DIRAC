""" VOMSService class encapsulates connection to the VOMS service for a given VO
"""
import requests

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.Locations import getProxyLocation, getCAsLocation
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO


class VOMSService:
    def __init__(self, vo=None):
        """c'tor

        :param str vo: name of the virtual organization (community)
        """

        if vo is None:
            vo = getVO()
        if not vo:
            raise Exception("No VO name given")

        self.vo = vo
        self.vomsVO = getVOOption(vo, "VOMSName")
        if not self.vomsVO:
            raise Exception("Can not get VOMS name for VO %s" % vo)

        self.urls = []
        result = gConfig.getSections("/Registry/VO/%s/VOMSServers" % self.vo)
        if result["OK"]:
            for server in result["Value"]:
                gLogger.verbose(f"Adding 'https://{server}:8443/voms/{self.vomsVO}/apiv2/users'")
                self.urls.append(f"https://{server}:8443/voms/{self.vomsVO}/apiv2/users")
        else:
            gLogger.error("Section '/Registry/VO/%s/VOMSServers' not found" % self.vo)

        self.userDict = None

    def attGetUserNickname(self, dn, _ca=None):
        """Get user nickname for a given DN if any

        :param str dn: user DN
        :param str _ca: CA, kept for backward compatibility
        :return:  S_OK with Value: nickname
        """

        if self.userDict is None:
            result = self.getUsers()
            if not result["OK"]:
                return result

        uDict = self.userDict.get(dn)
        if not uDict:
            return S_ERROR(DErrno.EVOMS, "No nickname defined")
        nickname = uDict.get("nickname")
        if not nickname:
            return S_ERROR(DErrno.EVOMS, "No nickname defined")
        return S_OK(nickname)

    def getUsers(self):
        """Get all the users of the VOMS VO with their detailed information

        :return: user dictionary keyed by the user DN
        """

        if not self.urls:
            return S_ERROR(DErrno.ENOAUTH, "No VOMS server defined")

        userProxy = getProxyLocation()
        caPath = getCAsLocation()
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
                        cert=userProxy,
                        verify=caPath,
                        params={"startIndex": str(startIndex), "pageSize": "100"},
                    )
                except requests.ConnectionError as exc:
                    error = f"{url}:{repr(exc)}"
                    urlDone = True
                    continue

                if result.status_code != 200:
                    error = "Failed to contact the VOMS server: %s" % result.text
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
            return S_ERROR(DErrno.ENOAUTH, "Failed to contact the VOMS server: %s" % error)

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
        return S_OK(resultDict)
