""" IAMService class encapsulates connection to the IAM service for a given VO
"""

import requests

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.Locations import getProxyLocation, getCAsLocation
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO


def convert_dn(inStr):
    """Convert a string separated DN into the slash one, like
    CN=Christophe Haen,CN=705305,CN=chaen,OU=Users,OU=Organic Units,DC=cern,DC=ch
    /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=705305/CN=Christophe Haen
    """
    return "/" + "/".join(inStr.split(",")[::-1])


class IAMService:
    def __init__(self, access_token, vo=None):
        """c'tor

        :param str vo: name of the virtual organization (community)
        :param str access_token: the token used to talk to IAM, with the scim:read property

        """

        if not access_token:
            raise ValueError("access_token not set")

        if vo is None:
            vo = getVO()
        if not vo:
            raise Exception("No VO name given")

        self.vo = vo

        self.iam_url = None

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
        self.access_token = access_token

    def _getIamUserDump(self):
        """List the users from IAM"""

        headers = {"Authorization": f"Bearer {self.access_token}"}
        iam_list_url = f"{self.iam_url}/scim/Users"
        iam_users = []
        startIndex = 1
        # These are just initial values, they are updated
        # while we loop to their actual values
        totalResults = 1000  # total number of users
        itemsPerPage = 10
        while startIndex <= totalResults:
            resp = requests.get(iam_list_url, headers=headers, params={"startIndex": startIndex})
            resp.raise_for_status()
            data = resp.json()
            # These 2 should never change while looping
            # but you may have a new user appearing
            # while looping
            totalResults = data["totalResults"]
            itemsPerPage = data["itemsPerPage"]

            startIndex += itemsPerPage
            iam_users.extend(data["Resources"])
        return iam_users

    @staticmethod
    def convert_iam_to_voms(iam_output):
        """Convert an IAM entry into the voms style, i.e. DN based"""
        converted_output = {}

        for cert in iam_output["urn:indigo-dc:scim:schemas:IndigoUser"]["certificates"]:
            cert_dict = {}
            dn = convert_dn(cert["subjectDn"])
            ca = convert_dn(cert["issuerDn"])
            cert_dict["CA"] = ca

            # The nickname is available in the list of attributes
            # (if configured so)
            # in the form {'name': 'nickname', 'value': 'chaen'}
            # otherwise, we take the userName
            try:
                cert_dict["nickname"] = [
                    attr["value"]
                    for attr in iam_output["urn:indigo-dc:scim:schemas:IndigoUser"]["attributes"]
                    if attr["name"] == "nickname"
                ][0]
            except (KeyError, IndexError):
                cert_dict["nickname"] = iam_output["userName"]

            # This is not correct, we take the overall status instead of the certificate one
            # however there are no known case of cert suspended while the user isn't
            cert_dict["certSuspended"] = not iam_output["active"]
            # There are still bugs in IAM regarding the active status vs voms suspended

            cert_dict["suspended"] = not iam_output["active"]
            # The mail may be different, in particular for robot accounts
            cert_dict["mail"] = iam_output["emails"][0]["value"].lower()

            # https://github.com/indigo-iam/voms-importer/blob/main/vomsimporter.py
            roles = []

            for role in iam_output["groups"]:
                role_name = role["display"]
                if "/" in role_name:
                    role_name = role_name.replace("/", "/Role=")
                roles.append(f"/{role_name}")

            cert_dict["Roles"] = roles
            converted_output[dn] = cert_dict
        return converted_output

    def getUsers(self):
        self.iam_users_raw = self._getIamUserDump()
        users = {}
        errors = 0
        for user in self.iam_users_raw:
            try:
                users.update(self.convert_iam_to_voms(user))
            except Exception as e:
                errors += 1
                print(f"Could not convert {user['name']} {e!r} ")
        print(f"There were in total {errors} errors")
        self.userDict = dict(users)
        return S_OK(users)
