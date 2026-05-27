""" IAMService class encapsulates connection to the IAM service for a given VO
"""

import requests

from DIRAC import S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO


def convert_dn(inStr):
    """Convert a string separated DN into the slash one, like
    CN=Christophe Haen,CN=705305,CN=chaen,OU=Users,OU=Organic Units,DC=cern,DC=ch
    /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=705305/CN=Christophe Haen
    """
    return "/" + "/".join(inStr.split(",")[::-1])


class IAMService:
    def __init__(self, access_token, vo=None, forceNickname=False):
        """c'tor

        :param str access_token: the token used to talk to IAM, with the scim:read property
        :param str vo: name of the virtual organization (community)
        :param bool forceNickname: if enforce the presence of a nickname attribute and do not fall back to username in IAM

        """
        self.log = gLogger.getSubLogger(self.__class__.__name__)

        if not access_token:
            raise ValueError("access_token not set")

        if vo is None:
            vo = getVO()
        if not vo:
            raise Exception("No VO name given")

        self.forceNickname = forceNickname

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
        self.iam_users_raw = []
        self.iam_groups_raw = []

    def _getIamPagedResources(self, url):
        """Get all items from IAM that are served on a paged endpoint"""
        all_items = []
        headers = {"Authorization": f"Bearer {self.access_token}"}
        startIndex = 1
        # These are just initial values, they are updated
        # while we loop to their actual values
        totalResults = 1000  # total number of users
        itemsPerPage = 10
        while startIndex <= totalResults:
            resp = requests.get(url, headers=headers, params={"startIndex": startIndex}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # These 2 should never change while looping
            # but you may have a new user appearing
            # while looping
            totalResults = data["totalResults"]
            itemsPerPage = data["itemsPerPage"]

            startIndex += itemsPerPage
            all_items.extend(data["Resources"])
        return all_items

    def _getIamUserDump(self):
        """List the users from IAM"""
        if not self.iam_users_raw:
            iam_users_url = f"{self.iam_url}/scim/Users"
            self.iam_users_raw = self._getIamPagedResources(iam_users_url)
        return self.iam_users_raw

    def _getIamGroupDump(self):
        """List the groups from IAM"""
        if not self.iam_groups_raw:
            iam_group_url = f"{self.iam_url}/scim/Groups"
            self.iam_groups_raw = self._getIamPagedResources(iam_group_url)
        return self.iam_groups_raw

    def convert_iam_to_voms(self, iam_user, iam_voms_groups):
        """Convert an IAM entry into the voms style, i.e. DN based"""
        converted_output = {}

        certificates = iam_user["urn:indigo-dc:scim:schemas:IndigoUser"]["certificates"]

        for cert in certificates:
            cert_dict = {}
            dn = convert_dn(cert["subjectDn"])
            ca = convert_dn(cert["issuerDn"])
            cert_dict["CA"] = ca

            # The nickname is available in the list of attributes
            # (if configured so)
            # in the form {'name': 'nickname', 'value': 'chaen'}
            # otherwise, we take the userName unless we forceNickname
            try:
                cert_dict["nickname"] = [
                    attr["value"]
                    for attr in iam_user["urn:indigo-dc:scim:schemas:IndigoUser"]["attributes"]
                    if attr["name"] == "nickname"
                ][0]
            except (KeyError, IndexError):
                if not self.forceNickname:
                    cert_dict["nickname"] = iam_user["userName"]

            # This is not correct, we take the overall status instead of the certificate one
            # however there are no known case of cert suspended while the user isn't
            cert_dict["certSuspended"] = not iam_user["active"]
            # There are still bugs in IAM regarding the active status vs voms suspended

            cert_dict["suspended"] = not iam_user["active"]
            # The mail may be different, in particular for robot accounts
            cert_dict["mail"] = iam_user["emails"][0]["value"].lower()

            # https://github.com/indigo-iam/voms-importer/blob/main/vomsimporter.py
            roles = []

            for group in iam_user.get("groups", []):
                # ignore non-voms-role groups
                if group["value"] not in iam_voms_groups:
                    continue

                group_name = group["display"]

                # filter also by selected vo
                if self.vo is not None and group_name.partition("/")[0] != self.vo:
                    continue

                role_name = IAMService._group_name_to_role_string(group_name)
                roles.append(role_name)

            if len(roles) == 0:
                raise ValueError("User must have at least one voms role")

            cert_dict["Roles"] = roles
            converted_output[dn] = cert_dict
        return converted_output

    @staticmethod
    def _group_name_to_role_string(group_name):
        parts = group_name.split("/")
        # last part is the role name, need to add Role=
        parts[-1] = f"Role={parts[-1]}"
        return "/" + "/".join(parts)

    @staticmethod
    def _is_voms_role(group):
        # labels is returned also with value None, so we cannot simply do get("labels", [])
        labels = group.get("urn:indigo-dc:scim:schemas:IndigoGroup", {}).get("labels")
        if labels is None:
            return False

        for label in labels:
            if label["name"] == "voms.role":
                return True

        return False

    @staticmethod
    def _filter_voms_groups(groups):
        return [g for g in groups if IAMService._is_voms_role(g)]

    def getUsers(self):
        """Extract users from IAM user dump.

        :return: dictionary of: "Users": user dictionary keyed by the user DN, "Errors": list of error messages
        """
        iam_users_raw = self._getIamUserDump()
        all_groups = self._getIamGroupDump()

        voms_groups = self._filter_voms_groups(all_groups)
        groups_by_id = {g["id"] for g in voms_groups}

        users = {}
        errors = []
        for user in iam_users_raw:
            try:
                users.update(self.convert_iam_to_voms(user, groups_by_id))
            except Exception as e:
                errors.append(f"{user['name']} {e!r}")
                self.log.error("Could not convert", f"{user['name']} {e!r}")
        self.log.error("There were in total", f"{len(errors)} errors")
        self.userDict = dict(users)
        result = S_OK({"Users": users, "Errors": errors})
        return result

    def getUsersSub(self, vo=None) -> dict[str, str]:
        """
        Return the mapping based on IAM sub:
        {nickname : sub}
        """
        iam_users_raw = self._getIamUserDump()
        diracx_user_section = {}
        for user_info in iam_users_raw:
            userGroups = [grp["display"] for grp in user_info.get("groups", [])]
            # The nickname is available in the list of attributes
            # (if configured so)
            # in the form {'name': 'nickname', 'value': 'chaen'}
            # otherwise, we take the userName
            try:
                nickname = [
                    attr["value"]
                    for attr in user_info["urn:indigo-dc:scim:schemas:IndigoUser"]["attributes"]
                    if attr["name"] == "nickname"
                ][0]
            except (KeyError, IndexError):
                nickname = user_info["userName"]
            sub = user_info["id"]
            if not vo or vo in userGroups:
                diracx_user_section[nickname] = sub
        # reorder it
        diracx_user_section = dict(sorted(diracx_user_section.items()))

        return diracx_user_section
