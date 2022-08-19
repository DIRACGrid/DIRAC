import ldap3


class CERNLDAPSyncPlugin:
    """Synchronization plugin for mapping new users to CERN accounts.

    This plugin results in new users having two additional fields in the CS,
    CERNAccountType and PrimaryCERNAccount. If the new nickname does not have a
    corresponding CERN account it will be rejected.
    """

    def __init__(self):
        """Initialise the plugin and underlying LDAP connection."""
        self._server = ldap3.Server("ldap://xldap.cern.ch")
        self._connection = ldap3.Connection(self._server, client_strategy=ldap3.SAFE_SYNC, auto_bind=True)

    def verifyAndUpdateUserInfo(self, username, userDict):
        """Add the "CERNAccountType" and "PrimaryCERNAccount" values to the CS attributes.

        :param username: DIRAC name of the user to be added
        :param userDict: user information collected by the VOMS2CSAgent
        :returns: None
        :raise ValueError: if no corresponding CERN account is found.
        """
        attributes = self._getUserInfo(username)
        cernAccountType = attributes["cernAccountType"]
        userDict["CERNAccountType"] = cernAccountType[0]
        if cernAccountType == ["Primary"]:
            userDict["PrimaryCERNAccount"] = username
        else:
            userDict["PrimaryCERNAccount"] = self._findOwnerAccountName(username, attributes)

    def _findOwnerAccountName(self, username, attributes):
        """Find the owner account from a CERN LDAP entry.

        :param username: DIRAC name of the user to be added
        :param attributes: output of ``_getUserInfo`` for ``username``
        :returns: The name of the owning CERN account
        """
        owners = attributes["cernAccountOwner"]
        if len(owners) != 1:
            raise ValueError(f"Expected exactly one cernAccountOwner for {username} but found {len(owners)}")
        commonNames = [v for k, v, _ in ldap3.utils.dn.parse_dn(owners[0]) if k == "CN"]
        if len(commonNames) != 1:
            raise ValueError(
                "Expected exactly one common name in the cernAccountOwner of %s but found %s"
                % (username, len(commonNames))
            )
        primaryAccountName = commonNames[0]
        primaryAttributes = self._getUserInfo(primaryAccountName)
        if primaryAttributes["cernAccountType"] != ["Primary"]:
            raise ValueError("Something is very wrong!")
        return primaryAccountName

    def _getUserInfo(self, commonName):
        """Query the CERN LDAP server for the given ``commonName``.

        :param commonName: Common Name of an account known to CERN
        :returns: ``dict`` of the account attributes from LDAP
        :raise ValueError: if no corresponding CERN account is found
        """
        status, result, response, _ = self._connection.search(
            "OU=Users,OU=Organic Units,DC=cern,DC=ch",
            "(CN=%s)" % commonName,
            attributes=["cernAccountOwner", "cernAccountType"],
        )
        if not status:
            raise ValueError("Bad status from LDAP search: %s" % result)
        if len(response) != 1:
            raise ValueError(f"Expected exactly one match for CN={commonName} but found {len(response)}")
        # https://github.com/PyCQA/pylint/issues/4148
        return response[0]["attributes"]  # pylint: disable=unsubscriptable-object
