""" Module that holds DISET Authorization class for services
"""
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import List
from DIRAC.FrameworkSystem.Client.Logger import gLogger


class AuthManager:
    """Handle Service Authorization"""

    __authLogger = gLogger.getSubLogger("Authorization")
    KW_HOSTS_GROUP = "hosts"
    KW_DN = "DN"
    KW_GROUP = "group"
    KW_EXTRA_CREDENTIALS = "extraCredentials"
    KW_PROPERTIES = "properties"
    KW_USERNAME = "username"

    def __init__(self, authSection):
        """
        Constructor

        :type authSection: string
        :param authSection: Section containing the authorization rules
        """
        self.authSection = authSection

    def authQuery(self, methodQuery, credDict, defaultProperties=False):
        """
        Check if the query is authorized for a credentials dictionary

        :type  methodQuery: string
        :param methodQuery: Method to test
        :type  credDict: dictionary
        :param credDict: dictionary containing credentials for test. The dictionary can contain the DN
                            and selected group.
        :return: Boolean result of test
        """
        userString = ""
        if self.KW_DN in credDict:
            userString += "DN=%s" % credDict[self.KW_DN]
        if self.KW_GROUP in credDict:
            userString += " group=%s" % credDict[self.KW_GROUP]
        if self.KW_EXTRA_CREDENTIALS in credDict:
            userString += " extraCredentials=%s" % str(credDict[self.KW_EXTRA_CREDENTIALS])
        self.__authLogger.debug("Trying to authenticate %s" % userString)
        # Get properties
        requiredProperties = self.getValidPropertiesForMethod(methodQuery, defaultProperties)
        # Extract valid groups
        validGroups = self.getValidGroups(requiredProperties)
        lowerCaseProperties = [prop.lower() for prop in requiredProperties]
        if not lowerCaseProperties:
            lowerCaseProperties = ["any"]

        allowAll = "any" in lowerCaseProperties or "all" in lowerCaseProperties
        # Set no properties by default
        credDict[self.KW_PROPERTIES] = []
        # Check non secure backends
        if self.KW_DN not in credDict or not credDict[self.KW_DN]:
            if allowAll and not validGroups:
                self.__authLogger.debug("Accepted request from unsecure transport")
                return True
            else:
                self.__authLogger.debug(
                    "Explicit property required and query seems to be coming through an unsecure transport"
                )
                return False
        # Check if query comes though a gateway/web server
        if self.forwardedCredentials(credDict):
            self.__authLogger.debug("Query comes from a gateway")
            self.unpackForwardedCredentials(credDict)
            return self.authQuery(methodQuery, credDict, requiredProperties)
        # Get the properties
        # Check for invalid forwarding
        if self.KW_EXTRA_CREDENTIALS in credDict:
            # Invalid forwarding?
            if not isinstance(credDict[self.KW_EXTRA_CREDENTIALS], str):
                self.__authLogger.debug("The credentials seem to be forwarded by a host, but it is not a trusted one")
                return False
        # Is it a host?
        if self.KW_EXTRA_CREDENTIALS in credDict and credDict[self.KW_EXTRA_CREDENTIALS] == self.KW_HOSTS_GROUP:
            # Get the nickname of the host
            credDict[self.KW_GROUP] = credDict[self.KW_EXTRA_CREDENTIALS]
        # HACK TO MAINTAIN COMPATIBILITY
        else:
            if self.KW_EXTRA_CREDENTIALS in credDict and self.KW_GROUP not in credDict:
                credDict[self.KW_GROUP] = credDict[self.KW_EXTRA_CREDENTIALS]
        # END OF HACK
        # Get the username
        if self.KW_DN in credDict and credDict[self.KW_DN]:
            if self.KW_GROUP not in credDict:
                result = Registry.findDefaultGroupForDN(credDict[self.KW_DN])
                if not result["OK"]:
                    credDict[self.KW_USERNAME] = "anonymous"
                    credDict[self.KW_GROUP] = "visitor"
                else:
                    credDict[self.KW_GROUP] = result["Value"]
            if credDict[self.KW_GROUP] == self.KW_HOSTS_GROUP:
                # For host
                if not self.getHostNickName(credDict):
                    self.__authLogger.warn("Host is invalid")
                    if not allowAll:
                        return False
                    # If all, then set anon credentials
                    credDict[self.KW_USERNAME] = "anonymous"
                    credDict[self.KW_GROUP] = "visitor"
            else:
                # For users
                username = self.getUsername(credDict)
                suspended = self.isUserSuspended(credDict)
                if not username:
                    self.__authLogger.warn("User is invalid or does not belong to the group it's saying")
                if suspended:
                    self.__authLogger.warn("User is Suspended")

                if not username or suspended:
                    if not allowAll:
                        return False
                    # If all, then set anon credentials
                    credDict[self.KW_USERNAME] = "anonymous"
                    credDict[self.KW_GROUP] = "visitor"
        else:
            if not allowAll:
                return False
            credDict[self.KW_USERNAME] = "anonymous"
            credDict[self.KW_GROUP] = "visitor"

        # If any or all in the props, allow
        allowGroup = not validGroups or credDict[self.KW_GROUP] in validGroups
        if allowAll and allowGroup:
            return True
        # Check authorized groups
        if "authenticated" in lowerCaseProperties and allowGroup:
            return True
        if not self.matchProperties(credDict, requiredProperties):
            self.__authLogger.warn(
                f"Client is not authorized\nValid properties: {requiredProperties}\nClient: {credDict}"
            )
            return False
        elif not allowGroup:
            self.__authLogger.warn(f"Client is not authorized\nValid groups: {validGroups}\nClient: {credDict}")
            return False
        return True

    def getHostNickName(self, credDict):
        """
        Discover the host nickname associated to the DN.
        The nickname will be included in the credentials dictionary.

        :type  credDict: dictionary
        :param credDict: Credentials to ckeck
        :return: Boolean specifying whether the nickname was found
        """
        if self.KW_DN not in credDict:
            return True
        if self.KW_GROUP not in credDict:
            return False
        retVal = Registry.getHostnameForDN(credDict[self.KW_DN])
        if not retVal["OK"]:
            gLogger.warn("Cannot find hostname for DN {}: {}".format(credDict[self.KW_DN], retVal["Message"]))
            return False
        credDict[self.KW_USERNAME] = retVal["Value"]
        credDict[self.KW_PROPERTIES] = Registry.getPropertiesForHost(credDict[self.KW_USERNAME], [])
        return True

    def getValidPropertiesForMethod(self, method, defaultProperties=False):
        """
        Get all authorized groups for calling a method

        :type  method: string
        :param method: Method to test
        :return: List containing the allowed groups
        """
        authProps = gConfig.getValue(f"{self.authSection}/{method}", [])
        if authProps:
            return authProps
        if defaultProperties:
            self.__authLogger.debug(f"Using hardcoded properties for method {method} : {defaultProperties}")
            if not isinstance(defaultProperties, (list, tuple)):
                return List.fromChar(defaultProperties)
            return defaultProperties
        defaultPath = "%s/Default" % "/".join(method.split("/")[:-1])
        authProps = gConfig.getValue(f"{self.authSection}/{defaultPath}", [])
        if authProps:
            self.__authLogger.debug(f"Method {method} has no properties defined using {defaultPath}")
            return authProps
        self.__authLogger.debug("Method %s has no authorization rules defined. Allowing no properties" % method)
        return []

    def getValidGroups(self, rawProperties):
        """Get valid groups as specified in the method authorization rules

        :param rawProperties: all method properties
        :type rawProperties: python:list
        :return: list of allowed groups or []
        """
        validGroups = []
        for prop in list(rawProperties):
            if prop.startswith("group:"):
                rawProperties.remove(prop)
                prop = prop.replace("group:", "")
                validGroups.append(prop)
            elif prop.startswith("vo:"):
                rawProperties.remove(prop)
                vo = prop.replace("vo:", "")
                result = Registry.getGroupsForVO(vo)
                if result["OK"]:
                    validGroups.extend(result["Value"])

        validGroups = list(set(validGroups))
        return validGroups

    def forwardedCredentials(self, credDict):
        """
        Check whether the credentials are being forwarded by a valid source

        :type  credDict: dictionary
        :param credDict: Credentials to ckeck
        :return: Boolean with the result
        """
        if self.KW_EXTRA_CREDENTIALS in credDict and isinstance(credDict[self.KW_EXTRA_CREDENTIALS], (tuple, list)):
            if self.KW_DN in credDict:
                retVal = Registry.getHostnameForDN(credDict[self.KW_DN])
                if retVal["OK"]:
                    hostname = retVal["Value"]
                    if Properties.TRUSTED_HOST in Registry.getPropertiesForHost(hostname, []):
                        return True
        return False

    def unpackForwardedCredentials(self, credDict):
        """
        Extract the forwarded credentials

        :type  credDict: dictionary
        :param credDict: Credentials to unpack
        """
        credDict[self.KW_DN] = credDict[self.KW_EXTRA_CREDENTIALS][0]
        credDict[self.KW_GROUP] = credDict[self.KW_EXTRA_CREDENTIALS][1]
        del credDict[self.KW_EXTRA_CREDENTIALS]

    def getUsername(self, credDict):
        """
        Discover the username associated to the DN. It will check if the selected group is valid.
        The username will be included in the credentials dictionary.

        :type  credDict: dictionary
        :param credDict: Credentials to check
        :return: Boolean specifying whether the username was found
        """
        if self.KW_DN not in credDict:
            return True
        if self.KW_GROUP not in credDict:
            result = Registry.findDefaultGroupForDN(credDict[self.KW_DN])
            if not result["OK"]:
                return False
            credDict[self.KW_GROUP] = result["Value"]
        credDict[self.KW_PROPERTIES] = Registry.getPropertiesForGroup(credDict[self.KW_GROUP], [])
        usersInGroup = Registry.getUsersInGroup(credDict[self.KW_GROUP], [])
        if not usersInGroup:
            return False
        retVal = Registry.getUsernameForDN(credDict[self.KW_DN], usersInGroup)
        if retVal["OK"]:
            credDict[self.KW_USERNAME] = retVal["Value"]
            return True
        return False

    def isUserSuspended(self, credDict):
        """Discover if the user is in Suspended status

        :param dict credDict: Credentials to check
        :return: Boolean True if user is Suspended
        """
        # Update credDict if the username is not there
        if self.KW_USERNAME not in credDict:
            self.getUsername(credDict)
        # If username or group is not known we can not judge if the user is suspended
        # These cases are treated elsewhere anyway
        if self.KW_USERNAME not in credDict or self.KW_GROUP not in credDict:
            return False
        suspendedVOList = Registry.getUserOption(credDict[self.KW_USERNAME], "Suspended", [])
        if not suspendedVOList:
            return False
        vo = Registry.getVOForGroup(credDict[self.KW_GROUP])
        if vo in suspendedVOList:
            return True
        return False

    def matchProperties(self, credDict, validProps, caseSensitive=False):
        """
        Return True if one or more properties are in the valid list of properties

        :type  props: list
        :param props: List of properties to match
        :type  validProps: list
        :param validProps: List of valid properties
        :return: Boolean specifying whether any property has matched the valid ones
        """

        # HACK: Map lower case properties to properties to make the check in lowercase but return the proper case
        if not caseSensitive:
            validProps = {prop.lower(): prop for prop in validProps}
        else:
            validProps = {prop: prop for prop in validProps}
        groupProperties = credDict[self.KW_PROPERTIES]
        foundProps = []
        for prop in groupProperties:
            if not caseSensitive:
                prop = prop.lower()
            if prop in validProps:
                foundProps.append(validProps[prop])
        credDict[self.KW_PROPERTIES] = foundProps
        return foundProps
