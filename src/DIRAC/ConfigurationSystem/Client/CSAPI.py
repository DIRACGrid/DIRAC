""" CSAPI exposes update functionalities to the Configuration.

    Most of these functions can only be done by administrators
"""

import datetime

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient
from DIRAC.Core.Utilities import List
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security import Locations
from DIRAC.ConfigurationSystem.private.Modificator import Modificator
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites, getCESiteMapping
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath


class CSAPI:
    """CSAPI objects need an initialization phase"""

    def __init__(self):
        """
        Initialization function
        """
        self.csModified = False
        self.__baseSecurity = "/Registry"
        self.__baseResources = "/Resources"

        self.__userDN = ""
        self.__userGroup = ""
        self.__rpcClient = None
        self.__csMod = None

        self.__initialized = S_ERROR("Not initialized")
        self.initialize()
        if not self.__initialized["OK"]:
            gLogger.error(self.__initialized)

    def __getProxyID(self):
        """Get proxy identity information

        :return: S_OK()/S_ERROR()
        """
        proxyLocation = Locations.getProxyLocation()
        if not proxyLocation:
            return S_ERROR("No proxy found!")
        chain = X509Chain()
        if not chain.loadProxyFromFile(proxyLocation):
            return S_ERROR("Can't read proxy!", proxyLocation)
        retVal = chain.getIssuerCert()
        if not retVal["OK"]:
            return S_ERROR("Can't parse proxy!", retVal["Message"])
        idCert = retVal["Value"]
        result = idCert.getSubjectDN()
        if result["OK"]:
            self.__userDN = result["Value"]
            result = chain.getDIRACGroup()
        if not result["OK"]:
            return result
        self.__userGroup = result["Value"]
        return S_OK()

    def __getCertificateID(self):
        """Get certificate identity information

        :return: S_OK()/S_ERROR()
        """
        certLocation = Locations.getHostCertificateAndKeyLocation()
        if not certLocation:
            return S_ERROR("No certificate found!")
        chain = X509Chain()
        retVal = chain.loadChainFromFile(certLocation[0])
        if not retVal["OK"]:
            return S_ERROR("Can't parse certificate!", retVal["Message"])
        result = chain.getIssuerCert()
        if result["OK"]:
            idCert = result["Value"]
            result = idCert.getSubjectDN()
        if not result["OK"]:
            return result
        self.__userDN = result["Value"]
        self.__userGroup = "host"
        return S_OK()

    def initialize(self):
        """API initialization

        :return: S_OK()/S_ERROR()
        """
        if self.__initialized["OK"]:
            return self.__initialized
        res = self.__getCertificateID() if gConfig.useServerCertificate() else self.__getProxyID()
        if not res["OK"]:
            gLogger.error(res["Message"])
            self.__initialized = S_ERROR("Cannot locate client credentials")
            return self.__initialized
        retVal = gConfig.getOption("/DIRAC/Configuration/MasterServer")
        if not retVal["OK"]:
            self.__initialized = S_ERROR("Master server is not known. Is everything initialized?")
            return self.__initialized
        self.__rpcClient = ConfigurationClient(url=gConfig.getValue("/DIRAC/Configuration/MasterServer", ""))
        self.__csMod = Modificator(
            self.__rpcClient,
            "%s - %s - %s"
            % (self.__userGroup, self.__userDN, datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
        )
        retVal = self.downloadCSData()
        if not retVal["OK"]:
            self.__initialized = S_ERROR("Can not download the remote cfg. Is everything initialized?")
            return self.__initialized
        self.__initialized = S_OK()
        return self.__initialized

    def downloadCSData(self):
        """Download CS data

        :return: S_OK()/S_ERROR()
        """
        if not self.__csMod:
            return S_ERROR("CSAPI not yet initialized")
        result = self.__csMod.loadFromRemote()
        if not result["OK"]:
            return result
        self.csModified = False
        self.__csMod.updateGConfigurationData()
        return S_OK()

    # Resources-related methods
    #########################################

    def addSite(self, siteName, optionsDict=None):
        """Adds a new site to the CS.
        A site is a container for services, so after calling this function,
        at least addCEtoSite() should be called.

        :param str siteName: FQN of the site (e.g. LCG.CERN.ch)
        :param dict optionsDict: optional dictionary of options
        :returns: S_OK/S_ERROR structure
        """

        if not self.__initialized["OK"]:
            return self.__initialized

        self.__csMod.createSection(cfgPath(self.__baseResources, "Sites"))
        self.__csMod.createSection(cfgPath(self.__baseResources, "Sites", siteName.split(".")[0]))
        self.__csMod.createSection(cfgPath(self.__baseResources, "Sites", siteName.split(".")[0], siteName))
        # add options if requested
        if optionsDict:
            for option, optionValue in optionsDict.items():  # can be an iterator
                self.__csMod.setOptionValue(
                    cfgPath(self.__baseResources, "Sites", siteName.split(".")[0], siteName, option), optionValue
                )
        self.csModified = True
        return S_OK(True)

    def addCEtoSite(self, siteName, ceName, optionsDict=None):
        """Adds a new CE to a site definition in the CS.
        A CE normally has queues, so addQueueToCE should be called after this one.

        :param str siteName: FQN of the site (e.g. LCG.CERN.ch)
        :param str ceName: FQN of the CE (e.g. ce503.cern.ch)
        :param dict optionsDict: optional dictionary of options
        :returns: S_OK/S_ERROR structure
        """
        res = getSites()
        if not res["OK"]:
            return res
        if siteName not in res["Value"]:
            res = self.addSite(siteName)
            if not res["OK"]:
                return res

        # CSAPI.createSection() always returns S_OK even if the section already exists
        self.__csMod.createSection(cfgPath(self.__baseResources, "Sites", siteName.split(".")[0], siteName, "CEs"))
        self.__csMod.createSection(
            cfgPath(self.__baseResources, "Sites", siteName.split(".")[0], siteName, "CEs", ceName)
        )
        # add options if requested
        if optionsDict is not None:
            for option, optionValue in optionsDict.items():  # can be an iterator
                self.__csMod.setOptionValue(
                    cfgPath(self.__baseResources, "Sites", siteName.split(".")[0], siteName, "CEs", ceName, option),
                    optionValue,
                )
        self.csModified = True
        return S_OK(True)

    def addQueueToCE(self, ceName, queueName, optionsDict=None):
        """Adds a new queue to a CE definition in the CS.

        :param str ceName: FQN of the CE (e.g. ce503.cern.ch)
        :param str queueName: name of the queue (e.g. ce503.cern.ch-condor)
        :param dict optionsDict: optional dictionary of options
        :returns: S_OK/S_ERROR structure
        """
        res = getCESiteMapping(ceName)
        if not res["OK"]:
            return res
        if ceName not in res["Value"]:
            return S_ERROR("CE does not exist")
        siteName = res["Value"][ceName]

        # CSAPI.createSection() always returns S_OK even if the section already exists
        self.__csMod.createSection(
            cfgPath(self.__baseResources, "Sites", siteName.split(".")[0], siteName, "CEs", ceName, "Queues", queueName)
        )
        # add options if requested
        if optionsDict is not None:
            for option, optionValue in optionsDict.items():  # can be an iterator
                self.__csMod.setOptionValue(
                    cfgPath(
                        self.__baseResources,
                        "Sites",
                        siteName.split(".")[0],
                        siteName,
                        "CEs",
                        ceName,
                        "Queues",
                        queueName,
                        option,
                    ),
                    optionValue,
                )
        self.csModified = True
        return S_OK(True)

    # Registry-related methods
    #########################################

    def listUsers(self, group=False):
        """List users

        :param str group: group name

        :return: S_OK(list)/S_ERROR()
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        if not group:
            return S_OK(self.__csMod.getSections("%s/Users" % self.__baseSecurity))
        users = self.__csMod.getValue(f"{self.__baseSecurity}/Groups/{group}/Users")
        return S_OK(List.fromChar(users) if users else [])

    def listHosts(self):
        """List hosts

        :return: S_OK(list)/S_ERROR()
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        return S_OK(self.__csMod.getSections("%s/Hosts" % self.__baseSecurity))

    def describeUsers(self, users=None):
        """describe users by nickname

        :param list users: list of users' nicknames

        :return: a S_OK(description) of the users in input
        """
        return self.__describeEntity(users)

    def describeHosts(self, hosts=None):
        """describe hosts

        :param list users: list of users' nicknames

        :return: a S_OK(description) of the users in input
        """
        return self.__describeEntity(hosts, True)

    def __describeEntity(self, mask, hosts=False):
        """Describe users or hosts

        :param list mask: list users or hosts
        :param bool hosts: if its hosts

        :return: S_OK(dict)/S_ERROR()
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        csSection = "{}/{}".format(self.__baseSecurity, ("Hosts" if hosts else "Users"))
        entities = self.__csMod.getSections(csSection)
        if mask:
            entities = [entity for entity in entities if entity in (mask or [])]
        entitiesDict = {}
        for entity in entities:
            entitiesDict[entity] = {}
            for option in self.__csMod.getOptions(f"{csSection}/{entity}"):
                entitiesDict[entity][option] = self.__csMod.getValue(f"{csSection}/{entity}/{option}")
            if not hosts:
                result = self.describeGroups()
                if not result["OK"]:
                    return result
                groupsDict = result["Value"]
                entitiesDict[entity]["Groups"] = []
                for group in groupsDict:
                    if "Users" in groupsDict[group] and entity in groupsDict[group]["Users"]:
                        entitiesDict[entity]["Groups"].append(group)
                entitiesDict[entity]["Groups"].sort()
        return S_OK(entitiesDict)

    def listGroups(self):
        """
        List all groups
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        return S_OK(self.__csMod.getSections("%s/Groups" % self.__baseSecurity))

    def describeGroups(self, mask=None):
        """
        List all groups that are in the mask (or all if no mask) with their properties
        """
        if mask is None:
            mask = []
        if not self.__initialized["OK"]:
            return self.__initialized
        groups = [
            group
            for group in self.__csMod.getSections("%s/Groups" % self.__baseSecurity)
            if not mask or (mask and group in mask)
        ]
        groupsDict = {}
        for group in groups:
            groupsDict[group] = {}
            for option in self.__csMod.getOptions(f"{self.__baseSecurity}/Groups/{group}"):
                groupsDict[group][option] = self.__csMod.getValue(f"{self.__baseSecurity}/Groups/{group}/{option}")
                if option in ("Users", "Properties"):
                    groupsDict[group][option] = List.fromChar(groupsDict[group][option])
        return S_OK(groupsDict)

    def deleteUsers(self, users):
        """Delete a user/s can receive as a param either a string or a list

        :param list users: users

        :return: S_OK(bool)/S_ERROR()
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        if isinstance(users, str):
            users = [users]
        result = self.describeUsers(users)
        if not result["OK"]:
            return result
        usersData = result["Value"]
        for username in users:
            if username not in usersData:
                gLogger.warn("User %s does not exist" % username)
                continue
            userGroups = usersData[username]["Groups"]
            for group in userGroups:
                self.__removeUserFromGroup(group, username)
                gLogger.info(f"Deleted user {username} from group {group}")
            self.__csMod.removeSection(f"{self.__baseSecurity}/Users/{username}")
            gLogger.info("Deleted user %s" % username)
            self.csModified = True
        return S_OK(True)

    def __removeUserFromGroup(self, group, username):
        """
        Remove user from a group
        """
        usersInGroup = self.__csMod.getValue(f"{self.__baseSecurity}/Groups/{group}/Users")
        if usersInGroup is not None:
            userList = List.fromChar(usersInGroup, ",")
            userPos = userList.index(username)
            userList.pop(userPos)
            self.__csMod.setOptionValue(f"{self.__baseSecurity}/Groups/{group}/Users", ",".join(userList))

    def __addUserToGroup(self, group, username):
        """
        Add user to a group
        """
        usersInGroup = self.__csMod.getValue(f"{self.__baseSecurity}/Groups/{group}/Users")
        if usersInGroup is not None:
            userList = List.fromChar(usersInGroup)
            if username not in userList:
                userList.append(username)
                self.__csMod.setOptionValue(f"{self.__baseSecurity}/Groups/{group}/Users", ",".join(userList))
            else:
                gLogger.warn(f"User {username} is already in group {group}")

    def addUser(self, username, properties):
        """
        Add a user to the cs

        :param str username: username
        :param dict properties: dictionary describing user properties:

          - DN
          - groups
          - <extra params>

        :return: S_OK(bool)/S_ERROR()
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        for prop in ("DN", "Groups"):
            if prop not in properties:
                gLogger.error("Missing property for user", f"{prop}: {username}")
                return S_OK(False)
        result = self.listUsers()
        if not result["OK"]:
            return result
        if username in result["Value"]:
            gLogger.error("User is already registered", username)
            return S_OK(False)
        result = self.listGroups()
        if not result["OK"]:
            return result
        groups = result["Value"]
        for userGroup in properties["Groups"]:
            if userGroup not in groups:
                gLogger.error("User group is not a valid group", f"{username} {userGroup}")
                return S_OK(False)
        self.__csMod.createSection(f"{self.__baseSecurity}/Users/{username}")
        for prop in properties:
            if prop == "Groups":
                continue
            self.__csMod.setOptionValue(f"{self.__baseSecurity}/Users/{username}/{prop}", properties[prop])
        for userGroup in properties["Groups"]:
            gLogger.info(f"Added user {username} to group {userGroup}")
            self.__addUserToGroup(userGroup, username)
        gLogger.info("Registered user %s" % username)
        self.csModified = True
        return S_OK(True)

    def modifyUser(self, username, properties, createIfNonExistant=False):
        """
        Modify a user

        :param str username: group name
        :param dict properties: dictionary describing user properties:

            - DN
            - Groups
            - <extra params>

        :param bool createIfNonExistant: if true, registers the users if it did not exist
        :return: S_OK, Value = True/False
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        modifiedUser = False
        result = self.describeUsers([username])
        if not result["OK"]:
            return result
        userData = result["Value"]
        if username not in userData:
            if createIfNonExistant:
                gLogger.info("Registering user %s" % username)
                return self.addUser(username, properties)
            gLogger.error("User is not registered: ", repr(username))
            return S_OK(False)
        for prop in properties:
            if prop == "Groups":
                continue
            prevVal = self.__csMod.getValue(f"{self.__baseSecurity}/Users/{username}/{prop}")
            if not prevVal or prevVal != properties[prop]:
                gLogger.info(f"Setting {prop} property for user {username} to {properties[prop]}")
                self.__csMod.setOptionValue(f"{self.__baseSecurity}/Users/{username}/{prop}", properties[prop])
                modifiedUser = True
        if "Groups" in properties:
            result = self.listGroups()
            if not result["OK"]:
                return result
            groups = result["Value"]
            for userGroup in properties["Groups"]:
                if userGroup not in groups:
                    gLogger.error("User group is not a valid group", f"{username} {userGroup}")
                    return S_OK(False)
            groupsToBeDeletedFrom = []
            groupsToBeAddedTo = []
            for prevGroup in userData[username]["Groups"]:
                if prevGroup not in properties["Groups"]:
                    groupsToBeDeletedFrom.append(prevGroup)
                    modifiedUser = True
            for newGroup in properties["Groups"]:
                if newGroup not in userData[username]["Groups"]:
                    groupsToBeAddedTo.append(newGroup)
                    modifiedUser = True
            for group in groupsToBeDeletedFrom:
                self.__removeUserFromGroup(group, username)
                gLogger.info(f"Removed user {username} from group {group}")
            for group in groupsToBeAddedTo:
                self.__addUserToGroup(group, username)
                gLogger.info(f"Added user {username} to group {group}")
        modified = False
        if modifiedUser:
            modified = True
            gLogger.info("Modified user %s" % username)
            self.csModified = True
        else:
            gLogger.info("Nothing to modify for user %s" % username)
        return S_OK(modified)

    def addGroup(self, groupname, properties):
        """
        Add a group to the cs

        :param str groupname: group name
        :param dict properties: dictionary describing group properties:

            - Users
            - Properties
            - <extra params>

        :return: S_OK, Value = True/False
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        result = self.listGroups()
        if not result["OK"]:
            return result
        if groupname in result["Value"]:
            gLogger.error("Group is already registered", groupname)
            return S_OK(False)
        self.__csMod.createSection(f"{self.__baseSecurity}/Groups/{groupname}")
        for prop in properties:
            self.__csMod.setOptionValue(f"{self.__baseSecurity}/Groups/{groupname}/{prop}", properties[prop])
        gLogger.info("Registered group %s" % groupname)
        self.csModified = True
        return S_OK(True)

    def modifyGroup(self, groupname, properties, createIfNonExistant=False):
        """
        Modify a group

        :param str groupname: group name
        :param dict properties: dictionary describing group properties:

            - Users
            - Properties
            - <extra params>

        :param bool createIfNonExistant: if true, creates the group if it did not exist
        :return: S_OK, Value = True/False
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        modifiedGroup = False
        result = self.describeGroups([groupname])
        if not result["OK"]:
            return result
        groupData = result["Value"]
        if groupname not in groupData:
            if createIfNonExistant:
                gLogger.info("Registering group %s" % groupname)
                return self.addGroup(groupname, properties)
            gLogger.error("Group is not registered", groupname)
            return S_OK(False)
        for prop in properties:
            prevVal = self.__csMod.getValue(f"{self.__baseSecurity}/Groups/{groupname}/{prop}")
            if not prevVal or prevVal != properties[prop]:
                gLogger.info(f"Setting {prop} property for group {groupname} to {properties[prop]}")
                self.__csMod.setOptionValue(f"{self.__baseSecurity}/Groups/{groupname}/{prop}", properties[prop])
                modifiedGroup = True
        if modifiedGroup:
            gLogger.info("Modified group %s" % groupname)
            self.csModified = True
        else:
            gLogger.info("Nothing to modify for group %s" % groupname)
        return S_OK(True)

    def addHost(self, hostname, properties):
        """
        Add a host to the cs

        :param str hostname: host name
        :param dict properties: dictionary describing host properties:

            - DN
            - Properties
            - <extra params>

        :return: S_OK, Value = True/False
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        for prop in ("DN",):
            if prop not in properties:
                gLogger.error("Missing property for host", f"{prop} {hostname}")
                return S_OK(False)
        result = self.listHosts()
        if not result["OK"]:
            return result
        if hostname in result["Value"]:
            gLogger.error("Host is already registered", hostname)
            return S_OK(False)
        self.__csMod.createSection(f"{self.__baseSecurity}/Hosts/{hostname}")
        for prop in properties:
            self.__csMod.setOptionValue(f"{self.__baseSecurity}/Hosts/{hostname}/{prop}", properties[prop])
        gLogger.info("Registered host %s" % hostname)
        self.csModified = True
        return S_OK(True)

    def addShifter(self, shifters=None):
        """
        Adds or modify one or more shifters. Also, adds the shifter section in case this is not present.
        Shifter identities are used in several places, mostly for running agents

        :param dict shifters: has to be in the form {'ShifterRole':{'User':'aUserName', 'Group':'aDIRACGroup'}}

        :return: S_OK/S_ERROR
        """

        def getOpsSection():
            """
            Where is the shifters section?
            """
            vo = CSGlobals.getVO()
            setup = CSGlobals.getSetup()

            if vo:
                res = gConfig.getSections(f"/Operations/{vo}/{setup}/Shifter")
                if res["OK"]:
                    return S_OK(f"/Operations/{vo}/{setup}/Shifter")

                res = gConfig.getSections("/Operations/%s/Defaults/Shifter" % vo)
                if res["OK"]:
                    return S_OK("/Operations/%s/Defaults/Shifter" % vo)

            else:
                res = gConfig.getSections("/Operations/%s/Shifter" % setup)
                if res["OK"]:
                    return S_OK("/Operations/%s/Shifter" % setup)

                res = gConfig.getSections("/Operations/Defaults/Shifter")
                if res["OK"]:
                    return S_OK("/Operations/Defaults/Shifter")

            return S_ERROR("No shifter section")

        if shifters is None:
            shifters = {}
        if not self.__initialized["OK"]:
            return self.__initialized

        # get current shifters
        opsH = Operations()
        currentShifterRoles = opsH.getSections("Shifter")
        if not currentShifterRoles["OK"]:
            # we assume the shifter section is not present
            currentShifterRoles = []
        else:
            currentShifterRoles = currentShifterRoles["Value"]
        currentShiftersDict = {}
        for currentShifterRole in currentShifterRoles:
            currentShifter = opsH.getOptionsDict("Shifter/%s" % currentShifterRole)
            if not currentShifter["OK"]:
                return currentShifter
            currentShifter = currentShifter["Value"]
            currentShiftersDict[currentShifterRole] = currentShifter

        # Removing from shifters what does not need to be changed
        for sRole in list(shifters):  # note the pop below
            if sRole in currentShiftersDict:
                if currentShiftersDict[sRole] == shifters[sRole]:
                    shifters.pop(sRole)

        # get shifters section to modify
        section = getOpsSection()

        # Is this section present?
        if not section["OK"]:
            if section["Message"] == "No shifter section":
                gLogger.warn(section["Message"])
                gLogger.info("Adding shifter section")
                vo = CSGlobals.getVO()
                if vo:
                    section = "/Operations/%s/Defaults/Shifter" % vo
                else:
                    section = "/Operations/Defaults/Shifter"
                res = self.__csMod.createSection(section)
                if not res:
                    gLogger.error("Section %s not created" % section)
                    return S_ERROR("Section %s not created" % section)
            else:
                gLogger.error(section["Message"])
                return section
        else:
            section = section["Value"]

        # add or modify shifters
        for shifter in shifters:
            self.__csMod.removeSection(section + "/" + shifter)
            self.__csMod.createSection(section + "/" + shifter)
            self.__csMod.createSection(section + "/" + shifter + "/" + "User")
            self.__csMod.createSection(section + "/" + shifter + "/" + "Group")
            self.__csMod.setOptionValue(section + "/" + shifter + "/" + "User", shifters[shifter]["User"])
            self.__csMod.setOptionValue(section + "/" + shifter + "/" + "Group", shifters[shifter]["Group"])

        self.csModified = True
        return S_OK(True)

    def modifyHost(self, hostname, properties, createIfNonExistant=False):
        """
        Modify a host

        :param str hostname: hostname name
        :param dict properties: dictionary describing host properties:

            - DN
            - Properties
            - <extra params>

        :param bool createIfNonExistant: if true, creates the host if it did not exist
        :return: S_OK, Value = True/False
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        modifiedHost = False
        result = self.describeHosts([hostname])
        if not result["OK"]:
            return result
        hostData = result["Value"]
        if hostname not in hostData:
            if createIfNonExistant:
                gLogger.info("Registering host %s" % hostname)
                return self.addHost(hostname, properties)
            gLogger.error("Host is not registered", hostname)
            return S_OK(False)
        for prop in properties:
            prevVal = self.__csMod.getValue(f"{self.__baseSecurity}/Hosts/{hostname}/{prop}")
            if not prevVal or prevVal != properties[prop]:
                gLogger.info(f"Setting {prop} property for host {hostname} to {properties[prop]}")
                self.__csMod.setOptionValue(f"{self.__baseSecurity}/Hosts/{hostname}/{prop}", properties[prop])
                modifiedHost = True
        if modifiedHost:
            gLogger.info("Modified host %s" % hostname)
            self.csModified = True
        else:
            gLogger.info("Nothing to modify for host %s" % hostname)
        return S_OK(True)

    def syncUsersWithCFG(self, usersCFG):
        """
        Sync users with the cfg contents. Usernames have to be sections containing
        DN, Groups, and extra properties as parameters
        """
        if not self.__initialized["OK"]:
            return self.__initialized
        done = True
        for user in usersCFG.listSections():
            properties = {}
            propList = usersCFG[user].listOptions()
            for prop in propList:
                if prop == "Groups":
                    properties[prop] = List.fromChar(usersCFG[user][prop])
                else:
                    properties[prop] = usersCFG[user][prop]
            if not self.modifyUser(user, properties, createIfNonExistant=True):
                done = False
        return S_OK(done)

    def sortUsersAndGroups(self):
        self.__csMod.sortAlphabetically("%s/Users" % self.__baseSecurity)
        self.__csMod.sortAlphabetically("%s/Hosts" % self.__baseSecurity)
        for group in self.__csMod.getSections("%s/Groups" % self.__baseSecurity):
            usersOptionPath = f"{self.__baseSecurity}/Groups/{group}/Users"
            users = self.__csMod.getValue(usersOptionPath)
            if users:
                usersList = sorted(List.fromChar(users))
                sortedUsers = ", ".join(usersList)
                if users != sortedUsers:
                    self.__csMod.setOptionValue(usersOptionPath, sortedUsers)

    def checkForUnexistantUsersInGroups(self):
        allUsers = self.__csMod.getSections("%s/Users" % self.__baseSecurity)
        allGroups = self.__csMod.getSections("%s/Groups" % self.__baseSecurity)
        for group in allGroups:
            usersInGroup = self.__csMod.getValue(f"{self.__baseSecurity}/Groups/{group}/Users")
            if usersInGroup:
                filteredUsers = []
                usersInGroup = List.fromChar(usersInGroup)
                for user in usersInGroup:
                    if user in allUsers:
                        filteredUsers.append(user)
                self.__csMod.setOptionValue(f"{self.__baseSecurity}/Groups/{group}/Users", ",".join(filteredUsers))

    def commitChanges(self, sortUsers=True):
        if not self.__initialized["OK"]:
            return self.__initialized
        if self.csModified:
            self.checkForUnexistantUsersInGroups()
            if sortUsers:
                self.sortUsersAndGroups()
            retVal = self.__csMod.commit()
            if not retVal["OK"]:
                gLogger.error("Can't commit new configuration data", "%s" % retVal["Message"])
                return retVal
            return self.downloadCSData()
        return S_OK()

    def commit(self):
        """Commit the accumulated changes to the CS server"""
        if not self.__initialized["OK"]:
            return self.__initialized
        if self.csModified:
            retVal = self.__csMod.commit()
            if not retVal["OK"]:
                gLogger.error("Can't commit new configuration data", "%s" % retVal["Message"])
                return retVal
            return self.downloadCSData()
        return S_OK()

    def mergeFromCFG(self, cfg):
        """Merge the internal CFG data with the input"""
        if not self.__initialized["OK"]:
            return self.__initialized
        self.__csMod.mergeFromCFG(cfg)
        self.csModified = True
        return S_OK()

    def modifyValue(self, optionPath, newValue):
        """Modify an existing value at the specified options path."""
        if not self.__initialized["OK"]:
            return self.__initialized
        prevVal = self.__csMod.getValue(optionPath)
        if prevVal is None:
            return S_ERROR(f"Trying to set {optionPath} to {newValue} but option does not exist")
        gLogger.verbose(f"Changing {optionPath} from \n{prevVal} \nto \n{newValue}")
        self.__csMod.setOptionValue(optionPath, newValue)
        self.csModified = True
        return S_OK("Modified %s" % optionPath)

    def setOption(self, optionPath, optionValue):
        """Create an option at the specified path."""
        if not self.__initialized["OK"]:
            return self.__initialized
        self.__csMod.setOptionValue(optionPath, optionValue)
        self.csModified = True
        return S_OK(f"Created new option {optionPath} = {optionValue}")

    def setOptionComment(self, optionPath, comment):
        """Create an option at the specified path."""
        if not self.__initialized["OK"]:
            return self.__initialized
        self.__csMod.setComment(optionPath, comment)
        self.csModified = True
        return S_OK(f"Set option comment {optionPath} : {comment}")

    def delOption(self, optionPath):
        """Delete an option"""
        if not self.__initialized["OK"]:
            return self.__initialized
        if not self.__csMod.removeOption(optionPath):
            return S_ERROR("Couldn't delete option %s" % optionPath)
        self.csModified = True
        return S_OK("Deleted option %s" % optionPath)

    def createSection(self, sectionPath, comment=""):
        """Create a new section"""
        if not self.__initialized["OK"]:
            return self.__initialized
        self.__csMod.createSection(sectionPath)
        self.csModified = True
        if comment:
            self.__csMod.setComment(sectionPath, comment)
        return S_OK()

    def delSection(self, sectionPath):
        """Delete a section"""
        if not self.__initialized["OK"]:
            return self.__initialized
        if not self.__csMod.removeSection(sectionPath):
            return S_ERROR("Could not delete section %s " % sectionPath)
        self.csModified = True
        return S_OK()

    def copySection(self, originalPath, targetPath):
        """Copy a whole section to a new location"""
        if not self.__initialized["OK"]:
            return self.__initialized
        cfg = self.__csMod.getCFG()
        sectionCfg = cfg[originalPath]
        result = self.createSection(targetPath)
        if not result["OK"]:
            return result
        if not self.__csMod.mergeSectionFromCFG(targetPath, sectionCfg):
            return S_ERROR("Could not merge cfg into section %s" % targetPath)
        self.csModified = True
        return S_OK()

    def moveSection(self, originalPath, targetPath):
        """Move a whole section to a new location"""
        result = self.copySection(originalPath, targetPath)
        if not result["OK"]:
            return result
        result = self.delSection(originalPath)
        if not result["OK"]:
            return result
        self.csModified = True
        return S_OK()

    def mergeCFGUnderSection(self, sectionPath, cfg):
        """Merge the given cfg under a certain section"""
        if not self.__initialized["OK"]:
            return self.__initialized
        result = self.createSection(sectionPath)
        if not result["OK"]:
            return result
        if not self.__csMod.mergeSectionFromCFG(sectionPath, cfg):
            return S_ERROR("Could not merge cfg into section %s" % sectionPath)
        self.csModified = True
        return S_OK()

    def mergeWithCFG(self, cfg):
        """Merge the given cfg with the current config"""
        if not self.__initialized["OK"]:
            return self.__initialized
        self.__csMod.mergeFromCFG(cfg)
        self.csModified = True
        return S_OK()

    def getCurrentCFG(self):
        """Get the current CFG as it is"""
        if not self.__initialized["OK"]:
            return self.__initialized
        return S_OK(self.__csMod.getCFG())

    def showDiff(self):
        """Just shows the differences accumulated within the Modificator object"""
        diffData = self.__csMod.showCurrentDiff()
        gLogger.notice("Accumulated diff with master CS")
        for line in diffData:
            if line[0] in ("+", "-"):
                gLogger.notice(line)

    def forceGlobalConfigurationUpdate(self):
        """
        Force global update of configuration on all the registered services

        :return: S_OK/S_ERROR
        """

        return self.__rpcClient.forceGlobalConfigurationUpdate()
