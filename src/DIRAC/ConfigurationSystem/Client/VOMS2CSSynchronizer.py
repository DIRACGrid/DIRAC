""" VOMS2CSSyncronizer is a helper class containing the logic for synchronization
    of the VOMS user data with the DIRAC Registry
"""

from collections import defaultdict

from diraccfg import CFG

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (
    getAllUsers,
    getGroupsForUser,
    getUserOption,
    getUsersInVO,
    getVOMSRoleGroupMapping,
    getVOOption,
)
from DIRAC.Core.Security.IAMService import IAMService
from DIRAC.Core.Security.VOMSService import VOMSService
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise


def _getUserNameFromMail(mail):
    """Utility to construct a reasonable user name from the user mail address

    :param str mail: e-mail address
    :return str: user name
    """

    mailName = mail.split("@")[0].lower()
    if "." in mailName:
        # Most likely the mail contains the full user name
        names = mailName.split(".")
        name = names[0][0] + names[-1].lower()
        return name

    return mailName


def _getUserNameFromDN(dn, vo):
    """Utility to construct a reasonable user name from the user DN
    :param str dn: user DN
    :return str: user name
    """

    shortVO = vo
    if "." in vo:
        vos = vo.split(".")
        if vos[0] == "vo":
            vos = vos[1:]
        if len(vos[-1]) == 2 or vos[-1] == "org":
            vos = vos[:1]
        shortVO = ".".join(vos)

    # Weird case of just a name as DN !
    if "/" not in dn and "CN=" not in dn:
        dn = "CN=" + dn
    entries = dn.split("/")
    entries.reverse()
    for entry in entries:
        if entry:
            # Weird case of no field name !
            if "=" not in entry:
                key, value = "CN", entry
            else:
                key, value = entry.split("=")
            key = key.strip()
            value = value.strip()
            if key.upper() == "CN":
                ind = value.find("(")
                # Strip of possible words in parenthesis in the name
                if ind != -1:
                    value = value[:ind]
                names = value.split()
                if len(names) == 1:
                    nname = names[0].lower()
                    if "." in nname:
                        names = nname.split(".")
                        nname = (names[0][0] + names[-1]).lower()
                    if "-" in nname:
                        names = nname.split("-")
                        nname = (names[0][0] + names[-1]).lower()
                    return nname
                else:
                    robot = False
                    # only pop if the remains are sufficient (i.e. not just digits)
                    if names[0].lower().startswith("robot"):
                        nameok = False
                        if len(names) > 1:
                            for name in names[1:]:
                                if not name.isdigit():
                                    nameok = True
                        if nameok:
                            names.pop(0)
                        robot = True
                    for name in list(names):
                        if name[0].isdigit() or "@" in name:
                            names.pop(names.index(name))
                    if robot:
                        nname = f"robot-{names[-1].lower()}-{shortVO}"
                    else:
                        nname = (names[0][0] + names[-1]).lower()
                        if "." in nname:
                            names = nname.split(".")
                            nname = (names[0][0] + names[-1]).lower()
                    return nname


def _getUserNameFromSurname(name, surname):
    """Construct a reasonable userName from the user name and surname

    :param str name: user name
    :param str surname: user surname
    :return str: constructed user name
    """
    names = name.split()
    initials = ""
    for nn in names:
        initials += nn[0]
    surnames = surname.split()
    result = initials + surnames[-1]
    if len(result) >= 12:
        return result[:11]
    result = result.lower()
    return result


class VOMS2CSSynchronizer:
    def __init__(
        self,
        vo,
        autoModifyUsers=True,
        autoAddUsers=True,
        autoDeleteUsers=False,
        autoLiftSuspendedStatus=False,
        syncPluginName=None,
        compareWithIAM=False,
        useIAM=False,
        accessToken=None,
        forceNickname=False,
    ):
        """VOMS2CSSynchronizer class constructor

        :param str vo: VO to be synced
        :param boolean autoModifyUsers: flag to automatically modify user data in CS
        :param autoAddUsers: flag to automatically add new users to CS
        :param autoDeleteUsers: flag to automatically delete users from CS if no more in VOMS
        :param autoLiftSuspendedStatus: flag to automatically remove Suspended status in CS
        :param syncPluginName: name of the plugin to validate or extend users' info
        :param compareWithIAM: if true, also dump the list of users from IAM and compare
        :param useIAM: if True, use Iam instead of VOMS
        :param accessToken: if talking to IAM, needs a token with scim:read property

        :return: None
        """

        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.csapi = CSAPI()
        self.vo = vo
        self.vomsVOName = getVOOption(vo, "VOMSName", "")
        if not self.vomsVOName:
            raise Exception(f"VOMS name not defined for VO {vo}")
        self.adminMsgs = {"Errors": [], "Info": []}
        self.vomsUserDict = {}
        self.autoModifyUsers = autoModifyUsers
        self.autoAddUsers = autoAddUsers
        self.autoDeleteUsers = autoDeleteUsers
        self.autoLiftSuspendedStatus = autoLiftSuspendedStatus
        self.voChanged = False
        self.syncPlugin = None
        self.iamSrv = None
        self.compareWithIAM = compareWithIAM
        self.useIAM = useIAM
        self.accessToken = accessToken
        self.forceNickname = forceNickname

        if syncPluginName:
            objLoader = ObjectLoader()
            _class = objLoader.loadObject(
                f"ConfigurationSystem.Client.SyncPlugins.{syncPluginName}SyncPlugin", f"{syncPluginName}SyncPlugin"
            )

            if not _class["OK"]:
                raise Exception(_class["Message"])

            self.syncPlugin = _class["Value"]()

    def compare_entry(self, iam_entry, voms_entry, is_robot):
        """Compare a VOMS and IAM entry"""

        if iam_entry.get("mail") != voms_entry.get("mail"):
            self.log.info(
                "Difference in mails",
                f"{iam_entry['nickname']} - mail : {iam_entry.get('mail')} vs {voms_entry.get('mail')}",
            )
            if is_robot:
                self.log.info("\t this is expected for robots !")

        for field in ("CA", "certSuspended", "suspended", "mail", "nickname"):
            if iam_entry.get(field) != voms_entry.get(field):
                self.log.info(
                    f"Difference in {field}",
                    f"{iam_entry['nickname']} - {field} : {iam_entry.get(field)} vs {voms_entry.get(field)}",
                )

        if sorted(iam_entry["Roles"]) != sorted(voms_entry["Roles"]):
            self.log.info(
                "Difference in roles",
                f"{iam_entry['nickname']} - Roles : {iam_entry['Roles']} vs {voms_entry['Roles']}",
            )

    def compareUsers(self, voms_users, iam_users):
        missing_in_iam = set(voms_users) - set(iam_users)
        if missing_in_iam:
            self.log.info("Missing entries in IAM:", missing_in_iam)
        else:
            self.log.info("No entry missing in IAM, GOOD !")
        # suspended_in_voms = {dn for dn in voms_users if voms_users[dn]["suspended"]}
        missing_in_voms = set(iam_users) - set(voms_users)

        if missing_in_voms:
            self.log.info("Entries in IAM that are not in VOMS:", missing_in_voms)
        else:
            self.log.info("No extra entry entries in IAM, GOOD !")

        for dn in set(iam_users) & set(voms_users):
            is_robot = "CN=Robot:" in dn
            self.compare_entry(iam_users[dn], voms_users[dn], is_robot=is_robot)

    @convertToReturnValue
    def _getUsers(self):
        if self.compareWithIAM or self.useIAM:
            self.iamSrv = IAMService(self.accessToken, vo=self.vo, forceNickname=self.forceNickname)
            iam_users = returnValueOrRaise(self.iamSrv.getUsers())
            if self.useIAM:
                return iam_users

        vomsSrv = VOMSService(self.vo)
        voms_users = returnValueOrRaise(vomsSrv.getUsers())
        if self.compareWithIAM:
            self.compareUsers(voms_users.get("Users", {}), iam_users.get("Users", {}))
        return voms_users

    def syncCSWithVOMS(self):
        """Performs the synchronization of the DIRAC registry with the VOMS data. The resulting
          CSAPI object containing modifications is returned as part of the output dictionary.
          Those changes can be applied by the caller depending on the mode (dry or a real run)


        :return: S_OK with a dictionary containing the results of the synchronization operation
        """
        resultDict = defaultdict(list)

        # Get DIRAC group vs VOMS Role Mappings
        result = getVOMSRoleGroupMapping(self.vo)
        if not result["OK"]:
            return result

        vomsDIRACMapping = result["Value"]["VOMSDIRAC"]
        diracVOMSMapping = result["Value"]["DIRACVOMS"]
        noVOMSGroups = result["Value"]["NoVOMS"]
        noSyncVOMSGroups = result["Value"]["NoSyncVOMS"]

        result = self._getUsers()
        if not result["OK"]:
            self.log.error("Could not retrieve user information", result["Message"])
            return result
        if getUserErrors := result["Value"]["Errors"]:
            self.adminMsgs["Errors"].extend(getUserErrors)
        self.vomsUserDict = result["Value"]["Users"]
        message = f"There are {len(self.vomsUserDict)} user entries in VOMS for VO {self.vomsVOName}"
        self.adminMsgs["Info"].append(message)
        self.log.info("VOMS user entries", message)
        self.log.debug(self.vomsUserDict)

        # Get DIRAC users
        result = self.getVOUserData(self.vo)
        if not result["OK"]:
            return result
        diracUserDict = result["Value"]
        self.adminMsgs["Info"].append(f"There are {len(diracUserDict)} registered users in DIRAC for VO {self.vo}")
        self.log.info(
            "Users already registered", f": there are {len(diracUserDict)} registered users in DIRAC VO {self.vo}"
        )

        # Find new and obsoleted user DNs
        existingDNs = []
        obsoletedDNs = []
        newDNs = []
        for user in diracUserDict:
            dn = diracUserDict[user]["DN"]
            # We can have users with more than one DN registered
            dnList = fromChar(dn)
            existingDNs.extend(dnList)
            for dn in dnList:
                if dn not in self.vomsUserDict:
                    obsoletedDNs.append(dn)

        for dn in self.vomsUserDict:
            if dn not in existingDNs:
                newDNs.append(dn)

        allDiracUsers = getAllUsers()
        nonVOUserDict = {}
        nonVOUsers = list(set(allDiracUsers) - set(diracUserDict))
        if nonVOUsers:
            result = self.csapi.describeUsers(nonVOUsers)
            if not result["OK"]:
                self.log.error("Could not retrieve CS User description")
                return result
            nonVOUserDict = result["Value"]

        # Process users
        defaultVOGroup = getVOOption(self.vo, "DefaultGroup", f"{self.vo}_user")
        # If a user is (previously put by hand) in a "QuarantineGroup",
        # then the default group will be ignored.
        # So, this option is only considered for the case of existing users.
        quarantineVOGroup = getVOOption(self.vo, "QuarantineGroup")

        newAddedUserDict = {}
        for dn in self.vomsUserDict:
            newDNForExistingUser = ""
            diracName = ""
            if dn in existingDNs:
                for user in diracUserDict:
                    if dn in fromChar(diracUserDict[user]["DN"]):
                        diracName = user
                        break

            if dn in newDNs:
                # Find if the DN is already registered in the DIRAC CS
                for user in nonVOUserDict:
                    if dn in fromChar(nonVOUserDict[user]["DN"]):
                        diracName = user
                        diracUserDict[diracName] = nonVOUserDict[user]
                        break

                # Check the nickName in the same VO to see if the user is already registered
                # with another DN
                nickName = self.vomsUserDict[dn].get("nickname")
                if not nickName and self.forceNickname:
                    resultDict["NoNickname"].append(self.vomsUserDict[dn])
                    self.log.error("No nickname defined for", self.vomsUserDict[dn])
                    continue
                if nickName in diracUserDict or nickName in newAddedUserDict:
                    diracName = nickName
                    # This is a flag for adding the new DN to an already existing user
                    newDNForExistingUser = dn

                # We have a real new user
                if not diracName:
                    # if we have a nickname, we use the nickname no
                    # matter what so we can have users from different
                    # VOs with the same nickname / username
                    if nickName:
                        newDiracName = nickName.strip()
                    else:
                        newDiracName = self.getUserName(dn)

                        # If the chosen user name exists already, append a distinguishing suffix
                        ind = 1
                        trialName = newDiracName
                        while newDiracName in allDiracUsers:
                            # We have a user with the same name but with a different DN
                            newDiracName = "%s_%d" % (trialName, ind)
                            ind += 1

                    # Do not consider users with Suspended status in VOMS
                    if self.vomsUserDict[dn]["suspended"] or self.vomsUserDict[dn]["certSuspended"]:
                        resultDict["SuspendedUsers"].append(newDiracName)
                        continue

                    # We now have everything to add the new user
                    userDict = {"DN": dn, "CA": self.vomsUserDict[dn]["CA"], "Email": self.vomsUserDict[dn]["mail"]}
                    groupsWithRole = []
                    for role in self.vomsUserDict[dn]["Roles"]:
                        groupList = vomsDIRACMapping.get(role, [])
                        for group in groupList:
                            if group not in noSyncVOMSGroups:
                                groupsWithRole.append(group)
                    userDict["Groups"] = list(set(groupsWithRole + [defaultVOGroup]))

                    # Run the sync plugins for extra info and/or validations
                    if self.syncPlugin:
                        try:
                            self.syncPlugin.verifyAndUpdateUserInfo(newDiracName, userDict)
                        except ValueError as e:
                            self.log.error("Error validating new user", f"nickname {newDiracName}\n error {e}")
                            self.adminMsgs["Errors"].append(
                                f"Error validating new user {newDiracName}: {userDict}\n  {e}"
                            )
                            continue

                    message = f"\n  Added new user {newDiracName}:\n"
                    for key in userDict:
                        message += f"    {key}: {str(userDict[key])}\n"
                    self.adminMsgs["Info"].append(message)
                    self.voChanged = True
                    if self.autoAddUsers:
                        self.log.info(f"Adding new user {newDiracName}: {str(userDict)}")
                        result = self.csapi.modifyUser(newDiracName, userDict, createIfNonExistant=True)
                        if not result["OK"]:
                            self.log.warn(f"Failed adding new user {newDiracName}", result["Message"])
                        resultDict["NewUsers"].append(newDiracName)
                        newAddedUserDict[newDiracName] = userDict
                    continue

            # If we have a new user with multiple DN,
            # it's a bit tricky, so first create it with a single one
            # and at the next iteration add more DNs
            if diracName in newAddedUserDict:
                continue
            # We have an already existing user
            modified = False
            suspendedInVOMS = self.vomsUserDict[dn]["suspended"] or self.vomsUserDict[dn]["certSuspended"]
            suspendedVOList = getUserOption(diracName, "Suspended", [])
            knownEmail = getUserOption(diracName, "Email", None)
            userDict = {
                "DN": diracUserDict[diracName]["DN"],
                "CA": diracUserDict[diracName]["CA"],
                "Email": self.vomsUserDict[dn].get("mail", self.vomsUserDict[dn].get("emailAddress")) or knownEmail,
            }

            # Set Suspended status for the user for this particular VO
            if suspendedInVOMS and self.vo not in suspendedVOList:
                suspendedVOList.append(self.vo)
                userDict["Suspended"] = ",".join(suspendedVOList)
                modified = True

            # Remove the lifted Suspended status
            if not suspendedInVOMS and self.vo in suspendedVOList and self.autoLiftSuspendedStatus:
                newList = []
                for vo in suspendedVOList:
                    if vo != self.vo:
                        newList.append(vo)
                if not newList:
                    newList = ["None"]
                userDict["Suspended"] = ",".join(newList)
                modified = True

            if newDNForExistingUser:
                userDict["DN"] = ",".join([dn, diracUserDict.get(diracName, newAddedUserDict.get(diracName))["DN"]])
                userDict["CA"] = ",".join(
                    [self.vomsUserDict[dn]["CA"], diracUserDict.get(diracName, newAddedUserDict.get(diracName))["CA"]]
                )
                modified = True
            existingGroups = diracUserDict.get(diracName, {}).get("Groups", [])
            nonVOGroups = list(set(existingGroups) - set(diracVOMSMapping))
            groupsWithRole = []
            for role in self.vomsUserDict[dn]["Roles"]:
                groupList = vomsDIRACMapping.get(role, [])
                for group in groupList:
                    if group not in noSyncVOMSGroups:
                        groupsWithRole.append(group)
            keepGroups = nonVOGroups + groupsWithRole
            if not quarantineVOGroup or quarantineVOGroup not in existingGroups:
                keepGroups += [defaultVOGroup]
            if quarantineVOGroup and quarantineVOGroup in existingGroups:
                keepGroups = [quarantineVOGroup]
            for group in existingGroups:
                if group in nonVOGroups:
                    continue
                role = diracVOMSMapping.get(group, "")
                # Among already existing groups for the user keep those without a special VOMS Role
                # because this membership is done by hand in the CS
                if "Role" not in role:
                    keepGroups.append(group)
                # Keep existing groups with no VOMS attribute if any
                if group in noVOMSGroups:
                    keepGroups.append(group)
                # Keep groups for which syncronization with VOMS is forbidden
                if group in noSyncVOMSGroups:
                    keepGroups.append(group)
            userDict["Groups"] = list(set(keepGroups))
            # Merge together groups for the same user but different DNs
            if diracName in newAddedUserDict:
                otherGroups = newAddedUserDict[diracName].get("Groups", [])
                userDict["Groups"] = list(set(keepGroups + otherGroups))
                modified = True
            if not existingGroups and diracName in allDiracUsers:
                groups = getGroupsForUser(diracName)
                if groups["OK"]:
                    self.log.info(f"Found groups for user {diracName} {groups['Value']}")
                    userDict["Groups"] = list(set(groups["Value"] + keepGroups))
                    addedGroups = list(set(userDict["Groups"]) - set(groups["Value"]))
                    modified = True
                    message = f"\n  Modified user {diracName}:\n"
                    message += f"    Added to group(s) {','.join(addedGroups)}\n"
                    self.adminMsgs["Info"].append(message)

            # Check if something changed before asking CSAPI to modify
            if diracName in diracUserDict:
                message = f"\n  Modified user {diracName}:\n"
                modMsg = ""
                for key in userDict:
                    if key == "Groups":
                        addedGroups = set(userDict[key]) - set(diracUserDict.get(diracName, {}).get(key, []))
                        removedGroups = set(diracUserDict.get(diracName, {}).get(key, [])) - set(userDict[key])
                        if addedGroups:
                            modMsg += f"    Added to group(s) {','.join(addedGroups)}\n"
                        if removedGroups:
                            modMsg += f"    Removed from group(s) {','.join(removedGroups)}\n"
                    elif key == "Suspended":
                        if userDict["Suspended"] == "None":
                            modMsg += "    Suspended status removed\n"
                        else:
                            modMsg += f"    User Suspended in VOs: {userDict['Suspended']}\n"
                    else:
                        oldValue = str(diracUserDict.get(diracName, {}).get(key, ""))
                        if str(userDict[key]) != oldValue:
                            modMsg += f"    {key}: {oldValue} -> {str(userDict[key])}\n"
                if modMsg:
                    self.adminMsgs["Info"].append(message + modMsg)
                    modified = True

            if self.autoModifyUsers and modified:
                result = self.csapi.modifyUser(diracName, userDict)
                if result["OK"] and result["Value"]:
                    self.log.info(f"Modified user {diracName}: {str(userDict)}")
                    self.voChanged = True
                    resultDict["ModifiedUsers"].append(diracName)

        # Check if there are potentially obsoleted users
        oldUsers = set()
        for user in diracUserDict:
            dnSet = set(fromChar(diracUserDict[user]["DN"]))
            if not dnSet.intersection(set(self.vomsUserDict)) and user not in nonVOUserDict:
                existingGroups = diracUserDict.get(user, {}).get("Groups", [])
                nonVOGroups = list(set(existingGroups) - set(diracVOMSMapping))
                removedGroups = list(set(existingGroups) - set(nonVOGroups))
                if removedGroups:
                    self.log.info("Checking user for deletion", f"{user}: {existingGroups}")
                    self.log.info("User has groups in other VOs", f"{user}: {nonVOGroups}")
                    userDict = diracUserDict[user]
                    userDict["Groups"] = nonVOGroups
                    if self.autoModifyUsers:
                        result = self.csapi.modifyUser(user, userDict)
                        if result["OK"] and result["Value"]:
                            self.log.info(f"Modified user {user}: {str(userDict)}")
                            self.voChanged = True
                            message = f"\n  Modified user {user}:\n"
                            modMsg = f"    Removed from group(s) {','.join(removedGroups)}\n"
                            self.adminMsgs["Info"].append(message + modMsg)
                            resultDict["ModifiedUsers"].append(user)
                    continue
                if not any(group in noVOMSGroups for group in existingGroups):
                    oldUsers.add(user)

        # Check for obsoleted DNs
        for user in diracUserDict:
            dnSet = set(fromChar(diracUserDict[user]["DN"]))
            for dn in dnSet:
                if dn in obsoletedDNs and user not in oldUsers:
                    existingGroups = diracUserDict.get(user, {}).get("Groups", [])
                    nonVOGroups = list(set(existingGroups) - set(diracVOMSMapping))
                    if nonVOGroups:
                        self.log.verbose("User has groups in other VOs", f"{user}: {nonVOGroups}")
                        continue
                    self.log.verbose(f"Modified user {user}: dropped DN {dn}")
                    if self.autoModifyUsers:
                        userDict = diracUserDict[user]
                        modDNSet = dnSet - {dn}
                        if modDNSet:
                            userDict["DN"] = ",".join(modDNSet)
                            result = self.csapi.modifyUser(user, userDict)
                            if result["OK"] and result["Value"]:
                                self.log.info(f"Modified user {user}: dropped DN {dn}")
                                self.adminMsgs["Info"].append(f"Modified user {user}: dropped DN {dn}")
                                self.voChanged = True
                                resultDict["ModifiedUsers"].append(diracName)
                        else:
                            oldUsers.add(user)

        if oldUsers:
            self.voChanged = True
            if self.autoDeleteUsers:
                self.log.info(f"The following users will be deleted: {str(oldUsers)}")
                result = self.csapi.deleteUsers(oldUsers)
                if result["OK"]:
                    self.adminMsgs["Info"].append(f"The following users are deleted from CS:\n  {str(oldUsers)}\n")
                    resultDict["DeletedUsers"] = oldUsers
                else:
                    self.adminMsgs["Errors"].append(f"Error in deleting users from CS:\n  {str(oldUsers)}")
                    self.log.error("Error while user deletion from CS", result)
            else:
                self.adminMsgs["Info"].append(
                    "The following users to be checked for deletion:\n\t%s" % "\n\t".join(sorted(oldUsers))
                )
                self.log.info("The following users to be checked for deletion:", "\n\t".join(sorted(oldUsers)))

        # Try to fill in the DiracX section
        if self.useIAM:
            iam_subs = self.iamSrv.getUsersSub(self.vo)
            diracx_vo_config = {"DiracX": {"CsSync": {"VOs": {self.vo: {"UserSubjects": iam_subs}}}}}
            iam_sub_cfg = CFG()
            iam_sub_cfg.loadFromDict(diracx_vo_config)
            result = self.csapi.mergeFromCFG(iam_sub_cfg)
            if not result["OK"]:
                return result

        resultDict["CSAPI"] = self.csapi
        resultDict["AdminMessages"] = self.adminMsgs
        resultDict["VOChanged"] = self.voChanged
        return S_OK(resultDict)

    def getVOUserData(self, refreshFlag=False):
        """Get a report for users of a given VO

        :param bool refreshFlag: flag to indicate that the configuration must be refreshed
                                 before looking up user data
        :return: S_OK/S_ERROR, Value = user description dictionary
        """
        if refreshFlag:
            gConfig.forceRefresh()

        # Get DIRAC users
        diracUsers = getUsersInVO(self.vo)
        if not diracUsers:
            return S_ERROR(f"No VO users found for {self.vo}")

        if refreshFlag:
            result = self.csapi.downloadCSData()
            if not result["OK"]:
                return result
        result = self.csapi.describeUsers(diracUsers)
        if not result["OK"]:
            self.log.error("Could not retrieve CS User description")
        return result

    def getVOUserReport(self):
        """Get a report string with the current status of the DIRAC Registry for the
            Virtual Organization

        :return: S_OK with the report string as Value
        """

        result = self.getVOUserData(refreshFlag=True)
        if not result["OK"]:
            return result

        userDict = result["Value"]

        # Get DIRAC group vs VOMS Role Mappings
        result = getVOMSRoleGroupMapping(self.vo)
        if not result["OK"]:
            return result

        diracVOMSMapping = result["Value"]["DIRACVOMS"]
        records = []
        groupDict = defaultdict(int)
        multiDNUsers = {}
        suspendedUsers = []
        for user in userDict:
            for group in userDict[user]["Groups"]:
                groupDict[group] += 1
            dnList = fromChar(userDict[user]["DN"])
            if len(dnList) > 1:
                multiDNUsers[user] = dnList
            if userDict[user].get("Status", "Active") == "Suspended":
                suspendedUsers.append(user)

        for group in diracVOMSMapping:
            records.append((group, str(groupDict[group]), diracVOMSMapping.get(group, "")))

        fields = ["Group", "Number of users", "VOMS Role"]
        output = printTable(fields, records, sortField="Group", printOut=False, numbering=False)

        if multiDNUsers:
            output += "\nUsers with multiple DNs:\n"
            for user in multiDNUsers:
                output += f"  {user}:\n"
                for dn in multiDNUsers[user]:
                    output += f"    {dn}\n"

        if suspendedUsers:
            output += f"\n{len(suspendedUsers)} suspended users:\n"
            output += f"  {','.join(suspendedUsers)}"

        return S_OK(output)

    def getUserName(self, dn):
        """Utility to construct user name

        :param str dn: user DN
        :return str: user name
        """
        name = self.vomsUserDict[dn].get("name")
        surname = self.vomsUserDict[dn].get("surname")
        if name and surname:
            surnameName = _getUserNameFromSurname(name, surname)
            return surnameName

        dnName = _getUserNameFromDN(dn, self.vo)

        # If robot, take the dn based name
        if dnName.startswith("robot"):
            return dnName

        # Is mailName reasonable ?
        mail = self.vomsUserDict[dn]["mail"]
        if mail:
            mailName = _getUserNameFromMail(mail)
            if len(mailName) > 5 and mailName.isalpha():
                return mailName

        # dnName too long
        if len(dnName) >= 12:
            dnName = dnName[:11]

        # May be the mail name is still more reasonable
        if mail and len(dnName) < len(mailName) and mailName.isalpha():
            return mailName

        return dnName
