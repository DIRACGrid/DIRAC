"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

import os
from datetime import datetime, timedelta

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Base.API import API
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import (
    killPilotsInQueues,
)

voName = ""
ret = getProxyInfo(disableVOMS=True)
if ret["OK"] and "group" in ret["Value"]:
    voName = getVOForGroup(ret["Value"]["group"])


class DiracAdmin(API):
    """Administrative functionalities"""

    #############################################################################
    def __init__(self):
        """Internal initialization of the DIRAC Admin API."""
        super().__init__()

        self.csAPI = CSAPI()

        self.currentDir = os.getcwd()
        self.sitestatus = SiteStatus()

    #############################################################################
    def uploadProxy(self):
        """Upload a proxy to the DIRAC WMS.  This method

        Example usage:

          >>> print diracAdmin.uploadProxy('dteam_pilot')
          {'OK': True, 'Value': 0L}

        :return: S_OK,S_ERROR

        :param permanent: Indefinitely update proxy
        :type permanent: boolean

        """
        return gProxyManager.uploadProxy()

    #############################################################################
    def checkProxyUploaded(self, userDN, userGroup, requiredTime):
        """Checks that a user has a proxy in the Proxy Manager

        Example usage:

          >>> gLogger.notice(diracAdmin.checkProxyUploaded( 'some DN', 'dirac group', True ))
          {'OK': True, 'Value' : True/False }

        :param userDN: User DN
        :type userDN: string
        :param userGroup: DIRAC Group
        :type userGroup: string
        :param requiredTime: Required life time of the uploaded proxy
        :type requiredTime: boolean
        :return: S_OK,S_ERROR
        """
        return gProxyManager.userHasProxy(userDN, userGroup, requiredTime)

    #############################################################################
    def getSiteMask(self, printOutput=False, status="Active"):
        """Retrieve current site mask from WMS Administrator service.

        Example usage:

          >>> gLogger.notice(diracAdmin.getSiteMask())
          {'OK': True, 'Value': 0L}

        :return: S_OK,S_ERROR

        """

        result = self.sitestatus.getSites(siteState=status)
        if result["OK"]:
            sites = result["Value"]
            if printOutput:
                sites.sort()
                for site in sites:
                    gLogger.notice(site)

        return result

    #############################################################################
    def getBannedSites(self, printOutput=False):
        """Retrieve current list of banned  and probing sites.

        Example usage:

          >>> gLogger.notice(diracAdmin.getBannedSites())
          {'OK': True, 'Value': []}

        :return: S_OK,S_ERROR

        """

        bannedSites = self.sitestatus.getSites(siteState="Banned")
        if not bannedSites["OK"]:
            return bannedSites

        probingSites = self.sitestatus.getSites(siteState="Probing")
        if not probingSites["OK"]:
            return probingSites

        mergedList = sorted(bannedSites["Value"] + probingSites["Value"])

        if printOutput:
            gLogger.notice("\n".join(mergedList))

        return S_OK(mergedList)

    #############################################################################
    def getSiteSection(self, site, printOutput=False):
        """Simple utility to get the list of CEs for DIRAC site name.

        Example usage:

          >>> gLogger.notice(diracAdmin.getSiteSection('LCG.CERN.ch'))
          {'OK': True, 'Value':}

        :return: S_OK,S_ERROR
        """
        gridType = site.split(".")[0]
        if not gConfig.getSections(f"/Resources/Sites/{gridType}")["OK"]:
            return S_ERROR(f"/Resources/Sites/{gridType} is not a valid site section")

        result = gConfig.getOptionsDict(f"/Resources/Sites/{gridType}/{site}")
        if printOutput and result["OK"]:
            gLogger.notice(self.pPrint.pformat(result["Value"]))
        return result

    #############################################################################
    def allowSite(self, site, comment, printOutput=False, days=1):
        """Adds the site to the site mask. The site must be a valid DIRAC site name

        Example usage:

          >>> gLogger.notice(diracAdmin.allowSite('LCG.CERN.ch'))
          {'OK': True, 'Value': }

        :param str site: DIRAC site name
        :param str comment: comment for the site status update
        :return: S_OK,S_ERROR

        """
        if not (result := self._checkSiteIsValid(site))["OK"]:
            return result

        if not (result := self.getSiteMask(status="Active"))["OK"]:
            return result
        siteMask = result["Value"]
        if site in siteMask:
            if printOutput:
                gLogger.notice(f"Site {site} is already Active")
            return S_OK(f"Site {site} is already Active")

        tokenLifetime = int(days)
        if tokenLifetime <= 0:
            tokenExpiration = datetime.max
        else:
            tokenExpiration = datetime.utcnow().replace(microsecond=0) + timedelta(days=tokenLifetime)

        if not (result := self.sitestatus.setSiteStatus(site, "Active", comment, expiry=tokenExpiration))["OK"]:
            return result

        if printOutput:
            gLogger.notice(f"Site {site} status is set to Active")

        return result

    #############################################################################
    def getSiteMaskLogging(self, site=None, printOutput=False):
        """Retrieves site mask logging information.

        Example usage:

          >>> gLogger.notice(diracAdmin.getSiteMaskLogging('LCG.AUVER.fr'))
          {'OK': True, 'Value': }

        :return: S_OK,S_ERROR
        """
        if not (result := self._checkSiteIsValid(site))["OK"]:
            return result

        if not (result := ResourceStatusClient().selectStatusElement("Site", "History", name=site))["OK"]:
            return result

        if printOutput:
            if site:
                gLogger.notice(f"\nSite Mask Logging Info for {site}\n")
            else:
                gLogger.notice("\nAll Site Mask Logging Info\n")

            sitesLogging = result["Value"]
            if isinstance(sitesLogging, dict):
                for siteName, tupleList in sitesLogging.items():  # can be an iterator
                    if not siteName:
                        gLogger.notice(f"\n===> {siteName}\n")
                    for tup in tupleList:
                        stup = str(tup[0]).ljust(8) + str(tup[1]).ljust(20)
                        stup += "( " + str(tup[2]).ljust(len(str(tup[2]))) + ' )  "' + str(tup[3]) + '"'
                        gLogger.notice(stup)
                    gLogger.notice(" ")
            elif isinstance(sitesLogging, list):
                sitesLoggingList = [(sl[1], sl[3], sl[4]) for sl in sitesLogging]
                for siteLog in sitesLoggingList:
                    gLogger.notice(siteLog)

        return S_OK()

    #############################################################################
    def banSite(self, site, comment, printOutput=False, days=1):
        """Removes the site from the site mask.

        Example usage:

          >>> gLogger.notice(diracAdmin.banSite())
          {'OK': True, 'Value': }

        :return: S_OK,S_ERROR

        """
        if not (result := self._checkSiteIsValid(site))["OK"]:
            return result
        mask = self.getSiteMask(status="Banned")
        if not mask["OK"]:
            return mask
        siteMask = mask["Value"]
        if site in siteMask:
            if printOutput:
                gLogger.notice(f"Site {site} is already Banned")
            return S_OK(f"Site {site} is already Banned")

        tokenLifetime = int(days)
        if tokenLifetime <= 0:
            tokenExpiration = datetime.max
        else:
            tokenExpiration = datetime.utcnow().replace(microsecond=0) + timedelta(days=tokenLifetime)
        if not (result := self.sitestatus.setSiteStatus(site, "Banned", comment, expiry=tokenExpiration))["OK"]:
            return result

        if printOutput:
            gLogger.notice(f"Site {site} status is set to Banned")

        return result

    #############################################################################
    def getProxy(self, userDN, userGroup, validity=43200, limited=False):
        """Retrieves a proxy with default 12hr validity and stores
        this in a file in the local directory by default.

        Example usage:

          >>> gLogger.notice(diracAdmin.getProxy())
          {'OK': True, 'Value': }

        :return: S_OK,S_ERROR

        """
        return gProxyManager.downloadProxy(userDN, userGroup, limited=limited, requiredTimeLeft=validity)

    #############################################################################
    def getVOMSProxy(self, userDN, userGroup, vomsAttr=False, validity=43200, limited=False):
        """Retrieves a proxy with default 12hr validity and VOMS extensions and stores
        this in a file in the local directory by default.

        Example usage:

          >>> gLogger.notice(diracAdmin.getVOMSProxy())
          {'OK': True, 'Value': }

        :return: S_OK,S_ERROR

        """
        return gProxyManager.downloadVOMSProxy(
            userDN, userGroup, limited=limited, requiredVOMSAttribute=vomsAttr, requiredTimeLeft=validity
        )

    #############################################################################
    def resetJob(self, jobID):
        """Reset a job or list of jobs in the WMS.  This operation resets the reschedule
        counter for a job or list of jobs and allows them to run as new.

        Example::

          >>> gLogger.notice(dirac.reset(12345))
          {'OK': True, 'Value': [12345]}

        :param job: JobID
        :type job: integer or list of integers
        :return: S_OK,S_ERROR

        """
        if isinstance(jobID, str):
            try:
                jobID = int(jobID)
            except Exception as x:
                return self._errorReport(str(x), "Expected integer or convertible integer for existing jobID")
        elif isinstance(jobID, list):
            try:
                jobID = [int(job) for job in jobID]
            except Exception as x:
                return self._errorReport(str(x), "Expected integer or convertible integer for existing jobIDs")

        result = JobManagerClient(useCertificates=False).resetJob(jobID)
        return result

    #############################################################################
    def getJobPilotOutput(self, jobID, directory=""):
        """Retrieve the pilot output for an existing job in the WMS.
        The output will be retrieved in a local directory unless
        otherwise specified.

          >>> gLogger.notice(dirac.getJobPilotOutput(12345))
          {'OK': True, StdOut:'',StdError:''}

        :param job: JobID
        :type job: integer or string
        :param str directory: a directory to download logs to.
        :return: S_OK,S_ERROR
        """
        if not directory:
            directory = self.currentDir

        if not os.path.exists(directory):
            return self._errorReport(f"Directory {directory} does not exist")

        result = WMSAdministratorClient().getJobPilotOutput(jobID)
        if not result["OK"]:
            return result

        outputPath = f"{directory}/pilot_{jobID}"
        if os.path.exists(outputPath):
            self.log.info(f"Remove {outputPath} and retry to continue")
            return S_ERROR(f"Remove {outputPath} and retry to continue")

        if not os.path.exists(outputPath):
            self.log.verbose(f"Creating directory {outputPath}")
            os.mkdir(outputPath)

        outputs = result["Value"]
        if "StdOut" in outputs:
            stdout = f"{outputPath}/std.out"
            with open(stdout, "w") as fopen:
                fopen.write(outputs["StdOut"])
            self.log.verbose(f"Standard output written to {stdout}")
        else:
            self.log.warn("No standard output returned")

        if "StdError" in outputs:
            stderr = f"{outputPath}/std.err"
            with open(stderr, "w") as fopen:
                fopen.write(outputs["StdError"])
            self.log.verbose(f"Standard error written to {stderr}")
        else:
            self.log.warn("No standard error returned")

        self.log.always(f"Outputs retrieved in {outputPath}")
        return result

    #############################################################################
    def getPilotOutput(self, gridReference, directory=""):
        """Retrieve the pilot output  (std.out and std.err) for an existing pilot reference.

          >>> gLogger.notice(dirac.getJobPilotOutput(12345))
          {'OK': True, 'Value': {}}

        :param str gridReference: pilot reference
        :param str directory: a directory to download logs to.
        :return: S_OK,S_ERROR
        """
        if not isinstance(gridReference, str):
            return self._errorReport("Expected string for pilot reference")

        if not directory:
            directory = self.currentDir

        if not os.path.exists(directory):
            return self._errorReport(f"Directory {directory} does not exist")

        result = PilotManagerClient().getPilotOutput(gridReference)
        if not result["OK"]:
            return result

        gridReferenceSmall = gridReference.split("/")[-1]
        if not gridReferenceSmall:
            gridReferenceSmall = "reference"
        outputPath = f"{directory}/pilot_{gridReferenceSmall}"

        if os.path.exists(outputPath):
            self.log.info(f"Remove {outputPath} and retry to continue")
            return S_ERROR(f"Remove {outputPath} and retry to continue")

        if not os.path.exists(outputPath):
            self.log.verbose(f"Creating directory {outputPath}")
            os.mkdir(outputPath)

        outputs = result["Value"]
        if "StdOut" in outputs:
            stdout = f"{outputPath}/std.out"
            with open(stdout, "w") as fopen:
                fopen.write(outputs["StdOut"])
            self.log.info(f"Standard output written to {stdout}")
        else:
            self.log.warn("No standard output returned")

        if "StdErr" in outputs:
            stderr = f"{outputPath}/std.err"
            with open(stderr, "w") as fopen:
                fopen.write(outputs["StdErr"])
            self.log.info(f"Standard error written to {stderr}")
        else:
            self.log.warn("No standard error returned")

        self.log.always(f"Outputs retrieved in {outputPath}")
        return result

    #############################################################################
    def getPilotInfo(self, gridReference):
        """Retrieve info relative to a pilot reference

          >>> gLogger.notice(dirac.getPilotInfo(12345))
          {'OK': True, 'Value': {}}

        :param gridReference: Pilot Job Reference
        :type gridReference: string
        :return: S_OK,S_ERROR
        """
        if not isinstance(gridReference, str):
            return self._errorReport("Expected string for pilot reference")

        result = PilotManagerClient().getPilotInfo(gridReference)
        return result

    #############################################################################
    def killPilot(self, gridReference):
        """Kill the pilot specified

          >>> gLogger.notice(dirac.getPilotInfo(12345))
          {'OK': True, 'Value': {}}

        :param gridReference: Pilot Job Reference
        :return: S_OK,S_ERROR
        """
        if not isinstance(gridReference, (str, list)):
            return self._errorReport("Expected string or list of strings for pilot reference")

        # Make a list if it is not yet
        if isinstance(gridReference, str):
            gridReference = [gridReference]

        # Regroup pilots per site
        pilotRefDict = {}
        for pilotReference in gridReference:
            result = PilotManagerClient().getPilotInfo(pilotReference)
            if not result["OK"] or not result["Value"]:
                return S_ERROR(f"Failed to get info for pilot {pilotReference}")

            pilotDict = result["Value"][pilotReference]
            queue = "@@@".join(
                [pilotDict["VO"], pilotDict["GridSite"], pilotDict["DestinationSite"], pilotDict["Queue"]]
            )
            gridType = pilotDict["GridType"]
            pilotRefDict.setdefault(queue, {})
            pilotRefDict[queue].setdefault("PilotList", [])
            pilotRefDict[queue]["PilotList"].append(pilotReference)
            pilotRefDict[queue]["GridType"] = gridType

        return killPilotsInQueues(pilotRefDict)

    #############################################################################
    def getPilotLoggingInfo(self, gridReference):
        """Retrieve the pilot logging info for an existing job in the WMS.

          >>> gLogger.notice(dirac.getPilotLoggingInfo(12345))
          {'OK': True, 'Value': {"The output of the command"}}

        :param gridReference: Gridp pilot job reference Id
        :type gridReference: string
        :return: S_OK,S_ERROR
        """
        if not isinstance(gridReference, str):
            return self._errorReport("Expected string for pilot reference")

        return PilotManagerClient().getPilotLoggingInfo(gridReference)

    #############################################################################
    def getJobPilots(self, jobID):
        """Extract the list of submitted pilots and their status for a given
        jobID from the WMS.  Useful information is printed to the screen.

          >>> gLogger.notice(dirac.getJobPilots())
          {'OK': True, 'Value': {PilotID:{StatusDict}}}

        :param job: JobID
        :type job: integer or string
        :return: S_OK,S_ERROR

        """
        if isinstance(jobID, str):
            try:
                jobID = int(jobID)
            except ValueError as x:
                return self._errorReport(str(x), "Expected integer or string for existing jobID")

        result = PilotManagerClient().getPilots(jobID)
        if result["OK"]:
            gLogger.notice(self.pPrint.pformat(result["Value"]))
        return result

    #############################################################################
    def getPilotSummary(self, startDate="", endDate=""):
        """Retrieve the pilot output for an existing job in the WMS.  Summary is
        printed at INFO level, full dictionary of results also returned.

          >>> gLogger.notice(dirac.getPilotSummary())
          {'OK': True, 'Value': {CE:{Status:Count}}}

        :param job: JobID
        :type job: integer or string
        :return: S_OK,S_ERROR
        """
        result = PilotManagerClient().getPilotSummary(startDate, endDate)
        if not result["OK"]:
            return result

        ceDict = result["Value"]
        headers = "CE".ljust(28)
        i = 0
        for ce, summary in ceDict.items():
            states = list(summary)
            if len(states) > i:
                i = len(states)

        for i in range(i):
            headers += "Status".ljust(12) + "Count".ljust(12)
        gLogger.notice(headers)

        for ce, summary in ceDict.items():
            line = ce.ljust(28)
            states = sorted(summary)
            for state in states:
                count = str(summary[state])
                line += state.ljust(12) + count.ljust(12)
            gLogger.notice(line)

        return result

    #############################################################################
    def setSiteProtocols(self, site, protocolsList, printOutput=False):
        """
        Allows to set the defined protocols for each SE for a given site.
        """
        result = self._checkSiteIsValid(site)
        if not result["OK"]:
            return result

        siteSection = f"/Resources/Sites/{site.split('.')[0]}/{site}/SE"
        siteSEs = gConfig.getValue(siteSection, [])
        if not siteSEs:
            return S_ERROR(f"No SEs found for site {site} in section {siteSection}")

        modifiedCS = False
        result = promptUser(
            "Do you want to add the following default protocols:"
            " %s for SE(s):\n%s" % (", ".join(protocolsList), ", ".join(siteSEs))
        )
        if not result["OK"]:
            return result
        if result["Value"].lower() != "y":
            self.log.always("No protocols will be added")
            return S_OK()

        for se in siteSEs:
            sections = gConfig.getSections(f"/Resources/StorageElements/{se}/")
            if not sections["OK"]:
                return sections
            for section in sections["Value"]:
                if gConfig.getValue(f"/Resources/StorageElements/{se}/{section}/ProtocolName", "") == "SRM2":
                    path = f"/Resources/StorageElements/{se}/{section}/ProtocolsList"
                    self.log.verbose(f"Setting {path} to {', '.join(protocolsList)}")
                    result = self.csSetOption(path, ", ".join(protocolsList))
                    if not result["OK"]:
                        return result
                    modifiedCS = True

        if modifiedCS:
            result = self.csCommitChanges(False)
            if not result["OK"]:
                return S_ERROR(f"CS Commit failed with message = {result['Message']}")
            else:
                if printOutput:
                    gLogger.notice("Successfully committed changes to CS")
        else:
            if printOutput:
                gLogger.notice("No modifications to CS required")

        return S_OK()

    #############################################################################
    def csSetOption(self, optionPath, optionValue):
        """
        Function to modify an existing value in the CS.
        """
        return self.csAPI.setOption(optionPath, optionValue)

    #############################################################################
    def csSetOptionComment(self, optionPath, comment):
        """
        Function to modify an existing value in the CS.
        """
        return self.csAPI.setOptionComment(optionPath, comment)

    #############################################################################
    def csModifyValue(self, optionPath, newValue):
        """
        Function to modify an existing value in the CS.
        """
        return self.csAPI.modifyValue(optionPath, newValue)

    #############################################################################
    def csRegisterUser(self, username, properties):
        """
        Registers a user in the CS.

            - username: Username of the user (easy;)
            - properties: Dict containing:
                - DN
                - groups : list/tuple of groups the user belongs to
                - <others> : More properties of the user, like mail

        """
        return self.csAPI.addUser(username, properties)

    #############################################################################
    def csDeleteUser(self, user):
        """
        Deletes a user from the CS. Can take a list of users
        """
        return self.csAPI.deleteUsers(user)

    #############################################################################
    def csModifyUser(self, username, properties, createIfNonExistant=False):
        """
        Modify a user in the CS. Takes the same params as in addUser and
        applies the changes
        """
        return self.csAPI.modifyUser(username, properties, createIfNonExistant)

    #############################################################################
    def csListUsers(self, group=False):
        """
        Lists the users in the CS. If no group is specified return all users.
        """
        return self.csAPI.listUsers(group)

    #############################################################################
    def csDescribeUsers(self, mask=False):
        """
        List users and their properties in the CS.
        If a mask is given, only users in the mask will be returned
        """
        return self.csAPI.describeUsers(mask)

    #############################################################################
    def csModifyGroup(self, groupname, properties, createIfNonExistant=False):
        """
        Modify a user in the CS. Takes the same params as in addGroup and applies
        the changes
        """
        return self.csAPI.modifyGroup(groupname, properties, createIfNonExistant)

    #############################################################################
    def csListHosts(self):
        """
        Lists the hosts in the CS
        """
        return self.csAPI.listHosts()

    #############################################################################
    def csDescribeHosts(self, mask=False):
        """
        Gets extended info for the hosts in the CS
        """
        return self.csAPI.describeHosts(mask)

    #############################################################################
    def csModifyHost(self, hostname, properties, createIfNonExistant=False):
        """
        Modify a host in the CS. Takes the same params as in addHost and applies
        the changes
        """
        return self.csAPI.modifyHost(hostname, properties, createIfNonExistant)

    #############################################################################
    def csListGroups(self):
        """
        Lists groups in the CS
        """
        return self.csAPI.listGroups()

    #############################################################################
    def csDescribeGroups(self, mask=False):
        """
        List groups and their properties in the CS.
        If a mask is given, only groups in the mask will be returned
        """
        return self.csAPI.describeGroups(mask)

    #############################################################################
    def csSyncUsersWithCFG(self, usersCFG):
        """
        Synchronize users in cfg with its contents
        """
        return self.csAPI.syncUsersWithCFG(usersCFG)

    #############################################################################
    def csCommitChanges(self, sortUsers=True):
        """
        Commit the changes in the CS
        """
        return self.csAPI.commitChanges(sortUsers=False)

    #############################################################################
    def sendMail(self, address, subject, body, fromAddress=None, localAttempt=True, html=False):
        """
        Send mail to specified address with body.
        """
        return NotificationClient().sendMail(address, subject, body, fromAddress, localAttempt, html)


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
