"""
This is a DIRAC WMS administrator interface.
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient


class WMSAdministratorHandlerMixin:
    @classmethod
    def initializeHandler(cls, svcInfoDict):
        """WMS AdministratorService initialization"""
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
            if not result["OK"]:
                return result
            cls.jobDB = result["Value"](parentLogger=cls.log)
        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp!r}")

        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobParametersDB", "JobParametersDB")
        if not result["OK"]:
            return result
        cls.elasticJobParametersDB = result["Value"]()

        cls.pilotManager = PilotManagerClient()

        return S_OK()

    ##############################################################################
    types_getJobPilotOutput = [[str, int]]

    def export_getJobPilotOutput(self, jobID):
        """Get the pilot job standard output and standard error files for the DIRAC
        job reference

        :param str jobID: job ID
        :return: S_OK(dict)/S_ERROR()
        """
        pilotReference = ""
        # Get the pilot grid reference first from the job parameters

        credDict = self.getRemoteCredentials()
        vo = credDict.get("VO", Registry.getVOForGroup(credDict["group"]))
        res = self.elasticJobParametersDB.getJobParameters(int(jobID), vo=vo, parNameList=["Pilot_Reference"])
        if not res["OK"]:
            return res
        if res["Value"].get(int(jobID)):
            pilotReference = res["Value"][int(jobID)]["Pilot_Reference"]

        if not pilotReference:
            res = self.jobDB.getJobParameter(int(jobID), "Pilot_Reference")
            if not res["OK"]:
                return res
            pilotReference = res["Value"]

        if not pilotReference:
            # Failed to get the pilot reference, try to look in the attic parameters
            res = self.jobDB.getAtticJobParameters(int(jobID), ["Pilot_Reference"])
            if res["OK"]:
                c = -1
                # Get the pilot reference for the last rescheduling cycle
                for cycle in res["Value"]:
                    if cycle > c:
                        pilotReference = res["Value"][cycle]["Pilot_Reference"]
                        c = cycle

        if pilotReference:
            return self.pilotManager.getPilotOutput(pilotReference)
        return S_ERROR("No pilot job reference found")

    ##############################################################################
    types_getSiteSummaryWeb = [dict, list, int, int]

    @classmethod
    def export_getSiteSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
        """Get the summary of the jobs running on sites in a generic format

        :param dict selectDict: selectors
        :param list sortList: sorting list
        :param int startItem: start item number
        :param int maxItems: maximum of items

        :return: S_OK(dict)/S_ERROR()
        """
        return cls.jobDB.getSiteSummaryWeb(selectDict, sortList, startItem, maxItems)

    ##############################################################################
    types_getSiteSummarySelectors = []

    @classmethod
    def export_getSiteSummarySelectors(cls):
        """Get all the distinct selector values for the site summary web portal page

        :return: S_OK(dict)/S_ERROR()
        """
        resultDict = {}
        statusList = ["Good", "Fair", "Poor", "Bad", "Idle"]
        resultDict["Status"] = statusList
        maskStatus = ["Active", "Banned", "NoMask", "Reduced"]
        resultDict["MaskStatus"] = maskStatus

        res = getSites()
        if not res["OK"]:
            return res
        siteList = res["Value"]

        countryList = []
        for site in siteList:
            if site.find(".") != -1:
                country = site.split(".")[2].lower()
                if country not in countryList:
                    countryList.append(country)
        countryList.sort()
        resultDict["Country"] = countryList
        siteList.sort()
        resultDict["Site"] = siteList

        return S_OK(resultDict)


class WMSAdministratorHandler(WMSAdministratorHandlerMixin, RequestHandler):
    pass
