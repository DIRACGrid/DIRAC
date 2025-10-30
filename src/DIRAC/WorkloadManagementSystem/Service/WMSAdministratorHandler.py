"""
This is a DIRAC WMS administrator interface.
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Decorators import deprecated
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

    @deprecated("no-op RPC")
    def export_setSiteMask(self, siteList):
        """Set the site mask for matching. The mask is given in a form of Classad string.

        :param list siteList: site, status
        :return: S_OK()/S_ERROR()
        """
        return S_OK()

    ##############################################################################
    types_getSiteMask = []

    @classmethod
    @deprecated("no-op RPC")
    def export_getSiteMask(cls, siteState="Active"):
        """Get the site mask

        :param str siteState: site status
        :return: S_OK(list)/S_ERROR()
        """
        return S_OK()

    types_getSiteMaskStatus = []

    @classmethod
    @deprecated("no-op RPC")
    def export_getSiteMaskStatus(cls, sites=None):
        """Get the site mask of given site(s) with columns 'site' and 'status' only

        :param sites: list of sites or site
        :type sites: list or str
        :return: S_OK()/S_ERROR() -- S_OK contain dict or str
        """
        return S_OK()

    ##############################################################################
    types_getAllSiteMaskStatus = []

    @classmethod
    @deprecated("no-op RPC")
    def export_getAllSiteMaskStatus(cls):
        """Get all the site parameters in the site mask

        :return: dict
        """
        return S_OK()

    ##############################################################################
    types_banSite = [str]

    @deprecated("no-op RPC")
    def export_banSite(self, site, comment="No comment"):
        """Ban the given site in the site mask

        :param str site: site
        :param str comment: comment
        :return: S_OK()/S_ERROR()
        """
        return S_OK()

    ##############################################################################
    types_allowSite = [str]

    @deprecated("no-op RPC")
    def export_allowSite(self, site, comment="No comment"):
        """Allow the given site in the site mask

        :param str site: site
        :param str comment: comment
        :return: S_OK()/S_ERROR()
        """
        return S_OK()

    ##############################################################################
    types_clearMask = []

    @classmethod
    @deprecated("no-op RPC")
    def export_clearMask(cls):
        """Clear up the entire site mask

        :return: S_OK()/S_ERROR()
        """
        return S_OK()

    ##############################################################################
    types_getSiteMaskLogging = [[str, list]]

    @classmethod
    @deprecated("no-op RPC")
    def export_getSiteMaskLogging(cls, sites):
        """Get the site mask logging history

        :param list sites: sites
        :return: S_OK(dict)/S_ERROR()
        """
        return S_OK()

    ##############################################################################
    types_getSiteMaskSummary = []

    @classmethod
    @deprecated("no-op RPC")
    def export_getSiteMaskSummary(cls):
        """Get the mask status for all the configured sites

        :return: S_OK(dict)/S_ERROR()
        """
        return S_OK()

    ##############################################################################
    types_getJobPilotOutput = [[str, int]]

    def export_getJobPilotOutput(self, jobID):
        """Get the pilot job standard output and standard error files for the DIRAC
        job reference

        :param str jobID: job ID
        :return: S_OK(dict)/S_ERROR()
        """
        result = self.pilotManager.getPilots(jobID)

        if not result["OK"]:
            return result
        pilotJobReferences = result["Value"].keys()

        outputs = {"StdOut": "", "StdErr": ""}
        for pilotRef in pilotJobReferences:
            result = self.pilotManager.getPilotOutput(pilotRef)
            if not result["OK"]:
                stdout = f"Could not retrieve output: {result['Message']}"
                error = f"Could not retrieve error: {result['Message']}"
            else:
                stdout, error = result["Value"]["StdOut"], result["Value"]["StdErr"]
            outputs["StdOut"] += f"# PilotJobReference: {pilotRef}\n\n{stdout}\n"
            outputs["StdErr"] += f"# PilotJobReference: {pilotRef}\n\n{error}\n"

        return S_OK(outputs)


class WMSAdministratorHandler(WMSAdministratorHandlerMixin, RequestHandler):
    pass
