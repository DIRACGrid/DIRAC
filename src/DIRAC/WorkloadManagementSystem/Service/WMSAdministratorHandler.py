"""
This is a DIRAC WMS administrator interface.
"""

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.MonitoringSystem.Client.WebAppClient import WebAppClient
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
        cls.web_app_manager = WebAppClient()

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
            result = self.web_app_manager.getPilotOutput(pilotRef)
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
