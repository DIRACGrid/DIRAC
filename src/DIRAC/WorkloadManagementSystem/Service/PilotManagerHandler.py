"""
This is the interface to DIRAC PilotAgentsDB.
"""

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class PilotManagerHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB objects"""

        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotAgentsDB", "PilotAgentsDB")
            if not result["OK"]:
                return result
            cls.pilotAgentsDB = result["Value"](parentLogger=cls.log)

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        return S_OK()

    ##########################################################################################
    types_addPilotReferences = [list, str]

    @classmethod
    def export_addPilotReferences(cls, pilotRef, VO, gridType="DIRAC", pilotStampDict={}):
        """Add a new pilot job reference"""
        return cls.pilotAgentsDB.addPilotReferences(pilotRef, VO, gridType, pilotStampDict)

    ##############################################################################

    types_getPilotInfo = [[list, str]]

    @classmethod
    def export_getPilotInfo(cls, pilotReference):
        """Get the info about a given pilot job reference"""
        return cls.pilotAgentsDB.getPilotInfo(pilotReference)

    ##############################################################################
    types_selectPilots = [dict]

    @classmethod
    def export_selectPilots(cls, condDict):
        """Select pilots given the selection conditions"""
        return cls.pilotAgentsDB.selectPilots(condDict)

    ##############################################################################
    types_getPilotSummary = []

    @classmethod
    def export_getPilotSummary(cls, startdate="", enddate=""):
        """Get summary of the status of the LCG Pilot Jobs"""

        return cls.pilotAgentsDB.getPilotSummary(startdate, enddate)

    ##############################################################################
    types_getPilots = [[str, int]]

    @classmethod
    def export_getPilots(cls, jobID):
        """Get pilots executing/having executed the Job"""
        result = cls.pilotAgentsDB.getPilotsForJobID(int(jobID))
        if not result["OK"] or not result["Value"]:
            return S_ERROR(f"Failed to get pilot for Job {int(jobID)}: {result.get('Message', '')}")

        return cls.pilotAgentsDB.getPilotInfo(pilotID=result["Value"])

    ##############################################################################
    types_setPilotStatus = [str, str]

    @classmethod
    def export_setPilotStatus(cls, pilotRef, status, destination=None, reason=None, gridSite=None, queue=None):
        """Set the pilot agent status"""

        return cls.pilotAgentsDB.setPilotStatus(
            pilotRef, status, destination=destination, statusReason=reason, gridSite=gridSite, queue=queue
        )
