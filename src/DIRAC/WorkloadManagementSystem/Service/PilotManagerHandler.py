"""
This is the interface to DIRAC PilotAgentsDB.
"""

import datetime

import DIRAC.Core.Utilities.TimeUtilities as TimeUtilities
from DIRAC import S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


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

    ##############################################################################
    types_getCurrentPilotCounters = [dict]

    @classmethod
    def export_getCurrentPilotCounters(cls, attrDict={}):
        """Get pilot counters per Status with attrDict selection. Final statuses are given for
        the last day.
        """

        result = cls.pilotAgentsDB.getCounters("PilotAgents", ["Status"], attrDict, timeStamp="LastUpdateTime")
        if not result["OK"]:
            return result
        last_update = datetime.datetime.utcnow() - TimeUtilities.day
        resultDay = cls.pilotAgentsDB.getCounters(
            "PilotAgents", ["Status"], attrDict, newer=last_update, timeStamp="LastUpdateTime"
        )
        if not resultDay["OK"]:
            return resultDay

        resultDict = {}
        for statusDict, count in result["Value"]:
            status = statusDict["Status"]
            resultDict[status] = count
            if status in PilotStatus.PILOT_FINAL_STATES:
                resultDict[status] = 0
                for statusDayDict, ccount in resultDay["Value"]:
                    if status == statusDayDict["Status"]:
                        resultDict[status] = ccount
                    break

        return S_OK(resultDict)

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
    types_storePilotOutput = [str, str, str]

    @classmethod
    def export_storePilotOutput(cls, pilotReference, output, error):
        """Store the pilot output and error"""
        return cls.pilotAgentsDB.storePilotOutput(pilotReference, output, error)

    ##############################################################################
    types_getPilotSummary = []

    @classmethod
    def export_getPilotSummary(cls, startdate="", enddate=""):
        """Get summary of the status of the LCG Pilot Jobs"""

        return cls.pilotAgentsDB.getPilotSummary(startdate, enddate)

    ##############################################################################
    types_getGroupedPilotSummary = [list]

    @classmethod
    def export_getGroupedPilotSummary(cls, columnList):
        """
        Get pilot summary showing grouped by columns in columnList, all pilot states
        and pilot efficiencies in a single row.

        :param columnList: a list of columns to GROUP BY (less status column)
        :return: a dictionary containing column names and data records
        """
        return cls.pilotAgentsDB.getGroupedPilotSummary(columnList)

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
    types_setJobForPilot = [[str, int], str]

    @classmethod
    def export_setJobForPilot(cls, jobID, pilotRef, destination=None):
        """Report the DIRAC job ID which is executed by the given pilot job"""

        result = cls.pilotAgentsDB.setJobForPilot(int(jobID), pilotRef)
        if not result["OK"]:
            return result
        result = cls.pilotAgentsDB.setCurrentJobID(pilotRef, int(jobID))
        if not result["OK"]:
            return result
        if destination:
            result = cls.pilotAgentsDB.setPilotDestinationSite(pilotRef, destination)

        return result

    ##########################################################################################
    types_setPilotBenchmark = [str, float]

    @classmethod
    def export_setPilotBenchmark(cls, pilotRef, mark):
        """Set the pilot agent benchmark"""
        return cls.pilotAgentsDB.setPilotBenchmark(pilotRef, mark)

    ##########################################################################################
    types_setAccountingFlag = [str]

    @classmethod
    def export_setAccountingFlag(cls, pilotRef, mark="True"):
        """Set the pilot AccountingSent flag"""
        return cls.pilotAgentsDB.setAccountingFlag(pilotRef, mark)

    ##########################################################################################
    types_setPilotStatus = [str, str]

    @classmethod
    def export_setPilotStatus(cls, pilotRef, status, destination=None, reason=None, gridSite=None, queue=None):
        """Set the pilot agent status"""

        return cls.pilotAgentsDB.setPilotStatus(
            pilotRef, status, destination=destination, statusReason=reason, gridSite=gridSite, queue=queue
        )

    ##########################################################################################
    types_countPilots = [dict]

    @classmethod
    def export_countPilots(cls, condDict, older=None, newer=None, timeStamp="SubmissionTime"):
        """Set the pilot agent status"""

        return cls.pilotAgentsDB.countPilots(condDict, older, newer, timeStamp)

    ##########################################################################################
    types_deletePilots = [[list, str, int]]

    @classmethod
    def export_deletePilots(cls, pilotIDs):
        if isinstance(pilotIDs, str):
            return cls.pilotAgentsDB.deletePilot(pilotIDs)

        if isinstance(pilotIDs, int):
            pilotIDs = [
                pilotIDs,
            ]

        result = cls.pilotAgentsDB.deletePilots(pilotIDs)
        if not result["OK"]:
            return result

        return S_OK()

    ##############################################################################
    types_clearPilots = [int, int]

    @classmethod
    def export_clearPilots(cls, interval=30, aborted_interval=7):
        return cls.pilotAgentsDB.clearPilots(interval, aborted_interval)
