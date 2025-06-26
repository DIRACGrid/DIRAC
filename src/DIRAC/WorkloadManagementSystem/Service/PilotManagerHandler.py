"""
This is the interface to DIRAC PilotAgentsDB.
"""
import datetime
import shutil

import DIRAC.Core.Utilities.TimeUtilities as TimeUtilities
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import (
    getPilotCE,
    getPilotRef,
    setPilotCredentials,
)


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

        # prepare remote pilot plugin initialization
        defaultOption, defaultClass = "DownloadPlugin", "FileCacheDownloadPlugin"
        cls.configValue = getServiceOption(serviceInfoDict, defaultOption, defaultClass)
        cls.loggingPlugin = None
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
    types_getPilotOutput = [str]

    def export_getPilotOutput(self, pilotReference):
        """
        Get the pilot job standard output and standard error files for a pilot reference.
        Handles both classic, CE-based logs and remote logs. The type of logs returned is determined
        by the server.

        :param str pilotReference: pilot reference (e.g. htcondorce://ce13.pic.es/19126986.5)
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        result = self.pilotAgentsDB.getPilotInfo(pilotReference)
        if not result["OK"]:
            self.log.error("Failed to get info for pilot", f"{pilotReference}: {result['Message']}")
            return S_ERROR("Failed to get info for pilot")
        if not result["Value"]:
            self.log.warn("The pilot info is empty for", pilotReference)
            return S_ERROR("Pilot info is empty")

        pilotDict = result["Value"][pilotReference]
        opsHelper = Operations(vo=pilotDict["VO"])
        remote = opsHelper.getValue("Pilot/RemoteLogsPriority", False)
        # classic logs first, by default
        funcs = [self._getPilotOutput, self._getRemotePilotOutput]
        if remote:
            self.log.info("Trying to retrieve output of pilot", f"{pilotReference} remotely first")
            funcs.reverse()

        result = funcs[0](pilotReference, pilotDict)
        if not result["OK"]:
            self.log.warn(
                "Failed getting output for pilot", f"{pilotReference}. Will try another approach: {result['Message']}"
            )
            result = funcs[1](pilotReference, pilotDict)
            return result
        else:
            return result

    def _getPilotOutput(self, pilotReference, pilotDict):
        """Get the pilot job standard output and standard error files for the Grid
        job reference
        """

        # FIXME: What if the OutputSandBox is not StdOut and StdErr, what do we do with other files?
        result = self.pilotAgentsDB.getPilotOutput(pilotReference)
        if result["OK"]:
            stdout = result["Value"]["StdOut"]
            error = result["Value"]["StdErr"]
            if stdout or error:
                resultDict = {}
                resultDict["StdOut"] = stdout
                resultDict["StdErr"] = error
                resultDict["VO"] = pilotDict["VO"]
                resultDict["FileList"] = []
                return S_OK(resultDict)
            else:
                self.log.warn("Empty pilot output found", f"for {pilotReference}")

        result = getPilotCE(pilotDict)
        if not result["OK"]:
            return result

        ce = result["Value"]
        if not hasattr(ce, "getJobOutput"):
            return S_ERROR(f"Pilot output not available for {pilotDict['GridType']} CEs")

        # Set proxy or token for the CE
        result = setPilotCredentials(ce, pilotDict)
        if not result["OK"]:
            return result

        result = getPilotRef(pilotReference, pilotDict)
        if not result["OK"]:
            return result
        pRef = result["Value"]

        result = ce.getJobOutput(pRef)
        if not result["OK"]:
            shutil.rmtree(ce.ceParameters["WorkingDirectory"])
            return result
        stdout, error = result["Value"]
        if stdout:
            result = self.pilotAgentsDB.storePilotOutput(pilotReference, stdout, error)
            if not result["OK"]:
                self.log.error("Failed to store pilot output:", result["Message"])

        resultDict = {}
        resultDict["StdOut"] = stdout
        resultDict["StdErr"] = error
        resultDict["VO"] = pilotDict["VO"]
        resultDict["FileList"] = []
        shutil.rmtree(ce.ceParameters["WorkingDirectory"])
        return S_OK(resultDict)

    def _getRemotePilotOutput(self, pilotReference, pilotDict):
        """
        Get remote pilot log files.

        :param str pilotReference:
        :return: S_OK Dirac object
        :rtype: dict
        """

        pilotStamp = pilotDict["PilotStamp"]

        if self.loggingPlugin is None:
            result = ObjectLoader().loadObject(
                f"WorkloadManagementSystem.Client.PilotLoggingPlugins.{self.configValue}", self.configValue
            )
            if not result["OK"]:
                self.log.error("Failed to load LoggingPlugin", f"{self.configValue}: {result['Message']}")
                return result

            componentClass = result["Value"]
            self.loggingPlugin = componentClass()
            self.log.info("Loaded: PilotLoggingPlugin class", self.configValue)

        res = self.loggingPlugin.getRemotePilotLogs(pilotStamp, pilotDict["VO"])

        if res["OK"]:
            res["Value"]["VO"] = pilotDict["VO"]
            res["Value"]["FileList"] = []
        # return res, correct or not
        return res

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
    types_getPilotLoggingInfo = [str]

    def export_getPilotLoggingInfo(self, pilotReference):
        """Get the pilot logging info for the Grid job reference"""
        result = self.pilotAgentsDB.getPilotInfo(pilotReference)
        if not result["OK"]:
            self.log.error("Failed to get info for pilot", result["Message"])
            result
        if not result["Value"]:
            self.log.warn("The pilot info is empty", pilotReference)
            return S_ERROR("Pilot info is empty")

        pilotDict = result["Value"][pilotReference]
        if not (result := getPilotCE(pilotDict))["OK"]:
            return result

        ce = result["Value"]
        if not hasattr(ce, "getJobLog"):
            self.log.info("Pilot logging not available for", f"{pilotDict['GridType']} CEs")
            return S_ERROR(f"Pilot logging not available for {pilotDict['GridType']} CEs")

        # Set proxy or token for the CE
        if not (result := setPilotCredentials(ce, pilotDict))["OK"]:
            return result

        if not (result := getPilotRef(pilotReference, pilotDict))["OK"]:
            return result
        pRef = result["Value"]

        result = ce.getJobLog(pRef)
        if not result["OK"]:
            shutil.rmtree(ce.ceParameters["WorkingDirectory"])
            return result
        loggingInfo = result["Value"]
        shutil.rmtree(ce.ceParameters["WorkingDirectory"])

        return S_OK(loggingInfo)

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
