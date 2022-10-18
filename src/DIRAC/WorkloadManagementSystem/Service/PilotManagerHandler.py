"""
This is the interface to DIRAC PilotAgentsDB.
"""
import shutil
import datetime

from DIRAC import S_OK, S_ERROR
import DIRAC.Core.Utilities.TimeUtilities as TimeUtilities

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, getDNForUsername
from DIRAC.WorkloadManagementSystem.Client import PilotStatus
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import (
    getPilotCE,
    getPilotProxy,
    getPilotRef,
    killPilotsInQueues,
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
            return S_ERROR("Can't connect to DB: %s" % excp)

        cls.pilotsLoggingDB = None
        enablePilotsLogging = Operations().getValue("/Services/JobMonitoring/usePilotsLoggingFlag", False)
        if enablePilotsLogging:
            try:
                result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotsLoggingDB", "PilotsLoggingDB")
                if not result["OK"]:
                    return result
                cls.pilotsLoggingDB = result["Value"](parentLogger=cls.log)
            except RuntimeError as excp:
                return S_ERROR("Can't connect to DB: %s" % excp)

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
    types_addPilotTQReference = [list, int, str, str]

    @classmethod
    def export_addPilotTQReference(
        cls, pilotRef, taskQueueID, ownerDN, ownerGroup, broker="Unknown", gridType="DIRAC", pilotStampDict={}
    ):
        """Add a new pilot job reference"""
        return cls.pilotAgentsDB.addPilotTQReference(
            pilotRef, taskQueueID, ownerDN, ownerGroup, broker, gridType, pilotStampDict
        )

    ##############################################################################
    types_getPilotOutput = [str]

    def export_getPilotOutput(self, pilotReference):
        """Get the pilot job standard output and standard error files for the Grid
        job reference
        """
        result = self.pilotAgentsDB.getPilotInfo(pilotReference)
        if not result["OK"]:
            self.log.error("Failed to get info for pilot", result["Message"])
            return S_ERROR("Failed to get info for pilot")
        if not result["Value"]:
            self.log.warn("The pilot info is empty", pilotReference)
            return S_ERROR("Pilot info is empty")

        pilotDict = result["Value"][pilotReference]

        owner = pilotDict["OwnerDN"]
        group = pilotDict["OwnerGroup"]

        # FIXME: What if the OutputSandBox is not StdOut and StdErr, what do we do with other files?
        result = self.pilotAgentsDB.getPilotOutput(pilotReference)
        if result["OK"]:
            stdout = result["Value"]["StdOut"]
            error = result["Value"]["StdErr"]
            if stdout or error:
                resultDict = {}
                resultDict["StdOut"] = stdout
                resultDict["StdErr"] = error
                resultDict["OwnerDN"] = owner
                resultDict["OwnerGroup"] = group
                resultDict["FileList"] = []
                return S_OK(resultDict)
            else:
                self.log.warn("Empty pilot output found", "for %s" % pilotReference)

        result = getPilotCE(pilotDict)
        if not result["OK"]:
            return result

        ce = result["Value"]
        if not hasattr(ce, "getJobOutput"):
            return S_ERROR("Pilot output not available for %s CEs" % pilotDict["GridType"])

        result = getPilotProxy(pilotDict)
        if not result["OK"]:
            return result

        proxy = result["Value"]
        ce.setProxy(proxy)

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
        resultDict["OwnerDN"] = owner
        resultDict["OwnerGroup"] = group
        resultDict["FileList"] = []
        shutil.rmtree(ce.ceParameters["WorkingDirectory"])
        return S_OK(resultDict)

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
            return S_ERROR("Failed to get info for pilot")
        if not result["Value"]:
            self.log.warn("The pilot info is empty", pilotReference)
            return S_ERROR("Pilot info is empty")

        pilotDict = result["Value"][pilotReference]
        result = getPilotCE(pilotDict)
        if not result["OK"]:
            return result

        ce = result["Value"]
        if not hasattr(ce, "getJobLog"):
            self.log.info("Pilot logging not available for", "%s CEs" % pilotDict["GridType"])
            return S_ERROR("Pilot logging not available for %s CEs" % pilotDict["GridType"])

        result = getPilotProxy(pilotDict)
        if not result["OK"]:
            return result

        proxy = result["Value"]
        ce.setProxy(proxy)

        result = getPilotRef(pilotReference, pilotDict)
        if not result["OK"]:
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
    types_getPilotMonitorWeb = [dict, list, int, int]

    @classmethod
    def export_getPilotMonitorWeb(cls, selectDict, sortList, startItem, maxItems):
        """Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
        """

        return cls.pilotAgentsDB.getPilotMonitorWeb(selectDict, sortList, startItem, maxItems)

    ##############################################################################
    types_getPilotMonitorSelectors = []

    @classmethod
    def export_getPilotMonitorSelectors(cls):
        """Get all the distinct selector values for the Pilot Monitor web portal page"""

        return cls.pilotAgentsDB.getPilotMonitorSelectors()

    ##############################################################################
    types_getPilotSummaryWeb = [dict, list, int, int]

    @classmethod
    def export_getPilotSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
        """Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
        """

        return cls.pilotAgentsDB.getPilotSummaryWeb(selectDict, sortList, startItem, maxItems)

    ##############################################################################
    types_getGroupedPilotSummary = [dict, list]

    @classmethod
    def export_getGroupedPilotSummary(cls, selectDict, columnList):
        """
        Get pilot summary showing grouped by columns in columnList, all pilot states
        and pilot efficiencies in a single row.

        :param selectDict: additional arguments to SELECT clause
        :param columnList: a list of columns to GROUP BY (less status column)
        :return: a dictionary containing column names and data records
        """
        return cls.pilotAgentsDB.getGroupedPilotSummary(selectDict, columnList)

    ##############################################################################
    types_getPilots = [[str, int]]

    @classmethod
    def export_getPilots(cls, jobID):
        """Get pilot references and their states for :
        - those pilots submitted for the TQ where job is sitting
        - (or) the pilots executing/having executed the Job
        """

        pilots = []
        result = cls.pilotAgentsDB.getPilotsForJobID(int(jobID))
        if not result["OK"]:
            if result["Message"].find("not found") == -1:
                return S_ERROR("Failed to get pilot: " + result["Message"])
        else:
            pilots += result["Value"]
        if not pilots:
            # Pilots were not found try to look in the Task Queue
            taskQueueID = 0
            try:
                result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.TaskQueueDB", "TaskQueueDB")
                if not result["OK"]:
                    return result
                tqDB = result["Value"]()
            except RuntimeError as excp:
                return S_ERROR("Can't connect to DB: %s" % excp)
            result = tqDB.getTaskQueueForJob(int(jobID))
            if result["OK"] and result["Value"]:
                taskQueueID = result["Value"]
            if taskQueueID:
                result = cls.pilotAgentsDB.getPilotsForTaskQueue(taskQueueID, limit=10)
                if not result["OK"]:
                    return S_ERROR("Failed to get pilot: " + result["Message"])
                pilots += result["Value"]

        if not pilots:
            return S_ERROR("Failed to get pilot for Job %d" % int(jobID))

        return cls.pilotAgentsDB.getPilotInfo(pilotID=pilots)

    ##############################################################################
    types_killPilot = [[str, list]]

    @classmethod
    def export_killPilot(cls, pilotRefList):
        """Kill the specified pilots"""
        # Make a list if it is not yet
        pilotRefs = list(pilotRefList)
        if isinstance(pilotRefList, str):
            pilotRefs = [pilotRefList]

        # Regroup pilots per site and per owner
        pilotRefDict = {}
        for pilotReference in pilotRefs:
            result = cls.pilotAgentsDB.getPilotInfo(pilotReference)
            if not result["OK"] or not result["Value"]:
                return S_ERROR("Failed to get info for pilot " + pilotReference)

            pilotDict = result["Value"][pilotReference]
            owner = pilotDict["OwnerDN"]
            group = pilotDict["OwnerGroup"]
            queue = "@@@".join([owner, group, pilotDict["GridSite"], pilotDict["DestinationSite"], pilotDict["Queue"]])
            gridType = pilotDict["GridType"]
            pilotRefDict.setdefault(queue, {})
            pilotRefDict[queue].setdefault("PilotList", [])
            pilotRefDict[queue]["PilotList"].append(pilotReference)
            pilotRefDict[queue]["GridType"] = gridType

        return killPilotsInQueues(pilotRefDict)

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
    types_getCounters = [str, list, dict]

    @classmethod
    def export_getCounters(cls, table, keys, condDict, newer=None, timeStamp="SubmissionTime"):
        """Set the pilot agent status"""

        return cls.pilotAgentsDB.getCounters(table, keys, condDict, newer=newer, timeStamp=timeStamp)

    ##############################################################################
    types_getPilotStatistics = [str, dict]

    @classmethod
    def export_getPilotStatistics(cls, attribute, selectDict):
        """Get pilot statistics distribution per attribute value with a given selection"""

        startDate = selectDict.get("FromDate", None)
        if startDate:
            del selectDict["FromDate"]

        if startDate is None:
            startDate = selectDict.get("LastUpdate", None)
            if startDate:
                del selectDict["LastUpdate"]
        endDate = selectDict.get("ToDate", None)
        if endDate:
            del selectDict["ToDate"]

        # Owner attribute is not part of PilotAgentsDB
        # It has to be converted into a OwnerDN
        owners = selectDict.get("Owner")
        if owners:
            ownerDNs = []
            for owner in owners:
                result = getDNForUsername(owner)
                if not result["OK"]:
                    return result
                ownerDNs.append(result["Value"])

            selectDict["OwnerDN"] = ownerDNs
            del selectDict["Owner"]

        result = cls.pilotAgentsDB.getCounters(
            "PilotAgents", [attribute], selectDict, newer=startDate, older=endDate, timeStamp="LastUpdateTime"
        )
        statistics = {}
        if result["OK"]:
            for status, count in result["Value"]:
                if "OwnerDN" in status:
                    userName = getUsernameForDN(status["OwnerDN"])
                    if userName["OK"]:
                        status["OwnerDN"] = userName["Value"]
                    statistics[status["OwnerDN"]] = count
                else:
                    statistics[status[attribute]] = count

        return S_OK(statistics)

    ##############################################################################
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
        if cls.pilotsLoggingDB:
            pilotIDs = result["Value"]
            pilots = cls.pilotAgentsDB.getPilotInfo(pilotID=pilotIDs)
            if not pilots["OK"]:
                return pilots
            pilotRefs = []
            for pilot in pilots:
                pilotRefs.append(pilot["PilotJobReference"])
            result = cls.pilotsLoggingDB.deletePilotsLogging(pilotRefs)
            if not result["OK"]:
                return result

        return S_OK()

    ##############################################################################
    types_clearPilots = [int, int]

    @classmethod
    def export_clearPilots(cls, interval=30, aborted_interval=7):

        result = cls.pilotAgentsDB.clearPilots(interval, aborted_interval)
        if not result["OK"]:
            return result
        if cls.pilotsLoggingDB:
            pilotIDs = result["Value"]
            pilots = cls.pilotAgentsDB.getPilotInfo(pilotID=pilotIDs)
            if not pilots["OK"]:
                return pilots
            pilotRefs = []
            for pilot in pilots:
                pilotRefs.append(pilot["PilotJobReference"])
            result = cls.pilotsLoggingDB.deletePilotsLogging(pilotRefs)
            if not result["OK"]:
                return result

        return S_OK()
