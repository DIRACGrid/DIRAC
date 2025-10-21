"""
The WebAppHandler module provides a class to handle web requests from the DIRAC WebApp.
It is not indented to be used in diracx
"""
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client import TransformationFilesStatus
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_GET_INFO, JobPolicy

TASKS_STATE_NAMES = ["TotalCreated", "Created"] + sorted(
    set(JobStatus.JOB_STATES) | set(Request.ALL_STATES) | set(Operation.ALL_STATES)
)
FILES_STATE_NAMES = ["PercentProcessed", "Total"] + TransformationFilesStatus.TRANSFORMATION_FILES_STATES


class WebAppHandler(RequestHandler):
    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialization of DB objects"""

        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotAgentsDB", "PilotAgentsDB")
            if not result["OK"]:
                return result
            try:
                cls.pilotAgentsDB = result["Value"](parentLogger=cls.log)
            except RuntimeError:
                cls.log.warn("Could not connect to PilotAgentsDB")

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
            if not result["OK"]:
                return result
            try:
                cls.jobDB = result["Value"](parentLogger=cls.log)
            except RuntimeError:
                cls.log.warn("Could not connect to JobDB")

            result = ObjectLoader().loadObject("TransformationSystem.DB.TransformationDB", "TransformationDB")
            if not result["OK"]:
                return result
            try:
                cls.transformationDB = result["Value"](parentLogger=cls.log)
            except RuntimeError:
                cls.log.warn("Could not connect to TransformationDB")

        except RuntimeError as excp:
            cls.log.exception()
            return S_ERROR(f"Can't connect to DB: {excp}")

        return S_OK()

    ##############################################################################
    # PilotAgents
    ##############################################################################

    types_getPilotMonitorWeb = [dict, list, int, int]

    @classmethod
    def export_getPilotMonitorWeb(cls, selectDict, sortList, startItem, maxItems):
        """Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
        """

        return cls.pilotAgentsDB.getPilotMonitorWeb(selectDict, sortList, startItem, maxItems)

    types_getPilotMonitorSelectors = []

    @classmethod
    def export_getPilotMonitorSelectors(cls):
        """Get all the distinct selector values for the Pilot Monitor web portal page"""

        return cls.pilotAgentsDB.getPilotMonitorSelectors()

    types_getPilotSummaryWeb = [dict, list, int, int]

    @classmethod
    def export_getPilotSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
        """Get the summary of the pilot information for a given page in the
        pilot monitor in a generic format
        """

        return cls.pilotAgentsDB.getPilotSummaryWeb(selectDict, sortList, startItem, maxItems)

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

        result = cls.pilotAgentsDB.getCounters(
            "PilotAgents", [attribute], selectDict, newer=startDate, older=endDate, timeStamp="LastUpdateTime"
        )
        statistics = {}
        if result["OK"]:
            for status, count in result["Value"]:
                statistics[status[attribute]] = count

        return S_OK(statistics)

    types_getPilotsCounters = [str, list, dict]

    # This was PilotManagerHandler.getCounters
    @classmethod
    def export_getPilotsCounters(cls, table, keys, condDict, newer=None, timeStamp="SubmissionTime"):
        """Set the pilot agent status"""

        return cls.pilotAgentsDB.getCounters(table, keys, condDict, newer=newer, timeStamp=timeStamp)

    ##############################################################################
    # Jobs
    ##############################################################################

    types_getJobPageSummaryWeb = [dict, list, int, int]

    def export_getJobPageSummaryWeb(self, selectDict, sortList, startItem, maxItems, selectJobs=True):
        """Get the summary of the job information for a given page in the
        job monitor in a generic format
        """

        resultDict = {}

        startDate, endDate, selectDict = self.parseSelectors(selectDict)

        # initialize jobPolicy
        credDict = self.getRemoteCredentials()
        owner = credDict["username"]
        ownerGroup = credDict["group"]
        operations = Operations(group=ownerGroup)
        globalJobsInfo = operations.getValue("/Services/JobMonitoring/GlobalJobsInfo", True)
        jobPolicy = JobPolicy(owner, ownerGroup, globalJobsInfo)
        jobPolicy.jobDB = self.jobDB
        result = jobPolicy.getControlledUsers(RIGHT_GET_INFO)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"User and group combination has no job rights ({owner!r}, {ownerGroup!r})")
        if result["Value"] != "ALL":
            selectDict[("Owner", "OwnerGroup")] = result["Value"]

        # Sorting instructions. Only one for the moment.
        if sortList:
            orderAttribute = sortList[0][0] + ":" + sortList[0][1]
        else:
            orderAttribute = None

        result = self.jobDB.getCounters(
            "Jobs", ["Status"], selectDict, newer=startDate, older=endDate, timeStamp="LastUpdateTime"
        )
        if not result["OK"]:
            return result

        statusDict = {}
        nJobs = 0
        for stDict, count in result["Value"]:
            nJobs += count
            statusDict[stDict["Status"]] = count

        resultDict["TotalRecords"] = nJobs
        if nJobs == 0:
            return S_OK(resultDict)

        resultDict["Extras"] = statusDict

        if selectJobs:
            iniJob = startItem
            if iniJob >= nJobs:
                return S_ERROR("Item number out of range")

            result = self.jobDB.selectJobs(
                selectDict, orderAttribute=orderAttribute, newer=startDate, older=endDate, limit=(maxItems, iniJob)
            )
            if not result["OK"]:
                return result

            summaryJobList = result["Value"]
            if not globalJobsInfo:
                validJobs, _invalidJobs, _nonauthJobs, _ownJobs = jobPolicy.evaluateJobRights(
                    summaryJobList, RIGHT_GET_INFO
                )
                summaryJobList = validJobs

            result = self.jobDB.getJobsAttributes(summaryJobList)
            if not result["OK"]:
                return result

            summaryDict = result["Value"]
            # If no jobs can be selected after the properties check
            if not summaryDict:
                return S_OK(resultDict)

            # Evaluate last sign of life time
            for jobDict in summaryDict.values():
                if not jobDict.get("HeartBeatTime") or jobDict["HeartBeatTime"] == "None":
                    jobDict["LastSignOfLife"] = jobDict["LastUpdateTime"]
                else:
                    jobDict["LastSignOfLife"] = jobDict["HeartBeatTime"]

            # prepare the standard structure now
            # This should be faster than making a list of values()
            for jobDict in summaryDict.values():
                paramNames = list(jobDict)
                break
            records = [list(jobDict.values()) for jobDict in summaryDict.values()]

            resultDict["ParameterNames"] = paramNames
            resultDict["Records"] = records

        return S_OK(resultDict)

    types_getJobStats = [str, dict]

    @classmethod
    def export_getJobStats(cls, attribute, selectDict):
        """Get job statistics distribution per attribute value with a given selection"""
        startDate, endDate, selectDict = cls.parseSelectors(selectDict)
        result = cls.jobDB.getCounters(
            "Jobs", [attribute], selectDict, newer=startDate, older=endDate, timeStamp="LastUpdateTime"
        )
        if not result["OK"]:
            return result
        resultDict = {}
        for cDict, count in result["Value"]:
            resultDict[cDict[attribute]] = count

        return S_OK(resultDict)

    @classmethod
    def parseSelectors(cls, selectDict=None):
        """Parse selectors before DB query

        :param dict selectDict: selectors

        :return: str, str, dict -- start/end date, selectors
        """
        selectDict = selectDict or {}

        # Get time period
        startDate = selectDict.get("FromDate", None)
        if startDate:
            del selectDict["FromDate"]
        # For backward compatibility
        if startDate is None:
            startDate = selectDict.get("LastUpdate", None)
            if startDate:
                del selectDict["LastUpdate"]
        endDate = selectDict.get("ToDate", None)
        if endDate:
            del selectDict["ToDate"]

        # Provide JobID bound to a specific PilotJobReference
        # There is no reason to have both PilotJobReference and JobID in selectDict
        # If that occurs, use the JobID instead of the PilotJobReference
        pilotJobRefs = selectDict.get("PilotJobReference")
        if pilotJobRefs:
            del selectDict["PilotJobReference"]
            if not selectDict.get("JobID"):
                for pilotJobRef in [pilotJobRefs] if isinstance(pilotJobRefs, str) else pilotJobRefs:
                    res = cls.pilotAgentsDB.getPilotInfo(pilotJobRef)
                    if res["OK"] and "Jobs" in res["Value"][pilotJobRef]:
                        selectDict["JobID"] = selectDict.get("JobID", [])
                        selectDict["JobID"].extend(res["Value"][pilotJobRef]["Jobs"])

        return startDate, endDate, selectDict

    types_getJobsCounters = [list]

    # This was JobManagerHanlder.getCounters
    @classmethod
    def export_getJobsCounters(cls, attrList, attrDict=None, cutDate=""):
        """
        Retrieve list of distinct attributes values from attrList
        with attrDict as condition.
        For each set of distinct values, count number of occurences.
        Return a list. Each item is a list with 2 items, the list of distinct
        attribute values and the counter
        """

        _, _, attrDict = cls.parseSelectors(attrDict)
        return cls.jobDB.getCounters("Jobs", attrList, attrDict, newer=str(cutDate), timeStamp="LastUpdateTime")

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

    types_getApplicationStates = []

    @classmethod
    def export_getApplicationStates(cls, condDict=None, older=None, newer=None):
        """Return Distinct Values of ApplicationStatus job Attribute in WMS"""
        return cls.jobDB.getDistinctJobAttributes("ApplicationStatus", condDict, older, newer)

    types_getJobTypes = []

    @classmethod
    def export_getJobTypes(cls, condDict=None, older=None, newer=None):
        """Return Distinct Values of JobType job Attribute in WMS"""
        return cls.jobDB.getDistinctJobAttributes("JobType", condDict, older, newer)

    types_getOwners = []

    @classmethod
    def export_getOwners(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Owner job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("Owner", condDict, older, newer)

    types_getOwnerGroup = []

    @classmethod
    def export_getOwnerGroup(cls):
        """
        Return Distinct Values of OwnerGroup from the JobDB
        """
        return cls.jobDB.getDistinctJobAttributes("OwnerGroup")

    types_getJobGroups = []

    @classmethod
    def export_getJobGroups(cls, condDict=None, older=None, cutDate=None):
        """
        Return Distinct Values of ProductionId job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("JobGroup", condDict, older, newer=cutDate)

    types_getSites = []

    @classmethod
    def export_getSites(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Site job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("Site", condDict, older, newer)

    types_getStates = []

    @classmethod
    def export_getStates(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Status job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("Status", condDict, older, newer)

    types_getMinorStates = []

    @classmethod
    def export_getMinorStates(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Minor Status job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("MinorStatus", condDict, older, newer)

    ##############################################################################
    # Transformations
    ##############################################################################

    types_getDistinctAttributeValues = [str, dict]

    @classmethod
    def export_getDistinctAttributeValues(cls, attribute, selectDict):
        res = cls.transformationDB.getTableDistinctAttributeValues("Transformations", [attribute], selectDict)
        if not res["OK"]:
            return res
        return S_OK(res["Value"][attribute])

    types_getTransformationFilesSummaryWeb = [dict, list, int, int]

    @classmethod
    def export_getTransformationFilesSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
        fromDate = selectDict.get("FromDate", None)
        if fromDate:
            del selectDict["FromDate"]
        toDate = selectDict.get("ToDate", None)
        if toDate:
            del selectDict["ToDate"]
        # Sorting instructions. Only one for the moment.
        if sortList:
            orderAttribute = sortList[0][0] + ":" + sortList[0][1]
        else:
            orderAttribute = None
        # Get the columns that match the selection
        res = cls.transformationDB.getTransformationFiles(
            condDict=selectDict, older=toDate, newer=fromDate, timeStamp="LastUpdate", orderAttribute=orderAttribute
        )
        if not res["OK"]:
            return res
        allRows = res["Value"]

        # Prepare the standard structure now within the resultDict dictionary
        resultDict = {}
        resultDict["TotalRecords"] = len(allRows)

        if not allRows:
            return S_OK(resultDict)

        # Get the rows which are within the selected window
        ini = startItem
        last = ini + maxItems
        if ini >= resultDict["TotalRecords"]:
            return S_ERROR("Item number out of range")
        if last > resultDict["TotalRecords"]:
            last = resultDict["TotalRecords"]

        selectedRows = allRows[ini:last]
        resultDict["Records"] = []
        for row in selectedRows:
            resultDict["Records"].append(list(row.values()))

        # Create the ParameterNames entry
        resultDict["ParameterNames"] = list(selectedRows[0].keys())

        # Generate the status dictionary
        statusDict = {}
        for row in selectedRows:
            status = row["Status"]
            statusDict[status] = statusDict.setdefault(status, 0) + 1
        resultDict["Extras"] = statusDict

        # Obtain the distinct values of the selection parameters
        res = cls.transformationDB.getTableDistinctAttributeValues(
            "TransformationFiles",
            ["TransformationID", "Status", "UsedSE", "TargetSE"],
            selectDict,
            older=toDate,
            newer=fromDate,
        )
        if not res["OK"]:
            return res
        resultDict["Selections"] = res["Value"]

        return S_OK(resultDict)

    types_getTransformationSummaryWeb = [dict, list, int, int]

    @classmethod
    def export_getTransformationSummaryWeb(cls, selectDict, sortList, startItem, maxItems):
        """Get the summary of the transformation information for a given page in the generic format"""

        # Obtain the timing information from the selectDict
        last_update = selectDict.get("CreationDate", None)
        if last_update:
            del selectDict["CreationDate"]
        fromDate = selectDict.get("FromDate", None)
        if fromDate:
            del selectDict["FromDate"]
        if not fromDate:
            fromDate = last_update
        toDate = selectDict.get("ToDate", None)
        if toDate:
            del selectDict["ToDate"]
        # Sorting instructions. Only one for the moment.
        if sortList:
            orderAttribute = []
            for i in sortList:
                orderAttribute += [i[0] + ":" + i[1]]
        else:
            orderAttribute = None

        # Get the transformations that match the selection
        res = cls.transformationDB.getTransformations(
            condDict=selectDict, older=toDate, newer=fromDate, orderAttribute=orderAttribute
        )
        if not res["OK"]:
            return res

        ops = Operations()
        # Prepare the standard structure now within the resultDict dictionary
        resultDict = {}
        trList = res["Records"]
        # Create the total records entry
        nTrans = len(trList)
        resultDict["TotalRecords"] = nTrans
        # Create the ParameterNames entry
        # As this list is a reference to the list in the DB, we cannot extend it, therefore copy it
        resultDict["ParameterNames"] = list(res["ParameterNames"])
        # Add the job states to the ParameterNames entry
        taskStateNames = TASKS_STATE_NAMES + ops.getValue("Transformations/AdditionalTaskStates", [])
        resultDict["ParameterNames"] += ["Jobs_" + x for x in taskStateNames]
        # Add the file states to the ParameterNames entry
        fileStateNames = FILES_STATE_NAMES + ops.getValue("Transformations/AdditionalFileStates", [])
        resultDict["ParameterNames"] += ["Files_" + x for x in fileStateNames]

        # Get the transformations which are within the selected window
        if nTrans == 0:
            return S_OK(resultDict)
        ini = startItem
        last = ini + maxItems
        if ini >= nTrans:
            return S_ERROR("Item number out of range")
        if last > nTrans:
            last = nTrans
        transList = trList[ini:last]

        statusDict = {}
        extendableTranfs = ops.getValue("Transformations/ExtendableTransfTypes", ["Simulation", "MCsimulation"])
        givenUpFileStatus = ops.getValue("Transformations/GivenUpFileStatus", ["MissingInFC"])
        problematicStatuses = ops.getValue("Transformations/ProblematicStatuses", ["Problematic"])
        # Add specific information for each selected transformation
        for trans in transList:
            transDict = dict(zip(resultDict["ParameterNames"], trans))

            # Update the status counters
            status = transDict["Status"]
            statusDict[status] = statusDict.setdefault(status, 0) + 1

            # Get the statistics on the number of jobs for the transformation
            transID = transDict["TransformationID"]
            res = cls.transformationDB.getTransformationTaskStats(transID)
            taskDict = {}
            if res["OK"] and res["Value"]:
                taskDict = res["Value"]
            for state in taskStateNames:
                trans.append(taskDict.get(state, 0))

            # Get the statistics for the number of files for the transformation
            fileDict = {}
            transType = transDict["Type"]
            if transType.lower() in extendableTranfs:
                fileDict["PercentProcessed"] = "-"
            else:
                res = cls.transformationDB.getTransformationStats(transID)
                if res["OK"]:
                    fileDict = res["Value"]
                    total = fileDict["Total"]
                    for stat in givenUpFileStatus:
                        total -= fileDict.get(stat, 0)
                    processed = fileDict.get(TransformationFilesStatus.PROCESSED, 0)
                    fileDict["PercentProcessed"] = f"{int(processed * 1000.0 / total) / 10.0:.1f}" if total else 0.0
            problematic = 0
            for stat in problematicStatuses:
                problematic += fileDict.get(stat, 0)
            fileDict["Problematic"] = problematic
            for state in fileStateNames:
                trans.append(fileDict.get(state, 0))

        resultDict["Records"] = transList
        resultDict["Extras"] = statusDict
        return S_OK(resultDict)
