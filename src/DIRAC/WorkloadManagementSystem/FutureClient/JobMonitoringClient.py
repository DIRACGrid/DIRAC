from collections import defaultdict
from datetime import datetime

from DIRAC import gConfig
from DIRAC.Core.Security.DiracX import DiracXClient
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


DATETIME_PARAMETERS = [
    "EndExecTime",
    "HeartBeatTime",
    "LastUpdateTime",
    "StartExecTime",
    "SubmissionTime",
    "RescheduleTime",
]

SUMMARY_WAITING_STATUSES = {"Submitted", "Assigned", "Waiting", "Matched"}
SUMMARY_STATUSES = {"Waiting", "Running", "Stalled", "Done", "Failed"}


class JobMonitoringClient:
    def _fetch_summary(self, grouping, search=None):
        with DiracXClient() as api:
            return api.jobs.summary(grouping=grouping, search=search)

    def _fetch_search(self, parameters, jobIDs):
        if not isinstance(jobIDs, list):
            jobIDs = [jobIDs]

        with DiracXClient() as api:
            jobs = api.jobs.search(
                parameters=(["JobID"] + parameters) if parameters else None,
                search=[{"parameter": "JobID", "operator": "in", "values": jobIDs}],
            )
            for j in jobs:
                for param in j:
                    if isinstance(j[param], bool):
                        j[param] = str(j[param])
                    elif param in DATETIME_PARAMETERS and j[param] is not None:
                        j[param] = datetime.strptime(j[param], "%Y-%m-%dT%H:%M:%S")
            if parameters is None:
                return {j["JobID"]: j for j in jobs}
            else:
                return {j["JobID"]: {param: j[param] for param in parameters} for j in jobs}

    def _fetch_search_scalar(self, jobID, key):
        result = self._fetch_search([key], jobID)
        return result.get(jobID, {key: None})[key]

    def _dict_to_search(self, condDict, older, newer):
        search = [
            {"parameter": k, "operator": "in", "values": v}
            if isinstance(v, list)
            else {"parameter": k, "operator": "eq", "value": v}
            for k, v in (condDict or {}).items()
        ]
        if older:
            search += [{"parameter": "LastUpdateTime", "operator": "lt", "value": older}]
        if newer:
            search += [
                # TODO: gte
                {"parameter": "LastUpdateTime", "operator": "gt", "value": newer}
                # {"parameter": "LastUpdateTime", "operator": "gte", "value": older}
            ]
        return search

    def _fetch_distinct_values(self, key, condDict, older, newer):
        search = self._dict_to_search(condDict, older, newer)
        # TODO: We should add the option to avoid the counting
        result = self._fetch_summary([key], search=search)
        # Apply the expected sort order
        result = sorted([x[key] for x in result])
        if "Unknown" in result:
            result.remove("Unknown")
            result.append("Unknown")
        return result

    @convertToReturnValue
    def getApplicationStates(self, condDict=None, older=None, newer=None):
        return self._fetch_distinct_values("ApplicationStatus", condDict, older, newer)

    @convertToReturnValue
    def getJobTypes(self, condDict=None, older=None, newer=None):
        return self._fetch_distinct_values("JobType", condDict, older, newer)

    @convertToReturnValue
    def getOwners(self, condDict=None, older=None, newer=None):
        return self._fetch_distinct_values("Owner", condDict, older, newer)

    # return self.getDistinctAttributeValues(
    #     "Jobs", "Owner", condDict=condDict, older=older, newer=newer, timeStamp="LastUpdateTime"
    # )
    # def getDistinctAttributeValues(
    #     self,
    #     table,
    #     attribute,
    #     condDict=None,
    #     older=None,
    #     newer=None,
    #     timeStamp=None,
    #     connection=False,
    #     greater=None,
    #     smaller=None,
    # ):
    #     """
    #     Get distinct values of a table attribute under specified conditions
    #     """
    #     try:
    #         cond = self.buildCondition(
    #             condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, greater=greater, smaller=smaller
    #         )
    #     except Exception as exc:
    #         return S_ERROR(DErrno.EMYSQL, exc)

    #     cmd = f"SELECT DISTINCT( {attributeName} ) FROM {table} {cond} ORDER BY {attributeName}"
    #     res = self._query(cmd, conn=connection)
    #     if not res["OK"]:
    #         return res
    #     attr_list = [x[0] for x in res["Value"]]
    #     return S_OK(attr_list)

    @convertToReturnValue
    def getOwnerGroup(self):
        result = self._fetch_summary(["OwnerGroup"])
        return [x["OwnerGroup"] for x in result]

    @convertToReturnValue
    def getJobGroups(self, condDict=None, older=None, cutDate=None):
        return self._fetch_distinct_values("JobGroup", condDict, older, cutDate)

    @convertToReturnValue
    def getSites(self, condDict=None, older=None, newer=None):
        return self._fetch_distinct_values("Site", condDict, older, newer)

    @convertToReturnValue
    def getStates(self, condDict=None, older=None, newer=None):
        return self._fetch_distinct_values("Status", condDict, older, newer)

    @convertToReturnValue
    def getMinorStates(self, condDict=None, older=None, newer=None):
        return self._fetch_distinct_values("MinorStatus", condDict, older, newer)

    # @convertToReturnValue
    # def getJobs(self, attrDict=None, cutDate=None):

    @convertToReturnValue
    def getCounters(self, attrList, attrDict=None, cutDate=""):
        if not attrList:
            raise ValueError("Missing mandatory attrList")
        if cutDate is None:
            raise ValueError('cutDate must be "" or a valid date')
        search = self._dict_to_search(attrDict, None, cutDate)
        # TODO: We should add the option to avoid the counting
        result = self._fetch_summary(attrList, search=search)
        result = [[k, k.pop("count")] for k in result]
        # Apply the expected sort order
        return sorted(result, key=lambda x: tuple(x[0].values()))

    # _, _, attrDict = cls.parseSelectors(attrDict)
    #     return cls.jobDB.getCounters("Jobs", attrList, attrDict, newer=str(cutDate), timeStamp="LastUpdateTime")\
    # cmd = f"SELECT {attrNames}, COUNT(*) FROM {table} {cond} GROUP BY {attrNames} ORDER BY {attrNames}"

    @convertToReturnValue
    def getJobOwner(self, jobID):
        return self._fetch_search_scalar(jobID, "Owner")

    @convertToReturnValue
    def getJobSite(self, jobID):
        return self._fetch_search_scalar(jobID, "Site")

    @convertToReturnValue
    def getJobJDL(self, jobID, original):
        return self._fetch_search_scalar(jobID, "OriginalJDL" if original else "JDL")

    # @convertToReturnValue
    # def getJobLoggingInfo(self, jobID):

    # @convertToReturnValue
    # def getJobsParameters(self, jobIDs, parameters):

    @convertToReturnValue
    def getJobsStates(self, jobIDs):
        return self._fetch_search(["Status", "MinorStatus", "ApplicationStatus"], jobIDs)

    @convertToReturnValue
    def getJobsStatus(self, jobIDs):
        return self._fetch_search(["Status"], jobIDs)

    @convertToReturnValue
    def getJobsMinorStatus(self, jobIDs):
        return self._fetch_search(["MinorStatus"], jobIDs)

    @convertToReturnValue
    def getJobsApplicationStatus(self, jobIDs):
        return self._fetch_search(["ApplicationStatus"], jobIDs)

    @convertToReturnValue
    def getJobsSites(self, jobIDs):
        return self._fetch_search(["Site"], jobIDs)

    # @convertToReturnValue
    # def getJobSummary(self, jobID):

    @convertToReturnValue
    def getJobsSummary(self, jobIDs):
        return {str(k): v for k, v in self._fetch_search(None, jobIDs).items()}

    # @convertToReturnValue
    # def getJobPageSummaryWeb(self, selectDict, sortList, startItem, maxItems, selectJobs=True):

    # @convertToReturnValue
    # def getJobStats(self, attribute, selectDict):

    # @convertToReturnValue
    # def getJobParameter(self, jobID, parName):

    # @convertToReturnValue
    # def getJobOptParameters(self, jobID):

    # @convertToReturnValue
    # def getJobParameters(self, jobIDs, parName=None):

    # @convertToReturnValue
    # def getAtticJobParameters(self, jobID, parameters=None, rescheduleCycle=-1):

    @convertToReturnValue
    def getJobAttributes(self, jobID, attrList=None):
        return self._fetch_search(attrList, [jobID]).get(jobID, {})

    @convertToReturnValue
    def getJobAttribute(self, jobID, attribute):
        return self._fetch_search_scalar(jobID, attribute)

    @convertToReturnValue
    def getSiteSummary(self):
        summary = self._fetch_summary(["Status", "Site"])

        result = defaultdict(lambda: {k: 0 for k in SUMMARY_STATUSES})
        for s in summary:
            if s["Site"] == "ANY":
                continue
            if s["Status"] in SUMMARY_WAITING_STATUSES:
                result[s["Site"]]["Waiting"] += s["count"]
            elif s["Status"] in SUMMARY_STATUSES:
                result[s["Site"]][s["Status"]] += s["count"]
            else:
                # Make sure that sites are included even if they have no jobs in a reported status
                result[s["Site"]]

        for status in result["Total"]:
            result["Total"][status] = sum(result[site][status] for site in result if site != "Total")

        return dict(result)

    # @convertToReturnValue
    # def getJobHeartBeatData(self, jobID):

    # @convertToReturnValue
    # def getInputData(self, jobID):
