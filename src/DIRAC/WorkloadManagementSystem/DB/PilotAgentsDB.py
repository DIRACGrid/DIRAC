""" PilotAgentsDB class is a front-end to the Pilot Agent Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot
    agents.

    Available methods are:

    addPilotReferences()
    setPilotStatus()
    deletePilot()
    clearPilots()
    setPilotDestinationSite()
    storePilotOutput()
    getPilotOutput()
    setJobForPilot()
    getPilotsSummary()
    getGroupedPilotSummary()

"""
import datetime
import decimal
import threading

import DIRAC.Core.Utilities.TimeUtilities as TimeUtilities
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.MySQL import _quotedList
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.FrameworkSystem.Client.Logger import contextLogger
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


class PilotAgentsDB(DB):
    def __init__(self, parentLogger=None):
        super().__init__("PilotAgentsDB", "WorkloadManagement/PilotAgentsDB", parentLogger=parentLogger)
        self._defaultLogger = self.log
        self.lock = threading.Lock()

    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    ##########################################################################################

    def addPilotReferences(self, pilotRef, VO, gridType="DIRAC", pilotStampDict={}):
        """Add a new pilot job reference"""
        for ref in pilotRef:
            stamp = ""
            if ref in pilotStampDict:
                stamp = pilotStampDict[ref]

            req = (
                "INSERT INTO PilotAgents "
                + "(PilotJobReference, VO, GridType, SubmissionTime, LastUpdateTime, Status, PilotStamp) "
                + "VALUES ('%s','%s','%s',UTC_TIMESTAMP(),UTC_TIMESTAMP(),'Submitted','%s')"
                % (ref, VO, gridType, stamp)
            )

            result = self._update(req)
            if not result["OK"]:
                return result

            if "lastRowId" not in result:
                return S_ERROR("PilotAgentsDB.addPilotReferences: Failed to retrieve a new Id.")

        return S_OK()

    ##########################################################################################
    def setPilotStatus(
        self,
        pilotRef,
        status,
        destination=None,
        statusReason=None,
        gridSite=None,
        queue=None,
        benchmark=None,
        currentJob=None,
        updateTime=None,
        conn=False,
    ):
        """Set pilot job status"""

        setList = []
        setList.append(f"Status='{status}'")
        if updateTime:
            setList.append(f"LastUpdateTime='{updateTime}'")
        else:
            setList.append("LastUpdateTime=UTC_TIMESTAMP()")
        if not statusReason:
            statusReason = "Not given"
        setList.append(f"StatusReason='{statusReason}'")
        if gridSite:
            setList.append(f"GridSite='{gridSite}'")
        if queue:
            setList.append(f"Queue='{queue}'")
        if benchmark:
            setList.append(f"BenchMark='{float(benchmark)}'")
        if currentJob:
            setList.append(f"CurrentJobID='{int(currentJob)}'")
        if destination:
            setList.append(f"DestinationSite='{destination}'")
            if not gridSite:
                res = getCESiteMapping(destination)
                if res["OK"] and res["Value"]:
                    setList.append(f"GridSite='{res['Value'][destination]}'")

        set_string = ",".join(setList)
        req = f"UPDATE PilotAgents SET {set_string} WHERE PilotJobReference='{pilotRef}'"
        return self._update(req, conn=conn)

    # ###########################################################################################
    # FIXME: this can't work ATM because of how the DB table is made. Maybe it would be useful later.
    #   def setPilotStatusBulk(self, pilotRefsStatusDict=None, statusReason=None,
    #                          conn=False):
    #     """ Set pilot job status in a bulk
    #     """
    #     if not pilotRefsStatusDict:
    #       return S_OK()

    #     # Building the request with "ON DUPLICATE KEY UPDATE"
    #     reqBase = "INSERT INTO PilotAgents (PilotJobReference, Status, StatusReason) VALUES "

    #     for pilotJobReference, status in pilotRefsStatusDict.items():
    #       req = reqBase + ','.join("('%s', '%s', '%s')" % (pilotJobReference, status, statusReason))
    #       req += " ON DUPLICATE KEY UPDATE Status=VALUES(Status),StatusReason=VALUES(StatusReason)"

    #     return self._update(req, conn=conn)

    ##########################################################################################
    def selectPilots(
        self, condDict, older=None, newer=None, timeStamp="SubmissionTime", orderAttribute=None, limit=None
    ):
        """Select pilot references according to the provided criteria. "newer" and "older"
        specify the time interval in minutes
        """

        condition = self.buildCondition(condDict, older, newer, timeStamp)
        if orderAttribute:
            orderType = None
            orderField = orderAttribute
            if orderAttribute.find(":") != -1:
                orderType = orderAttribute.split(":")[1].upper()
                orderField = orderAttribute.split(":")[0]
            condition = condition + " ORDER BY " + orderField
            if orderType:
                condition = condition + " " + orderType

        if limit:
            condition = condition + " LIMIT " + str(limit)

        req = "SELECT PilotJobReference from PilotAgents"
        if condition:
            req += f" {condition} "
        result = self._query(req)
        if not result["OK"]:
            return result

        pilotList = []
        if result["Value"]:
            pilotList = [x[0] for x in result["Value"]]

        return S_OK(pilotList)

    ##########################################################################################
    def countPilots(self, condDict, older=None, newer=None, timeStamp="SubmissionTime"):
        """Select pilot references according to the provided criteria. "newer" and "older"
        specify the time interval in minutes
        """

        condition = self.buildCondition(condDict, older, newer, timeStamp)

        req = "SELECT COUNT(PilotID) from PilotAgents"
        if condition:
            req += f" {condition} "
        result = self._query(req)
        if not result["OK"]:
            return result

        return S_OK(result["Value"][0][0])

    ##########################################################################################
    def deletePilots(self, pilotIDs, conn=False):
        """Delete Pilots with IDs in the given list from the PilotAgentsDB"""

        if not isinstance(pilotIDs, list):
            return S_ERROR("Input argument is not a List")

        failed = []

        result = self._escapeValues(pilotIDs)
        if not result["OK"]:
            return S_ERROR(f"Failed to remove pilot: {result['Value']}")
        stringIDs = ",".join(result["Value"])
        for table in ["PilotOutput", "JobToPilotMapping", "PilotAgents"]:
            result = self._update(f"DELETE FROM {table} WHERE PilotID in ({stringIDs})", conn=conn)
            if not result["OK"]:
                failed.append(table)

        if failed:
            return S_ERROR(f"Failed to remove pilot from {', '.join(failed)} tables")
        return S_OK(pilotIDs)

    ##########################################################################################
    def deletePilot(self, pilotRef, conn=False):
        """Delete Pilot with the given reference from the PilotAgentsDB"""

        if isinstance(pilotRef, str):
            pilotID = self.__getPilotID(pilotRef)
        else:
            pilotID = pilotRef

        return self.deletePilots([pilotID], conn=conn)

    ##########################################################################################
    def clearPilots(self, interval=30, aborted_interval=7):
        """Delete all the pilot references submitted before <interval> days"""

        reqList = []
        reqList.append(
            "SELECT PilotID FROM PilotAgents WHERE SubmissionTime < DATE_SUB(UTC_TIMESTAMP(),INTERVAL %d DAY)"
            % interval
        )
        reqList.append(
            "SELECT PilotID FROM PilotAgents WHERE Status='Aborted' \
AND SubmissionTime < DATE_SUB(UTC_TIMESTAMP(),INTERVAL %d DAY)"
            % aborted_interval
        )

        idList = None

        for req in reqList:
            result = self._query(req)
            if not result["OK"]:
                self.log.warn("Error while clearing up pilots")
            else:
                if result["Value"]:
                    idList = [x[0] for x in result["Value"]]
                    result = self.deletePilots(idList)
                    if not result["OK"]:
                        self.log.warn("Error while deleting pilots")

        return S_OK(idList)

    ##########################################################################################
    def getPilotInfo(self, pilotRef=False, conn=False, paramNames=[], pilotID=False):
        """Get all the information for the pilot job reference or reference list"""

        parameters = (
            [
                "PilotJobReference",
                "VO",
                "GridType",
                "Status",
                "DestinationSite",
                "BenchMark",
                "AccountingSent",
                "SubmissionTime",
                "PilotID",
                "LastUpdateTime",
                "GridSite",
                "PilotStamp",
                "Queue",
            ]
            if not paramNames
            else paramNames
        )

        cmd = f"SELECT {', '.join(parameters)} FROM PilotAgents"
        condSQL = []
        for key, value in [
            ("PilotJobReference", pilotRef),
            ("PilotID", pilotID),
        ]:
            resList = []
            for v in value if isinstance(value, list) else [value] if value else []:
                result = self._escapeString(v)
                if not result["OK"]:
                    return result
                resList.append(result["Value"])
            if resList:
                condSQL.append(f"{key} IN ({','.join(resList)})")
        if condSQL:
            cmd = f"{cmd} WHERE {' AND '.join(condSQL)}"

        result = self._query(cmd, conn=conn)
        if not result["OK"]:
            return result
        if not result["Value"]:
            msg = "No pilots found"
            if pilotRef:
                msg += f" for PilotJobReference(s): {pilotRef}"
            return S_ERROR(DErrno.EWMSNOPILOT, msg)

        resDict = {}
        pilotIDs = []
        for row in result["Value"]:
            pilotDict = {}
            for i, par in enumerate(parameters):
                pilotDict[par] = row[i]
                if par == "PilotID":
                    pilotIDs.append(row[i])
            resDict[row[0]] = pilotDict

        result = self.getJobsForPilot(pilotIDs)
        if not result["OK"]:
            return S_OK(resDict)

        jobsDict = result["Value"]
        for pilotRef, pilotInfo in resDict.items():
            pilotID = pilotInfo["PilotID"]
            if pilotID in jobsDict:
                pilotInfo["Jobs"] = jobsDict[pilotID]

        return S_OK(resDict)

    ##########################################################################################
    def setPilotDestinationSite(self, pilotRef, destination, conn=False):
        """Set the pilot agent destination site"""

        gridSite = "Unknown"
        res = getCESiteMapping(destination)
        if res["OK"] and res["Value"]:
            gridSite = res["Value"][destination]

        req = "UPDATE PilotAgents SET DestinationSite='%s', GridSite='%s' WHERE PilotJobReference='%s'"
        req = req % (destination, gridSite, pilotRef)
        return self._update(req, conn=conn)

    ##########################################################################################
    def setPilotBenchmark(self, pilotRef, mark):
        """Set the pilot agent benchmark"""

        req = f"UPDATE PilotAgents SET BenchMark='{mark:f}' WHERE PilotJobReference='{pilotRef}'"
        return self._update(req)

    ##########################################################################################
    def setAccountingFlag(self, pilotRef, mark="True"):
        """Set the pilot AccountingSent flag"""

        req = f"UPDATE PilotAgents SET AccountingSent='{mark}' WHERE PilotJobReference='{pilotRef}'"
        return self._update(req)

    ##########################################################################################
    def storePilotOutput(self, pilotRef, output, error):
        """Store standard output and error for a pilot with pilotRef"""
        pilotID = self.__getPilotID(pilotRef)
        if not pilotID:
            return S_ERROR(f"Pilot reference not found {pilotRef}")

        result = self._escapeString(output)
        if not result["OK"]:
            return result
        e_output = result["Value"]
        result = self._escapeString(error)
        if not result["OK"]:
            return result
        e_error = result["Value"]
        req = f"INSERT INTO PilotOutput (PilotID,StdOutput,StdError) VALUES ({pilotID}, {e_output}, {e_error})"
        return self._update(req)

    ##########################################################################################
    def getPilotOutput(self, pilotRef):
        """Retrieve standard output and error for pilot with pilotRef"""

        req = "SELECT StdOutput, StdError FROM PilotOutput,PilotAgents WHERE "
        req += f"PilotOutput.PilotID = PilotAgents.PilotID AND PilotAgents.PilotJobReference='{pilotRef}'"
        result = self._query(req)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"PilotJobReference {pilotRef} not found")
        try:
            stdout = result["Value"][0][0].decode()  # account for the use of BLOBs
            error = result["Value"][0][1].decode()
        except AttributeError:
            stdout = result["Value"][0][0]
            error = result["Value"][0][1]
        if stdout == '""':
            stdout = ""
        if error == '""':
            error = ""
        return S_OK({"StdOut": stdout, "StdErr": error})

    ##########################################################################################
    def __getPilotID(self, pilotRef):
        """Get Pilot ID for the given pilot reference or a list of references"""

        if isinstance(pilotRef, (str, int)):
            req = f"SELECT PilotID from PilotAgents WHERE PilotJobReference='{pilotRef}'"
            result = self._query(req)
            if not result["OK"]:
                return 0
            if result["Value"]:
                return int(result["Value"][0][0])
            return 0

        sqlCmd = "CREATE TEMPORARY TABLE to_select_PilotAgents (PilotID VARCHAR(255) NOT NULL, PRIMARY KEY (PilotID)) ENGINE=MEMORY;"
        returnValueOrRaise(self._update(sqlCmd))
        try:
            sqlCmd = "INSERT INTO to_select_PilotAgents (PilotID) VALUES ( %s )"
            returnValueOrRaise(self._updatemany(sqlCmd, [(p,) for p in pilotRef]))
            sqlCmd = "SELECT PilotID FROM PilotAgents JOIN to_select_PilotAgents USING (PilotID)"
            result = self._query(sqlCmd)
        finally:
            sqlCmd = "DROP TEMPORARY TABLE to_select_PilotAgents"
            returnValueOrRaise(self._update(sqlCmd))

        if not result["OK"]:
            return []
        if result["Value"]:
            return [x[0] for x in result["Value"]]
        return []

    ##########################################################################################
    def setJobForPilot(self, jobID, pilotRef, site=None, updateStatus=True):
        """Store the jobID of the job executed by the pilot with reference pilotRef"""

        pilotID = self.__getPilotID(pilotRef)
        if pilotID:
            if updateStatus:
                reason = f"Report from job {jobID}"
                result = self.setPilotStatus(pilotRef, status=PilotStatus.RUNNING, statusReason=reason, gridSite=site)
                if not result["OK"]:
                    return result
            req = f"INSERT INTO JobToPilotMapping (PilotID,JobID,StartTime) VALUES ({int(pilotID)}, {int(jobID)}, UTC_TIMESTAMP())"
            return self._update(req)
        return S_ERROR(f"PilotJobReference {pilotRef} not found")

    ##########################################################################################
    def setCurrentJobID(self, pilotRef, jobID):
        """Set the pilot agent current DIRAC job ID"""

        req = "UPDATE PilotAgents SET CurrentJobID=%d WHERE PilotJobReference='%s'" % (jobID, pilotRef)
        return self._update(req)

    ##########################################################################################
    def getJobsForPilot(self, pilotID):
        """Get IDs of Jobs that were executed by a pilot"""
        cmd = "SELECT pilotID,JobID FROM JobToPilotMapping "
        if isinstance(pilotID, list):
            pilotIDs_string = ",".join(str(int(x)) for x in pilotID)
            cmd = f"{cmd} WHERE pilotID IN ({pilotIDs_string})"
        else:
            cmd = f"{cmd} WHERE pilotID = {pilotID}"

        result = self._query(cmd)
        if not result["OK"]:
            return result

        resDict = {}
        for row in result["Value"]:
            if row[0] not in resDict:
                resDict[row[0]] = []
            resDict[row[0]].append(row[1])
        return S_OK(resDict)

    ##########################################################################################
    def getPilotsForJobID(self, jobID):
        """Get ID of Pilot Agent that is running a given JobID"""

        result = self._query(f"SELECT PilotID FROM JobToPilotMapping WHERE JobID={jobID}")

        if not result["OK"]:
            self.log.error("getPilotsForJobID failed", result["Message"])
            return result

        if result["Value"]:
            pilotList = [x[0] for x in result["Value"]]
            return S_OK(pilotList)
        self.log.verbose("PilotID for job not found: either not matched yet, or already deleted", f"id={jobID}")
        return S_OK([])

    ##########################################################################################
    def getPilotCurrentJob(self, pilotRef):
        """The job ID currently executed by the pilot"""
        req = f"SELECT CurrentJobID FROM PilotAgents WHERE PilotJobReference='{pilotRef}' "

        result = self._query(req)
        if not result["OK"]:
            return result
        if result["Value"]:
            jobID = int(result["Value"][0][0])
            return S_OK(jobID)
        self.log.warn(f"Current job ID for pilot {pilotRef} is not known: pilot did not match jobs yet?")
        return S_OK()

    ##########################################################################################
    # FIXME: investigate it getPilotSummaryShort can replace this method
    def getPilotSummary(self, startdate="", enddate=""):
        """Get summary of the pilot jobs status by site"""
        summary_dict = {}
        summary_dict["Total"] = {}

        for st in PilotStatus.PILOT_STATES:
            summary_dict["Total"][st] = 0
            req = "SELECT DestinationSite,count(DestinationSite) FROM PilotAgents " + f"WHERE Status='{st}' "
            if startdate:
                req = req + f" AND SubmissionTime >= '{startdate}'"
            if enddate:
                req = req + f" AND SubmissionTime <= '{enddate}'"

            req = req + " GROUP BY DestinationSite"
            result = self._query(req)
            if not result["OK"]:
                return result
            if result["Value"]:
                for res in result["Value"]:
                    site = res[0]
                    count = res[1]
                    if site:
                        if site not in summary_dict:
                            summary_dict[site] = {}
                        summary_dict[site][st] = int(count)
                        summary_dict["Total"][st] += int(count)

        # Get aborted pilots in the last hour, day
        req = "SELECT DestinationSite,count(DestinationSite) FROM PilotAgents WHERE Status='Aborted' AND "
        reqDict = {}
        reqDict["Aborted_Hour"] = req + " LastUpdateTime >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR)"
        reqDict["Aborted_Day"] = req + " LastUpdateTime >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)"

        for key, req in reqDict.items():
            result = self._query(req)
            if not result["OK"]:
                break
            if result["Value"]:
                for res in result["Value"]:
                    site = res[0]
                    count = res[1]
                    if site:
                        if site in summary_dict:
                            summary_dict[site][key] = int(count)

        return S_OK(summary_dict)

    ##########################################################################################
    def getGroupedPilotSummary(self, columnList):
        """
        The simplified pilot summary based on getPilotSummaryWeb method. It calculates pilot efficiency
        based on the same algorithm as in the Web version, basically takes into account Done and
        Aborted pilots only from the last day. The selection is done entirely in SQL.

        :param list columnList: A list of column to consider when grouping to calculate efficiencies.
                           e.g. ['GridSite', 'DestinationSite'] is used to calculate efficiencies
                           for sites and  CEs.
        :return: S_OK/S_ERROR with a dict containing the ParameterNames and Records lists.
        """

        # TODO:
        #  add startItem and maxItems to the argument list
        #  limit output to  finalDict['Records'] = records[startItem:startItem + maxItems]
        table = PivotedPilotSummaryTable(columnList)
        sqlQuery = table.buildSQL()

        self.log.debug("SQL query : ")
        self.log.debug("\n" + sqlQuery)
        res = self._query(sqlQuery)
        if not res["OK"]:
            return res

        rows = []
        columns = table.getColumnList()
        try:
            groupIndex = columns.index("VO")
        except ValueError:
            groupIndex = None
        result = {"ParameterNames": columns}
        multiple = False
        # If not grouped by CE:
        if "CE" not in columns:
            multiple = True

        for row in res["Value"]:
            lrow = list(row)
            if groupIndex:
                lrow[groupIndex] = getVOForGroup(row[groupIndex])
            if multiple:
                lrow.append("Multiple")
            for index, value in enumerate(row):
                if isinstance(value, decimal.Decimal):
                    lrow[index] = float(value)
            # get the value of the Total column
            if "Total" in columnList:
                total = lrow[columnList.index("Total")]
            else:
                total = 0
            if "PilotJobEff" in columnList:
                eff = lrow[columnList.index("PilotJobEff")]
            else:
                eff = 0.0
            lrow.append(self._getElementStatus(total, eff))
            rows.append(list(lrow))
        # If not grouped by CE and more then 1 CE in the result:
        if multiple:
            columns.append("CE")  # 'DestinationSite' re-mapped to 'CE' already
        columns.append("Status")
        result["Records"] = rows
        result["TotalRecords"] = len(rows)
        return S_OK(result)

    def _getElementStatus(self, total, eff):
        """
        Assign status to a site or resource based on pilot efficiency.
        :param total: number of pilots to assign the status, otherwise 'Idle'
        :param eff:  efficiency in %

        :return: status string
        """

        # Evaluate the quality status of the Site/CE
        if total > 10:
            if eff < 25.0:
                return "Bad"
            if eff < 60.0:
                return "Poor"
            if eff < 85.0:
                return "Fair"
            return "Good"
        return "Idle"

    def getPilotSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        """Get summary of the pilot jobs status by CE/site in a standard structure"""
        allStateNames = PilotStatus.PILOT_STATES + ["Done_Empty", "Aborted_Hour"]
        paramNames = ["Site", "CE"] + allStateNames

        last_update = None
        if "LastUpdateTime" in selectDict:
            last_update = selectDict["LastUpdateTime"]
            del selectDict["LastUpdateTime"]
        site_select = []
        if "GridSite" in selectDict:
            site_select = selectDict["GridSite"]
            if not isinstance(site_select, list):
                site_select = [site_select]
            del selectDict["GridSite"]

        status_select = []
        if "Status" in selectDict:
            status_select = selectDict["Status"]
            if not isinstance(status_select, list):
                status_select = [status_select]
            del selectDict["Status"]

        expand_site = ""
        if "ExpandSite" in selectDict:
            expand_site = selectDict["ExpandSite"]
            site_select = [expand_site]
            del selectDict["ExpandSite"]

        # Get all the data from the database with various selections
        result = self.getCounters(
            "PilotAgents",
            ["GridSite", "DestinationSite", "Status"],
            selectDict,
            newer=last_update,
            timeStamp="LastUpdateTime",
        )
        if not result["OK"]:
            return result

        last_update = datetime.datetime.utcnow() - TimeUtilities.hour
        selectDict["Status"] = PilotStatus.ABORTED
        resultHour = self.getCounters(
            "PilotAgents",
            ["GridSite", "DestinationSite", "Status"],
            selectDict,
            newer=last_update,
            timeStamp="LastUpdateTime",
        )
        if not resultHour["OK"]:
            return resultHour

        last_update = datetime.datetime.utcnow() - TimeUtilities.day
        selectDict["Status"] = [PilotStatus.ABORTED, PilotStatus.DONE]
        resultDay = self.getCounters(
            "PilotAgents",
            ["GridSite", "DestinationSite", "Status"],
            selectDict,
            newer=last_update,
            timeStamp="LastUpdateTime",
        )
        if not resultDay["OK"]:
            return resultDay
        selectDict["CurrentJobID"] = 0
        selectDict["Status"] = PilotStatus.DONE
        resultDayEmpty = self.getCounters(
            "PilotAgents",
            ["GridSite", "DestinationSite", "Status"],
            selectDict,
            newer=last_update,
            timeStamp="LastUpdateTime",
        )
        if not resultDayEmpty["OK"]:
            return resultDayEmpty

        ceMap = {}
        resMap = getCESiteMapping()
        if resMap["OK"]:
            ceMap = resMap["Value"]

        # Sort out different counters
        resultDict = {}
        resultDict["Unknown"] = {}
        for attDict, count in result["Value"]:
            site = attDict["GridSite"]
            ce = attDict["DestinationSite"]
            state = attDict["Status"]
            if site == "Unknown" and ce != "Unknown" and ce != "Multiple" and ce in ceMap:
                site = ceMap[ce]
            if site not in resultDict:
                resultDict[site] = {}
            if ce not in resultDict[site]:
                resultDict[site][ce] = {}
                for p in allStateNames:
                    resultDict[site][ce][p] = 0

            resultDict[site][ce][state] = count

        for attDict, count in resultDay["Value"]:
            site = attDict["GridSite"]
            ce = attDict["DestinationSite"]
            state = attDict["Status"]
            if site == "Unknown" and ce != "Unknown" and ce in ceMap:
                site = ceMap[ce]
            if state == PilotStatus.DONE:
                resultDict[site][ce][PilotStatus.DONE] = count
            if state == PilotStatus.ABORTED:
                resultDict[site][ce][PilotStatus.ABORTED] = count

        for attDict, count in resultDayEmpty["Value"]:
            site = attDict["GridSite"]
            ce = attDict["DestinationSite"]
            state = attDict["Status"]
            if site == "Unknown" and ce != "Unknown" and ce in ceMap:
                site = ceMap[ce]
            if state == PilotStatus.DONE:
                resultDict[site][ce]["Done_Empty"] = count

        for attDict, count in resultHour["Value"]:
            site = attDict["GridSite"]
            ce = attDict["DestinationSite"]
            state = attDict["Status"]
            if site == "Unknown" and ce != "Unknown" and ce in ceMap:
                site = ceMap[ce]
            if state == PilotStatus.ABORTED:
                resultDict[site][ce]["Aborted_Hour"] = count

        records = []
        siteSumDict = {}
        for site, ces in resultDict.items():
            sumDict = {}
            for state in allStateNames:
                if state not in sumDict:
                    sumDict[state] = 0
            sumDict["Total"] = 0
            for ce, ceDict in ces.items():
                itemList = [site, ce]
                total = 0
                for state in allStateNames:
                    itemList.append(ceDict[state])
                    sumDict[state] += ceDict[state]
                    if state == PilotStatus.DONE:
                        done = ceDict[state]
                    if state == "Done_Empty":
                        empty = ceDict[state]
                    if state == PilotStatus.ABORTED:
                        aborted = ceDict[state]
                    if state not in ("Aborted_Hour", "Done_Empty"):
                        total += ceDict[state]

                sumDict["Total"] += total
                # Add the total number of pilots seen in the last day
                itemList.append(total)
                # Add pilot submission efficiency evaluation
                if (done - empty) > 0:
                    eff = done / (done - empty)
                elif done == 0:
                    eff = 0.0
                elif empty == done:
                    eff = 99.0
                else:
                    eff = 0.0
                itemList.append(f"{eff:.2f}")
                # Add pilot job efficiency evaluation
                if total > 0:
                    eff = (total - aborted) / total * 100
                else:
                    eff = 100.0
                itemList.append(f"{eff:.2f}")

                # Evaluate the quality status of the CE
                if total > 10:
                    if eff < 25.0:
                        itemList.append("Bad")
                    elif eff < 60.0:
                        itemList.append("Poor")
                    elif eff < 85.0:
                        itemList.append("Fair")
                    else:
                        itemList.append("Good")
                else:
                    itemList.append("Idle")

                if len(ces) == 1 or expand_site:
                    records.append(itemList)

            if len(ces) > 1 and not expand_site:
                itemList = [site, "Multiple"]
                for state in allStateNames + ["Total"]:
                    if state in sumDict:
                        itemList.append(sumDict[state])
                    else:
                        itemList.append(0)
                done = sumDict[PilotStatus.DONE]
                empty = sumDict["Done_Empty"]
                aborted = sumDict[PilotStatus.ABORTED]
                total = sumDict["Total"]

                # Add pilot submission efficiency evaluation
                if (done - empty) > 0:
                    eff = done / (done - empty)
                elif done == 0:
                    eff = 0.0
                elif empty == done:
                    eff = 99.0
                else:
                    eff = 0.0
                itemList.append(f"{eff:.2f}")
                # Add pilot job efficiency evaluation
                if total > 0:
                    eff = (total - aborted) / total * 100
                else:
                    eff = 100.0
                itemList.append(f"{eff:.2f}")

                # Evaluate the quality status of the Site
                if total > 10:
                    if eff < 25.0:
                        itemList.append("Bad")
                    elif eff < 60.0:
                        itemList.append("Poor")
                    elif eff < 85.0:
                        itemList.append("Fair")
                    else:
                        itemList.append("Good")
                else:
                    itemList.append("Idle")
                records.append(itemList)

            for state in allStateNames + ["Total"]:
                if state not in siteSumDict:
                    siteSumDict[state] = sumDict[state]
                else:
                    siteSumDict[state] += sumDict[state]

        # Perform site selection
        if site_select:
            new_records = []
            for r in records:
                if r[0] in site_select:
                    new_records.append(r)
            records = new_records

        # Perform status selection
        if status_select:
            new_records = []
            for r in records:
                if r[14] in status_select:
                    new_records.append(r)
            records = new_records

        # Get the Site Mask data
        result = SiteStatus().getUsableSites()
        if result["OK"]:
            siteMask = result["Value"]
            for r in records:
                if r[0] in siteMask:
                    r.append("Yes")
                else:
                    r.append("No")
        else:
            for r in records:
                r.append("Unknown")

        finalDict = {}
        finalDict["TotalRecords"] = len(records)
        finalDict["ParameterNames"] = paramNames + ["Total", "PilotsPerJob", "PilotJobEff", "Status", "InMask"]

        # Return all the records if maxItems == 0 or the specified number otherwise
        if maxItems:
            finalDict["Records"] = records[startItem : startItem + maxItems]
        else:
            finalDict["Records"] = records

        done = siteSumDict[PilotStatus.DONE]
        empty = siteSumDict["Done_Empty"]
        aborted = siteSumDict[PilotStatus.ABORTED]
        total = siteSumDict["Total"]

        # Add pilot submission efficiency evaluation
        if (done - empty) > 0:
            eff = done / (done - empty)
        elif done == 0:
            eff = 0.0
        elif empty == done:
            eff = 99.0
        else:
            eff = 0.0
        siteSumDict["PilotsPerJob"] = f"{eff:.2f}"
        # Add pilot job efficiency evaluation
        if total > 0:
            eff = (total - aborted) / total * 100
        else:
            eff = 100.0
        siteSumDict["PilotJobEff"] = f"{eff:.2f}"

        # Evaluate the overall quality status
        if total > 100:
            if eff < 25.0:
                siteSumDict["Status"] = "Bad"
            elif eff < 60.0:
                siteSumDict["Status"] = "Poor"
            elif eff < 85.0:
                siteSumDict["Status"] = "Fair"
            else:
                siteSumDict["Status"] = "Good"
        else:
            siteSumDict["Status"] = "Idle"
        finalDict["Extras"] = siteSumDict

        return S_OK(finalDict)

    ##########################################################################################
    def getPilotMonitorSelectors(self):
        """Get distinct values for the Pilot Monitor page selectors"""

        paramNames = ["VO", "GridType", "Status", "DestinationSite", "GridSite"]

        resultDict = {}
        for param in paramNames:
            result = self.getDistinctAttributeValues("PilotAgents", param)
            if result["OK"]:
                resultDict[param] = result["Value"]
            else:
                resultDict = []

        return S_OK(resultDict)

    ##########################################################################################
    def getPilotMonitorWeb(self, selectDict, sortList, startItem, maxItems):
        """Get summary of the pilot job information in a standard structure"""

        resultDict = {}
        if "LastUpdateTime" in selectDict:
            del selectDict["LastUpdateTime"]

        startDate = selectDict.get("FromDate", None)
        if startDate:
            del selectDict["FromDate"]
        # For backward compatibility
        if startDate is None:
            startDate = selectDict.get("LastUpdateTime", None)
            if startDate:
                del selectDict["LastUpdateTime"]
        endDate = selectDict.get("ToDate", None)
        if endDate:
            del selectDict["ToDate"]

        # Sorting instructions. Only one for the moment.
        if sortList:
            orderAttribute = sortList[0][0] + ":" + sortList[0][1]
        else:
            orderAttribute = None

        # Select pilots for the summary
        result = self.selectPilots(
            selectDict, orderAttribute=orderAttribute, newer=startDate, older=endDate, timeStamp="LastUpdateTime"
        )
        if not result["OK"]:
            return S_ERROR("Failed to select pilots: " + result["Message"])

        pList = result["Value"]
        nPilots = len(pList)
        resultDict["TotalRecords"] = nPilots
        if nPilots == 0:
            return S_OK(resultDict)

        ini = startItem
        last = ini + maxItems
        if ini >= nPilots:
            return S_ERROR("Item number out of range")
        if last > nPilots:
            last = nPilots
        pilotList = pList[ini:last]

        paramNames = [
            "PilotJobReference",
            "VO",
            "GridType",
            "Status",
            "DestinationSite",
            "BenchMark",
            "SubmissionTime",
            "PilotID",
            "LastUpdateTime",
            "CurrentJobID",
            "GridSite",
        ]

        result = self.getPilotInfo(pilotList, paramNames=paramNames)
        if not result["OK"]:
            return S_ERROR("Failed to get pilot info: " + result["Message"])

        pilotDict = result["Value"]
        records = []
        for pilot in pilotList:
            gridSite = None
            parList = []
            for parameter in paramNames:
                if not isinstance(pilotDict[pilot][parameter], int):
                    parList.append(str(pilotDict[pilot][parameter]))
                else:
                    parList.append(pilotDict[pilot][parameter])
                if parameter == "GridSite":
                    gridSite = pilotDict[pilot][parameter]

            # If the Grid Site is unknown try to recover it in the last moment
            if gridSite == "Unknown":
                ce = pilotDict[pilot]["DestinationSite"]
                result = getCESiteMapping(ce)
                if result["OK"]:
                    gridSite = result["Value"].get(ce)
                    del parList[-1]
                    parList.append(gridSite)
            records.append(parList)

        resultDict["ParameterNames"] = paramNames
        resultDict["Records"] = records

        return S_OK(resultDict)

    def getSummarySnapshot(self, requestedFields=False):
        """Get the summary snapshot for a given combination"""
        if not requestedFields:
            requestedFields = ["GridSite", "GridType", "Status"]
        valueFields = ["COUNT(PilotID)"]
        defString = ", ".join(requestedFields)
        valueString = ", ".join(valueFields)
        result = self._query(f"SELECT {defString}, {valueString} FROM PilotAgents GROUP BY {defString}")
        if not result["OK"]:
            return result
        return S_OK(((requestedFields + valueFields), result["Value"]))


class PivotedPilotSummaryTable:
    """
    The class creates a 'pivoted' table by combining records with the same group
    of self.columnList into a single row. It allows an easy calculation of pilot efficiencies.
    """

    pstates = ["Submitted", "Done", "Failed", "Aborted", "Running", "Waiting", "Scheduled", "Ready"]

    def __init__(self, columnList):
        """
        Initialise a table with columns to be grouped by.

        :param columnList: i.e. ['GridSite', 'DestinationSite']
        :return:
        """

        self.columnList = columnList

        # we want 'Site' and 'CE' in the final result
        colMap = {"GridSite": "Site", "DestinationSite": "CE"}
        self._columns = [colMap.get(val, val) for val in columnList]

        self._columns += self.pstates  # MySQL._query() does not give us column names, sadly.

    def buildSQL(self):
        """
        Build an SQL query to create a table with all status counts in one row, ("pivoted")
        grouped by columns in the column list.

        :param dict selectDict:
        :return: SQL query
        """

        lastUpdate = datetime.datetime.utcnow() - TimeUtilities.day
        lastHour = datetime.datetime.utcnow() - TimeUtilities.hour

        pvtable = "pivoted"
        innerGroupBy = (
            "(SELECT %s, Status,\n "
            "count(CASE WHEN CurrentJobID=0  THEN 1 END) AS Empties,"
            "count(CASE WHEN LastUpdateTime > '%s' THEN 1 END) AS Last_Hour,"
            " count(*) AS qty FROM PilotAgents\n "
            "WHERE Status NOT IN ('Done', 'Aborted') OR (Status in ('Done', 'Aborted') \n"
            " AND \n"
            " LastUpdateTime > '%s')"
            " GROUP by %s, Status)\n AS %s"
            % (_quotedList(self.columnList), lastHour, lastUpdate, _quotedList(self.columnList), pvtable)
        )

        # pivoted table: combine records with the same group of self.columnList into a single row.

        pivotedQuery = f"SELECT {', '.join([(pvtable + '.' + item) for item in self.columnList])},\n"
        lineTemplate = " SUM(if (pivoted.Status={state!r}, pivoted.qty, 0)) AS {state}"
        pivotedQuery += ",\n".join(lineTemplate.format(state=state) for state in self.pstates)
        pivotedQuery += ",\n  SUM(if (pivoted.Status='Aborted', pivoted.Last_Hour, 0)) AS Aborted_Hour"
        pivotedQuery += (
            ",\n  SUM(if (%s.Status='Done', %s.Empties,0)) AS Done_Empty,\n"
            "  SUM(%s.qty) AS Total "
            "FROM\n" % (pvtable, pvtable, pvtable)
        )

        outerGroupBy = f" GROUP BY {_quotedList(self.columnList)}) \nAS pivotedEff;"

        # add efficiency columns using aliases defined in the pivoted table
        effCase = (
            "(CASE\n  WHEN pivotedEff.Done - pivotedEff.Done_Empty > 0 \n"
            "  THEN pivotedEff.Done/(pivotedEff.Done-pivotedEff.Done_Empty) \n"
            "  WHEN pivotedEff.Done=0 THEN 0 \n"
            "  WHEN pivotedEff.Done=pivotedEff.Done_Empty \n"
            "  THEN 99.0 ELSE 0.0 END) AS PilotsPerJob,\n"
            " (pivotedEff.Total - pivotedEff.Aborted)/pivotedEff.Total*100.0 AS PilotJobEff \nFROM \n("
        )
        effSelectTemplate = " CAST(pivotedEff.{state} AS UNSIGNED) AS {state} "
        # now select the columns + states:
        pivotedEff = (
            f"SELECT {', '.join([('pivotedEff' + '.' + item) for item in self.columnList])},\n"
            + ", ".join(effSelectTemplate.format(state=state) for state in self.pstates + ["Aborted_Hour", "Total"])
            + ", \n"
        )

        finalQuery = pivotedEff + effCase + pivotedQuery + innerGroupBy + outerGroupBy
        self._columns += ["Aborted_Hour", "Total", "PilotsPerJob", "PilotJobEff"]
        return finalQuery

    def getColumnList(self):
        return self._columns


try:
    gPilotAgentsDB = PilotAgentsDB()
except:
    gPilotAgentsDB = None
