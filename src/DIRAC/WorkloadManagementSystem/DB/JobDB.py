""" The JobDB class is a front-end to the main WMS database containing
    job definitions and status information. It is used in most of the WMS
    components


**Configuration Parameters**:

The following options can be set in ``Systems/WorkloadManagement/Databases/JobDB``

* *MaxRescheduling*:     Set the maximum number of times a job can be rescheduled, default *3*.
* *CompressJDLs*:        Enable compression of JDLs when they are stored in the database, default *False*.

"""
from __future__ import annotations

import datetime
import operator
from typing import overload

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSiteTier
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.DErrno import EWMSJMAN, EWMSSUBM, cmpError
from DIRAC.Core.Utilities.ReturnValues import (
    S_ERROR,
    S_OK,
    convertToReturnValue,
    returnValueOrRaise,
    SErrorException,
    DReturnType,
)
from DIRAC.FrameworkSystem.Client.Logger import contextLogger
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
    checkAndAddOwner,
    checkAndPrepareJob,
    compressJDL,
    createJDLWithInitialStatus,
    extractJDL,
    fixJDL,
)


class JobDB(DB):
    """Interface to MySQL-based JobDB"""

    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        DB.__init__(self, "JobDB", "WorkloadManagement/JobDB", parentLogger=parentLogger)

        self._defaultLogger = self.log

        # data member to check if __init__ went through without error
        self.__initialized = False
        self.maxRescheduling = self.getCSOption("MaxRescheduling", 3)

        # loading the function that will be used to determine the platform (it can be VO specific)

        self.jobAttributeNames = []

        self.siteClient = SiteStatus()

        result = self.__getAttributeNames()

        if not result["OK"]:
            self.log.fatal("JobDB: Can not retrieve job Attributes")
            return

        self.jdl2DBParameters = ["JobName", "JobType", "JobGroup"]

        self.log.info("MaxReschedule", self.maxRescheduling)
        self.log.info("==================================================")
        self.__initialized = True

    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    def isValid(self):
        """Check if correctly initialised"""
        return self.__initialized

    def __getAttributeNames(self):
        """get Name of Job Attributes defined in DB
        set self.jobAttributeNames to the list of Names
        return S_OK()
        return S_ERROR upon error
        """

        res = self._query("DESCRIBE Jobs")
        if not res["OK"]:
            return res
        self.jobAttributeNames = [row[0] for row in res["Value"]]

        return S_OK()

    #############################################################################
    def getDistinctJobAttributes(self, attribute, condDict=None, older=None, newer=None, timeStamp="LastUpdateTime"):
        """Get distinct values of the job attribute under specified conditions"""
        return self.getDistinctAttributeValues(
            "Jobs", attribute, condDict=condDict, older=older, newer=newer, timeStamp=timeStamp
        )

    #############################################################################
    def getJobParameters(self, jobID, paramList=None):
        """Get Job Parameters defined for jobID.
        Returns a dictionary with the Job Parameters.
        If parameterList is empty - all the parameters are returned.
        """
        jobIDList = [jobID] if isinstance(jobID, (str, int)) else jobID

        resultDict = {}
        if paramList:
            if isinstance(paramList, str):
                paramList = paramList.split(",")
            paramNameList = []
            for pn in paramList:
                ret = self._escapeString(pn)
                if not ret["OK"]:
                    return ret
                paramNameList.append(ret["Value"])
            cmd = "SELECT JobID, Name, Value FROM JobParameters WHERE JobID IN ({}) AND Name IN ({})".format(
                ",".join(str(int(j)) for j in jobIDList),
                ",".join(paramNameList),
            )
            result = self._query(cmd)
            if result["OK"]:
                if result["Value"]:
                    for res_jobID, res_name, res_value in result["Value"]:
                        try:
                            res_value = res_value.decode(errors="replace")  # account for use of BLOBs
                        except AttributeError:
                            pass
                        resultDict.setdefault(int(res_jobID), {})[res_name] = res_value

                return S_OK(resultDict)  # there's a slim chance that this is an empty dictionary
            else:
                return S_ERROR("JobDB.getJobParameters: failed to retrieve parameters")

        else:
            result = self.getFields("JobParameters", ["JobID", "Name", "Value"], {"JobID": jobID})
            if not result["OK"]:
                return result

            for res_jobID, res_name, res_value in result["Value"]:
                try:
                    res_value = res_value.decode(errors="replace")  # account for use of BLOBs
                except AttributeError:
                    pass
                resultDict.setdefault(int(res_jobID), {})[res_name] = res_value

            return S_OK(resultDict)  # there's a slim chance that this is an empty dictionary

    #############################################################################
    def getAtticJobParameters(self, jobID, paramList=None, rescheduleCounter=-1):
        """Get Attic Job Parameters defined for a job with jobID.
        Returns a dictionary with the Attic Job Parameters per each rescheduling cycle.
        If parameterList is empty - all the parameters are returned.
        If recheduleCounter = -1, all cycles are returned.
        """

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        # self.log.debug('JobDB.getAtticJobParameters: Getting Attic Parameters for job %s' % jobID)

        resultDict = {}
        paramCondition = ""
        if paramList:
            paramNameList = []
            for x in paramList:
                ret = self._escapeString(x)
                if not ret["OK"]:
                    return ret
                paramNameList.append(ret["Value"])
            paramNames = ",".join(paramNameList)
            paramCondition = f" AND Name in ({paramNames})"
        rCounter = ""
        if rescheduleCounter != -1:
            rCounter = " AND RescheduleCycle=%d" % int(rescheduleCounter)
        cmd = "SELECT Name, Value, RescheduleCycle from AtticJobParameters"
        cmd += f" WHERE JobID={jobID} {paramCondition} {rCounter}"
        result = self._query(cmd)
        if result["OK"]:
            if result["Value"]:
                for name, value, counter in result["Value"]:
                    try:
                        value = value.decode()  # account for use of BLOBs
                    except AttributeError:
                        pass
                    resultDict.setdefault(counter, {})[name] = value

            return S_OK(resultDict)
        else:
            return S_ERROR("JobDB.getAtticJobParameters: failed to retrieve parameters")

    #############################################################################
    @convertToReturnValue
    def getJobsAttributes(self, jobIDs, attrList=None):
        """Get all Job(s) Attributes for a given list of jobIDs.
        Return a dictionary with all Job Attributes as value pairs
        """
        if not jobIDs:
            return {}

        # If no list of attributes is given, return all attributes
        if not attrList:
            attrList = self.jobAttributeNames
        if isinstance(attrList, str):
            attrList = attrList.replace(" ", "").split(",")
        attrList.sort()

        if isinstance(jobIDs, str):
            jobIDs = [int(jID) for jID in jobIDs.replace(" ", "").split(",")]
        if isinstance(jobIDs, int):
            jobIDs = [jobIDs]

        attrNameListS = []
        for x in attrList:
            x = "`" + returnValueOrRaise(self._escapeString(x))[1:-1] + "`"
            attrNameListS.append(x)
        attrNames = "JobID," + ",".join(attrNameListS)

        sqlCmd = "CREATE TEMPORARY TABLE to_select_Jobs (JobID INTEGER NOT NULL, PRIMARY KEY (JobID)) ENGINE=MEMORY;"
        returnValueOrRaise(self._update(sqlCmd))
        try:
            sqlCmd = "INSERT INTO to_select_Jobs (JobID) VALUES ( %s )"
            returnValueOrRaise(self._updatemany(sqlCmd, [(int(j),) for j in jobIDs]))
            sqlCmd = f"SELECT {attrNames} FROM Jobs JOIN to_select_Jobs USING (JobID)"
            result = returnValueOrRaise(self._query(sqlCmd))
        finally:
            sqlCmd = "DROP TEMPORARY TABLE to_select_Jobs"
            returnValueOrRaise(self._update(sqlCmd))

        attributes = {}
        for t_att in result:
            jobID = int(t_att[0])
            attributes.setdefault(jobID, {})
            for tx, ax in zip(t_att[1:], attrList):
                attributes[jobID].setdefault(ax, tx)

        return attributes

    #############################################################################
    def getJobAttributes(self, jobID, attrList=None):
        """Get all Job Attributes for a given jobID.
        Return a dictionary with all Job Attributes as value pairs
        """

        """Get the given attribute of a job specified by its jobID"""

        result = self.getJobsAttributes([jobID], attrList)
        if not result["OK"]:
            return result
        return S_OK(result["Value"].get(int(jobID), {}))

    #############################################################################
    def getJobAttribute(self, jobID, attribute):
        """Get the given attribute of a job specified by its jobID"""

        result = self.getJobAttributes(jobID, [attribute])
        if not result["OK"]:
            return result
        return S_OK(result["Value"].get(attribute))

    #############################################################################
    @deprecated("Use JobParametersDB instead")
    def getJobParameter(self, jobID, parameter):
        """Get the given parameter of a job specified by its jobID"""

        result = self.getJobParameters(jobID, [parameter])
        if not result["OK"]:
            return result
        return S_OK(result.get("Value", {}).get(int(jobID), {}).get(parameter))

    #############################################################################
    def getJobOptParameter(self, jobID, parameter):
        """Get optimizer parameters for the given job."""

        result = self.getFields("OptimizerParameters", ["Value"], {"JobID": jobID, "Name": parameter})
        if result["OK"]:
            if result["Value"]:
                return S_OK(result["Value"][0][0])
            return S_ERROR("Parameter not found")

        return S_ERROR("Failed to access database")

    #############################################################################
    def getJobOptParameters(self, jobID, paramList=None):
        """Get optimizer parameters for the given job. If the list of parameter names is
        empty, get all the parameters then
        """

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        if paramList:
            paramNameList = []
            for x in paramList:
                ret = self._escapeString(x)
                if not ret["OK"]:
                    return ret
                paramNameList.append(ret["Value"])
            paramNames = ",".join(paramNameList)
            cmd = f"SELECT Name, Value from OptimizerParameters WHERE JobID={jobID} and Name in ({paramNames})"
        else:
            cmd = f"SELECT Name, Value from OptimizerParameters WHERE JobID={jobID}"

        result = self._query(cmd)
        if not result["OK"]:
            return S_ERROR("JobDB.getJobOptParameters: failed to retrieve parameters")
        try:
            jobOptParameters = {name: value.decode() for name, value in result.get("Value", {})}  # account for BLOBs
        except AttributeError:
            jobOptParameters = {name: value for name, value in result.get("Value", {})}
        return S_OK(jobOptParameters)

    #############################################################################

    @overload
    def getInputData(self, jobID: int | str) -> DReturnType[list[str]]:
        ...

    @overload
    def getInputData(self, jobID: list[int | str]) -> DReturnType[dict[int, list[str]]]:
        ...

    def getInputData(self, jobID: int | str | list[int | str]) -> DReturnType[list[str] | dict[int, list[str]]]:
        """Get input data for the given job"""
        if isinstance(jobID, (int, str)):
            ret = self._escapeString(jobID)
            if not ret["OK"]:
                return ret
            jobID = ret["Value"]
            query = f"JobID={jobID}"
            result = []
        else:
            job_ids = {int(i) for i in jobID}
            query = f"JobID IN ({','.join(map(str, job_ids))})"
            result = {i: [] for i in job_ids}
        cmd = f"SELECT JobID, LFN FROM InputData WHERE {query}"
        res = self._query(cmd)
        if not res["OK"]:
            return res

        for jid, lfn in res["Value"]:
            lfn = lfn.strip()
            if lfn.lower().startswith("lfn:"):
                lfn = lfn[4:]
            if isinstance(result, list):
                result.append(lfn)
            else:
                result[jid].append(lfn)

        return S_OK(result)

    #############################################################################
    def setInputData(self, jobID, inputData):
        """Inserts input data for the given job"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]
        cmd = f"DELETE FROM InputData WHERE JobID={jobID}"
        result = self._update(cmd)
        if not result["OK"]:
            result = S_ERROR("JobDB.setInputData: operation failed.")

        for lfn in inputData:
            # some jobs are setting empty string as InputData
            if not lfn:
                continue
            ret = self._escapeString(lfn.strip())
            if not ret["OK"]:
                return ret
            lfn = ret["Value"]
            cmd = f"INSERT INTO InputData (JobID,LFN) VALUES ({jobID}, {lfn} )"
            res = self._update(cmd)
            if not res["OK"]:
                return res

        return S_OK("Files added")

    #############################################################################
    def setOptimizerChain(self, jobID, optimizerList):
        """Set the optimizer chain for the given job. The 'TaskQueue'
        optimizer should be the last one in the chain, it is added
        if not present in the optimizerList
        """

        optString = ",".join(optimizerList)
        return self.setJobOptParameter(jobID, "OptimizerChain", optString)

    #############################################################################
    def setNextOptimizer(self, jobID, currentOptimizer):
        """Set the job status to be processed by the next optimizer in the
        chain
        """

        result = self.getJobOptParameter(jobID, "OptimizerChain")
        if not result["OK"]:
            return result

        optList = result["Value"].split(",")
        if currentOptimizer not in optList:
            return S_ERROR("Could not find " + currentOptimizer + " in chain")
        try:
            # Append None to get a list of (opt,nextOpt)
            optList.append(None)
            nextOptimizer = None
            for opt, nextOptimizer in zip(optList[:-1], optList[1:]):
                if opt == currentOptimizer:
                    break
            if nextOptimizer is None:
                return S_ERROR("Unexpected end of the Optimizer Chain")
        except ValueError:
            return S_ERROR("The " + currentOptimizer + " not found in the chain")

        result = self.setJobStatus(jobID, status=JobStatus.CHECKING, minorStatus=nextOptimizer)
        if not result["OK"]:
            return result
        return S_OK(nextOptimizer)

    ############################################################################
    def selectJobs(self, condDict, older=None, newer=None, timeStamp="LastUpdateTime", orderAttribute=None, limit=None):
        """Select jobs matching the following conditions:
        - condDict dictionary of required Key = Value pairs;
        - with the last update date older and/or newer than given dates;

        The result is ordered by JobID if requested, the result is limited to a given
        number of jobs if requested.
        """

        # self.log.debug('JobDB.selectJobs: retrieving jobs.')

        res = self.getFields(
            "Jobs",
            ["JobID"],
            condDict=condDict,
            limit=limit,
            older=older,
            newer=newer,
            timeStamp=timeStamp,
            orderAttribute=orderAttribute,
        )

        if not res["OK"]:
            return res

        if not res["Value"]:
            return S_OK([])
        return S_OK([self._to_value(i) for i in res["Value"]])

    #############################################################################
    def setJobAttribute(self, jobID, attrName, attrValue, update=False, myDate=None, force=False):
        """Set an attribute value for job specified by jobID.
        The LastUpdate time stamp is refreshed if explicitly requested

        :param jobID: job ID
        :type jobID: int or str
        :param str attrName: attribute name
        :param str attrValue: attribute value
        :param bool update: optional flag to update the job LastUpdateTime stamp
        :param str myDate: optional time stamp for the LastUpdateTime attribute

        :return: S_OK/S_ERROR
        """

        if not jobID:
            return S_OK()

        if attrName not in self.jobAttributeNames:
            return S_ERROR(EWMSJMAN, "Request to set non-existing job attribute")

        if attrName == "Status":
            # Treat this update separately
            res = self.setJobsMajorStatus([jobID], attrValue, force=force)
            if not res["OK"]:
                return res
            if update:
                cmd = f"UPDATE Jobs SET LastUpdateTime=UTC_TIMESTAMP() WHERE JobID={jobID}"
                if myDate:
                    cmd += f" AND LastUpdateTime < {myDate}"
                return self._update(cmd)
            else:
                return res

        # if we are here it's because we are not updating the status
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        ret = self._escapeString(attrValue)
        if not ret["OK"]:
            return ret
        value = ret["Value"]

        if update:
            cmd = f"UPDATE Jobs SET {attrName}={value},LastUpdateTime=UTC_TIMESTAMP() WHERE JobID={jobID}"
        else:
            cmd = f"UPDATE Jobs SET {attrName}={value} WHERE JobID={jobID}"

        if myDate:
            cmd += f" AND LastUpdateTime < {myDate}"

        return self._update(cmd)

    #############################################################################
    def setJobAttributes(self, jobID, attrNames, attrValues, update=False, myDate=None, force=False):
        """Set one or more attribute values for one or more jobs specified by jobID.
        The LastUpdate time stamp is refreshed if explicitly requested with the update flag

        This method is also used for updating the Status, MinorStatus, ApplicationStatus
        of a job, as self.setJobsStatus also calls this method.
        If the status is already final, we don't update it.

        :param jobID: one or more job IDs
        :type jobID: int or str or list
        :param list attrNames: names of attributes to update
        :param list attrValues: corresponding values of attributes to update
        :param bool update: optional flag to update the job LastUpdateTime stamp
        :param str myDate: optional time stamp for the LastUpdateTime attribute
        :param bool force: force update of Status (override State Machine decision)

        :return: S_OK/S_ERROR
        """

        if not jobID:
            return S_OK()

        jobIDList = jobID
        if not isinstance(jobID, (list, tuple)):
            jobIDList = [jobID]

        try:
            jIDList = [int(jID) for jID in jobIDList]
        except ValueError as e:
            return S_ERROR(f"JobDB.setAttributes: {e}")

        if len(attrNames) != len(attrValues):
            return S_ERROR("JobDB.setAttributes: incompatible Argument length")

        for attrName in attrNames:
            if attrName not in self.jobAttributeNames:
                return S_ERROR(EWMSJMAN, "Request to set non-existing job attribute")

        if "Status" in attrNames:
            # Treat this update separately
            res = self.setJobsMajorStatus(jIDList, attrValues[attrNames.index("Status")], force=force)
            if not res["OK"]:
                return res
            attrValues.pop(attrNames.index("Status"))
            attrNames.remove("Status")

        attr = []

        for name, value in zip(attrNames, attrValues):
            ret = self._escapeString(value)
            if not ret["OK"]:
                return ret
            attr.append(f"{name}={ret['Value']}")
        if update:
            attr.append("LastUpdateTime=UTC_TIMESTAMP()")
        if not attr:
            return S_ERROR("JobDB.setAttributes: Nothing to do")

        cmd = f"UPDATE Jobs SET {', '.join(attr)} WHERE JobID in ( {', '.join(str(int(j)) for j in jIDList)} )"

        if myDate:
            cmd += f" AND LastUpdateTime < {myDate}"

        return self._update(cmd)

    def setJobsMajorStatus(self, jIDList, candidateStatus, force=False):
        """
        Sets jobs major status, considering the JobStateMachine result

        :param list jIDList: list of one or more job IDs
        :param str candidateStatus: candidate major Status
        """

        # get the current statuses of the jobs
        res = self.getJobsAttributes(jIDList, ["Status"])
        if not res["OK"]:
            return res
        jIDStatusDict = res["Value"]

        newStatuses = {}
        for jID, jIDStatus in jIDStatusDict.items():
            if force:
                self.log.info("Status update forced", f"({str(jID)}: {jIDStatus} -> {candidateStatus})")
                nextState = candidateStatus
            else:
                res = JobStatus.JobsStateMachine(jIDStatus["Status"]).getNextState(candidateStatus)
                if not res["OK"]:
                    return res
                nextState = res["Value"]

                # The JobsStateMachine might force a different status
                if candidateStatus != nextState:
                    self.log.error(
                        "Job Status Error",
                        "%s can't move from %s to %s: using %s"
                        % (jID, jIDStatus["Status"], candidateStatus, nextState),
                    )

            newStatuses[jID] = nextState

        cmd = "INSERT INTO Jobs (JobID, Status) VALUES "

        ns = []
        for jID, status in newStatuses.items():
            ret_status = self._escapeString(status)
            if not ret_status["OK"]:
                return ret_status
            status = ret_status["Value"]
            ns.append(f"({jID}, {status})")
        cmd += ",".join(ns)

        cmd += " ON DUPLICATE KEY UPDATE Status=VALUES(Status)"

        return self._update(cmd)

    def setJobStatus(self, jobID, status="", minorStatus="", applicationStatus="", force=False):
        """Set status of the job specified by its jobID"""
        # Do not update the LastUpdate time stamp if setting the Stalled status
        update_flag = True
        if status == JobStatus.STALLED:
            update_flag = False

        attrNames = []
        attrValues = []
        if status:
            attrNames.append("Status")
            attrValues.append(status)
        if minorStatus:
            attrNames.append("MinorStatus")
            attrValues.append(minorStatus)
        if applicationStatus:
            attrNames.append("ApplicationStatus")
            attrValues.append(applicationStatus[:255])

        result = self.setJobAttributes(jobID, attrNames, attrValues, update=update_flag, force=force)
        if not result["OK"]:
            return result

        return S_OK()

    #############################################################################
    def setEndExecTime(self, jobID, endDate=None):
        """Set EndExecTime time stamp"""

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        if endDate:
            ret = self._escapeString(endDate)
            if not ret["OK"]:
                return ret
            endDate = ret["Value"]
        else:
            endDate = "UTC_TIMESTAMP()"
        req = f"UPDATE Jobs SET EndExecTime={endDate} WHERE JobID={jobID} AND EndExecTime IS NULL"
        return self._update(req)

    #############################################################################
    def setStartExecTime(self, jobID, startDate=None):
        """Set StartExecTime time stamp and HeartBeatTime if not already set"""

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        if startDate:
            ret = self._escapeString(startDate)
            if not ret["OK"]:
                return ret
            startDate = ret["Value"]
        else:
            startDate = "UTC_TIMESTAMP()"
        # Set also the HeartBeatTime in case the job gets stuck before sending the first HeartBeat
        req = f"UPDATE Jobs SET HeartBeatTime={startDate} WHERE JobID={jobID} AND HeartBeatTime IS NULL"
        ret = self._update(req)
        if not ret["OK"]:
            return ret
        req = f"UPDATE Jobs SET StartExecTime={startDate} WHERE JobID={jobID} AND StartExecTime IS NULL"
        return self._update(req)

    #############################################################################
    def setJobOptParameter(self, jobID, name, value):
        """Set an optimzer parameter specified by name,value pair for the job JobID"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        e_jobID = ret["Value"]

        ret = self._escapeString(name)
        if not ret["OK"]:
            return ret
        e_name = ret["Value"]

        cmd = f"DELETE FROM OptimizerParameters WHERE JobID={e_jobID} AND Name={e_name}"
        res = self._update(cmd)
        if not res["OK"]:
            return res

        return self.insertFields("OptimizerParameters", ["JobID", "Name", "Value"], [jobID, name, value])

    #############################################################################
    def removeJobOptParameter(self, jobID, name):
        """Remove the specified optimizer parameter for jobID"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]
        ret = self._escapeString(name)
        if not ret["OK"]:
            return ret
        name = ret["Value"]

        cmd = f"DELETE FROM OptimizerParameters WHERE JobID={jobID} AND Name={name}"
        return self._update(cmd)

    #############################################################################
    def setAtticJobParameter(self, jobID, key, value, rescheduleCounter):
        """Set attic parameter for job specified by its jobID when job rescheduling
        for later debugging
        """
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        ret = self._escapeString(key)
        if not ret["OK"]:
            return ret
        key = ret["Value"]

        ret = self._escapeString(value)
        if not ret["OK"]:
            return ret
        value = ret["Value"]

        ret = self._escapeString(rescheduleCounter)
        if not ret["OK"]:
            return ret
        rescheduleCounter = ret["Value"]

        cmd = "INSERT INTO AtticJobParameters (JobID,RescheduleCycle,Name,Value) VALUES({},{},{},{})".format(
            jobID,
            rescheduleCounter,
            key,
            value,
        )
        return self._update(cmd)

    #############################################################################
    def setJobJDL(self, jobID, jdl=None, originalJDL=None):
        """Insert JDL's for job specified by jobID"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        req = f"SELECT OriginalJDL FROM JobJDLs WHERE JobID={jobID}"
        result = self._query(req)
        updateFlag = False
        if result["OK"] and result["Value"]:
            updateFlag = True

        if jdl:
            ret = self._escapeString(compressJDL(jdl))
            if not ret["OK"]:
                return ret
            e_JDL = ret["Value"]

            if updateFlag:
                cmd = f"UPDATE JobJDLs Set JDL={e_JDL} WHERE JobID={jobID}"
            else:
                cmd = f"INSERT INTO JobJDLs (JobID,JDL) VALUES ({jobID},{e_JDL})"
            result = self._update(cmd)
            if not result["OK"]:
                return result

        if originalJDL:
            ret = self._escapeString(compressJDL(originalJDL))
            if not ret["OK"]:
                return ret
            e_originalJDL = ret["Value"]

            if updateFlag:
                cmd = f"UPDATE JobJDLs Set OriginalJDL={e_originalJDL} WHERE JobID={jobID}"
            else:
                cmd = f"INSERT INTO JobJDLs (JobID,OriginalJDL) VALUES ({jobID},{e_originalJDL})"

            result = self._update(cmd)

        return result

    #############################################################################
    def __insertNewJDL(self, jdl):
        """Insert a new JDL in the system, this produces a new JobID"""

        err = "JobDB.__insertNewJDL: Failed to retrieve a new Id."

        result = self.insertFields("JobJDLs", ["JDL", "JobRequirements", "OriginalJDL"], ["", "", compressJDL(jdl)])
        if not result["OK"]:
            self.log.error("Can not insert New JDL", result["Message"])
            return result

        if "lastRowId" not in result:
            return S_ERROR(f"{err}")

        jobID = int(result["lastRowId"])

        self.log.info("JobDB: New JobID served", f"{jobID}")

        return S_OK(jobID)

    #############################################################################
    def getJobJDL(self, jobID, original=False):
        """Get JDL for job specified by its jobID. By default the current job JDL
        is returned. If 'original' argument is True, original JDL is returned
        """
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        if original:
            cmd = f"SELECT OriginalJDL FROM JobJDLs WHERE JobID={jobID}"
        else:
            cmd = f"SELECT JDL FROM JobJDLs WHERE JobID={jobID}"

        result = self._query(cmd)
        if result["OK"]:
            jdl = result["Value"]
            if not jdl:
                return S_OK(jdl)
            return S_OK(extractJDL(jdl[0][0]))
        return result

    #############################################################################
    def insertNewJobIntoDB(
        self,
        jdl,
        owner,
        ownerGroup,
        initialStatus=JobStatus.RECEIVED,
        initialMinorStatus="Job accepted",
        vo=None,
    ):
        """Insert the initial JDL into the Job database,
        Do initial JDL crosscheck,
        Set Initial job Attributes and Status

        :param str jdl: job description JDL
        :param str owner: job owner user name
        :param str ownerGroup: job owner group
        :param str initialStatus: optional initial job status (Received by default)
        :param str initialMinorStatus: optional initial minor job status
        :return: new job ID
        """
        # Workaround for the case when a custom version of dirac would be
        # calling this method
        if not vo:
            vo = getVOForGroup(ownerGroup)

        jobAttrs = {
            "LastUpdateTime": str(datetime.datetime.utcnow()),
            "SubmissionTime": str(datetime.datetime.utcnow()),
            "Owner": owner,
            "OwnerGroup": ownerGroup,
            "VO": vo,
        }

        result = checkAndAddOwner(jdl, owner, ownerGroup)
        if not result["OK"]:
            return result
        jobManifest = result["Value"]
        jdl = fixJDL(jdl)

        result = self.__insertNewJDL(jdl)
        if not result["OK"]:
            return S_ERROR(EWMSSUBM, "Failed to insert JDL in to DB")

        jobID = result["Value"]

        jobManifest.setOption("JobID", jobID)

        jobAttrs["JobID"] = jobID

        # 2.- Check JDL and Prepare DIRAC JDL
        jobJDL = jobManifest.dumpAsJDL()

        # Replace the JobID placeholder if any
        if jobJDL.find("%j") != -1:
            jobJDL = jobJDL.replace("%j", str(jobID))

        classAdJob = ClassAd(jobJDL)
        classAdReq = ClassAd("[]")
        retVal = S_OK(jobID)
        retVal["JobID"] = jobID
        if not classAdJob.isOK():
            jobAttrs["Status"] = JobStatus.FAILED

            jobAttrs["MinorStatus"] = "Error in JDL syntax"

            result = self.insertFields("Jobs", inDict=jobAttrs)
            if not result["OK"]:
                return result

            retVal["Status"] = JobStatus.FAILED
            retVal["MinorStatus"] = "Error in JDL syntax"
            return retVal

        classAdJob.insertAttributeInt("JobID", jobID)
        vo = getVOForGroup(ownerGroup)
        result = self.__checkAndPrepareJob(jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo)
        if not result["OK"]:
            return result

        jobJDL = createJDLWithInitialStatus(
            classAdJob, classAdReq, self.jdl2DBParameters, jobAttrs, initialStatus, initialMinorStatus
        )

        result = self.setJobJDL(jobID, jobJDL)
        if not result["OK"]:
            return result

        # Adding the job in the Jobs table
        result = self.insertFields("Jobs", inDict=jobAttrs)
        if not result["OK"]:
            return result

        # Looking for the Input Data
        inputData = []
        if classAdJob.lookupAttribute("InputData"):
            inputData = classAdJob.getListFromExpression("InputData")
        values = []

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        e_jobID = ret["Value"]

        for lfn in inputData:
            # some jobs are setting empty string as InputData
            if not lfn:
                continue
            ret = self._escapeString(lfn.strip())
            if not ret["OK"]:
                return ret
            lfn = ret["Value"]

            values.append(f"({e_jobID}, {lfn} )")

        if values:
            cmd = f"INSERT INTO InputData (JobID,LFN) VALUES {', '.join(values)}"
            result = self._update(cmd)
            if not result["OK"]:
                return result

        retVal["Status"] = initialStatus
        retVal["MinorStatus"] = initialMinorStatus
        retVal["TimeStamp"] = str(datetime.datetime.utcnow())

        return retVal

    def __checkAndPrepareJob(self, jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo):
        """
        Check Consistency of Submitted JDL and set some defaults
        Prepare subJDL with Job Requirements
        """
        retVal = checkAndPrepareJob(jobID, classAdJob, classAdReq, owner, ownerGroup, jobAttrs, vo)

        if not retVal["OK"]:
            if cmpError(retVal, EWMSSUBM):
                resultInsert = self.setJobAttributes(jobID, list(jobAttrs), list(jobAttrs.values()))
                if not resultInsert["OK"]:
                    retVal["MinorStatus"] += f"; {resultInsert['Message']}"

                return retVal
            else:
                return retVal

        return S_OK()

    #############################################################################
    @convertToReturnValue
    def removeJobFromDB(self, jobIDs):
        """
        Remove jobs from the Job DB and clean up all the job related data in various tables
        """
        if not jobIDs:
            return None
        jobIDList = jobIDs if isinstance(jobIDs, list) else [jobIDs]

        failedTablesList = []

        sqlCmd = "CREATE TEMPORARY TABLE to_delete_Jobs (JobID INT(11) UNSIGNED NOT NULL, PRIMARY KEY (JobID)) ENGINE=MEMORY;"
        returnValueOrRaise(self._update(sqlCmd))
        try:
            sqlCmd = "INSERT INTO to_delete_Jobs (JobID) VALUES ( %s )"
            returnValueOrRaise(self._updatemany(sqlCmd, [(j,) for j in jobIDList]))

            for table in [
                "InputData",
                "JobParameters",
                "AtticJobParameters",
                "HeartBeatLoggingInfo",
                "OptimizerParameters",
                "JobCommands",
                "Jobs",
                "JobJDLs",
            ]:
                sqlCmd = f"DELETE m from `{table}` m JOIN to_delete_Jobs t USING (JobID)"
                if not self._update(sqlCmd)["OK"]:
                    failedTablesList.append(table)
        finally:
            sqlCmd = "DROP TEMPORARY TABLE to_delete_Jobs"
            returnValueOrRaise(self._update(sqlCmd))

        if failedTablesList:
            raise SErrorException(f"Errors while job removal (tables {','.join(failedTablesList)})")

    #############################################################################
    def rescheduleJob(self, jobID):
        """Reschedule the given job to run again from scratch. Retain the already
        defined parameters in the parameter Attic
        """
        # Check Verified Flag
        result = self.getJobAttributes(
            jobID,
            [
                "Status",
                "MinorStatus",
                "VerifiedFlag",
                "RescheduleCounter",
                "Owner",
                "OwnerGroup",
            ],
        )
        if result["OK"]:
            resultDict = result["Value"]
        else:
            return S_ERROR("JobDB.getJobAttributes: can not retrieve job attributes")

        if "VerifiedFlag" not in resultDict:
            return S_ERROR(f"Job {jobID} not found in the system")

        if not resultDict["VerifiedFlag"]:
            return S_ERROR(
                "Job %s not Verified: Status = %s, MinorStatus = %s"
                % (jobID, resultDict["Status"], resultDict["MinorStatus"])
            )

        # Check the Reschedule counter first
        rescheduleCounter = int(resultDict["RescheduleCounter"]) + 1

        self.maxRescheduling = self.getCSOption("MaxRescheduling", self.maxRescheduling)

        # Exit if the limit of the reschedulings is reached
        if rescheduleCounter > self.maxRescheduling:
            self.log.warn("Maximum number of reschedulings is reached", f"Job {jobID}")
            res = self.setJobStatus(jobID, status=JobStatus.FAILED, minorStatus="Maximum of reschedulings reached")
            if not res["OK"]:
                return res
            return S_ERROR(f"Maximum number of reschedulings is reached: {self.maxRescheduling}")

        jobAttrs = {"RescheduleCounter": rescheduleCounter}

        # Save the job parameters for later debugging
        result = JobMonitoringClient().getJobParameters(jobID)
        if result["OK"]:
            parDict = result["Value"]
            for key, value in parDict.get(int(jobID), {}).items():
                result = self.setAtticJobParameter(jobID, key, value, rescheduleCounter - 1)
                if not result["OK"]:
                    break

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        e_jobID = ret["Value"]

        res = self._update(f"DELETE FROM JobParameters WHERE JobID={e_jobID}")
        if not res["OK"]:
            return res

        # Delete optimizer parameters
        if not self._update(f"DELETE FROM OptimizerParameters WHERE JobID={e_jobID}")["OK"]:
            return S_ERROR("JobDB.removeJobOptParameter: operation failed.")

        # the JobManager needs to know if there is InputData ??? to decide which optimizer to call
        # proposal: - use the getInputData method
        res = self.getJobJDL(jobID, original=True)
        if not res["OK"]:
            return res

        jdl = res["Value"]
        # Fix the possible lack of the brackets in the JDL
        if jdl.strip()[0].find("[") != 0:
            jdl = "[" + jdl + "]"
        classAdJob = ClassAd(jdl)
        classAdReq = ClassAd("[]")
        retVal = S_OK(jobID)
        retVal["JobID"] = jobID

        classAdJob.insertAttributeInt("JobID", jobID)

        result = self.__checkAndPrepareJob(
            jobID,
            classAdJob,
            classAdReq,
            resultDict["Owner"],
            resultDict["OwnerGroup"],
            jobAttrs,
            getVOForGroup(resultDict["OwnerGroup"]),
        )

        if not result["OK"]:
            return result

        priority = classAdJob.getAttributeInt("Priority")
        if priority is None:
            priority = 0
        jobAttrs["UserPriority"] = priority

        siteList = classAdJob.getListFromExpression("Site")
        if not siteList:
            site = "ANY"
        elif len(siteList) > 1:
            site = "Multiple"
        else:
            site = siteList[0]

        jobAttrs["Site"] = site

        jobAttrs["Status"] = JobStatus.RECEIVED

        jobAttrs["MinorStatus"] = JobMinorStatus.RESCHEDULED

        jobAttrs["ApplicationStatus"] = "Unknown"

        jobAttrs["LastUpdateTime"] = str(datetime.datetime.utcnow())

        jobAttrs["RescheduleTime"] = str(datetime.datetime.utcnow())

        jobAttrs["VO"] = getVOForGroup(resultDict["OwnerGroup"])

        reqJDL = classAdReq.asJDL()
        classAdJob.insertAttributeInt("JobRequirements", reqJDL)

        jobJDL = classAdJob.asJDL()

        # Replace the JobID placeholder if any
        if jobJDL.find("%j") != -1:
            jobJDL = jobJDL.replace("%j", str(jobID))

        result = self.setJobJDL(jobID, jobJDL)
        if not result["OK"]:
            return result

        result = self.setJobAttributes(jobID, list(jobAttrs), list(jobAttrs.values()), force=True)
        if not result["OK"]:
            return result

        retVal["InputData"] = classAdJob.lookupAttribute("InputData")
        retVal["RescheduleCounter"] = rescheduleCounter
        retVal["Status"] = JobStatus.RECEIVED
        retVal["MinorStatus"] = JobMinorStatus.RESCHEDULED

        return retVal

    #################################################################################
    def getSiteSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        """Get the summary of jobs in a given status on all the sites in the standard Web form"""

        paramNames = ["Site", "GridType", "Country", "Tier", "MaskStatus"]
        paramNames += JobStatus.JOB_STATES
        paramNames += ["Efficiency", "Status"]

        # Sort out records as requested
        sortItem = -1
        sortOrder = "ASC"
        if sortList:
            item = sortList[0][0]  # only one item for the moment
            sortItem = paramNames.index(item)
            sortOrder = sortList[0][1]

        last_update = None
        if "LastUpdateTime" in selectDict:
            last_update = selectDict["LastUpdateTime"]
            del selectDict["LastUpdateTime"]

        result = self.getCounters("Jobs", ["Site", "Status"], {}, newer=last_update, timeStamp="LastUpdateTime")
        last_day = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        resultDay = self.getCounters("Jobs", ["Site", "Status"], {}, newer=last_day, timeStamp="EndExecTime")

        # Get the site mask status
        siteMask = {}
        resultMask = self.siteClient.getSites("All")
        if resultMask["OK"]:
            for site in resultMask["Value"]:
                siteMask[site] = "NoMask"
        resultMask = self.siteClient.getSites("Active")
        if resultMask["OK"]:
            for site in resultMask["Value"]:
                siteMask[site] = "Active"
        resultMask = self.siteClient.getSites("Banned")
        if resultMask["OK"]:
            for site in resultMask["Value"]:
                siteMask[site] = "Banned"

        # Sort out different counters
        resultDict = {}
        if result["OK"]:
            for attDict, count in result["Value"]:
                siteFullName = attDict["Site"]
                status = attDict["Status"]
                if siteFullName not in resultDict:
                    resultDict[siteFullName] = {}
                    for state in JobStatus.JOB_STATES:
                        resultDict[siteFullName][state] = 0
                if status not in JobStatus.JOB_FINAL_STATES:
                    resultDict[siteFullName][status] = count
        if resultDay["OK"]:
            for attDict, count in resultDay["Value"]:
                siteFullName = attDict["Site"]
                if siteFullName not in resultDict:
                    resultDict[siteFullName] = {}
                    for state in JobStatus.JOB_STATES:
                        resultDict[siteFullName][state] = 0
                status = attDict["Status"]
                if status in JobStatus.JOB_FINAL_STATES:
                    resultDict[siteFullName][status] = count

        # Collect records now
        records = []
        countryCounts = {}
        for siteFullName in resultDict:
            siteDict = resultDict[siteFullName]
            if siteFullName.count(".") == 2:
                grid, _, country = siteFullName.split(".")
            else:
                grid, _, country = "Unknown", "Unknown", "Unknown"

            res = getSiteTier(siteFullName)
            if not res["OK"]:
                self.log.error(res["Message"])
                continue
            tier = res["Value"]

            if country not in countryCounts:
                countryCounts[country] = {}
                for state in JobStatus.JOB_STATES:
                    countryCounts[country][state] = 0
            rList = [siteFullName, grid, country, tier]
            if siteFullName in siteMask:
                rList.append(siteMask[siteFullName])
            else:
                rList.append("NoMask")
            for status in JobStatus.JOB_STATES:
                rList.append(siteDict[status])
                countryCounts[country][status] += siteDict[status]
            efficiency = 0
            total_finished = 0
            for state in JobStatus.JOB_FINAL_STATES:
                total_finished += resultDict[siteFullName][state]
            if total_finished > 0:
                efficiency = float(siteDict[JobStatus.DONE] + siteDict[JobStatus.COMPLETED]) / float(total_finished)
            rList.append(f"{efficiency * 100.0:.1f}")
            # Estimate the site verbose status
            if efficiency > 0.95:
                rList.append("Good")
            elif efficiency > 0.80:
                rList.append("Fair")
            elif efficiency > 0.60:
                rList.append("Poor")
            elif total_finished == 0:
                rList.append("Idle")
            else:
                rList.append("Bad")
            records.append(rList)

        # Select records as requested
        if selectDict:
            for item in selectDict:
                selectItem = paramNames.index(item)
                values = selectDict[item]
                if not isinstance(values, list):
                    values = [values]
                indices = list(range(len(records)))
                indices.reverse()
                for ind in indices:
                    if records[ind][selectItem] not in values:
                        del records[ind]

        # Sort records as requested
        if sortItem != -1:
            if sortOrder.lower() == "asc":
                records.sort(key=operator.itemgetter(sortItem))
            else:
                records.sort(key=operator.itemgetter(sortItem), reverse=True)

        # Collect the final result
        finalDict = {}
        finalDict["ParameterNames"] = paramNames
        # Return all the records if maxItems == 0 or the specified number otherwise
        if maxItems:
            if startItem + maxItems > len(records):
                finalDict["Records"] = records[startItem:]
            else:
                finalDict["Records"] = records[startItem : startItem + maxItems]
        else:
            finalDict["Records"] = records

        finalDict["TotalRecords"] = len(records)
        finalDict["Extras"] = countryCounts

        return S_OK(finalDict)

    #####################################################################################
    def setHeartBeatData(self, jobID, dynamicDataDict):
        """Add the job's heart beat data to the database"""

        # Set the time stamp first
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        e_jobID = ret["Value"]

        # If HeartBeatTime is being set, set it...
        timeStamp = dynamicDataDict.pop("HeartBeatTime", None)
        if timeStamp:
            result = self._escapeString(timeStamp)
            if not result["OK"]:
                self.log.warn("Failed to escape string ", timeStamp)
                return result
            req = f"UPDATE Jobs SET HeartBeatTime={result['Value']} WHERE JobID={e_jobID}"
        else:
            req = f"UPDATE Jobs SET HeartBeatTime=UTC_TIMESTAMP(), Status='{JobStatus.RUNNING}' WHERE JobID={e_jobID}"

        result = self._update(req)

        if not result["OK"]:
            return S_ERROR(f"Failed to set the heart beat time: {result['Message']}")

        ok = True
        # Add dynamic data to the job heart beat log
        valueList = []
        for key, value in dynamicDataDict.items():
            result = self._escapeString(key)
            if not result["OK"]:
                self.log.warn("Failed to escape string", key)
                continue
            e_key = result["Value"]
            result = self._escapeString(value)
            if not result["OK"]:
                self.log.warn("Failed to escape string", value)
                continue
            e_value = result["Value"]
            valueList.append(f"( {e_jobID}, {e_key}, {e_value}, UTC_TIMESTAMP())")

        if valueList:
            req = "INSERT INTO HeartBeatLoggingInfo (JobID,Name,Value,HeartBeatTime) VALUES "
            req += ",".join(valueList)
            result = self._update(req)
            if not result["OK"]:
                ok = False
                self.log.warn("Error storing heart beat data", result["Message"])

        return S_OK() if ok else S_ERROR("Failed to store some or all the parameters")

    #####################################################################################
    def getHeartBeatData(self, jobID):
        """Retrieve the job's heart beat data"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        res = self._query(f"SELECT Name,Value,HeartBeatTime from HeartBeatLoggingInfo WHERE JobID={jobID}")
        if not res["OK"]:
            return res

        if not res["Value"]:
            return S_OK([])

        result = []
        values = res["Value"]
        for row in values:
            name, value, heartbeattime = row
            if isinstance(value, bytes):
                value = value.decode()
            result.append((str(name), "%.01f" % (float(value.replace('"', ""))), str(heartbeattime)))

        return S_OK(result)

    #####################################################################################
    def setJobCommand(self, jobID, command, arguments=None):
        """Store a command to be passed to the job together with the next heart beat"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        ret = self._escapeString(command)
        if not ret["OK"]:
            return ret
        command = ret["Value"]

        if arguments:
            ret = self._escapeString(arguments)
            if not ret["OK"]:
                return ret
            arguments = ret["Value"]
        else:
            arguments = "''"

        req = "INSERT INTO JobCommands (JobID,Command,Arguments,ReceptionTime) "
        req += f"VALUES ({jobID}, {command}, {arguments}, UTC_TIMESTAMP())"
        return self._update(req)

    #####################################################################################
    def getJobCommand(self, jobID, status=JobStatus.RECEIVED):
        """Get a command to be passed to the job together with the next heart beat"""

        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        ret = self._escapeString(status)
        if not ret["OK"]:
            return ret
        status = ret["Value"]

        result = self._query(f"SELECT Command, Arguments FROM JobCommands WHERE JobID={jobID} AND Status={status}")
        if not result["OK"]:
            return result

        return S_OK(dict(result["Value"]))

    #####################################################################################
    def setJobCommandStatus(self, jobID, command, status):
        """Set the command status"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        ret = self._escapeString(command)
        if not ret["OK"]:
            return ret
        command = ret["Value"]

        ret = self._escapeString(status)
        if not ret["OK"]:
            return ret
        status = ret["Value"]

        return self._update(f"UPDATE JobCommands SET Status={status} WHERE JobID={jobID} AND Command={command}")

    #####################################################################################
    def getSummarySnapshot(self, requestedFields=False):
        """Get the summary snapshot for a given combination"""
        if not requestedFields:
            requestedFields = ["Status", "MinorStatus", "Site", "Owner", "OwnerGroup", "JobGroup"]
        valueFields = ["COUNT(JobID)", "SUM(RescheduleCounter)"]
        defString = ", ".join(requestedFields)
        valueString = ", ".join(valueFields)
        result = self._query(f"SELECT {defString}, {valueString} FROM Jobs GROUP BY {defString}")
        if not result["OK"]:
            return result
        return S_OK(((requestedFields + valueFields), result["Value"]))

    def removeInfoFromHeartBeatLogging(self, status, delTime, maxLines):
        """Remove HeartBeatLoggingInfo from DB.

        :param str status: status of the jobs
        :param str delTime: timestamp of the age of the jobs
        :param int maxLines: maximum number of lines to be removed
        :returns: S_OK/S_ERROR
        """
        ret = self._escapeString(status)
        if not ret["OK"]:
            return ret
        status = ret["Value"]

        ret = self._escapeString(delTime)
        if not ret["OK"]:
            return ret
        delTime = ret["Value"]

        self.log.verbose("Removing HeartBeatLogginInfo for", f"{status!r} {delTime!r} {maxLines!r}")
        cmd = """DELETE h FROM HeartBeatLoggingInfo AS h
             JOIN (SELECT hi.JobID FROM HeartBeatLoggingInfo AS hi
                LEFT JOIN Jobs j on j.JobID = hi.JobID
                WHERE j.Status = %(status)s
                    AND
                  LastUpdateTime < %(delay)s
                LIMIT %(maxLines)d) h2
              ON h2.JobID = h.JobID""" % {
            "maxLines": maxLines,
            "status": status,
            "delay": delTime,
        }
        result = self._update(cmd)
        self.log.verbose("Removed from HBLI", result)
        return result
