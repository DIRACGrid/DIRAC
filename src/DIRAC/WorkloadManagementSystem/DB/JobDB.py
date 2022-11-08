""" The JobDB class is a front-end to the main WMS database containing
    job definitions and status information. It is used in most of the WMS
    components


**Configuration Parameters**:

The following options can be set in ``Systems/WorkloadManagement/<Setup>/Databases/JobDB``

* *MaxRescheduling*:     Set the maximum number of times a job can be rescheduled, default *3*.
* *CompressJDLs*:        Enable compression of JDLs when they are stored in the database, default *False*.

"""
import base64
import zlib
import datetime

import operator

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSiteTier
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EWMSSUBM, EWMSJMAN
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

#############################################################################
# utility functions


def compressJDL(jdl):
    """Return compressed JDL string."""
    return base64.b64encode(zlib.compress(jdl.encode(), -1)).decode()


def extractJDL(compressedJDL):
    """Return decompressed JDL string."""
    # the starting bracket is guaranteeed by JobManager.submitJob
    # we need the check to be backward compatible
    if isinstance(compressedJDL, bytes):
        if compressedJDL.startswith(b"["):
            return compressedJDL.decode()
    else:
        if compressedJDL.startswith("["):
            return compressedJDL
    return zlib.decompress(base64.b64decode(compressedJDL)).decode()


#############################################################################


class JobDB(DB):
    """Interface to MySQL-based JobDB"""

    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        DB.__init__(self, "JobDB", "WorkloadManagement/JobDB", parentLogger=parentLogger)

        # data member to check if __init__ went through without error
        self.__initialized = False
        self.maxRescheduling = self.getCSOption("MaxRescheduling", 3)

        # loading the function that will be used to determine the platform (it can be VO specific)
        res = ObjectLoader().loadObject("ConfigurationSystem.Client.Helpers.Resources", "getDIRACPlatform")
        if not res["OK"]:
            self.log.fatal(res["Message"])
        self.getDIRACPlatform = res["Value"]

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

        if isinstance(jobID, (str, int)):
            jobID = [jobID]

        jobIDList = []
        for jID in jobID:
            ret = self._escapeString(str(jID))
            if not ret["OK"]:
                return ret
            jobIDList.append(ret["Value"])

        # self.log.debug('JobDB.getParameters: Getting Parameters for jobs %s' % ','.join(jobIDList))

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
                ",".join(jobIDList),
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
            paramCondition = " AND Name in (%s)" % paramNames
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
    def getJobsAttributes(self, jobIDs, attrList=None):
        """Get all Job(s) Attributes for a given list of jobIDs.
        Return a dictionary with all Job Attributes as value pairs
        """

        if not jobIDs:
            return S_OK({})

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
            ret = self._escapeString(x)
            if not ret["OK"]:
                return ret
            x = "`" + ret["Value"][1:-1] + "`"
            attrNameListS.append(x)
        attrNames = "JobID," + ",".join(attrNameListS)

        cmd = "SELECT {} FROM Jobs WHERE JobID IN ({})".format(attrNames, ",".join(str(jobID) for jobID in jobIDs))
        res = self._query(cmd)
        if not res["OK"]:
            return res
        if not res["Value"]:
            return S_OK({})

        attributes = {}
        for t_att in res["Value"]:
            jobID = int(t_att[0])
            attributes.setdefault(jobID, {})
            for tx, ax in zip(t_att[1:], attrList):
                attributes[jobID].setdefault(ax, tx)

        return S_OK(attributes)

    #############################################################################
    def getJobAttributes(self, jobID, attrList=None):
        """Get all Job Attributes for a given jobID.
        Return a dictionary with all Job Attributes as value pairs
        """

        """Get the given attribute of a job specified by its jobID"""

        result = self.getJobsAttributes([jobID], attrList)
        if not result["OK"]:
            return result
        return S_OK(result["Value"].get(jobID, {}))

    #############################################################################
    def getJobAttribute(self, jobID, attribute):
        """Get the given attribute of a job specified by its jobID"""

        result = self.getJobAttributes(jobID, [attribute])
        if not result["OK"]:
            return result
        return S_OK(result["Value"].get(attribute))

    #############################################################################
    def getJobParameter(self, jobID, parameter):
        """Get the given parameter of a job specified by its jobID"""

        result = self.getJobParameters(jobID, [parameter])
        if not result["OK"]:
            return result
        return S_OK(result.get("Value", {}).get(jobID, {}).get(parameter))

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
            cmd = "SELECT Name, Value from OptimizerParameters WHERE JobID=%s" % jobID

        result = self._query(cmd)
        if not result["OK"]:
            return S_ERROR("JobDB.getJobOptParameters: failed to retrieve parameters")
        try:
            jobOptParameters = {name: value.decode() for name, value in result.get("Value", {})}  # account for BLOBs
        except AttributeError:
            jobOptParameters = {name: value for name, value in result.get("Value", {})}
        return S_OK(jobOptParameters)

    #############################################################################

    def getInputData(self, jobID):
        """Get input data for the given job"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]
        cmd = "SELECT LFN FROM InputData WHERE JobID=%s" % jobID
        res = self._query(cmd)
        if not res["OK"]:
            return res

        inputData = [i[0] for i in res["Value"] if i[0].strip()]
        for index, lfn in enumerate(inputData):
            if lfn.lower().startswith("lfn:"):
                inputData[index] = lfn[4:]

        return S_OK(inputData)

    #############################################################################
    def setInputData(self, jobID, inputData):
        """Inserts input data for the given job"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]
        cmd = "DELETE FROM InputData WHERE JobID=%s" % (jobID)
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

        if attrName not in self.jobAttributeNames:
            return S_ERROR(EWMSJMAN, "Request to set non-existing job attribute")

        if attrName == "Status":
            # Treat this update separately
            res = self.setJobsMajorStatus([jobID], attrValue, force=force)
            if not res["OK"]:
                return res
            if update:
                cmd = "UPDATE Jobs SET LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%s" % jobID
                if myDate:
                    cmd += " AND LastUpdateTime < %s" % myDate
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
            cmd += " AND LastUpdateTime < %s" % myDate

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

        jobIDList = jobID
        if not isinstance(jobID, (list, tuple)):
            jobIDList = [jobID]

        jIDList = []
        for jID in jobIDList:
            ret = self._escapeString(jID)
            if not ret["OK"]:
                return ret
            jIDList.append(ret["Value"])

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
            attr.append("{}={}".format(name, ret["Value"]))
        if update:
            attr.append("LastUpdateTime=UTC_TIMESTAMP()")
        if not attr:
            return S_ERROR("JobDB.setAttributes: Nothing to do")

        cmd = "UPDATE Jobs SET {} WHERE JobID in ( {} )".format(", ".join(attr), ", ".join(jIDList))

        if myDate:
            cmd += " AND LastUpdateTime < %s" % myDate

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

    def setJobStatus(self, jobID, status="", minorStatus="", applicationStatus=""):
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

        result = self.setJobAttributes(jobID, attrNames, attrValues, update=update_flag)
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
    def setJobParameter(self, jobID, key, value):
        """Set a parameter specified by name,value pair for the job JobID"""

        ret = self._escapeString(key)
        if not ret["OK"]:
            return ret
        e_key = ret["Value"]
        ret = self._escapeString(value)
        if not ret["OK"]:
            return ret
        e_value = ret["Value"]

        cmd = "REPLACE JobParameters (JobID,Name,Value) VALUES (%d,%s,%s)" % (int(jobID), e_key, e_value)
        return self._update(cmd)

    #############################################################################
    def setJobParameters(self, jobID, parameters):
        """Set parameters specified by a list of name/value pairs for the job JobID

        :param int jobID: Job ID
        :param list parameters: list of tuples (name, value) pairs

        :return: S_OK/S_ERROR
        """

        if not parameters:
            return S_OK()

        insertValueList = []
        for name, value in parameters:
            ret = self._escapeString(name)
            if not ret["OK"]:
                return ret
            e_name = ret["Value"]
            ret = self._escapeString(value)
            if not ret["OK"]:
                return ret
            e_value = ret["Value"]
            insertValueList.append(f"({jobID},{e_name},{e_value})")

        cmd = "REPLACE JobParameters (JobID,Name,Value) VALUES %s" % ", ".join(insertValueList)
        return self._update(cmd)

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
    def __setInitialJobParameters(self, classadJob, jobID):
        """Set initial job parameters as was defined in the Classad"""

        # Extract initital job parameters
        parameters = {}
        if classadJob.lookupAttribute("Parameters"):
            parameters = classadJob.getDictionaryFromSubJDL("Parameters")
        return self.setJobParameters(jobID, list(parameters.items()))

    #############################################################################
    def setJobJDL(self, jobID, jdl=None, originalJDL=None):
        """Insert JDL's for job specified by jobID"""
        ret = self._escapeString(jobID)
        if not ret["OK"]:
            return ret
        jobID = ret["Value"]

        req = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%s" % jobID
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
            return S_ERROR("%s" % err)

        jobID = int(result["lastRowId"])

        self.log.info("JobDB: New JobID served", "%s" % jobID)

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
            cmd = "SELECT OriginalJDL FROM JobJDLs WHERE JobID=%s" % jobID
        else:
            cmd = "SELECT JDL FROM JobJDLs WHERE JobID=%s" % jobID

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
        ownerDN,
        ownerGroup,
        initialStatus=JobStatus.RECEIVED,
        initialMinorStatus="Job accepted",
    ):
        """Insert the initial JDL into the Job database,
        Do initial JDL crosscheck,
        Set Initial job Attributes and Status

        :param str jdl: job description JDL
        :param str owner: job owner user name
        :param str ownerDN: job owner DN
        :param str ownerGroup: job owner group
        :param str initialStatus: optional initial job status (Received by default)
        :param str initialMinorStatus: optional initial minor job status
        :return: new job ID
        """
        jobManifest = JobManifest()
        result = jobManifest.load(jdl)
        if not result["OK"]:
            return result
        jobManifest.setOptionsFromDict({"Owner": owner, "OwnerDN": ownerDN, "OwnerGroup": ownerGroup})
        result = jobManifest.check()
        if not result["OK"]:
            return result
        jobAttrNames = []
        jobAttrValues = []

        # 1.- insert original JDL on DB and get new JobID
        # Fix the possible lack of the brackets in the JDL
        if jdl.strip()[0].find("[") != 0:
            jdl = "[" + jdl + "]"
        result = self.__insertNewJDL(jdl)
        if not result["OK"]:
            return S_ERROR(EWMSSUBM, "Failed to insert JDL in to DB")
        jobID = result["Value"]

        jobManifest.setOption("JobID", jobID)

        jobAttrNames.append("JobID")
        jobAttrValues.append(jobID)

        jobAttrNames.append("LastUpdateTime")
        jobAttrValues.append(str(datetime.datetime.utcnow()))

        jobAttrNames.append("SubmissionTime")
        jobAttrValues.append(str(datetime.datetime.utcnow()))

        jobAttrNames.append("Owner")
        jobAttrValues.append(owner)

        jobAttrNames.append("OwnerDN")
        jobAttrValues.append(ownerDN)

        jobAttrNames.append("OwnerGroup")
        jobAttrValues.append(ownerGroup)

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
            jobAttrNames.append("Status")
            jobAttrValues.append(JobStatus.FAILED)

            jobAttrNames.append("MinorStatus")
            jobAttrValues.append("Error in JDL syntax")

            result = self.insertFields("Jobs", jobAttrNames, jobAttrValues)
            if not result["OK"]:
                return result

            retVal["Status"] = JobStatus.FAILED
            retVal["MinorStatus"] = "Error in JDL syntax"
            return retVal

        classAdJob.insertAttributeInt("JobID", jobID)
        result = self.__checkAndPrepareJob(
            jobID, classAdJob, classAdReq, owner, ownerDN, ownerGroup, jobAttrNames, jobAttrValues
        )
        if not result["OK"]:
            return result

        priority = classAdJob.getAttributeInt("Priority")
        if priority is None:
            priority = 0
        jobAttrNames.append("UserPriority")
        jobAttrValues.append(priority)

        for jdlName in self.jdl2DBParameters:
            # Defaults are set by the DB.
            jdlValue = classAdJob.getAttributeString(jdlName)
            if jdlValue:
                jobAttrNames.append(jdlName)
                jobAttrValues.append(jdlValue)

        jdlValue = classAdJob.getAttributeString("Site")
        if jdlValue:
            jobAttrNames.append("Site")
            if jdlValue.find(",") != -1:
                jobAttrValues.append("Multiple")
            else:
                jobAttrValues.append(jdlValue)

        jobAttrNames.append("VerifiedFlag")
        jobAttrValues.append("True")

        jobAttrNames.append("Status")
        jobAttrValues.append(initialStatus)

        jobAttrNames.append("MinorStatus")
        jobAttrValues.append(initialMinorStatus)

        reqJDL = classAdReq.asJDL()
        classAdJob.insertAttributeInt("JobRequirements", reqJDL)

        jobJDL = classAdJob.asJDL()

        result = self.setJobJDL(jobID, jobJDL)
        if not result["OK"]:
            return result

        # Adding the job in the Jobs table
        result = self.insertFields("Jobs", jobAttrNames, jobAttrValues)
        if not result["OK"]:
            return result

        # Setting the Job parameters
        result = self.__setInitialJobParameters(classAdJob, jobID)
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
            cmd = "INSERT INTO InputData (JobID,LFN) VALUES %s" % ", ".join(values)
            result = self._update(cmd)
            if not result["OK"]:
                return result

        retVal["Status"] = initialStatus
        retVal["MinorStatus"] = initialMinorStatus
        retVal["TimeStamp"] = str(datetime.datetime.utcnow())

        return retVal

    def __checkAndPrepareJob(
        self, jobID, classAdJob, classAdReq, owner, ownerDN, ownerGroup, jobAttrNames, jobAttrValues
    ):
        """
        Check Consistency of Submitted JDL and set some defaults
        Prepare subJDL with Job Requirements
        """
        error = ""
        vo = getVOForGroup(ownerGroup)

        jdlOwner = classAdJob.getAttributeString("Owner")
        jdlOwnerDN = classAdJob.getAttributeString("OwnerDN")
        jdlOwnerGroup = classAdJob.getAttributeString("OwnerGroup")
        jdlVO = classAdJob.getAttributeString("VirtualOrganization")

        if jdlOwner and jdlOwner != owner:
            error = "Wrong Owner in JDL"
        elif jdlOwnerDN and jdlOwnerDN != ownerDN:
            error = "Wrong Owner DN in JDL"
        elif jdlOwnerGroup and jdlOwnerGroup != ownerGroup:
            error = "Wrong Owner Group in JDL"
        elif jdlVO and jdlVO != vo:
            error = "Wrong Virtual Organization in JDL"

        classAdJob.insertAttributeString("Owner", owner)
        classAdJob.insertAttributeString("OwnerDN", ownerDN)
        classAdJob.insertAttributeString("OwnerGroup", ownerGroup)

        if vo:
            classAdJob.insertAttributeString("VirtualOrganization", vo)

        classAdReq.insertAttributeString("OwnerDN", ownerDN)
        classAdReq.insertAttributeString("OwnerGroup", ownerGroup)
        if vo:
            classAdReq.insertAttributeString("VirtualOrganization", vo)

        inputDataPolicy = Operations(vo=vo).getValue("InputDataPolicy/InputDataModule")
        if inputDataPolicy and not classAdJob.lookupAttribute("InputDataModule"):
            classAdJob.insertAttributeString("InputDataModule", inputDataPolicy)

        # priority
        priority = classAdJob.getAttributeInt("Priority")
        if priority is None:
            priority = 0
        classAdReq.insertAttributeInt("UserPriority", priority)

        # CPU time
        cpuTime = classAdJob.getAttributeInt("CPUTime")
        if cpuTime is None:
            opsHelper = Operations(group=ownerGroup)
            cpuTime = opsHelper.getValue("JobDescription/DefaultCPUTime", 86400)
        classAdReq.insertAttributeInt("CPUTime", cpuTime)

        # platform(s)
        platformList = classAdJob.getListFromExpression("Platform")
        if platformList:
            result = self.getDIRACPlatform(platformList)
            if not result["OK"]:
                return result
            if result["Value"]:
                classAdReq.insertAttributeVectorString("Platforms", result["Value"])
            else:
                error = "OS compatibility info not found"

        if error:
            retVal = S_ERROR(EWMSSUBM, error)
            retVal["JobId"] = jobID
            retVal["Status"] = JobStatus.FAILED
            retVal["MinorStatus"] = error

            jobAttrNames.append("Status")
            jobAttrValues.append(JobStatus.FAILED)

            jobAttrNames.append("MinorStatus")
            jobAttrValues.append(error)
            resultInsert = self.setJobAttributes(jobID, jobAttrNames, jobAttrValues)
            if not resultInsert["OK"]:
                retVal["MinorStatus"] += "; %s" % resultInsert["Message"]

            return retVal

        return S_OK()

    #############################################################################
    def removeJobFromDB(self, jobIDs):
        """
        Remove jobs from the Job DB and clean up all the job related data in various tables
        """

        # ret = self._escapeString(jobID)
        # if not ret['OK']:
        #  return ret
        # e_jobID = ret['Value']

        if not jobIDs:
            return S_OK()

        if not isinstance(jobIDs, list):
            jobIDList = [jobIDs]
        else:
            jobIDList = jobIDs

        failedTablesList = []
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

            cmd = "DELETE FROM {} WHERE JobID in ({})".format(table, ",".join(str(j) for j in jobIDList))
            result = self._update(cmd)
            if not result["OK"]:
                failedTablesList.append(table)

        result = S_OK()
        if failedTablesList:
            result = S_ERROR("Errors while job removal (tables %s)" % ",".join(failedTablesList))

        return result

    #################################################################
    def rescheduleJobs(self, jobIDs):
        """Reschedule all the jobs in the given list"""

        result = S_OK()

        failedJobs = []
        for jobID in jobIDs:
            result = self.rescheduleJob(jobID)
            if not result["OK"]:
                failedJobs.append(jobID)

        if failedJobs:
            result = S_ERROR("JobDB.rescheduleJobs: Not all the jobs were rescheduled")
            result["FailedJobs"] = failedJobs

        return result

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
                "OwnerDN",
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

        jobAttrNames = []
        jobAttrValues = []

        jobAttrNames.append("RescheduleCounter")
        jobAttrValues.append(rescheduleCounter)

        # Save the job parameters for later debugging
        result = JobMonitoringClient().getJobParameters(jobID)
        if result["OK"]:
            parDict = result["Value"]
            for key, value in parDict.get(jobID, {}).items():
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
            resultDict["OwnerDN"],
            resultDict["OwnerGroup"],
            jobAttrNames,
            jobAttrValues,
        )

        if not result["OK"]:
            return result

        priority = classAdJob.getAttributeInt("Priority")
        if priority is None:
            priority = 0
        jobAttrNames.append("UserPriority")
        jobAttrValues.append(priority)

        siteList = classAdJob.getListFromExpression("Site")
        if not siteList:
            site = "ANY"
        elif len(siteList) > 1:
            site = "Multiple"
        else:
            site = siteList[0]

        jobAttrNames.append("Site")
        jobAttrValues.append(site)

        jobAttrNames.append("Status")
        jobAttrValues.append(JobStatus.RECEIVED)

        jobAttrNames.append("MinorStatus")
        jobAttrValues.append(JobMinorStatus.RESCHEDULED)

        jobAttrNames.append("ApplicationStatus")
        jobAttrValues.append("Unknown")

        jobAttrNames.append("ApplicationNumStatus")
        jobAttrValues.append(0)

        jobAttrNames.append("LastUpdateTime")
        jobAttrValues.append(str(datetime.datetime.utcnow()))

        jobAttrNames.append("RescheduleTime")
        jobAttrValues.append(str(datetime.datetime.utcnow()))

        reqJDL = classAdReq.asJDL()
        classAdJob.insertAttributeInt("JobRequirements", reqJDL)

        jobJDL = classAdJob.asJDL()

        # Replace the JobID placeholder if any
        if jobJDL.find("%j") != -1:
            jobJDL = jobJDL.replace("%j", str(jobID))

        result = self.setJobJDL(jobID, jobJDL)
        if not result["OK"]:
            return result

        result = self.__setInitialJobParameters(classAdJob, jobID)
        if not result["OK"]:
            return result

        result = self.setJobAttributes(jobID, jobAttrNames, jobAttrValues, force=True)
        if not result["OK"]:
            return result

        retVal["InputData"] = classAdJob.lookupAttribute("InputData")
        retVal["RescheduleCounter"] = rescheduleCounter
        retVal["Status"] = JobStatus.RECEIVED
        retVal["MinorStatus"] = JobMinorStatus.RESCHEDULED

        return retVal

    #############################################################################
    def getUserSitesTuple(self, sites):
        """Returns tuple of active/banned/invalid sties from a user provided list."""
        ret = self._escapeValues(sites)
        if not ret["OK"]:
            return ret

        sites = set(sites)
        sitesSql = ret["Value"]
        sitesSql[0] = "SELECT %s AS Site" % sitesSql[0]
        sitesSql = " UNION SELECT ".join(sitesSql)
        cmd = "SELECT Site FROM (%s) " % sitesSql
        cmd += "AS tmptable WHERE Site NOT IN (SELECT Site FROM SiteMask WHERE Status='Active')"
        result = self._query(cmd)
        if not result["OK"]:
            return result
        nonActiveSites = {x[0] for x in result["Value"]}
        activeSites = sites.difference(nonActiveSites)
        bannedSites = nonActiveSites.intersection(set(self.getSiteMask("Banned")))
        invalidSites = nonActiveSites.difference(bannedSites)
        return S_OK((activeSites, bannedSites, invalidSites))

    #############################################################################
    def getSiteMask(self, siteState="Active"):
        """Get the currently active site list"""

        ret = self._escapeString(siteState)
        if not ret["OK"]:
            return ret
        siteState = ret["Value"]

        if siteState == "All":
            cmd = "SELECT Site FROM SiteMask"
        else:
            cmd = "SELECT Site FROM SiteMask WHERE Status=%s" % siteState

        result = self._query(cmd)
        siteList = []
        if result["OK"]:
            siteList = [x[0] for x in result["Value"]]
        else:
            return S_ERROR(DErrno.EMYSQL, "SQL query failed: %s" % cmd)

        return S_OK(siteList)

    #############################################################################
    def getSiteMaskStatus(self, sites=None):
        """Get the current site mask status

        :param sites: A string for a single site to check, or a list to check multiple sites.
        :returns: If input was a list, a dictionary of sites, keys are site
                 names and values are the site statuses. Unknown sites are
                 not included in the output dictionary.
                 If input was a string, then a single value with that site's
                 status, or S_ERROR if the site does not exist in the DB.
        """
        if isinstance(sites, list):
            safeSites = []
            for site in sites:
                res = self._escapeString(site)
                if not res["OK"]:
                    return res
                safeSites.append(res["Value"])
            sitesString = ",".join(safeSites)
            cmd = "SELECT Site, Status FROM SiteMask WHERE Site in (%s)" % sitesString

            result = self._query(cmd)
            return S_OK(dict(result["Value"]))

        elif isinstance(sites, str):

            ret = self._escapeString(sites)
            if not ret["OK"]:
                return ret
            cmd = "SELECT Status FROM SiteMask WHERE Site=%s" % ret["Value"]
            result = self._query(cmd)
            if result["Value"]:
                return S_OK(result["Value"][0][0])
            return S_ERROR("Unknown site %s" % sites)

        else:
            cmd = "SELECT Site,Status FROM SiteMask"

        result = self._query(cmd)
        siteDict = {}
        if result["OK"]:
            for site, status in result["Value"]:
                siteDict[site] = status
        else:
            return S_ERROR(DErrno.EMYSQL, "SQL query failed: %s" % cmd)

        return S_OK(siteDict)

    #############################################################################
    def getAllSiteMaskStatus(self):
        """Get the everything from site mask status"""
        cmd = "SELECT Site,Status,LastUpdateTime,Author,Comment FROM SiteMask"

        result = self._query(cmd)

        if not result["OK"]:
            return result["Message"]

        siteDict = {}
        if result["OK"]:
            for site, status, lastUpdateTime, author, comment in result["Value"]:
                try:
                    # TODO: This is only needed in DIRAC v8.0.x while moving from BLOB -> TEXT
                    comment = comment.decode()
                except AttributeError:
                    pass
                siteDict[site] = status, lastUpdateTime, author, comment

        return S_OK(siteDict)

    #############################################################################
    def setSiteMask(self, siteMaskList, authorDN="Unknown", comment="No comment"):
        """Set the Site Mask to the given mask in a form of a list of tuples (site,status)"""

        for site, status in siteMaskList:
            result = self.__setSiteStatusInMask(site, status, authorDN, comment)
            if not result["OK"]:
                return result

        return S_OK()

    #############################################################################
    def __setSiteStatusInMask(self, site, status, authorDN="Unknown", comment="No comment"):
        """Set the given site status to 'status' or add a new active site"""

        result = self._escapeString(site)
        if not result["OK"]:
            return result
        site = result["Value"]

        result = self._escapeString(status)
        if not result["OK"]:
            return result
        status = result["Value"]

        result = self._escapeString(authorDN)
        if not result["OK"]:
            return result
        authorDN = result["Value"]

        result = self._escapeString(comment)
        if not result["OK"]:
            return result
        comment = result["Value"]

        req = "SELECT Status FROM SiteMask WHERE Site=%s" % site
        result = self._query(req)
        if result["OK"]:
            if result["Value"]:
                current_status = result["Value"][0][0]
                if current_status == status:
                    return S_OK()
                else:
                    req = (
                        "UPDATE SiteMask SET Status=%s,LastUpdateTime=UTC_TIMESTAMP(),"
                        "Author=%s, Comment=%s WHERE Site=%s"
                    )
                    req = req % (status, authorDN, comment, site)
            else:
                req = f"INSERT INTO SiteMask VALUES ({site},{status},UTC_TIMESTAMP(),{authorDN},{comment})"
            result = self._update(req)
            if not result["OK"]:
                return S_ERROR("Failed to update the Site Mask")
            # update the site mask logging record
            req = f"INSERT INTO SiteMaskLogging VALUES ({site},{status},UTC_TIMESTAMP(),{authorDN},{comment})"
            result = self._update(req)
            if not result["OK"]:
                self.log.warn("Failed to update site mask logging", "for %s" % site)
        else:
            return S_ERROR("Failed to get the Site Status from the Mask")

        return S_OK()

    #############################################################################
    def banSiteInMask(self, site, authorDN="Unknown", comment="No comment"):
        """Forbid the given site in the Site Mask"""

        return self.__setSiteStatusInMask(site, "Banned", authorDN, comment)

    #############################################################################
    def allowSiteInMask(self, site, authorDN="Unknown", comment="No comment"):
        """Forbid the given site in the Site Mask"""

        return self.__setSiteStatusInMask(site, "Active", authorDN, comment)

    #############################################################################
    def removeSiteFromMask(self, site=None):
        """Remove the given site from the mask"""
        if not site:
            req = "DELETE FROM SiteMask"
        else:
            ret = self._escapeString(site)
            if not ret["OK"]:
                return ret
            site = ret["Value"]
            req = "DELETE FROM SiteMask WHERE Site=%s" % site

        return self._update(req)

    #############################################################################
    def getSiteMaskLogging(self, siteList):
        """Get the site mask logging history for the list if site names"""

        if siteList:
            siteString = ",".join(["'" + x + "'" for x in siteList])
            req = f"SELECT Site,Status,UpdateTime,Author,Comment FROM SiteMaskLogging WHERE Site in ({siteString})"
        else:
            req = "SELECT Site,Status,UpdateTime,Author,Comment FROM SiteMaskLogging"
        req += " ORDER BY UpdateTime ASC"

        result = self._query(req)
        if not result["OK"]:
            return result

        availableSiteList = []
        for site, status, utime, author, comment in result["Value"]:
            availableSiteList.append(site)

        resultDict = {}
        for site in siteList:
            if not result["Value"] or site not in availableSiteList:
                ret = self._escapeString(site)
                if not ret["OK"]:
                    continue
                e_site = ret["Value"]
                req = f"SELECT Status Site,Status,LastUpdateTime,Author,Comment FROM SiteMask WHERE Site={e_site}"
                resSite = self._query(req)
                if resSite["OK"]:
                    if resSite["Value"]:
                        site, status, lastUpdate, author, comment = resSite["Value"][0]
                        try:
                            # TODO: This is only needed in DIRAC v8.0.x while moving from BLOB -> TEXT
                            comment = comment.decode()
                        except AttributeError:
                            pass
                        resultDict[site] = [[status, str(lastUpdate), author, comment]]
                    else:
                        resultDict[site] = [["Unknown", "", "", "Site not present in logging table"]]

        for site, status, utime, author, comment in result["Value"]:
            if site not in resultDict:
                resultDict[site] = []
            try:
                # TODO: This is only needed in DIRAC v8.0.x while moving from BLOB -> TEXT
                comment = comment.decode()
            except AttributeError:
                pass
            resultDict[site].append([status, str(utime), author, comment])

        return S_OK(resultDict)

    #############################################################################
    def getSiteSummary(self):
        """Get the summary of jobs in a given status on all the sites"""

        waitingList = ['"Submitted"', '"Assigned"', '"Waiting"', '"Matched"']
        waitingString = ",".join(waitingList)

        result = self.getDistinctJobAttributes("Site")
        if not result["OK"]:
            return result

        siteList = result["Value"]
        siteDict = {}
        totalDict = {
            JobStatus.WAITING: 0,
            JobStatus.RUNNING: 0,
            JobStatus.STALLED: 0,
            JobStatus.DONE: 0,
            JobStatus.FAILED: 0,
        }

        for site in siteList:
            if site == "ANY":
                continue
            # Waiting
            siteDict[site] = {}
            ret = self._escapeString(site)
            if not ret["OK"]:
                return ret
            e_site = ret["Value"]

            req = f"SELECT COUNT(JobID) FROM Jobs WHERE Status IN ({waitingString}) AND Site={e_site}"
            result = self._query(req)
            if result["OK"]:
                count = result["Value"][0][0]
            else:
                return S_ERROR("Failed to get Site data from the JobDB")
            siteDict[site][JobStatus.WAITING] = count
            totalDict[JobStatus.WAITING] += count
            # Running,Stalled,Done,Failed
            for status in [
                '"%s"' % JobStatus.RUNNING,
                '"%s"' % JobStatus.STALLED,
                '"%s"' % JobStatus.DONE,
                '"%s"' % JobStatus.FAILED,
            ]:
                req = f"SELECT COUNT(JobID) FROM Jobs WHERE Status={status} AND Site={e_site}"
                result = self._query(req)
                if result["OK"]:
                    count = result["Value"][0][0]
                else:
                    return S_ERROR("Failed to get Site data from the JobDB")
                siteDict[site][status.replace('"', "")] = count
                totalDict[status.replace('"', "")] += count

        siteDict["Total"] = totalDict
        return S_OK(siteDict)

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
            rList.append("%.1f" % (efficiency * 100.0))
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
            requestedFields = ["Status", "MinorStatus", "Site", "Owner", "OwnerGroup", "JobGroup", "JobSplitType"]
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
