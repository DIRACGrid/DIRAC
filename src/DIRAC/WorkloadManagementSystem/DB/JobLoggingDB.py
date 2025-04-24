""" JobLoggingDB class is a front-end to the Job Logging Database.
    The following methods are provided

    addLoggingRecord()
    getJobLoggingInfo()
    deleteJob()
    getWMSTimeStamps()
"""
import datetime

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise, convertToReturnValue
from DIRAC.FrameworkSystem.Client.Logger import contextLogger

MAGIC_EPOC_NUMBER = 1270000000

#############################################################################


class JobLoggingDB(DB):
    """Frontend to JobLoggingDB MySQL table"""

    def __init__(self, parentLogger=None):
        """Standard Constructor"""

        DB.__init__(self, "JobLoggingDB", "WorkloadManagement/JobLoggingDB", parentLogger=parentLogger)
        self._defaultLogger = self.log

    @property
    def log(self):
        return contextLogger.get() or self._defaultLogger

    @log.setter
    def log(self, value):
        self._defaultLogger = value

    #############################################################################
    def addLoggingRecord(
        self,
        jobID,
        status="idem",
        minorStatus="idem",
        applicationStatus="idem",
        date=None,
        source="Unknown",
    ):
        """Add a new entry to the JobLoggingDB table. One, two or all the three status
        components (status, minorStatus, applicationStatus) can be specified.
        Optionally the time stamp of the status can
        be provided in a form of a string in a format '%Y-%m-%d %H:%M:%S' or
        as datetime.datetime object. If the time stamp is not provided the current
        UTC time is used.
        """

        event = f"status/minor/app={status}/{minorStatus}/{applicationStatus}"
        self.log.info("Adding record for job ", str(jobID) + ": '" + event + "' from " + source)

        def _get_date(date):
            # We need to specify that timezone is UTC because otherwise timestamp
            # assumes local time while we mean UTC.
            if not date:
                # Make the UTC datetime
                return datetime.datetime.utcnow()
            elif isinstance(date, str):
                # The date is provided as a string in UTC
                return TimeUtilities.fromString(date)
            elif isinstance(date, datetime.datetime):
                return date
            else:
                raise Exception("Incorrect date for the logging record")

        try:
            if isinstance(date, list):
                _date = []
                for d in date:
                    try:
                        _date.append(_get_date(d))
                    except Exception:
                        self.log.exception("Exception while date evaluation")
                        _date.append(datetime.datetime.utcnow())
            else:
                _date = _get_date(date)
        except Exception:
            self.log.exception("Exception while date evaluation")
            _date = [datetime.datetime.utcnow()]

        cmd = (
            "INSERT INTO LoggingInfo (JobId, Status, MinorStatus, ApplicationStatus, "
            + "StatusTime, StatusTimeOrder, StatusSource) VALUES "
        )

        if not isinstance(jobID, list):
            jobID = [jobID]

        if isinstance(status, str):
            status = [status] * len(jobID)
        if isinstance(minorStatus, str):
            minorStatus = [minorStatus] * len(jobID)
        if isinstance(applicationStatus, str):
            applicationStatus = [applicationStatus[:255]] * len(jobID)
        if isinstance(_date, datetime.datetime):
            _date = [_date] * len(jobID)

        epocs = []
        for dt in _date:
            epoc = dt.replace(tzinfo=datetime.timezone.utc).timestamp() - MAGIC_EPOC_NUMBER
            epocs.append(epoc)
        cmd = cmd + "(%s, %s, %s, %s, %s, %s, %s)"
        data = list(
            zip(jobID, status, minorStatus, applicationStatus, _date, epocs, [source[:32]] * len(jobID), strict=True)
        )
        return self._updatemany(cmd, data)

    #############################################################################
    def getJobLoggingInfo(self, jobID):
        """Returns a Status,MinorStatus,ApplicationStatus,StatusTime,StatusSource tuple
        for each record found for job specified by its jobID in historical order
        """

        cmd = (
            "SELECT Status,MinorStatus,ApplicationStatus,StatusTime,StatusSource FROM"
            " LoggingInfo WHERE JobId=%d ORDER BY StatusTimeOrder,StatusTime" % int(jobID)
        )

        result = self._query(cmd)
        if not result["OK"]:
            return result
        if result["OK"] and not result["Value"]:
            return S_ERROR("No Logging information for job %d" % int(jobID))

        return_value = []
        status, minor, app = result["Value"][0][:3]
        if app == "idem":
            app = "Unknown"
        for row in result["Value"]:
            if row[0] != "idem":
                status = row[0]
            if row[1] != "idem":
                minor = row[1]
            if row[2] != "idem":
                app = row[2]
            return_value.append((status, minor, app, str(row[3]), row[4]))

        return S_OK(return_value)

    #############################################################################
    @convertToReturnValue
    def deleteJob(self, jobID):
        """Delete logging records for given jobs"""
        if not jobID:
            return None

        if isinstance(jobID, int):
            jobList = [jobID]
        elif isinstance(jobID, str):
            jobList = jobID.replace(" ", "").split(",")
        else:
            jobList = jobID

        sqlCmd = (
            "CREATE TEMPORARY TABLE to_delete_LoggingInfo (JobID INTEGER NOT NULL, PRIMARY KEY (JobID)) ENGINE=MEMORY;"
        )
        returnValueOrRaise(self._update(sqlCmd))
        try:
            sqlCmd = "INSERT INTO to_delete_LoggingInfo (JobID) VALUES ( %s )"
            returnValueOrRaise(self._updatemany(sqlCmd, [(j,) for j in jobList]))
            sqlCmd = "DELETE l from `LoggingInfo` l JOIN to_delete_LoggingInfo t USING (JobID)"
            result = returnValueOrRaise(self._update(sqlCmd))
        finally:
            sqlCmd = "DROP TEMPORARY TABLE to_delete_LoggingInfo"
            returnValueOrRaise(self._update(sqlCmd))

        return result

    #############################################################################
    def getWMSTimeStamps(self, jobID):
        """Get TimeStamps for job MajorState transitions
        return a {State:timestamp} dictionary
        """
        # self.log.debug('getWMSTimeStamps: Retrieving Timestamps for Job %d' % int(jobID))

        result = {}
        cmd = "SELECT Status,StatusTimeOrder FROM LoggingInfo WHERE JobID=%d ORDER BY StatusTimeOrder" % int(jobID)
        resCmd = self._query(cmd)
        if not resCmd["OK"]:
            return resCmd
        if not resCmd["Value"]:
            return S_ERROR("No Logging Info for job %d" % int(jobID))

        for event, etime in resCmd["Value"]:
            result[event] = str(etime + MAGIC_EPOC_NUMBER)

        # Get last date and time
        cmd = "SELECT MAX(StatusTime) FROM LoggingInfo WHERE JobID=%d" % int(jobID)
        resCmd = self._query(cmd)
        if not resCmd["OK"]:
            return resCmd
        if resCmd["Value"]:
            result["LastTime"] = str(resCmd["Value"][0][0])
        else:
            result["LastTime"] = "Unknown"

        return S_OK(result)
