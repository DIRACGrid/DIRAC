""" JobReport class encapsulates various methods of the job status reporting.
    It's an interface to JobStateUpdateClient, used when bulk submission is needed.
"""
import datetime
import decimal
import math
from collections import defaultdict

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient


def isFiniteParameterValue(value):
    """Check that a job parameter value holds no non-finite number.

    Non-finite floats (NaN, +/-Infinity) have no representation in JSON: they make
    the encoded payload invalid, which the receiving side rejects outright. Values
    are inspected recursively, since a parameter may well be a container.

    :param value: any job parameter value
    :return: False if a NaN or an infinity is found anywhere in `value`
    """
    # bool and int are exact and always finite, and math.isfinite() would raise
    # OverflowError on a large enough int
    if isinstance(value, int):
        return True
    if isinstance(value, (float, decimal.Decimal)):
        try:
            return math.isfinite(value)
        except (TypeError, ValueError):
            # e.g. decimal.Decimal("sNaN"), which cannot even be converted to float
            return False
    if isinstance(value, dict):
        return all(isFiniteParameterValue(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return all(isFiniteParameterValue(item) for item in value)
    return True


class JobReport:
    """
    .. class:: JobReport
    """

    def __init__(self, jobid, source=""):
        """c'tor"""
        self.jobStatusInfo = []  # where job status updates are cumulated
        self.appStatusInfo = []  # where application status updates are cumulated
        self.jobParameters = []  # where job parameters are cumulated
        self.jobID = int(jobid)
        self.source = source
        if not source:
            self.source = "Job_%d" % self.jobID
        self.log = gLogger.getSubLogger(self.__class__.__name__)

    def setJob(self, jobID):
        """Set the job ID for which to send reports"""
        self.jobID = jobID

    def setJobStatus(self, status="", minorStatus="", applicationStatus="", sendFlag=True):
        """Accumulate and possibly send job status information to the JobState service"""
        timeStamp = str(datetime.datetime.utcnow())
        # add job status record
        self.jobStatusInfo.append((status.replace("'", ""), minorStatus.replace("'", ""), timeStamp))
        if applicationStatus:
            self.appStatusInfo.append((applicationStatus.replace("'", ""), timeStamp))
        if sendFlag and self.jobID:
            # and send
            return self.sendStoredStatusInfo()

        return S_OK()

    def setApplicationStatus(self, appStatus, sendFlag=True):
        """Send application status information to the JobState service for jobID"""
        timeStamp = str(datetime.datetime.utcnow())
        # add Application status record
        if not isinstance(appStatus, str):
            appStatus = repr(appStatus)
        self.appStatusInfo.append((appStatus.replace("'", ""), timeStamp))
        if sendFlag and self.jobID:
            # and send
            return self.sendStoredStatusInfo()

        return S_OK()

    def setJobParameter(self, par_name, par_value, sendFlag=True):
        """Set job parameter for jobID"""
        return self.setJobParameters([(par_name, par_value)], sendFlag)

    def setJobParameters(self, parameters, sendFlag=True):
        """Set job parameters for jobID"""
        for pname, pvalue in parameters:
            if self._isValidParameterValue(pname, pvalue):
                self.jobParameters.append((pname, pvalue))

        if sendFlag and self.jobID:
            # and send
            return self.sendStoredJobParameters()

        return S_OK()

    def _isValidParameterValue(self, par_name, par_value):
        """Check that a parameter value can be reported.

        Non-finite floats (NaN, +/-Infinity) cannot be represented in JSON nor
        stored in the job parameters backends, so they are dropped here with a
        warning rather than failing the whole parameters update.
        """
        if not isFiniteParameterValue(par_value):
            self.log.warn("Dropping non-finite value for job parameter", f"{par_name} = {par_value}")
            return False
        return True

    def sendStoredStatusInfo(self):
        """Send the job status information stored in the internal cache"""

        statusDict = defaultdict(lambda: {"Source": self.source})
        for status, minor, dtime in self.jobStatusInfo:
            # No need to send empty items in dictionary
            if status:
                statusDict[dtime]["Status"] = status
            if minor:
                statusDict[dtime]["MinorStatus"] = minor
        for appStatus, dtime in self.appStatusInfo:
            # No need to send empty items in dictionary
            if appStatus:
                statusDict[dtime]["ApplicationStatus"] = appStatus

        if statusDict:
            result = JobStateUpdateClient().setJobStatusBulk(self.jobID, dict(statusDict), False)
            if result["OK"]:
                # Empty the internal status containers
                self.jobStatusInfo = []
                self.appStatusInfo = []
            return result

        else:
            return S_OK("Empty")

    def sendStoredJobParameters(self):
        """Send the job parameters stored in the internal cache"""

        if self.jobParameters:
            result = JobStateUpdateClient().setJobParameters(self.jobID, self.jobParameters)
            if result["OK"]:
                # Empty the internal parameter container
                self.jobParameters = []
            return result
        else:
            return S_OK("Empty")

    def commit(self):
        """Send all the accumulated information"""

        success = True
        result = self.sendStoredStatusInfo()
        success &= result["OK"]
        result = self.sendStoredJobParameters()
        success &= result["OK"]

        if success:
            return S_OK()
        return S_ERROR("Information upload to JobStateUpdate service failed")

    def dump(self):
        """Print out the contents of the internal cached information"""

        print("Job status info:")
        for status, minor, timeStamp in self.jobStatusInfo:
            print(status.ljust(20), minor.ljust(30), timeStamp)

        print("Application status info:")
        for status, timeStamp in self.appStatusInfo:
            print(status.ljust(20), timeStamp)

        print("Job parameters:")
        for pname, pvalue in self.jobParameters:
            print(pname.ljust(20), pvalue.ljust(30))

    def generateForwardDISET(self):
        """Generate and return failover requests for the operations in the internal cache"""
        forwardDISETOp = None

        result = self.sendStoredStatusInfo()
        if not result["OK"]:
            gLogger.error("Error while sending the job status", result["Message"])
            if "rpcStub" in result:
                rpcStub = result["rpcStub"]

                forwardDISETOp = Operation()
                forwardDISETOp.Type = "ForwardDISET"
                forwardDISETOp.Arguments = DEncode.encode(rpcStub)

            else:
                return S_ERROR("Could not create ForwardDISET operation")

        return S_OK(forwardDISETOp)
