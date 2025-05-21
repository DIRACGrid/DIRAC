""" JobMonitoringHandler is the implementation of the JobMonitoring service
    in the DISET framework

    The following methods are available in the Service interface
"""

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.JEncode import strToIntDict
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient
from DIRAC.WorkloadManagementSystem.Utilities.JobParameters import getJobParameters


class JobMonitoringHandlerMixin:
    @classmethod
    def initializeHandler(cls, svcInfoDict):
        """initialize DBs"""
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
            if not result["OK"]:
                return result
            cls.jobDB = result["Value"](parentLogger=cls.log)

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
            if not result["OK"]:
                return result
            cls.jobLoggingDB = result["Value"](parentLogger=cls.log)

        except RuntimeError as excp:
            return S_ERROR(f"Can't connect to DB: {excp}")

        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobParametersDB", "JobParametersDB")
        if not result["OK"]:
            return result
        cls.elasticJobParametersDB = result["Value"](parentLogger=cls.log)

        cls.pilotManager = PilotManagerClient()
        return S_OK()

    def initializeRequest(self):
        credDict = self.getRemoteCredentials()
        self.vo = credDict.get("VO", Registry.getVOForGroup(credDict["group"]))

    @classmethod
    def getJobsAttributes(cls, *args, **kwargs):
        """Utility function for unpacking"""
        res = cls.jobDB.getJobsAttributes(*args, **kwargs)
        if not res["OK"]:
            return res
        return S_OK(strToIntDict(res["Value"]))

    ##############################################################################
    types_getApplicationStates = []

    @classmethod
    def export_getApplicationStates(cls, condDict=None, older=None, newer=None):
        """Return Distinct Values of ApplicationStatus job Attribute in WMS"""
        return cls.jobDB.getDistinctJobAttributes("ApplicationStatus", condDict, older, newer)

    ##############################################################################
    types_getJobTypes = []

    @classmethod
    def export_getJobTypes(cls, condDict=None, older=None, newer=None):
        """Return Distinct Values of JobType job Attribute in WMS"""
        return cls.jobDB.getDistinctJobAttributes("JobType", condDict, older, newer)

    ##############################################################################
    types_getOwners = []

    @classmethod
    def export_getOwners(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Owner job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("Owner", condDict, older, newer)

    ##############################################################################
    types_getOwnerGroup = []

    @classmethod
    def export_getOwnerGroup(cls):
        """
        Return Distinct Values of OwnerGroup from the JobDB
        """
        return cls.jobDB.getDistinctJobAttributes("OwnerGroup")

    ##############################################################################
    types_getJobGroups = []

    @classmethod
    def export_getJobGroups(cls, condDict=None, older=None, cutDate=None):
        """
        Return Distinct Values of ProductionId job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("JobGroup", condDict, older, newer=cutDate)

    ##############################################################################
    types_getSites = []

    @classmethod
    def export_getSites(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Site job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("Site", condDict, older, newer)

    ##############################################################################
    types_getStates = []

    @classmethod
    def export_getStates(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Status job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("Status", condDict, older, newer)

    ##############################################################################
    types_getMinorStates = []

    @classmethod
    def export_getMinorStates(cls, condDict=None, older=None, newer=None):
        """
        Return Distinct Values of Minor Status job Attribute in WMS
        """
        return cls.jobDB.getDistinctJobAttributes("MinorStatus", condDict, older, newer)

    ##############################################################################
    types_getJobs = []

    @classmethod
    def export_getJobs(cls, attrDict=None, cutDate=None):
        """
        Return list of JobIds matching the condition given in attrDict
        """
        # queryDict = {}

        # if attrDict:
        #  if type ( attrDict ) != dict:
        #    return S_ERROR( 'Argument must be of Dict Type' )
        #  for attribute in self.queryAttributes:
        #    # Only those Attribute in self.queryAttributes can be used
        #    if attrDict.has_key(attribute):
        #      queryDict[attribute] = attrDict[attribute]

        return cls.jobDB.selectJobs(attrDict, newer=cutDate)

    ##############################################################################
    types_getJobOwner = [int]

    @classmethod
    def export_getJobOwner(cls, jobID):
        return cls.jobDB.getJobAttribute(jobID, "Owner")

    ##############################################################################
    types_getJobSite = [int]

    @classmethod
    def export_getJobSite(cls, jobID):
        return cls.jobDB.getJobAttribute(jobID, "Site")

    ##############################################################################
    types_getJobJDL = [int, bool]

    @classmethod
    def export_getJobJDL(cls, jobID, original):
        return cls.jobDB.getJobJDL(jobID, original=original)

    ##############################################################################
    types_getJobLoggingInfo = [int]

    @classmethod
    def export_getJobLoggingInfo(cls, jobID):
        return cls.jobLoggingDB.getJobLoggingInfo(jobID)

    ##############################################################################
    types_getJobsParameters = [[str, int, list], list]

    @classmethod
    @ignoreEncodeWarning
    def export_getJobsParameters(cls, jobIDs, parameters):
        return cls.getJobsAttributes(jobIDs, parameters)

    ##############################################################################
    types_getJobsStates = [[str, int, list]]

    @classmethod
    @ignoreEncodeWarning
    def export_getJobsStates(cls, jobIDs):
        return cls.getJobsAttributes(jobIDs, ["Status", "MinorStatus", "ApplicationStatus"])

    ##############################################################################
    types_getJobsStatus = [[str, int, list]]

    @classmethod
    @ignoreEncodeWarning
    def export_getJobsStatus(cls, jobIDs):
        return cls.getJobsAttributes(jobIDs, ["Status"])

    ##############################################################################
    types_getJobsMinorStatus = [[str, int, list]]

    @classmethod
    @ignoreEncodeWarning
    def export_getJobsMinorStatus(cls, jobIDs):
        return cls.getJobsAttributes(jobIDs, ["MinorStatus"])

    ##############################################################################
    types_getJobsApplicationStatus = [[str, int, list]]

    @classmethod
    @ignoreEncodeWarning
    def export_getJobsApplicationStatus(cls, jobIDs):
        return cls.getJobsAttributes(jobIDs, ["ApplicationStatus"])

    ##############################################################################
    types_getJobsSites = [[str, int, list]]

    @classmethod
    @ignoreEncodeWarning
    def export_getJobsSites(cls, jobIDs):
        return cls.getJobsAttributes(jobIDs, ["Site"])

    ##############################################################################
    types_getJobSummary = [int]

    @classmethod
    def export_getJobSummary(cls, jobID):
        return cls.jobDB.getJobAttributes(jobID)

    ##############################################################################
    types_getJobsSummary = [list]

    @classmethod
    def export_getJobsSummary(cls, jobIDs):
        return cls.getJobsAttributes(jobIDs)

    ##############################################################################
    types_getJobParameter = [[str, int], str]

    @ignoreEncodeWarning
    def export_getJobParameter(self, jobID, parName):
        """
        :param str/int jobID: one single Job ID
        :param str parName: one single parameter name
        """
        res = getJobParameters([int(jobID)], parName, self.vo or "")
        if not res["OK"]:
            return res
        return S_OK(res["Value"].get(int(jobID), {}))

    ##############################################################################
    types_getJobOptParameters = [int]

    @classmethod
    def export_getJobOptParameters(cls, jobID):
        return cls.jobDB.getJobOptParameters(jobID)

    ##############################################################################
    types_getJobParameters = [[str, int, list]]

    @ignoreEncodeWarning
    def export_getJobParameters(self, jobIDs, parName=None):
        """
        :param str/int/list jobIDs: one single job ID or a list of them
        :param str parName: one single parameter name, or None (meaning all of them)
        """
        if not isinstance(jobIDs, list):
            jobIDs = [jobIDs]
        jobIDs = [int(jobID) for jobID in jobIDs]

        return getJobParameters(jobIDs, parName, self.vo or "")

    ##############################################################################
    types_getAtticJobParameters = [int]

    @classmethod
    def export_getAtticJobParameters(cls, jobID, parameters=None, rescheduleCycle=-1):
        if not parameters:
            parameters = []
        return cls.jobDB.getAtticJobParameters(jobID, parameters, rescheduleCycle)

    ##############################################################################
    types_getJobAttributes = [int]

    @classmethod
    def export_getJobAttributes(cls, jobID, attrList=None):
        """
        :param int jobID: one single Job ID
        :param list attrList: optional list of attributes
        """

        return cls.jobDB.getJobAttributes(jobID, attrList=attrList)

    ##############################################################################
    types_getJobAttribute = [int, str]

    @classmethod
    def export_getJobAttribute(cls, jobID, attribute):
        """
        :param int jobID: one single Job ID
        :param str attribute: one single attribute name
        """

        return cls.jobDB.getJobAttribute(jobID, attribute)

    ##############################################################################
    types_getSiteSummary = []

    @classmethod
    def export_getSiteSummary(cls):
        return cls.jobDB.getSiteSummary()

    ##############################################################################
    types_getJobHeartBeatData = [int]

    @classmethod
    def export_getJobHeartBeatData(cls, jobID):
        return cls.jobDB.getHeartBeatData(jobID)

    ##############################################################################
    types_getInputData = [(int, list)]

    @classmethod
    def export_getInputData(cls, jobID):
        """Get input data for the specified jobs"""
        return cls.jobDB.getInputData(jobID)


##############################################################################


class JobMonitoringHandler(JobMonitoringHandlerMixin, RequestHandler):
    def initialize(self):
        return self.initializeRequest()
