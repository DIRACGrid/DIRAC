""" Module that holds the ReportGeneratorHandler class

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ReportGenerator
  :end-before: ##END
  :dedent: 2
  :caption: ReportGenerator options
"""
import os
import datetime

from DIRAC import S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.AccountingSystem.DB.MultiAccountingDB import MultiAccountingDB
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.AccountingSystem.private.MainReporter import MainReporter
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.Core.Utilities.Plotting.Plots import generateErrorMessagePlot
from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler


class ReportGeneratorHandler(RequestHandler):
    """DIRAC service class to retrieve information from the AccountingDB"""

    __acDB = None
    __reportRequestDict = {
        "typeName": str,
        "reportName": str,
        "startTime": (datetime.datetime, datetime.date),
        "endTime": (datetime.datetime, datetime.date),
        "condDict": dict,
        "grouping": str,
        "extraArgs": dict,
    }

    @classmethod
    def initializeHandler(cls, serviceInfo):
        multiPath = PathFinder.getDatabaseSection("Accounting/MultiDB")
        cls.__acDB = MultiAccountingDB(multiPath, readOnly=True)
        # Get data location
        reportSection = serviceInfo["serviceSectionPath"]
        dataPath = gConfig.getValue(f"{reportSection}/DataLocation", "data/accountingGraphs")
        dataPath = dataPath.strip()
        if "/" != dataPath[0]:
            dataPath = os.path.realpath(f"{rootPath}/{dataPath}")
        cls.log.info(f"Data will be written into {dataPath}")
        mkDir(dataPath)
        try:
            testFile = f"{dataPath}/acc.jarl.test"
            with open(testFile, "w"):
                pass
            os.unlink(testFile)
        except OSError:
            cls.log.fatal("Can't write to", dataPath)
            return S_ERROR("Data location is not writable")
        gDataCache.setGraphsLocation(dataPath)
        return S_OK()

    def __checkPlotRequest(self, reportRequest):
        # If extraArgs is not there add it
        if "extraArgs" not in reportRequest:
            reportRequest["extraArgs"] = {}
        if not isinstance(reportRequest["extraArgs"], self.__reportRequestDict["extraArgs"]):
            return S_ERROR(f"Extra args has to be of type {self.__reportRequestDict['extraArgs']}")
        reportRequestExtra = reportRequest["extraArgs"]
        # Check sliding plots
        if "lastSeconds" in reportRequestExtra:
            try:
                lastSeconds = int(reportRequestExtra["lastSeconds"])
            except ValueError:
                self.log.error("lastSeconds key must be a number")
                return S_ERROR("Value Error")
            if lastSeconds < 3600:
                return S_ERROR("lastSeconds must be more than 3600")
            now = datetime.datetime.utcnow()
            reportRequest["endTime"] = now
            reportRequest["startTime"] = now - datetime.timedelta(seconds=lastSeconds)
        else:
            # if enddate is not there, just set it to now
            if not reportRequest.get("endTime", False):
                reportRequest["endTime"] = datetime.datetime.utcnow()
        # Check keys
        for key, keyType in self.__reportRequestDict.items():
            if key not in reportRequest:
                return S_ERROR(f"Missing mandatory field {key} in plot reques")

            if not isinstance(reportRequest[key], keyType):
                return S_ERROR(f"Type mismatch for field {key} ({type(reportRequest[key])}), required one of {keyType}")
            if key in ("startTime", "endTime"):
                reportRequest[key] = int(TimeUtilities.toEpoch(reportRequest[key]))

        return S_OK(reportRequest)

    types_generatePlot = [dict]

    def export_generatePlot(self, reportRequest):
        """
        Generate an accounting plot

        :param dict reportRequest: dictionary with arguments:
          - viewName
          - startTime
          - endTime
          - argsDict (Arguments to the view)
          - grouping
          - extraArgs
        """
        retVal = self.__checkPlotRequest(reportRequest)
        if not retVal["OK"]:
            return retVal
        reporter = MainReporter(self.__acDB)
        reportRequest["generatePlot"] = True
        return reporter.generate(reportRequest, self.getRemoteCredentials())

    types_getReport = [dict]

    def export_getReport(self, reportRequest):
        """
        Gets the report but does not generate a plot

        :param dict reportRequest: dictionary with arguments:
          - viewName
          - startTime
          - endTime
          - argsDict (Arguments to the view)
          - grouping
          - extraArgs
        """
        retVal = self.__checkPlotRequest(reportRequest)
        if not retVal["OK"]:
            return retVal
        reporter = MainReporter(self.__acDB)
        reportRequest["generatePlot"] = False
        return reporter.generate(reportRequest, self.getRemoteCredentials())

    types_listReports = [str]

    def export_listReports(self, typeName):
        """
        List all available plots

        Arguments:
          - none
        """
        reporter = MainReporter(self.__acDB)
        return reporter.list(typeName)

    types_listUniqueKeyValues = [str]

    def export_listUniqueKeyValues(self, typeName):
        """
        List all values for all keys in a type

        Arguments:
          - none
        """
        dbUtils = DBUtils(self.__acDB)
        credDict = self.getRemoteCredentials()
        if typeName in gPoliciesList:
            policyFilter = gPoliciesList[typeName]
            filterCond = policyFilter.getListingConditions(credDict)
        else:
            policyFilter = gPoliciesList["Null"]
            filterCond = {}
        retVal = dbUtils.getKeyValues(typeName, filterCond)
        if not policyFilter or not retVal["OK"]:
            return retVal
        return policyFilter.filterListingValues(credDict, retVal["Value"])

    def __generatePlotFromFileId(self, fileId):
        result = extractRequestFromFileId(fileId)
        if not result["OK"]:
            return result
        plotRequest = result["Value"]
        self.log.info("Generating the plots..")
        result = self.export_generatePlot(plotRequest)
        if not result["OK"]:
            self.log.error("Error while generating the plots", result["Message"])
            return result
        fileToReturn = "plot"
        if "extraArgs" in plotRequest:
            extraArgs = plotRequest["extraArgs"]
            if "thumbnail" in extraArgs and extraArgs["thumbnail"]:
                fileToReturn = "thumbnail"
        self.log.info(f"Returning {fileToReturn} file: {result['Value'][fileToReturn]} ")
        return S_OK(result["Value"][fileToReturn])

    def __sendErrorAsImg(self, msgText, fileHelper):
        fileHelper.sendData(generateErrorMessagePlot(msgText))
        fileHelper.sendEOF()

    def transfer_toClient(self, fileId, token, fileHelper):
        """
        Get graphs data
        """
        # First check if we've got to generate the plot
        if len(fileId) > 5 and fileId[1] == ":":
            self.log.verbose("Seems the file request is a plot generation request!")
            # Seems a request for a plot!
            try:
                result = self.__generatePlotFromFileId(fileId)
            except Exception as e:
                self.log.exception("Exception while generating plot")
                result = S_ERROR(f"Error while generating plot: {str(e)}")
            if not result["OK"]:
                self.__sendErrorAsImg(result["Message"], fileHelper)
                fileHelper.sendEOF()
                return result
            fileId = result["Value"]
        retVal = gDataCache.getPlotData(fileId)
        if not retVal["OK"]:
            self.__sendErrorAsImg(retVal["Message"], fileHelper)
            return retVal
        retVal = fileHelper.sendData(retVal["Value"])
        if not retVal["OK"]:
            return retVal
        fileHelper.sendEOF()
        return S_OK()
