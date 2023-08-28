import time
import copy

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.Core.Utilities.Plotting.Plots import (
    generateNoDataPlot,
    generateTimedStackedBarPlot,
    generateQualityPlot,
    generateCumulativePlot,
    generatePiePlot,
    generateStackedLinePlot,
    generateHistogram,
)


class BaseReporter(DBUtils):
    _PARAM_CHECK_FOR_NONE = "checkNone"
    _PARAM_CALCULATE_PROPORTIONAL_GAUGES = "calculateProportionalGauges"
    _PARAM_CONVERT_TO_GRANULARITY = "convertToGranularity"
    _VALID_PARAM_CONVERT_TO_GRANULARITY = ("sum", "average")
    _PARAM_CONSOLIDATION_FUNCTION = "consolidationFunction"

    _EA_THUMBNAIL = "thumbnail"
    _EA_WIDTH = "width"
    _EA_HEIGHT = "height"
    _EA_THB_WIDTH = "thbWidth"
    _EA_THB_HEIGHT = "thbHeight"
    _EA_PADDING = "figurePadding"
    _EA_TITLE = "plotTitle"

    _RATE_UNITS = {
        "time": (
            ("seconds / s", 1, 24),
            ("hours / s", 3600, 24),
            ("days / s", 86400, 15),
            ("weeks / s", 86400 * 7, 10),
            ("months / s", 86400 * 30, 12),
            ("years / s", 86400 * 365, 1),
        ),
        "cpupower": (("HS06", 1, 750), ("kHS06", 1000, 750), ("MHS06", 10**6, 1)),
        "bytes": (
            ("MB / s", 10**6, 1024),
            ("GB / s", 10**9, 1024),
            ("TB / s", 10**12, 1024),
            ("PB / s", 10**15, 1),
        ),
        "jobs": (
            ("jobs / hour", 1 / 3600.0, 1000),
            ("kjobs / hour", (10**3) / 3600.0, 1000),
            ("Mjobs / hour", (10**6) / 3600.0, 1),
        ),
        "files": (
            ("files / hour", 1 / 3600.0, 1000),
            ("kfiles / hour", (10**3) / 3600.0, 1000),
            ("Mfiles / hour", (10**6) / 3600.0, 1),
        ),
    }

    _UNITS = {
        "time": (
            ("seconds", 1, 24),
            ("hours", 3600, 24),
            ("days", 86400, 15),
            ("weeks", 86400 * 7, 10),
            ("months", 86400 * 30, 12),
            ("years", 86400 * 365, 1),
        ),
        "cpupower": (
            ("HS06 hours", 3600, 24),
            ("HS06 days", 86400, 750),
            ("kHS06 days", 86400 * 1000, 750),
            ("MHS06 days", 86400 * 10**6, 1),
        ),
        "bytes": (("MB", 10**6, 1024), ("GB", 10**9, 1024), ("TB", 10**12, 1024), ("PB", 10**15, 1)),
        "jobs": (("jobs", 1, 1000), ("kjobs", 10**3, 1000), ("Mjobs", 10**6, 1)),
        "files": (("files", 1, 1000), ("kfiles", 10**3, 1000), ("Mfiles", 10**6, 1)),
    }

    # To be defined in the derived classes
    _typeKeyFields = []
    _typeName = ""

    def __init__(self, db, extraArgs=None):
        DBUtils.__init__(self, db)
        if isinstance(extraArgs, dict):
            self._extraArgs = extraArgs
        else:
            self._extraArgs = {}
        reportsRevMap = {}
        for attr in dir(self):
            if attr.find("_report") == 0:
                if attr.find("Name", len(attr) - 4) == len(attr) - 4:
                    reportId = attr[7:-4]
                    reportName = getattr(self, attr)
                    reportsRevMap[reportId] = reportName
                else:
                    reportId = attr[7:]
                    if reportId not in reportsRevMap:
                        reportsRevMap[reportId] = reportId
        self.__reportNameMapping = {}
        for rId in reportsRevMap:
            self.__reportNameMapping[reportsRevMap[rId]] = rId

    def _translateGrouping(self, grouping):
        return ("%s", [grouping])

    def _averageConsolidation(self, total, count):
        if count == 0:
            return 0
        return float(total) / float(count)

    def _efficiencyConsolidation(self, total, count):
        if count == 0:
            return 0
        return (float(total) / float(count)) * 100.0

    def generate(self, reportRequest):
        reportRequest["groupingFields"] = self._translateGrouping(reportRequest["grouping"])
        reportHash = reportRequest["hash"]
        reportName = reportRequest["reportName"]
        if reportName in self.__reportNameMapping:
            reportRequest["reportName"] = self.__reportNameMapping[reportName]
        gLogger.info(f"Retrieving data for {reportRequest['typeName']}:{reportRequest['reportName']}")
        sT = time.time()
        retVal = self.__retrieveReportData(reportRequest, reportHash)
        reportGenerationTime = time.time() - sT
        if not retVal["OK"]:
            return retVal
        if not reportRequest["generatePlot"]:
            return retVal
        reportData = retVal["Value"]
        gLogger.info(f"Plotting data for {reportRequest['typeName']}:{reportRequest['reportName']}")
        sT = time.time()
        retVal = self.__generatePlotForReport(reportRequest, reportHash, reportData)
        plotGenerationTime = time.time() - sT
        gLogger.info(
            "Time for %s:%s - Report %.2f Plot %.2f (%.2f%% r/p)"
            % (
                reportRequest["typeName"],
                reportRequest["reportName"],
                reportGenerationTime,
                plotGenerationTime,
                ((reportGenerationTime * 100 / plotGenerationTime) if plotGenerationTime else 0.0),
            )
        )
        if not retVal["OK"]:
            return retVal
        plotDict = retVal["Value"]
        if "retrieveReportData" in reportRequest["extraArgs"] and reportRequest["extraArgs"]["retrieveReportData"]:
            plotDict["reportData"] = reportData
        return S_OK(plotDict)

    def plotsList(self):
        return sorted(k for k in self.__reportNameMapping)

    def __retrieveReportData(self, reportRequest, reportHash):
        funcName = f"_report{reportRequest['reportName']}"
        try:
            funcObj = getattr(self, funcName)
        except Exception:
            return S_ERROR(f"Report {reportRequest['reportName']} is not defined")
        return gDataCache.getReportData(reportRequest, reportHash, funcObj)

    def __generatePlotForReport(self, reportRequest, reportHash, reportData):
        funcName = f"_plot{reportRequest['reportName']}"
        try:
            funcObj = getattr(self, funcName)
        except Exception:
            return S_ERROR(f"Plot function for report {reportRequest['reportName']} is not defined")
        return gDataCache.getReportPlot(reportRequest, reportHash, reportData, funcObj)

    ###
    # Helper functions for reporters
    ###

    def _getTimedData(self, startTime, endTime, selectFields, preCondDict, groupingFields, metadataDict):
        condDict = {}
        # Check params
        if self._PARAM_CHECK_FOR_NONE not in metadataDict:
            metadataDict[self._PARAM_CHECK_FOR_NONE] = False
        if self._PARAM_CONVERT_TO_GRANULARITY not in metadataDict:
            metadataDict[self._PARAM_CONVERT_TO_GRANULARITY] = "sum"
        elif metadataDict[self._PARAM_CONVERT_TO_GRANULARITY] not in self._VALID_PARAM_CONVERT_TO_GRANULARITY:
            return S_ERROR(f"{self._PARAM_CONVERT_TO_GRANULARITY} field metadata is invalid")
        if self._PARAM_CALCULATE_PROPORTIONAL_GAUGES not in metadataDict:
            metadataDict[self._PARAM_CALCULATE_PROPORTIONAL_GAUGES] = False
        # Make safe selections
        for keyword in self._typeKeyFields:
            if keyword in preCondDict:
                condDict[keyword] = preCondDict[keyword]
        # Query!
        timeGrouping = ("%%s, %s" % groupingFields[0], ["startTime"] + groupingFields[1])
        retVal = self._retrieveBucketedData(
            self._typeName, startTime, endTime, selectFields, condDict, timeGrouping, ("%s", ["startTime"])
        )
        if not retVal["OK"]:
            return retVal
        dataDict = self._groupByField(0, retVal["Value"])
        coarsestGranularity = self._getBucketLengthForTime(self._typeName, startTime)
        # Transform!
        for keyField in dataDict:
            if metadataDict[self._PARAM_CHECK_FOR_NONE]:
                dataDict[keyField] = self._convertNoneToZero(dataDict[keyField])
            if metadataDict[self._PARAM_CONVERT_TO_GRANULARITY] == "average":
                dataDict[keyField] = self._averageToGranularity(coarsestGranularity, dataDict[keyField])
            if metadataDict[self._PARAM_CONVERT_TO_GRANULARITY] == "sum":
                dataDict[keyField] = self._sumToGranularity(coarsestGranularity, dataDict[keyField])
            if self._PARAM_CONSOLIDATION_FUNCTION in metadataDict:
                dataDict[keyField] = self._executeConsolidation(
                    metadataDict[self._PARAM_CONSOLIDATION_FUNCTION], dataDict[keyField]
                )
        if metadataDict[self._PARAM_CALCULATE_PROPORTIONAL_GAUGES]:
            dataDict = self._calculateProportionalGauges(dataDict)
        return S_OK((dataDict, coarsestGranularity))

    def _executeConsolidation(self, functor, dataDict):
        for timeKey in dataDict:
            dataDict[timeKey] = [functor(*dataDict[timeKey])]
        return dataDict

    def _getSummaryData(
        self, startTime, endTime, selectFields, preCondDict, groupingFields, metadataDict=None, reduceFunc=False
    ):
        condDict = {}
        # Make safe selections
        for keyword in self._typeKeyFields:
            if keyword in preCondDict:
                condDict[keyword] = preCondDict[keyword]
        # Query!
        retVal = self._retrieveBucketedData(
            self._typeName, startTime, endTime, selectFields, condDict, groupingFields, []
        )
        if not retVal["OK"]:
            return retVal
        dataDict = self._groupByField(0, retVal["Value"])
        for key in dataDict:
            if not reduceFunc:
                dataDict[key] = dataDict[key][0][0]
            else:
                dataDict[key] = reduceFunc(*dataDict[key][0])
        # HACK to allow serialization of the type because MySQL decides to return data in a non python standard format
        for key in dataDict:
            dataDict[key] = float(dataDict[key])
        return S_OK(dataDict)

    def _getBucketData(self, startTime, endTime, selectFields, preCondDict):
        """
        It retrieves data for histogram.

        :param int startTime: epoch time
        :param int endTime: epoch time
        :param list selectFields: the value being plotted
        :param dict preCondDict: plot attributes (conditions)

        """

        condDict = {}
        # Make safe selections
        for keyword in self._typeKeyFields:
            if keyword in preCondDict:
                condDict[keyword] = preCondDict[keyword]
        retVal = self._retrieveBucketedData(self._typeName, startTime, endTime, selectFields, condDict)
        if not retVal["OK"]:
            return retVal
        dataDict = self._groupByField(0, retVal["Value"])
        data = {}
        # convert values to the correct format
        for key, values in dataDict.items():
            data[key] = [float(i[0]) for i in values]
        return S_OK(data)

    def _getSelectStringForGrouping(self, groupingFields):
        if len(groupingFields) == 3:
            return groupingFields[2]
        if len(groupingFields[1]) == 1:
            # If there's only one field, then we send the sql representation in pos 0
            return groupingFields[0]
        return "CONCAT( %s )" % ", ".join(["%s, '-'" % sqlRep for sqlRep in groupingFields[0]])

    def _findSuitableRateUnit(self, dataDict, maxValue, unit):
        return self._findUnitMagic(dataDict, maxValue, unit, self._RATE_UNITS)

    def _findSuitableUnit(self, dataDict, maxValue, unit):
        return self._findUnitMagic(dataDict, maxValue, unit, self._UNITS)

    def _findUnitMagic(self, reportDataDict, maxValue, unit, selectedUnits):
        if unit not in selectedUnits:
            raise AttributeError(f"{unit} is not a known rate unit")
        baseUnitData = selectedUnits[unit][0]
        if "staticUnits" in self._extraArgs and self._extraArgs["staticUnits"]:
            unitData = selectedUnits[unit][0]
        else:
            unitList = selectedUnits[unit]
            unitIndex = -1
            for _unitName, unitDivFactor, unitThreshold in unitList:
                unitIndex += 1
                if maxValue / unitDivFactor < unitThreshold:
                    break
            unitData = selectedUnits[unit][unitIndex]
        # Apply divFactor to all units
        graphDataDict, maxValue = self._divideByFactor(copy.deepcopy(reportDataDict), unitData[1])
        if unitData == baseUnitData:
            reportDataDict = graphDataDict
        else:
            reportDataDict, dummyMaxValue = self._divideByFactor(reportDataDict, baseUnitData[1])
        return reportDataDict, graphDataDict, maxValue, unitData[0]

    ##
    # Plotting
    ##

    def __checkPlotMetadata(self, metadata):
        if self._EA_WIDTH in self._extraArgs and self._extraArgs[self._EA_WIDTH]:
            try:
                metadata[self._EA_WIDTH] = min(1600, max(200, int(self._extraArgs[self._EA_WIDTH])))
            except Exception:
                pass
        if self._EA_HEIGHT in self._extraArgs and self._extraArgs[self._EA_HEIGHT]:
            try:
                metadata[self._EA_HEIGHT] = min(1600, max(200, int(self._extraArgs[self._EA_HEIGHT])))
            except Exception:
                pass
        if self._EA_TITLE in self._extraArgs and self._extraArgs[self._EA_TITLE]:
            metadata["title"] = self._extraArgs[self._EA_TITLE]

    def __checkThumbnailMetadata(self, metadata):
        if self._EA_THUMBNAIL in self._extraArgs and self._extraArgs[self._EA_THUMBNAIL]:
            thbMD = dict(metadata)
            thbMD["legend"] = False
            if self._EA_THB_HEIGHT in self._extraArgs:
                thbMD[self._EA_HEIGHT] = self._extraArgs[self._EA_THB_HEIGHT]
            else:
                thbMD[self._EA_HEIGHT] = 125
            if self._EA_THB_WIDTH in self._extraArgs:
                thbMD[self._EA_WIDTH] = self._extraArgs[self._EA_THB_WIDTH]
            else:
                thbMD[self._EA_WIDTH] = 200
            thbMD[self._EA_PADDING] = 20
            for key in ("title", "ylabel", "xlabel"):
                if key in thbMD:
                    del thbMD[key]
            return thbMD
        return False

    def __plotData(self, filename, dataDict, metadata, funcToPlot):
        self.__checkPlotMetadata(metadata)
        if not dataDict:
            funcToPlot = generateNoDataPlot
        plotFileName = f"{filename}.png"
        finalResult = funcToPlot(plotFileName, dataDict, metadata)
        if not finalResult["OK"]:
            return finalResult
        thbMD = self.__checkThumbnailMetadata(metadata)
        if not thbMD:
            return S_OK({"plot": True, "thumbnail": False})
        thbFilename = f"{filename}.thb.png"
        retVal = funcToPlot(thbFilename, dataDict, thbMD)
        if not retVal["OK"]:
            return retVal
        return S_OK({"plot": True, "thumbnail": True})

    def _generateTimedStackedBarPlot(self, filename, dataDict, metadata):
        return self.__plotData(filename, dataDict, metadata, generateTimedStackedBarPlot)

    def _generateQualityPlot(self, filename, dataDict, metadata):
        return self.__plotData(filename, dataDict, metadata, generateQualityPlot)

    def _generateCumulativePlot(self, filename, dataDict, metadata):
        return self.__plotData(filename, dataDict, metadata, generateCumulativePlot)

    def _generatePiePlot(self, filename, dataDict, metadata):
        return self.__plotData(filename, dataDict, metadata, generatePiePlot)

    def _generateStackedLinePlot(self, filename, dataDict, metadata):
        return self.__plotData(filename, dataDict, metadata, generateStackedLinePlot)

    def _generateHistogram(self, filename, dataDict, metadata):
        return self.__plotData(filename, dataDict, metadata, generateHistogram)
