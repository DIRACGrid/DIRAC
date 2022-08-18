"""
It is used to create several plots
"""
import time
import copy

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.MonitoringSystem.private.DBUtils import DBUtils
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.Core.Utilities.Plotting.Plots import (
    generateNoDataPlot,
    generateTimedStackedBarPlot,
    generateQualityPlot,
    generateCumulativePlot,
    generatePiePlot,
    generateStackedLinePlot,
)


class BasePlotter(DBUtils):

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
            ("MB / s", 10**6, 1000),
            ("GB / s", 10**9, 1000),
            ("TB / s", 10**12, 1000),
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
        "bytes": (("MB", 10**6, 1000), ("GB", 10**9, 1000), ("TB", 10**12, 1000), ("PB", 10**15, 1)),
        "jobs": (("jobs", 1, 1000), ("kjobs", 10**3, 1000), ("Mjobs", 10**6, 1)),
        "files": (("files", 1, 1000), ("kfiles", 10**3, 1000), ("Mfiles", 10**6, 1)),
    }

    # To be defined in the derived classes
    _typeKeyFields = []
    _typeName = ""

    def __init__(self, db, setup, extraArgs=None):
        super().__init__(db, setup)
        """ c'tor

    :param self: self reference
    """

        if isinstance(extraArgs, dict):
            self._extraArgs = extraArgs
        else:
            self._extraArgs = {}
        reportsRevMap = {}
        for attr in dir(self):
            if attr.startswith("_report"):
                if attr.endswith("Name"):
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

    def generate(self, reportRequest):
        """
        It retrives the data from the database and creates the plot

        :param dict reportRequest: contains the plot attributes
        """
        reportHash = reportRequest["hash"]
        reportName = reportRequest["reportName"]
        if reportName in self.__reportNameMapping:
            reportRequest["reportName"] = self.__reportNameMapping[reportName]

        gLogger.info("Retrieving data for {}:{}".format(reportRequest["typeName"], reportRequest["reportName"]))
        sT = time.time()
        retVal = self.__retrieveReportData(reportRequest, reportHash)
        reportGenerationTime = time.time() - sT
        if not retVal["OK"]:
            return retVal
        if not reportRequest["generatePlot"]:
            return retVal
        reportData = retVal["Value"]
        gLogger.info("Plotting data for {}:{}".format(reportRequest["typeName"], reportRequest["reportName"]))
        sT = time.time()
        retVal = self.__generatePlotForReport(reportRequest, reportHash, reportData)
        plotGenerationTime = time.time() - sT
        gLogger.verbose(
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
        """
        It returns the list of available plots.
        """
        return sorted(self.__reportNameMapping)

    def __retrieveReportData(self, reportRequest, reportHash):
        """
        It uses the appropriate Plotter to retrieve the data from the database.

        :param dict reportRequest: the dictionary which contains the conditions used to create the plot
        :param str reportHash: it is the unique identifier used to cache a plot
        :return: dict S_OK/S_ERROR if the data found in the cache it returns from it otherwise it uses the cache.
        """
        funcName = "_report%s" % reportRequest["reportName"]
        if not hasattr(self, funcName):
            return S_ERROR("Report %s is not defined" % reportRequest["reportName"])
        else:
            funcObj = getattr(self, funcName)

        return gDataCache.getReportData(reportRequest=reportRequest, reportHash=reportHash, dataFunc=funcObj)

    def __generatePlotForReport(self, reportRequest, reportHash, reportData):
        """It creates the plot

        :param dict reportRequest: contains the plot attributes
        :param str reportHash: unique string which identify the plot
        :param dict repotData: contains the data used to generate the plot.
        """

        funcName = "_plot%s" % reportRequest["reportName"]
        try:
            funcObj = getattr(self, funcName)
        except Exception:
            return S_ERROR("Plot function for report %s is not defined" % reportRequest["reportName"])

        return gDataCache.getReportPlot(
            reportRequest=reportRequest, reportHash=reportHash, reportData=reportData, plotFunc=funcObj
        )

    def _getTimedData(self, startTime, endTime, selectField, preCondDict, metadataDict=None):
        """
        It retrieves the time series data from the ES database.

        :param int startTime: epoch time
        :param int endTime: epoch time
        :param str selectField: the value that we want to plot
        :param dict preCondDict: plot attributes
        :param dict metadataDict: extra arguments used to create the plot.

        """

        condDict = {}

        if metadataDict is None:
            metadataDict = {}

        grouping = preCondDict["grouping"][0]
        # Make safe selections
        for keyword in self._typeKeyFields:
            if keyword in preCondDict:
                condDict[keyword] = preCondDict[keyword]

        retVal = self._determineBucketSize(startTime, endTime)
        if not retVal["OK"]:
            return retVal
        interval, granularity = retVal["Value"]

        dynamicBucketing = metadataDict.get("DynamicBucketing", True)
        # by default we use dynamic bucketing
        if dynamicBucketing:
            retVal = self._retrieveBucketedData(
                self._typeName, startTime, endTime, interval, selectField, condDict, grouping, metadataDict
            )
        else:
            retVal = self._retrieveAggregatedData(
                self._typeName, startTime, endTime, interval, selectField, condDict, grouping, metadataDict
            )

        if not retVal["OK"]:
            return retVal
        dataDict = retVal["Value"]

        return S_OK((dataDict, granularity))

    def _getSummaryData(self, startTime, endTime, selectField, preCondDict, metadataDict=None):
        """
        It returns the data used to create the pie chart plot.

        :param int startTime: epoch time
        :param int endTime: epoch time
        :param str selectField: the value what we want to plot
        :param dict preCondDict: plot attributes
        :param dict metadataDict: extra arguments used to create the plot.
        """
        grouping = preCondDict["grouping"][0]
        condDict = {}
        # Make safe selections
        for keyword in self._typeKeyFields:
            if keyword in preCondDict:
                condDict[keyword] = preCondDict[keyword]

        retVal = self._determineBucketSize(startTime, endTime)
        if not retVal["OK"]:
            return retVal
        interval, _ = retVal["Value"]

        retVal = self._retrieveBucketedData(
            typeName=self._typeName,
            startTime=startTime,
            endTime=endTime,
            interval=interval,
            selectField=selectField,
            condDict=condDict,
            grouping=grouping,
            metadataDict=metadataDict,
        )
        if not retVal["OK"]:
            return retVal
        dataDict = retVal["Value"]
        return S_OK(dataDict)

    def _findSuitableRateUnit(self, dataDict, maxValue, unit):
        """
        Returns the suitable unit for a given dataset.
        """
        return self._findUnitMagic(dataDict, maxValue, unit, self._RATE_UNITS)

    def _findSuitableUnit(self, dataDict, maxValue, unit):
        """
        Returns the suitable unit for a given dataset.
        """
        return self._findUnitMagic(dataDict, maxValue, unit, self._UNITS)

    def _findUnitMagic(self, reportDataDict, maxValue, unit, selectedUnits):
        """
        Returns the suitable unit for a given dataset.
        """
        if unit not in selectedUnits:
            raise AttributeError("%s is not a known rate unit" % unit)
        baseUnitData = selectedUnits[unit][0]
        if self._extraArgs.get("staticUnits"):
            unitData = selectedUnits[unit][0]
        else:
            unitList = selectedUnits[unit]
            unitIndex = -1
            for _, unitDivFactor, unitThreshold in unitList:
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

    def __checkPlotMetadata(self, metadata):
        """It check the plot metadata arguments

        :param dict metadata: contains the plot metadata
        """
        if self._extraArgs.get(self._EA_WIDTH):
            try:
                metadata[self._EA_WIDTH] = min(1600, max(200, int(self._extraArgs[self._EA_WIDTH])))
            except Exception:
                pass
        if self._EA_HEIGHT in self._extraArgs and self._extraArgs[self._EA_HEIGHT]:
            try:
                metadata[self._EA_HEIGHT] = min(1600, max(200, int(self._extraArgs[self._EA_HEIGHT])))
            except Exception:
                pass
        if self._extraArgs.get(self._EA_TITLE):
            metadata["title"] = self._extraArgs[self._EA_TITLE]

    def __checkThumbnailMetadata(self, metadata):
        """checks the plot thumbnail data

        :param dict metadata: contains the thumbnail data
        """
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
        """It creates the plot.

        :param str filename: the name of the file which contains the plot
        :param dict dataDict: data used to crate the plot
        :param dict metadata: plot metadata
        :param object funcToPlot: the method which create the plot using the appropriate method.
        """
        self.__checkPlotMetadata(metadata)
        if not dataDict:
            funcToPlot = generateNoDataPlot
        plotFileName = "%s.png" % filename

        finalResult = funcToPlot(fileName=plotFileName, data=dataDict, metadata=metadata)
        if not finalResult["OK"]:
            return finalResult
        thbMD = self.__checkThumbnailMetadata(metadata)
        if not thbMD:
            return S_OK({"plot": True, "thumbnail": False})
        thbFilename = "%s.thb.png" % filename
        retVal = funcToPlot(thbFilename, dataDict, thbMD)
        if not retVal["OK"]:
            return retVal
        return S_OK({"plot": True, "thumbnail": True})

    def _generateTimedStackedBarPlot(self, filename, dataDict, metadata):
        """
        it creates a bar plot
        """
        return self.__plotData(filename, dataDict, metadata, generateTimedStackedBarPlot)

    def _generateQualityPlot(self, filename, dataDict, metadata):
        """
        it creates a quality plot
        """
        return self.__plotData(filename, dataDict, metadata, generateQualityPlot)

    def _generateCumulativePlot(self, filename, dataDict, metadata):
        """
        It creates a cumulative plot
        """
        return self.__plotData(filename, dataDict, metadata, generateCumulativePlot)

    def _generatePiePlot(self, filename, dataDict, metadata):
        """
        It creates a pie chart plot
        """
        return self.__plotData(filename, dataDict, metadata, generatePiePlot)

    def _generateStackedLinePlot(self, filename, dataDict, metadata):
        """
        It creates a stacked lien plot
        """
        return self.__plotData(filename, dataDict, metadata, generateStackedLinePlot)

    def _fillWithZero(self, granularity, startEpoch, endEpoch, dataDict):
        """
        Fill with zeros missing buckets
          - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
        """
        startBucketEpoch = startEpoch - startEpoch % granularity
        for key in dataDict:
            currentDict = dataDict[key]
            for timeEpoch in range(int(startBucketEpoch), int(endEpoch), granularity):
                if timeEpoch not in currentDict:
                    currentDict[timeEpoch] = 0
        return dataDict

    def _calculateEfficiencyDict(self, totDataDict, dataDict):
        """
        It returns a dict with efficiency calculated from each key in totDataDict and dataDict
        """
        for item, val in dataDict.items():
            totVal = totDataDict.get(item, {})
            try:
                dataDict[item] = {key: (float(val[key]) / float(totVal[key])) * 100 for key in val if key in totVal}
            except (ZeroDivisionError, TypeError):
                gLogger.warn("Error in ", val)
                gLogger.warn("Dividing by zero or using None type field. Skipping the key of this dict item...")
                pass
        return dataDict

    def _sumDictValues(self, dataDict):
        """
        Sums the values of each item in `dataDict`.
        Returns the dictionary with the same keys and the values replaced by their sum.
        """
        for key, values in dataDict.items():
            sum = 0
            for val in values.values():
                try:
                    sum += val
                except TypeError as e:
                    gLogger.warn("Error in the operation: ", e)
                    gLogger.warn("Skipping this value: ", val)
                    pass
            dataDict[key] = sum
        return dataDict
