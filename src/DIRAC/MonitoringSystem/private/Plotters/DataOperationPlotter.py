"""
this class is used to define the plot using the plot attributes.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.Types.DataOperation import DataOperation
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter


class DataOperationPlotter(BasePlotter):

    """
    .. class:: DataOperationPlotter

    It is used to create the plots.

    param: str _typeName monitoring type
    param: list _typeKeyFields list of keys what we monitor (list of attributes)
    """

    _typeName = "DataOperation"

    _typeKeyFields = DataOperation().keyFields

    _reportSuceededTransfersName = "Successful transfers"

    def _reportSuceededTransfers(self, reportRequest):
        return self.__reportTransfers(reportRequest, "Succeeded", ("Failed", 0))

    _reportFailedTransfersName = "Failed transfers"

    def _reportFailedTransfers(self, reportRequest):
        return self.__reportTransfers(reportRequest, "Failed", ("Succeeded", 1))

    def __reportTransfers(self, reportRequest, titleType, togetherFieldsToPlot):
        retVal = self._getTimedData(
            reportRequest["startTime"],
            reportRequest["endTime"],
            "TransferTotal",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal
        dataDict, granularity = retVal["Value"]

        return S_OK({"data": dataDict, "granularity": granularity, "unit": "files"})

    def _plotSuceededTransfers(self, reportRequest, plotInfo, filename):
        return self.__plotTransfers(reportRequest, plotInfo, filename, "Succeeded", ("Failed", 0))

    def _plotFailedTransfers(self, reportRequest, plotInfo, filename):
        return self.__plotTransfers(reportRequest, plotInfo, filename, "Failed", ("Succeeded", 1))

    def __plotTransfers(self, reportRequest, plotInfo, filename, titleType, togetherFieldsToPlot):
        metadata = {
            "title": "%s Transfers by %s" % (titleType, reportRequest["grouping"]),
            "ylabel": plotInfo["unit"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
        }
        return self._generateTimedStackedBarPlot(filename, plotInfo["data"], metadata)

    _reportQualityName = "Transfer Efficiency"

    def _reportQuality(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        # Retrieve the number of succeded transfers
        retVal = self._getTimedData(
            reportRequest["startTime"],
            reportRequest["endTime"],
            "TransferOK",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        # Retrieve the number of total transfers
        retTotVal = self._getTimedData(
            reportRequest["startTime"],
            reportRequest["endTime"],
            "TransferTotal",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal
        if not retTotVal["OK"]:
            return retTotVal
        dataDict, granularity = retVal["Value"]
        totDataDict, granularity = retTotVal["Value"]
        # Check that the dicts are not empty
        if dataDict and totDataDict:
            # Return the efficiency in dataDict
            effDict = self._calculateEfficiencyDict(totDataDict, dataDict)
            return S_OK({"data": effDict, "granularity": granularity})
        return S_OK("No data available for this plot selection.")

    def _plotQuality(self, reportRequest, plotInfo, filename):
        """Make 2 dimensional pilotSubmission efficiency plot

        :param dict reportRequest: Condition to select data
        :param dict plotInfo: Data for plot.
        :param str  filename: File name
        """

        metadata = {
            "title": "Transfer quality by %s" % reportRequest["grouping"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
        }
        return self._generateQualityPlot(filename, plotInfo["data"], metadata)

    _reportTransferedDataName = "Cumulative transferred data"

    def _reportTransferedData(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        retVal = self._getTimedData(
            reportRequest["startTime"],
            reportRequest["endTime"],
            "TransferSize",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal
        dataDict, granularity = retVal["Value"]
        return S_OK({"data": dataDict, "granularity": granularity})

    def _plotTransferedData(self, reportRequest, plotInfo, filename):
        """It creates the plot.

        :param dict reportRequest: plot attributes
        :param dict plotInfo: contains all the data which are used to create the plot
        :param str filename:
        :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
        """
        metadata = {
            "title": "Transfered data by %s" % reportRequest["grouping"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
            "ylabel": "MB",
            "sort_labels": "last_value",
        }
        return self._generateCumulativePlot(filename, plotInfo["data"], metadata)

    def _reportThroughput(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        retVal = self._getTimedData(
            reportRequest["startTime"],
            reportRequest["endTime"],
            "TransferSize",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal
        dataDict, granularity = retVal["Value"]
        return S_OK({"data": dataDict, "granularity": granularity, "unit": "Bytes"})

    def _plotThroughput(self, reportRequest, plotInfo, filename):
        """It creates the plot.

        :param dict reportRequest: plot attributes
        :param dict plotInfo: contains all the data which are used to create the plot
        :param str filename:
        :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
        """
        metadata = {
            "title": "Throughput by %s" % reportRequest["grouping"],
            "ylabel": plotInfo["unit"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
        }
        return self._generateTimedStackedBarPlot(filename, plotInfo["data"], metadata)

    _reportDataTransferedName = "Pie chart of transferred data"

    def _reportDataTransfered(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        retVal = self._getTimedData(
            reportRequest["startTime"],
            reportRequest["endTime"],
            "TransferSize",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal
        dataDict, granularity = retVal["Value"]
        dataDict = self._sumDictValues(dataDict)
        for key in dataDict:
            dataDict[key] = int(dataDict[key])
        return S_OK({"data": dataDict})

    def _plotDataTransfered(self, reportRequest, plotInfo, filename):
        """It creates the plot.

        :param dict reportRequest: plot attributes
        :param dict plotInfo: contains all the data which are used to create the plot
        :param str filename:
        :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
        """
        metadata = {
            "title": "Total transfered data [bytes] by %s" % reportRequest["grouping"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
        }
        return self._generatePiePlot(filename, plotInfo["data"], metadata)
