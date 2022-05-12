"""
This class is used to define the plot using the plot attributes.
"""

from DIRAC import S_OK, S_ERROR

from DIRAC.MonitoringSystem.Client.Types.PilotSubmissionMonitoring import PilotSubmissionMonitoring
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter


class PilotSubmissionMonitoringPlotter(BasePlotter):
    """
    .. class:: PilotSubmissionMonitoringPlotter

    It is used to crate the plots.

    param: str _typeName monitoring type
    param: list _typeKeyFields list of keys what we monitor (list of attributes)
    """

    _typeName = "PilotSubmissionMonitoring"
    _typeKeyFields = PilotSubmissionMonitoring().keyFields

    _reportNumberOfSubmissions = "Total Number of Submissions"

    def _reportNumberOfSubmissions(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        retVal = self._getTimedData(
            startTime=reportRequest["startTime"],
            endTime=reportRequest["endTime"],
            selectField="NumTotal",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal
        dataDict, granularity = retVal["Value"]
        return S_OK({"data": dataDict, "granularity": granularity})

    def _plotNumberOfSubmissions(self, reportRequest, plotInfo, filename):
        """It creates the plot.

        :param dict reportRequest: plot attributes
        :param dict plotInfo: contains all the data which are used to create the plot
        :param str filename:
        :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
        """
        metadata = {
            "title": "Pilot Submissions by %s" % reportRequest["grouping"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
            "skipEdgeColor": True,
            "ylabel": "Submissions",
        }

        plotInfo["data"] = self._fillWithZero(
            granularity=plotInfo["granularity"],
            startEpoch=reportRequest["startTime"],
            endEpoch=reportRequest["endTime"],
            dataDict=plotInfo["data"],
        )

        return self._generateStackedLinePlot(filename=filename, dataDict=plotInfo["data"], metadata=metadata)

    _reportNumSucceededName = "Submission Efficiency"

    def _reportNumSucceeded(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        # Retrieve the number of succeeded submissions
        retVal = self._getTimedData(
            startTime=reportRequest["startTime"],
            endTime=reportRequest["endTime"],
            selectField="NumSucceeded",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retVal["OK"]:
            return retVal

        # Retrieve the number of total submissions
        retTotVal = self._getTimedData(
            startTime=reportRequest["startTime"],
            endTime=reportRequest["endTime"],
            selectField="NumTotal",
            preCondDict=reportRequest["condDict"],
            metadataDict=None,
        )
        if not retTotVal["OK"]:
            return retTotVal

        dataDict, granularity = retVal["Value"]
        totDataDict, granularity = retTotVal["Value"]
        # Check that the dicts are not empty
        if dataDict and totDataDict:
            # Return the efficiency in dataDict
            effDict = self._calculateEfficiencyDict(totDataDict, dataDict)
            return S_OK({"data": effDict, "granularity": granularity})
        else:
            return S_ERROR("No data available for this selection")

    def _plotNumSucceeded(self, reportRequest, plotInfo, filename):
        """
        Make 2 dimensional pilotSubmission efficiency plot

        :param dict reportRequest: Condition to select data
        :param dict plotInfo: Data for plot.
        :param str  filename: File name
        """
        metadata = {
            "title": "Pilot Submission efficiency by %s" % reportRequest["grouping"],
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
        }
        return self._generateQualityPlot(filename, plotInfo["data"], metadata)
