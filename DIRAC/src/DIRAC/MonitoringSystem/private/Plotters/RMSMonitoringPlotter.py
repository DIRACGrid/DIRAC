"""
This class is used to define the plot using the plot attributes.
"""

from DIRAC import S_OK, gLogger

from DIRAC.MonitoringSystem.Client.Types.RMSMonitoring import RMSMonitoring
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter


class RMSMonitoringPlotter(BasePlotter):

    """
    .. class:: RMSMonitoringPlotter

    It is used to crate the plots.

    param: str _typeName monitoring type
    param: list _typeKeyFields list of keys what we monitor (list of attributes)
    """

    _typeName = "RMSMonitoring"
    _typeKeyFields = RMSMonitoring().keyFields

    def __reportAllResources(self, reportRequest, metric, unit):
        retVal = self._getTimedData(
            startTime=reportRequest["startTime"],
            endTime=reportRequest["endTime"],
            selectField=metric,
            preCondDict=reportRequest["condDict"],
            metadataDict={"DynamicBucketing": False, "metric": "sum"},
        )
        if not retVal["OK"]:
            return retVal

        dataDict, granularity = retVal["Value"]
        try:
            _, _, _, unitName = self._findSuitableUnit(dataDict, self._getAccumulationMaxValue(dataDict), unit)
        except AttributeError as e:
            gLogger.warn(e)
            unitName = unit

        return S_OK({"data": dataDict, "granularity": granularity, "unit": unitName})

    def __plotAllResources(self, reportRequest, plotInfo, filename, title):
        metadata = {
            "title": f"{title} by {reportRequest['grouping']}",
            "starttime": reportRequest["startTime"],
            "endtime": reportRequest["endTime"],
            "span": plotInfo["granularity"],
            "skipEdgeColor": True,
            "ylabel": plotInfo["unit"],
        }

        plotInfo["data"] = self._fillWithZero(
            granularity=plotInfo["granularity"],
            startEpoch=reportRequest["startTime"],
            endEpoch=reportRequest["endTime"],
            dataDict=plotInfo["data"],
        )

        return self._generateStackedLinePlot(filename=filename, dataDict=plotInfo["data"], metadata=metadata)

    _reportnbObjectName = "Number of objects(Request, Operation, or File)"

    def _reportnbObject(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """
        return self.__reportAllResources(reportRequest, "nbObject", "objects")

    def _plotnbObject(self, reportRequest, plotInfo, filename):
        """It creates the plot.

        :param dict reportRequest: plot attributes
        :param dict plotInfo: contains all the data which are used to create the plot
        :param str filename:
        :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
        """
        return self.__plotAllResources(
            reportRequest, plotInfo, filename, "Number of objects(Request, Operation, or File)"
        )
