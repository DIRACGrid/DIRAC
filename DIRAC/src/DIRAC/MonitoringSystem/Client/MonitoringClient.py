"""
This is the client of the Monitoring service based on Elasticsearch.
"""
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.Plotting.FileCoding import codeRequestInFileId


@createClient("Monitoring/Monitoring")
class MonitoringClient(Client):
    """
    .. class:: MonitoringClient

    This class expose the methods of the Monitoring Service

    """

    def __init__(self, **kwargs):
        """Simple constructor"""

        super().__init__(**kwargs)
        self.setServer("Monitoring/Monitoring")

    def generateDelayedPlot(
        self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs=None, compress=True
    ):
        """
        It is used to encode the plot parameters used to create a certain plot.

        :param str typeName: the type of the monitoring
        :param int startTime: epoch time, start time of the plot
        :param int endTime: epoch time, end time of the plot
        :param dict condDict: is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
        :param str grouping: is the grouping of the data for example: 'Site'
        :param dict extraArgs: epoch time which can be last day, last week, last month
        :param bool compress: apply compression of the encoded values.

        :return: S_OK(str) or S_ERROR() it returns the encoded plot parameters
        """
        if not isinstance(extraArgs, dict):
            extraArgs = {}
        plotRequest = {
            "typeName": typeName,
            "reportName": reportName,
            "startTime": startTime,
            "endTime": endTime,
            "condDict": condDict,
            "grouping": grouping,
            "extraArgs": extraArgs,
        }
        return codeRequestInFileId(plotRequest, compress)

    def getReport(self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs=None):
        """
        It is used to get the raw data used to create a plot.

        :param str typeName: the type of the monitoring
        :param str reportName: the name of the plotter used to create the plot for example:  NumberOfJobs
        :param int startTime: epoch time, start time of the plot
        :param int endTime: epoch time, end time of the plot
        :param dict condDict: is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
        :param str grouping: is the grouping of the data for example: 'Site'
        :param dict extraArgs: epoch time which can be last day, last week, last month
        :rerturn: S_OK or S_ERROR
        """
        if not isinstance(extraArgs, dict):
            extraArgs = {}
        plotRequest = {
            "typeName": typeName,
            "reportName": reportName,
            "startTime": startTime,
            "endTime": endTime,
            "condDict": condDict,
            "grouping": grouping,
            "extraArgs": extraArgs,
        }
        result = self._getRPC().getReport(plotRequest)
        if "rpcStub" in result:
            del result["rpcStub"]
        return result
