"""
This class is used to define the plot using the plot attributes.
"""

from DIRAC import S_OK

from DIRAC.MonitoringSystem.Client.Types.DataOperation import DataOperation
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter


class DataOperationPlotter(BasePlotter):

    """
    .. class:: DataOperationPlotter

    It is used to crate the plots.

    param: str _typeName monitoring type
    param: list _typeKeyFields list of keys what we monitor (list of attributes)
    """

    _typeName = "DataOperation"
    _typeKeyFields = DataOperation().keyFields

    def _reportTransferSize(self, reportRequest):
        """It is used to retrieve the data from the database.

        :param dict reportRequest: contains attributes used to create the plot.
        :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
        """

    def _plotTransferSize(self, reportRequest, plotInfo, filename):
        """It creates the plot.

        :param dict reportRequest: plot attributes
        :param dict plotInfo: contains all the data which are used to create the plot
        :param str filename:
        :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
        """
