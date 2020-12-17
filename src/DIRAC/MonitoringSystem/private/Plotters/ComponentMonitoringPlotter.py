"""
This class is used to define the plot using the plot attributes.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, gLogger

from DIRAC.MonitoringSystem.Client.Types.ComponentMonitoring import ComponentMonitoring
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter

__RCSID__ = "$Id$"


class ComponentMonitoringPlotter(BasePlotter):

  """
  .. class:: ComponentMonitoringPlotter

  It is used to create the plots.

  param: str _typeName monitoring type
  param: list _typeKeyFields list of keys what we monitor (list of attributes)
  """

  _typeName = "ComponentMonitoring"
  _typeKeyFields = ComponentMonitoring().keyFields

  def __reportAllResources(self, reportRequest, metric, unit):

    retVal = self._getTimedData(startTime=reportRequest['startTime'],
                                endTime=reportRequest['endTime'],
                                selectField=metric,
                                preCondDict=reportRequest['condDict'],
                                metadataDict={'DynamicBucketing': False,
                                              "metric": "avg"})
    if not retVal['OK']:
      return retVal

    dataDict, granularity = retVal['Value']
    try:
      _, _, _, unitName = self._findSuitableUnit(dataDict, self._getAccumulationMaxValue(dataDict), unit)
    except AttributeError as e:
      gLogger.warn(e)
      unitName = unit

    return S_OK({'data': dataDict, 'granularity': granularity, 'unit': unitName})

  def __plotAllResources(self, reportRequest, plotInfo, filename, title):
    metadata = {'title': '%s by %s' % (title, reportRequest['grouping']),
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'skipEdgeColor': True,
                'ylabel': plotInfo['unit']}

    plotInfo['data'] = self._fillWithZero(granularity=plotInfo['granularity'],
                                          startEpoch=reportRequest['startTime'],
                                          endEpoch=reportRequest['endTime'],
                                          dataDict=plotInfo['data'])

    return self._generateStackedLinePlot(filename=filename,
                                         dataDict=plotInfo['data'],
                                         metadata=metadata)

  _reportRunningThreadsName = "Number of running threads"

  def _reportRunningThreads(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "threads", "threads")

  def _plotRunningThreads(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Number of running threads')

  _reportCpuUsageName = "CPU usage"

  def _reportCpuUsage(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "cpuPercentage", "percentage")

  def _plotCpuUsage(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'CPU usage')

  _reportMemoryUsageName = "Memory usage"

  def _reportMemoryUsage(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "memoryUsage", "bytes")

  def _plotMemoryUsage(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Memory usage')

  _reportRunningTimeName = "Running time"

  def _reportRunningTime(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "runningTime", "time")

  def _plotRunningTime(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Running time')

  _reportConnectionsName = "Number of Connections"

  def _reportConnections(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "Connections", "Connections")

  def _plotConnections(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Number of Connections')

  _reportActiveQueriesName = "Number of ActiveQueries"

  def _reportActiveQueries(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "ActiveQueries", "ActiveQueries")

  def _plotActiveQueries(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Number of ActiveQueries')

  _reportPendingQueriesName = "Number of PendingQueries"

  def _reportPendingQueries(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "PendingQueries", "PendingQueries")

  def _plotPendingQueries(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Number of PendingQueries')

  _reportMaxFDName = "Max File Descriptors"

  def _reportMaxFD(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return: S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "MaxFD", "MaxFD")

  def _plotMaxFD(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Max File Descriptors')

  _reportActivityRunningThreadsName = "Number of activity running threads(activity)"

  def _reportActivityRunningThreads(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "RunningThreads", "RunningThreads")

  def _plotActivityRunningThreads(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Number of running threads(activity)')

  _reportServiceResponseTimeName = "Service response time"

  def _reportServiceResponseTime(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "ServiceResponseTime", "seconds")

  def _plotServiceResponseTime(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Service response time')

  _reportAgentCycleDurationName = "Agent cycle duration"

  def _reportAgentCycleDuration(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "cycleDuration", "seconds")

  def _plotAgentCycleDuration(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Agent cycle duration')

  _reportAgentCyclesName = "Number of agent cycles"

  def _reportAgentCycles(self, reportRequest):
    """It is used to retrieve the data from the database.

    :param dict reportRequest: contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length
    """
    return self.__reportAllResources(reportRequest, "cycles", "AgentCycles")

  def _plotAgentCycles(self, reportRequest, plotInfo, filename):
    """It creates the plot.

    :param dict reportRequest: plot attributes
    :param dict plotInfo: contains all the data which are used to create the plot
    :param str filename:
    :return: S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    return self.__plotAllResources(reportRequest, plotInfo, filename, 'Number of agent cycles')
