"""
This is the client of the Monitoring service based on Elasticsearch.
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Plotting.FileCoding import codeRequestInFileId

__RCSID__ = "$Id$"


class MonitoringClient(object):
  """
  .. class:: MonitoringClient

  This class expose the methods of the Monitoring Service

  :param ~DIRAC.Core.DISET.RPCClient.RPCClient __rpcClient: stores the rpc client used to connect to the service

  """

  def __init__(self, rpcClient=None):
    self.__rpcClient = rpcClient

  def __getServer(self, timeout=3600):
    """It returns the access protocol to the Monitoring service"""
    if self.__rpcClient:
      return self.__rpcClient
    return RPCClient('Monitoring/Monitoring', timeout=timeout)

  #############################################################################
  def listUniqueKeyValues(self, typeName):
    """
    :param str typeName: is the monitoring type registered in the Types.

    :return: S_OK({key:[]}) or S_ERROR()   The key is element of the __keyFields of the BaseType
    """
    server = self.__getServer()
    return server.listUniqueKeyValues(typeName)

  #############################################################################
  def listReports(self, typeName):
    """
    :param str typeName: monitoring type for example WMSHistory

    :return: S_OK([]) or S_ERROR() the list of available plots
    """
    rpcClient = self.__getServer()
    return rpcClient.listReports(typeName)

  def generateDelayedPlot(
          self,
          typeName,
          reportName,
          startTime,
          endTime,
          condDict,
          grouping,
          extraArgs=None,
          compress=True):
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
    plotRequest = {'typeName': typeName,
                   'reportName': reportName,
                   'startTime': startTime,
                   'endTime': endTime,
                   'condDict': condDict,
                   'grouping': grouping,
                   'extraArgs': extraArgs}
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
    rpcClient = self.__getServer()
    if not isinstance(extraArgs, dict):
      extraArgs = {}
    plotRequest = {'typeName': typeName,
                   'reportName': reportName,
                   'startTime': startTime,
                   'endTime': endTime,
                   'condDict': condDict,
                   'grouping': grouping,
                   'extraArgs': extraArgs}
    result = rpcClient.getReport(plotRequest)
    if 'rpcStub' in result:
      del result['rpcStub']
    return result

  def addMonitoringRecords(self, monitoringtype, doc_type, data):
    """
    :param str monitoringtype:
    :param str doc_type:
    :param dict data:
    """
    rpcClient = self.__getServer()
    return rpcClient.addMonitoringRecords(monitoringtype, doc_type, data)

  def addRecords(self, indexName, doc_type, data):
    """
    add records to the database

    :param str indexName:
    :param str doc_type:
    :param dict data:
    """
    rpcClient = self.__getServer()
    return rpcClient.addRecords(indexName, doc_type, data)

  def deleteIndex(self, indexName):
    """
    It deletes a specific index...

    :param str indexName:
    """
    rpcClient = self.__getServer()
    return rpcClient.deleteIndex(indexName)

  def getLastDayData(self, typeName, condDict):
    """
    It returns the data from the last day index. Note: we create daily indexes.

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                  * key -> name of the field
                  * value -> list of possible values
    """
    rpcClient = self.__getServer()
    return rpcClient.getLastDayData(typeName, condDict)

  def getLimitedData(self, typeName, condDict, size=10):
    '''
    Returns a list of records for a given selection.

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                  * key -> name of the field
                  * value -> list of possible values

    :return: Up to size entries for the given component from the database
    '''
    rpcClient = self.__getServer()
    return rpcClient.getLimitedData(typeName, condDict, size)

  def getDataForAGivenPeriod(self, typeName, condDict, initialDate='', endDate=''):
    """
    Retrieves the history of logging entries for the given component during a given given time period

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                  * key -> name of the field
                  * value -> list of possible values

    :param str initialDate: Indicates the start of the time period in the format 'DD/MM/YYYY hh:mm'
    :param str endDate: Indicate the end of the time period in the format 'DD/MM/YYYY hh:mm'
    :return: Entries from the database for the given component recorded between the initial and the end dates

    """
    rpcClient = self.__getServer()
    return rpcClient.getDataForAGivenPeriod(typeName, condDict, initialDate, endDate)
