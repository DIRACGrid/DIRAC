"""
The Monitoring/Monitoring service interacts with the ElasticSearch backend
exposed by MonitoringDB.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN Monitoring
  :end-before: ##END
  :dedent: 2
  :caption: Monitoring options


"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import os
import six

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId
from DIRAC.Core.Utilities.Plotting.Plots import generateErrorMessagePlot
from DIRAC.Core.Utilities.File import mkDir

from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB
from DIRAC.MonitoringSystem.private.MainReporter import MainReporter

__RCSID__ = "$Id$"


class MonitoringHandler(RequestHandler):

  """
  .. class:: MonitoringHandler

  :param dict __reportRequestDict: contains the arguments used to create a certain plot
  :param object __db: used to retrieve the data from the db.

  """

  __reportRequestDict = {'typeName': six.string_types,
                         'reportName': six.string_types,
                         'startTime': Time._allDateTypes,
                         'endTime': Time._allDateTypes,
                         'condDict': dict,
                         'grouping': six.string_types,
                         'extraArgs': dict}

  __db = None

  @classmethod
  def initializeHandler(cls, serviceInfo):
    cls.__db = MonitoringDB()
    reportSection = serviceInfo['serviceSectionPath']
    dataPath = gConfig.getValue("%s/DataLocation" % reportSection, "data/monitoringPlots")
    gLogger.info("Data will be written into %s" % dataPath)
    mkDir(dataPath)
    try:
      testFile = "%s/moni.plot.test" % dataPath
      with open(testFile, "w") as _:
        os.unlink(testFile)
    except IOError as err:
      gLogger.fatal("Can't write to %s" % dataPath, err)
      return S_ERROR("Data location is not writable: %s" % repr(err))
    gDataCache.setGraphsLocation(dataPath)

    return S_OK()

  types_listUniqueKeyValues = [six.string_types]

  def export_listUniqueKeyValues(self, typeName):
    """
    :param str typeName: is the monitoring type registered in the Types.

    :return: S_OK({key:[]}) or S_ERROR()   The key is element of the __keyFields of the BaseType
    """
    setup = self.serviceInfoDict.get('clientSetup', None)
    if not setup:
      return S_ERROR("FATAL ERROR:  Problem with the service configuration!")
    # NOTE: we can apply some policies if it will be needed!
    return self.__db.getKeyValues(typeName)

  types_listReports = [six.string_types]

  def export_listReports(self, typeName):
    """
    :param str typeName: monitoring type for example WMSHistory

    :return: S_OK([]) or S_ERROR() the list of available plots
    """

    reporter = MainReporter(self.__db, self.serviceInfoDict['clientSetup'])
    return reporter.list(typeName)

  def transfer_toClient(self, fileId, token, fileHelper):
    """
    Get graphs data

    :param str fileId: encoded plot attributes
    :param object token: ???
    :param DIRAC.Core.DISET.private.FileHelper.FileHelper fileHelper:

    """

    # First check if we've got to generate the plot
    if len(fileId) > 5 and fileId[1] == ':':
      gLogger.info("Seems the file request is a plot generation request!")
      try:
        result = self.__generatePlotFromFileId(fileId)
      except Exception as e:  # pylint: disable=broad-except
        gLogger.exception("Exception while generating plot", str(e))
        result = S_ERROR("Error while generating plot: %s" % str(e))
      if not result['OK']:
        self.__sendErrorAsImg(result['Message'], fileHelper)
        fileHelper.sendEOF()
        return result
      fileId = result['Value']

    retVal = gDataCache.getPlotData(fileId)
    if not retVal['OK']:
      self.__sendErrorAsImg(retVal['Message'], fileHelper)
      return retVal
    retVal = fileHelper.sendData(retVal['Value'])
    if not retVal['OK']:
      return retVal
    fileHelper.sendEOF()
    return S_OK()

  def __generatePlotFromFileId(self, fileId):
    """
    It create the plots using the encode parameters

    :param str fileId: the encoded plot attributes
    :return S_OK or S_ERROR returns the file name
    """

    result = extractRequestFromFileId(fileId)
    if not result['OK']:
      return result
    plotRequest = result['Value']
    gLogger.info("Generating the plots..")
    result = self.export_generatePlot(plotRequest)
    if not result['OK']:
      gLogger.error("Error while generating the plots", result['Message'])
      return result
    fileToReturn = 'plot'
    if 'extraArgs' in plotRequest:
      extraArgs = plotRequest['extraArgs']
      if 'thumbnail' in extraArgs and extraArgs['thumbnail']:
        fileToReturn = 'thumbnail'
    gLogger.info("Returning %s file: %s " % (fileToReturn, result['Value'][fileToReturn]))
    return S_OK(result['Value'][fileToReturn])

  def __sendErrorAsImg(self, msgText, fileHelper):
    """
    In case of an error message a whcite plot is created with the error message.
    """

    retVal = generateErrorMessagePlot(msgText)
    if not retVal:
      retVal = fileHelper.sendData(retVal['Message'])
    else:
      retVal = fileHelper.sendData(retVal['Value'])
      if not retVal['OK']:
        return retVal
      fileHelper.sendEOF()
    return S_OK()

  def __checkPlotRequest(self, reportRequest):
    """
    It check the plot attributes. We have to make sure that all attributes which are needed are provided.

    :param dict reportRequest: contains the plot attributes.
    """
    # If extraArgs is not there add it
    if 'extraArgs' not in reportRequest:
      reportRequest['extraArgs'] = {}
    if not isinstance(reportRequest['extraArgs'], self.__reportRequestDict['extraArgs']):
      return S_ERROR("Extra args has to be of type %s" % self.__reportRequestDict['extraArgs'])
    reportRequestExtra = reportRequest['extraArgs']

    # Check sliding plots
    if 'lastSeconds' in reportRequestExtra:
      try:
        lastSeconds = int(reportRequestExtra['lastSeconds'])
      except ValueError:
        gLogger.error("lastSeconds key must be a number")
        return S_ERROR("Value Error")
      if lastSeconds < 3600:
        return S_ERROR("lastSeconds must be more than 3600")
      now = Time.dateTime()  # this is an UTC time
      reportRequest['endTime'] = now
      reportRequest['startTime'] = now - datetime.timedelta(seconds=lastSeconds)
    else:
      # if end date is not there, just set it to now
      if not reportRequest.get('endTime'):
        # check the existence of the endTime it can be present and empty
        reportRequest['endTime'] = Time.dateTime()
    # Check keys
    for key in self.__reportRequestDict:
      if key not in reportRequest:
        return S_ERROR('Missing mandatory field %s in plot request' % key)

      if not isinstance(reportRequest[key], self.__reportRequestDict[key]):
        return S_ERROR("Type mismatch for field %s (%s), required one of %s" % (
            key, str(type(reportRequest[key])), str(self.__reportRequestDict[key])))
      if key in ('startTime', 'endTime'):
        reportRequest[key] = int(Time.toEpoch(reportRequest[key]))

    return S_OK(reportRequest)

  types_generatePlot = [dict]

  def export_generatePlot(self, reportRequest):
    """
    It creates a plots for a given request

    :param dict reportRequest: contains the plot arguments...
    """
    retVal = self.__checkPlotRequest(reportRequest)
    if not retVal['OK']:
      return retVal
    reporter = MainReporter(self.__db, self.serviceInfoDict['clientSetup'])
    reportRequest['generatePlot'] = True
    return reporter.generate(reportRequest, self.getRemoteCredentials())

  types_getReport = [dict]

  def export_getReport(self, reportRequest):
    """
    It is used to get the raw data used to create a plot.
    The reportRequest has the following keys:

    str typeName: the type of the monitoring
    str reportName: the name of the plotter used to create the plot for example: NumberOfJobs
    int startTime: epoch time, start time of the plot
    int endTime: epoch time, end time of the plot
    dict condDict: is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
    str grouping: is the grouping of the data for example: 'Site'
    dict extraArgs: epoch time which can be last day, last week, last month

    :returns: S_OK or S_ERROR S_OK value is a dictionary which contains all values used to create the plot
    """
    retVal = self.__checkPlotRequest(reportRequest)
    if not retVal['OK']:
      return retVal
    reporter = MainReporter(self.__db, self.serviceInfoDict['clientSetup'])
    reportRequest['generatePlot'] = False
    return reporter.generate(reportRequest, self.getRemoteCredentials())

  types_addMonitoringRecords = [six.string_types, list]

  def export_addMonitoringRecords(self, monitoringtype, data):
    """
    Bulk insert data directly to the given monitoring type.

    :param str monitoringtype: monitoring type name
    :param list data: list of documents
    :returns: S_OK or S_ERROR
    """

    retVal = self.__db.getIndexName(monitoringtype)
    if not retVal['OK']:
      return retVal
    prefix = retVal['Value']
    gLogger.debug("addMonitoringRecords:", prefix)
    return self.__db.bulk_index(prefix, data)

  types_addRecords = [six.string_types, six.string_types, list]

  def export_addRecords(self, indexname, monitoringType, data):
    """
    It is used to insert data directly to the database... The data will be inserted to the given index.

    :param str indexname: name of the index
    :param str monitoringType: type of the monitoring
    :param list data: data to insert
    :returns: S_OK or S_ERROR
    """
    setup = self.serviceInfoDict.get('clientSetup', '')
    indexname = "%s_%s" % (setup.lower(), indexname)
    gLogger.debug("Bulk index:", indexname)
    mapping = self.__db.getMapping(monitoringType)
    gLogger.debug("Mapping:", mapping)
    return self.__db.bulk_index(indexname, data, mapping)

  types_deleteIndex = [six.string_types]

  def export_deleteIndex(self, indexName):
    """
    It is used to delete an index!
    Note this is for experienced users!!!

    :param str indexName: name of the index
    """
    setup = self.serviceInfoDict.get('clientSetup', '')
    indexName = "%s_%s" % (setup.lower(), indexName)
    gLogger.debug("delete index:", indexName)
    return self.__db.deleteIndex(indexName)

  types_getLastDayData = [six.string_types, dict]

  def export_getLastDayData(self, typeName, condDict):
    """
    It returns the data from the last day index. Note: we create daily indexes.

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                   * key -> name of the field
                   * value -> list of possible values
    """

    return self.__db.getLastDayData(typeName, condDict)

  types_getLimitedDat = [six.string_types, dict, int]

  def export_getLimitedData(self, typeName, condDict, size):
    '''
    Returns a list of records for a given selection.

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                   * key -> name of the field
                   * value -> list of possible values

    :param int size: Indicates how many entries should be retrieved from the log
    :return: Up to size entries for the given component from the database
    '''
    return self.__db.getLimitedData(typeName, condDict, size)

  types_getDataForAGivenPeriod = [six.string_types, dict, six.string_types, six.string_types]

  def export_getDataForAGivenPeriod(self, typeName, condDict, initialDate='', endDate=''):
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
    return self.__db.getDataForAGivenPeriod(typeName, condDict, initialDate, endDate)

  types_put = [list, six.string_types]

  def export_put(self, recordsToInsert, monitoringType):
    """
    It is used to insert records to the db.

    :param recordsToInsert: records to be inserted to the db
    :param str monitoringType: monitoring type...
    :type recordsToInsert: python:list
    """

    return self.__db.put(recordsToInsert, monitoringType)

  types_pingDB = []

  def export_pingDB(self):
    """
    We can check, if the db is available.
    """
    return self.__db.pingDB()
