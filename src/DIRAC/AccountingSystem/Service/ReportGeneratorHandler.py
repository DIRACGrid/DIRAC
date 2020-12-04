""" Module that holds the ReportGeneratorHandler class

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ReportGenerator
  :end-before: ##END
  :dedent: 2
  :caption: ReportGenerator options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import os
import datetime

from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Utilities import Time
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.AccountingSystem.DB.MultiAccountingDB import MultiAccountingDB
from DIRAC.Core.Utilities.Plotting import gDataCache
from DIRAC.AccountingSystem.private.MainReporter import MainReporter
from DIRAC.AccountingSystem.private.DBUtils import DBUtils
from DIRAC.AccountingSystem.private.Policies import gPoliciesList
from DIRAC.Core.Utilities.Plotting.Plots import generateErrorMessagePlot
from DIRAC.Core.Utilities.Plotting.FileCoding import extractRequestFromFileId
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RequestHandler import RequestHandler


class ReportGeneratorHandler(RequestHandler):
  """ DIRAC service class to retrieve information from the AccountingDB
  """

  __acDB = None
  __reportRequestDict = {'typeName': six.string_types,
                         'reportName': six.string_types,
                         'startTime': (datetime.datetime, datetime.date),
                         'endTime': (datetime.datetime, datetime.date),
                         'condDict': dict,
                         'grouping': six.string_types,
                         'extraArgs': dict}

  @classmethod
  def initializeHandler(cls, serviceInfo):
    multiPath = PathFinder.getDatabaseSection("Accounting/MultiDB")
    cls.__acDB = MultiAccountingDB(multiPath, readOnly=True)
    # Get data location
    reportSection = serviceInfo['serviceSectionPath']
    dataPath = gConfig.getValue("%s/DataLocation" % reportSection, "data/accountingGraphs")
    dataPath = dataPath.strip()
    if "/" != dataPath[0]:
      dataPath = os.path.realpath("%s/%s" % (gConfig.getValue('/LocalSite/InstancePath', rootPath), dataPath))
    gLogger.info("Data will be written into %s" % dataPath)
    mkDir(dataPath)
    try:
      testFile = "%s/acc.jarl.test" % dataPath
      with open(testFile, "w"):
        pass
      os.unlink(testFile)
    except IOError:
      gLogger.fatal("Can't write to %s" % dataPath)
      return S_ERROR("Data location is not writable")
    gDataCache.setGraphsLocation(dataPath)
    gMonitor.registerActivity("plotsDrawn", "Drawn plot images", "Accounting reports", "plots", gMonitor.OP_SUM)
    gMonitor.registerActivity("reportsRequested", "Generated reports", "Accounting reports",
                              "reports", gMonitor.OP_SUM)
    return S_OK()

  def __checkPlotRequest(self, reportRequest):
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
      now = Time.dateTime()
      reportRequest['endTime'] = now
      reportRequest['startTime'] = now - datetime.timedelta(seconds=lastSeconds)
    else:
      # if enddate is not there, just set it to now
      if not reportRequest.get('endTime', False):
        reportRequest['endTime'] = Time.dateTime()
    # Check keys
    for key in self.__reportRequestDict:
      if key not in reportRequest:
        return S_ERROR('Missing mandatory field %s in plot reques' % key)

      if not isinstance(reportRequest[key], self.__reportRequestDict[key]):
        return S_ERROR("Type mismatch for field %s (%s), required one of %s" % (key,
                                                                                str(type(reportRequest[key])),
                                                                                str(self.__reportRequestDict[key])))
      if key in ('startTime', 'endTime'):
        reportRequest[key] = int(Time.toEpoch(reportRequest[key]))

    return S_OK(reportRequest)

  types_generatePlot = [dict]

  def export_generatePlot(self, reportRequest):
    """
    Plot a accounting

    Arguments:
      - viewName : Name of view (easy!)
      - startTime
      - endTime
      - argsDict : Arguments to the view.
      - grouping
      - extraArgs
    """
    retVal = self.__checkPlotRequest(reportRequest)
    if not retVal['OK']:
      return retVal
    reporter = MainReporter(self.__acDB, self.serviceInfoDict['clientSetup'])
    gMonitor.addMark("plotsDrawn")
    reportRequest['generatePlot'] = True
    return reporter.generate(reportRequest, self.getRemoteCredentials())

  types_getReport = [dict]

  def export_getReport(self, reportRequest):
    """
    Plot a accounting

    Arguments:
      - viewName : Name of view (easy!)
      - startTime
      - endTime
      - argsDict : Arguments to the view.
      - grouping
      - extraArgs
    """
    retVal = self.__checkPlotRequest(reportRequest)
    if not retVal['OK']:
      return retVal
    reporter = MainReporter(self.__acDB, self.serviceInfoDict['clientSetup'])
    gMonitor.addMark("reportsRequested")
    reportRequest['generatePlot'] = False
    return reporter.generate(reportRequest, self.getRemoteCredentials())

  types_listReports = [six.string_types]

  def export_listReports(self, typeName):
    """
    List all available plots

    Arguments:
      - none
    """
    reporter = MainReporter(self.__acDB, self.serviceInfoDict['clientSetup'])
    return reporter.list(typeName)

  types_listUniqueKeyValues = [six.string_types]

  def export_listUniqueKeyValues(self, typeName):
    """
    List all values for all keys in a type

    Arguments:
      - none
    """
    dbUtils = DBUtils(self.__acDB, self.serviceInfoDict['clientSetup'])
    credDict = self.getRemoteCredentials()
    if typeName in gPoliciesList:
      policyFilter = gPoliciesList[typeName]
      filterCond = policyFilter.getListingConditions(credDict)
    else:
      policyFilter = gPoliciesList['Null']
      filterCond = {}
    retVal = dbUtils.getKeyValues(typeName, filterCond)
    if not policyFilter or not retVal['OK']:
      return retVal
    return policyFilter.filterListingValues(credDict, retVal['Value'])

  def __generatePlotFromFileId(self, fileId):
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
    retVal = generateErrorMessagePlot(msgText)
    retVal = fileHelper.sendData(retVal['Value'])
    if not retVal['OK']:
      return retVal
    fileHelper.sendEOF()
    return S_OK()

  def transfer_toClient(self, fileId, token, fileHelper):
    """
    Get graphs data
    """
    # First check if we've got to generate the plot
    if len(fileId) > 5 and fileId[1] == ':':
      gLogger.info("Seems the file request is a plot generation request!")
      # Seems a request for a plot!
      try:
        result = self.__generatePlotFromFileId(fileId)
      except Exception as e:
        gLogger.exception("Exception while generating plot")
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
