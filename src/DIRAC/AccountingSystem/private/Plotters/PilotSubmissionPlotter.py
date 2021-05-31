from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.AccountingSystem.Client.Types.PilotSubmission import PilotSubmission

__RCSID__ = "$Id$"


class PilotSubmissionPlotter(BaseReporter):
  '''
  Plotter for the Pilot Submission Accounting
  '''

  _typeName = "PilotSubmission"
  _typeKeyFields = [dF[0] for dF in PilotSubmission().definitionKeyFields]

  def _reportSubmission(self, reportRequest):
    '''
    Get data for Submission plot

    :param dict reportRequest: Condition to select data
    '''

    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', 'NumTotal'])
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {})
    if not retVal['OK']:
      return retVal
    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    dataDict, _ = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _, unit = self._findSuitableRateUnit(dataDict,
                                                                      self._getAccumulationMaxValue(dataDict),
                                                                      "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unit})

  def _plotSubmission(self, reportRequest, plotInfo, filename):
    '''
    Make 1 dimensional pilotSubmission plot

    :param dict reportRequest: Condition to select data
    :param dict plotInfo: Data for plot
    :param str  filename: File name
    '''

    metadata = {'title': 'Number of Submission by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateTimedStackedBarPlot(filename, plotInfo['graphDataDict'], metadata)

  _reportSubmissionEfficiencyName = "Submission efficiency"

  def _reportSubmissionEfficiency(self, reportRequest):
    '''
    Get data for Submission Efficiency plot

    :param dict reportRequest: Condition to select data
    '''

    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s), SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'NumSucceeded', 'NumTotal'])

    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'checkNone': True,
                                 'convertToGranularity': 'sum',
                                 'calculateProportionalGauges': False,
                                 'consolidationFunction': self._efficiencyConsolidation})
    if not retVal['OK']:
      return retVal
    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    if len(dataDict) > 1:
      # Get the total for the plot
      selectFields = ("'Total', %s, %s, SUM(%s),SUM(%s)",
                      ['startTime', 'bucketLength',
                       'NumSucceeded', 'NumTotal'])

      retVal = self._getTimedData(reportRequest['startTime'],
                                  reportRequest['endTime'],
                                  selectFields,
                                  reportRequest['condDict'],
                                  reportRequest['groupingFields'],
                                  {'scheckNone': True,
                                   'convertToGranularity': 'sum',
                                   'calculateProportionalGauges': False,
                                   'consolidationFunction': self._efficiencyConsolidation})
      if not retVal['OK']:
        return retVal
      totalDict = retVal['Value'][0]
      self.stripDataField(totalDict, 0)
      for key in totalDict:
        dataDict[key] = totalDict[key]
    return S_OK({'data': dataDict, 'granularity': granularity})

  def _plotSubmissionEfficiency(self, reportRequest, plotInfo, filename):
    '''
    Make 2 dimensional pilotSubmission efficiency plot

    :param dict reportRequest: Condition to select data
    :param dict plotInfo: Data for plot.
    :param str  filename: File name
    '''

    metadata = {'title': 'Pilot Submission efficiency by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity']}

    return self._generateQualityPlot(filename, plotInfo['data'], metadata)
