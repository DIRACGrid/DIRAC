
from DIRAC import S_OK, S_ERROR
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.AccountingSystem.Client.Types.PilotSubmission import PilotSubmission


class PilotSubmissionPlotter(BaseReporter):

  _typeName = "PilotSubmission"
  _typeKeyFields = [dF[0] for dF in PilotSubmission().definitionKeyFields]

  def _reportSubmission(self, reportRequest):
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
    dataDict, maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit(dataDict,
                                                                                 self._getAccumulationMaxValue(dataDict),
                                                                                 "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotSubmission(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Number of Submission by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateTimedStackedBarPlot(filename, plotInfo['graphDataDict'], metadata)

  _reportSubmissionEfficiencyName = "Submission efficiency"

  def _reportSubmissionEfficiency(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s), SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'NumTotal', 'NumSucceeded'])

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
    metadata = {'title': 'Pilot Submission efficiency by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity']}

    return self._generateQualityPlot(filename, plotInfo['data'], metadata)
