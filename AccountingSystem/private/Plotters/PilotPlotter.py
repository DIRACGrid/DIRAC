from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.Types.Pilot import Pilot
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter


class PilotPlotter(BaseReporter):

  _typeName = "Pilot"
  _typeKeyFields = [dF[0] for dF in Pilot().definitionKeyFields]

  def _reportCumulativeNumberOfJobs(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'Jobs'
                                                          ]
                    )
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
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    dataDict = self._accumulate(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                             self._getAccumulationMaxValue(dataDict),
                                                                             "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotCumulativeNumberOfJobs(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Cumulative Jobs by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'],
                'sort_labels': 'last_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  def _reportNumberOfJobs(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'Jobs'
                                                          ]
                    )
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
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotNumberOfJobs(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Jobs by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateTimedStackedBarPlot(filename, plotInfo['graphDataDict'], metadata)

  def _reportCumulativeNumberOfPilots(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'entriesInBucket'
                                                          ]
                    )
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
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    dataDict = self._accumulate(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                             self._getAccumulationMaxValue(dataDict),
                                                                             "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotCumulativeNumberOfPilots(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Cumulative Pilots by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'].replace('job', 'pilot'),
                'sort_labels': 'last_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  def _reportNumberOfPilots(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'entriesInBucket'
                                                          ]
                    )
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
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotNumberOfPilots(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Pilots by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'].replace('job', 'pilot')}
    return self._generateTimedStackedBarPlot(filename, plotInfo['graphDataDict'], metadata)

  def _reportJobsPerPilot(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s), SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'Jobs', 'entriesInBucket'
                                                          ]
                    )
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'checkNone': True,
                                 'convertToGranularity': 'sum',
                                 'calculateProportionalGauges': False,
                                 'consolidationFunction': self._averageConsolidation})
    if not retVal['OK']:
      return retVal
    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    return S_OK({'data': dataDict, 'granularity': granularity})

  def _plotJobsPerPilot(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Jobs per pilot by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': "jobs/pilot",
                'normalization': max(x for y in plotInfo['data'].values() for x in y.values())}
    return self._generateQualityPlot(filename, plotInfo['data'], metadata)

  def _reportTotalNumberOfPilots(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", SUM(%s)",
                    reportRequest['groupingFields'][1] + ['entriesInBucket'
                                                          ]
                    )
    retVal = self._getSummaryData(reportRequest['startTime'],
                                  reportRequest['endTime'],
                                  selectFields,
                                  reportRequest['condDict'],
                                  reportRequest['groupingFields'],
                                  {})
    if not retVal['OK']:
      return retVal
    dataDict = retVal['Value']
    return S_OK({'data': dataDict})

  def _plotTotalNumberOfPilots(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Total Number of Pilots by %s' % reportRequest['grouping'],
                'ylabel': 'Pilots',
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime']
                }
    return self._generatePiePlot(filename, plotInfo['data'], metadata)
