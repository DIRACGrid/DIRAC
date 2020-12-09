from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter


class DataOperationPlotter(BaseReporter):

  _typeName = "DataOperation"
  _typeKeyFields = [dF[0] for dF in DataOperation().definitionKeyFields]

  def _translateGrouping(self, grouping):
    if grouping == "Channel":
      return ("%s, %s", ['Source', 'Destination'], "CONCAT( %s, ' -> ', %s )")
    else:
      return ("%s", [grouping])

  _reportSuceededTransfersName = "Successful transfers"

  def _reportSuceededTransfers(self, reportRequest):
    return self.__reportTransfers(reportRequest, 'Succeeded', ('Failed', 0))

  _reportFailedTransfersName = "Failed transfers"

  def _reportFailedTransfers(self, reportRequest):
    return self.__reportTransfers(reportRequest, 'Failed', ('Succeeded', 1))

  def __reportTransfers(self, reportRequest, titleType, togetherFieldsToPlot):
    selectFields = (
        self._getSelectStringForGrouping(
            reportRequest['groupingFields']) + ", %s, %s, SUM(%s), SUM(%s)-SUM(%s)",
        reportRequest['groupingFields'][1] + [
            'startTime',
            'bucketLength',
            'TransferOK',
            'TransferTotal',
            'TransferOK',
        ])
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {})
    if not retVal['OK']:
      return retVal
    dataDict, granularity = retVal['Value']
    strippedData = self.stripDataField(dataDict, togetherFieldsToPlot[1])
    if strippedData:
      dataDict[togetherFieldsToPlot[0]] = strippedData[0]
    dataDict, maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "files")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotSuceededTransfers(self, reportRequest, plotInfo, filename):
    return self.__plotTransfers(reportRequest, plotInfo, filename, 'Succeeded', ('Failed', 0))

  def _plotFailedTransfers(self, reportRequest, plotInfo, filename):
    return self.__plotTransfers(reportRequest, plotInfo, filename, 'Failed', ('Succeeded', 1))

  def __plotTransfers(self, reportRequest, plotInfo, filename, titleType, togetherFieldsToPlot):
    metadata = {'title': '%s Transfers by %s' % (titleType, reportRequest['grouping']),
                'ylabel': plotInfo['unit'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity']}
    return self._generateTimedStackedBarPlot(filename, plotInfo['graphDataDict'], metadata)

  _reportQualityName = "Efficiency by protocol"

  def _reportQuality(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s), SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'TransferOK', 'TransferTotal'
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
                                 'consolidationFunction': self._efficiencyConsolidation})
    if not retVal['OK']:
      return retVal
    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    if len(dataDict) > 1:
      # Get the total for the plot
      selectFields = ("'Total', %s, %s, SUM(%s),SUM(%s)",
                      ['startTime', 'bucketLength',
                       'TransferOK', 'TransferTotal'
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
                                   'consolidationFunction': self._efficiencyConsolidation})
      if not retVal['OK']:
        return retVal
      totalDict = retVal['Value'][0]
      self.stripDataField(totalDict, 0)
      for key in totalDict:
        dataDict[key] = totalDict[key]
    return S_OK({'data': dataDict, 'granularity': granularity})

  def _plotQuality(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Transfer quality by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity']}
    return self._generateQualityPlot(filename, plotInfo['data'], metadata)

  _reportTransferedDataName = "Cumulative transferred data"

  def _reportTransferedData(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'TransferSize'
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
                                                                             "bytes")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotTransferedData(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Transfered data by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'],
                'sort_labels': 'last_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  def _reportThroughput(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'TransferSize'
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
        dataDict, self._getAccumulationMaxValue(dataDict), "bytes")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotThroughput(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Throughput by %s' % reportRequest['grouping'],
                'ylabel': plotInfo['unit'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity']}
    return self._generateTimedStackedBarPlot(filename, plotInfo['graphDataDict'], metadata)

  _reportDataTransferedName = "Pie chart of transferred data"

  def _reportDataTransfered(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", SUM(%s)",
                    reportRequest['groupingFields'][1] + ['TransferSize'
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
    for key in dataDict:
      dataDict[key] = int(dataDict[key])
    return S_OK({'data': dataDict})

  def _plotDataTransfered(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Total data transfered by %s' % reportRequest['grouping'],
                'ylabel': 'bytes',
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime']
                }
    return self._generatePiePlot(filename, plotInfo['data'], metadata)
