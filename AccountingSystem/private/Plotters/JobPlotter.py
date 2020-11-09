""" Recipes on how to create job plots
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.Types.Job import Job
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter


class JobPlotter(BaseReporter):

  _typeName = "Job"
  _typeKeyFields = [dF[0] for dF in Job().definitionKeyFields]

  def _translateGrouping(self, grouping):
    if grouping == "Country":
      sqlRepr = 'upper( substring( %s, locate( ".", %s, length( %s ) - 4 ) + 1 ) )'
      return (sqlRepr, ['Site', 'Site', 'Site'], sqlRepr)
    elif grouping == "Grid":
      return ('substring_index( %s, ".", 1 )', ['Site'])
    else:
      return ("%s", [grouping])

  _reportCPUEfficiencyName = "CPU efficiency"

  def _reportCPUEfficiency(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s), SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'CPUTime', 'ExecTime'
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
                       'CPUTime', 'ExecTime'
                       ]
                      )

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

  def _plotCPUEfficiency(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Job CPU efficiency by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity']}
    return self._generateQualityPlot(filename, plotInfo['data'], metadata)

  _reportCPUUsedName = "Cumulative CPU time"

  def _reportCPUUsed(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'CPUTime'
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
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                              self._getAccumulationMaxValue(dataDict),
                                                                              "time")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotCPUUsed(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'CPU used by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'],
                'sort_labels': 'max_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportCPUUsageName = "CPU time"

  def _reportCPUUsage(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'CPUTime'
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "time")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotCPUUsage(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'CPU usage by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportNormCPUUsedName = "Cumulative Normalized CPU"

  def _reportNormCPUUsed(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'NormCPUTime'
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
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                              self._getAccumulationMaxValue(dataDict),
                                                                              "cpupower")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotNormCPUUsed(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Normalized CPU used by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'],
                'sort_labels': 'max_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportNormCPUUsageName = "Normalized CPU power"

  def _reportNormCPUUsage(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'NormCPUTime'
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "cpupower")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotNormCPUUsage(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Normalized CPU usage by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportWallTimeName = "Wall time"

  def _reportWallTime(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'ExecTime'
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "time")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotWallTime(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Wall Time by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportRunningJobsName = "Running jobs"

  def _reportRunningJobs(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'ExecTime'
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                              self._getAccumulationMaxValue(dataDict),
                                                                              "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotRunningJobs(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Running jobs by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportTotalCPUUsedName = "Pie plot of CPU used"

  def _reportTotalCPUUsed(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", SUM(%s)/86400",
                    reportRequest['groupingFields'][1] + ['CPUTime'
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

  def _plotTotalCPUUsed(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'CPU days used by %s' % reportRequest['grouping'],
                'ylabel': 'CPU days',
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime']
                }
    return self._generatePiePlot(filename, plotInfo['data'], metadata)

  _reportAccumulatedWallTimeName = "Cumulative wall time"

  def _reportAccumulatedWallTime(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength',
                                                          'ExecTime'
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
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                              self._getAccumulationMaxValue(dataDict),
                                                                              "time")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotAccumulatedWallTime(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Cumulative wall time by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit'],
                'sort_labels': 'max_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportTotalWallTimeName = "Pie plot of wall time usage"

  def _reportTotalWallTime(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", SUM(%s)/86400",
                    reportRequest['groupingFields'][1] + ['ExecTime'
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

  def _plotTotalWallTime(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Wall time days used by %s' % reportRequest['grouping'],
                'ylabel': 'CPU days',
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime']
                }
    return self._generatePiePlot(filename, plotInfo['data'], metadata)

##
#  Jobs
##

  _reportCumulativeNumberOfJobsName = "Cumulative executed jobs"

  def _reportCumulativeNumberOfJobs(self, reportRequest):
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
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableUnit(dataDict,
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
                'sort_labels': 'max_value'}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportNumberOfJobsName = "Job execution rate"

  def _reportNumberOfJobs(self, reportRequest):
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "jobs")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotNumberOfJobs(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Jobs by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportTotalNumberOfJobsName = "Pie plot of executed jobs"

  def _reportTotalNumberOfJobs(self, reportRequest):
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

  def _plotTotalNumberOfJobs(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Total Number of Jobs by %s' % reportRequest['grouping'],
                'ylabel': 'Jobs',
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime']
                }
    return self._generatePiePlot(filename, plotInfo['data'], metadata)

##
# Proc bw
##

  _reportProcessingBandwidthName = "Processing bandwidth"

  def _reportProcessingBandwidth(self, reportRequest):
    selectFields = (
        self._getSelectStringForGrouping(
            reportRequest['groupingFields']) +
        ", %s, %s, SUM((%s)/(%s))/SUM(%s)",
        reportRequest['groupingFields'][1] +
        [
            'startTime',
            'bucketLength',
            'InputDataSize',
            'CPUTime',
            'entriesInBucket'])
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "bytes")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotProcessingBandwidth(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Processing Bandwidth by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

##
# Data sizes
##

  _reportInputSandboxSizeName = "Input sandbox"

  def _reportInputSandboxSize(self, reportRequest):
    return self.__reportFieldSizeinMB(reportRequest, ("InputSandBoxSize", "Input sand box size"))

  _reportOutputSandboxSizeName = "Ouput sandbox"

  def _reportOutputSandboxSize(self, reportRequest):
    return self.__reportFieldSizeinMB(reportRequest, ("OutputSandBoxSize", "Output sand box size"))

  _reportDiskSpaceSizeName = "Disk space"

  def _reportDiskSpaceSize(self, reportRequest):
    return self.__reportFieldSizeinMB(reportRequest, ("DiskSpace", "Used disk space"))

  _reportInputDataSizeName = "Input data"

  def _reportInputDataSize(self, reportRequest):
    return self.__reportFieldSizeinMB(reportRequest, ("InputDataSize", "Input data"))

  _reportOutputDataSizeName = "Output data"

  def _reportOutputDataSize(self, reportRequest):
    return self.__reportFieldSizeinMB(reportRequest, ("OutputDataSize", "Output data"))

  def __reportFieldSizeinMB(self, reportRequest, fieldTuple):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', fieldTuple[0]]
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "bytes")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotInputSandboxSize(self, reportRequest, plotInfo, filename):
    return self.__plotFieldSizeinMB(reportRequest, plotInfo, filename, ("InputSandBoxSize", "Input sand box size"))

  def _plotOutputSandboxSize(self, reportRequest, plotInfo, filename):
    return self.__plotFieldSizeinMB(reportRequest, plotInfo, filename, ("OutputSandBoxSize", "Output sand box size"))

  def _plotDiskSpaceSize(self, reportRequest, plotInfo, filename):
    return self.__plotFieldSizeinMB(reportRequest, plotInfo, filename, ("DiskSpace", "Used disk space"))

  def _plotInputDataSize(self, reportRequest, plotInfo, filename):
    return self.__plotFieldSizeinMB(reportRequest, plotInfo, filename, ("InputDataSize", "Input data"))

  def _plotOutputDataSize(self, reportRequest, plotInfo, filename):
    return self.__plotFieldSizeinMB(reportRequest, plotInfo, filename, ("OutputDataSize", "Output data"))

  def __plotFieldSizeinMB(self, reportRequest, plotInfo, filename, fieldTuple):
    metadata = {'title': '%s by %s' % (fieldTuple[1], reportRequest['grouping']),
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

##
#  Cumulative data sizes
##

  _reportCumulativeInputSandboxSizeName = "Cumulative Input sandbox"

  def _reportCumulativeInputSandboxSize(self, reportRequest):
    return self.__reportCumulativeFieldSizeinMB(reportRequest, ("InputSandBoxSize", "Input sand box size"))

  _reportCumulativeOutputSandboxSizeName = "Cumulative Ouput sandbox"

  def _reportCumulativeOutputSandboxSize(self, reportRequest):
    return self.__reportCumulativeFieldSizeinMB(reportRequest, ("OutputSandBoxSize", "Output sand box size"))

  _reportCumulativeDiskSpaceSizeName = "Cumulative Disk space"

  def _reportCumulativeDiskSpaceSize(self, reportRequest):
    return self.__reportCumulativeFieldSizeinMB(reportRequest, ("DiskSpace", "Used disk space"))

  _reportCumulativeInputDataSizeName = "Cumulative Input data"

  def _reportCumulativeInputDataSize(self, reportRequest):
    return self.__reportCumulativeFieldSizeinMB(reportRequest, ("InputDataSize", "Input data"))

  _reportCumulativeOutputDataSizeName = "Cumulative Output data"

  def _reportCumulativeOutputDataSize(self, reportRequest):
    return self.__reportCumulativeFieldSizeinMB(reportRequest, ("OutputDataSize", "Output data"))

  def __reportCumulativeFieldSizeinMB(self, reportRequest, fieldTuple):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', fieldTuple[0]]
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
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableUnit(dataDict,
                                                                              self._getAccumulationMaxValue(dataDict),
                                                                              "bytes")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotCumulativeInputSandboxSize(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeFieldSizeinMB(
        reportRequest, plotInfo, filename, ("InputSandBoxSize", "Input sand box size"))

  def _plotCumulativeOutputSandboxSize(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeFieldSizeinMB(
        reportRequest, plotInfo, filename, ("OutputSandBoxSize", "Output sand box size"))

  def _plotCumulativeDiskSpaceSize(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeFieldSizeinMB(reportRequest, plotInfo, filename, ("DiskSpace", "Used disk space"))

  def _plotCumulativeInputDataSize(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeFieldSizeinMB(reportRequest, plotInfo, filename, ("InputDataSize", "Input data"))

  def _plotCumulativeOutputDataSize(self, reportRequest, plotInfo, filename):
    return self.__plotCumulativeFieldSizeinMB(reportRequest, plotInfo, filename, ("OutputDataSize", "Output data"))

  def __plotCumulativeFieldSizeinMB(self, reportRequest, plotInfo, filename, fieldTuple):
    metadata = {'title': 'Cumulative %s by %s' % (fieldTuple[1], reportRequest['grouping']),
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateCumulativePlot(filename, plotInfo['graphDataDict'], metadata)

##
#  Input/Ouput data files
##

  _reportInputDataFilesName = "Input data files"

  def _reportInputDataFiles(self, reportRequest):
    return self.__reportDataFiles(reportRequest, ("InputDataFiles", "Input files"))

  _reportOuputDataFilesName = "Output data files"

  def _reportOuputDataFiles(self, reportRequest):
    return self.__reportDataFiles(reportRequest, ("OutputDataFiles", "Output files"))

  def __reportDataFiles(self, reportRequest, fieldTuple):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', fieldTuple[0]]
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
    dataDict, _maxValue = self._divideByFactor(dataDict, granularity)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)
    baseDataDict, graphDataDict, _maxValue, unitName = self._findSuitableRateUnit(
        dataDict, self._getAccumulationMaxValue(dataDict), "files")
    return S_OK({'data': baseDataDict, 'graphDataDict': graphDataDict,
                 'granularity': granularity, 'unit': unitName})

  def _plotInputDataFiles(self, reportRequest, plotInfo, filename):
    return self.__plotDataFiles(reportRequest, plotInfo, filename, ("InputDataFiles", "Input files"))

  def _plotOuputDataFiles(self, reportRequest, plotInfo, filename):
    return self.__plotDataFiles(reportRequest, plotInfo, filename, ("OutputDataFiles", "Output files"))

  def __plotDataFiles(self, reportRequest, plotInfo, filename, fieldTuple):
    metadata = {'title': '%s by %s' % (fieldTuple[1], reportRequest['grouping']),
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'ylabel': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['graphDataDict'], metadata)

  _reportHistogramCPUUsedName = "Histogram CPU time"

  def _reportHistogramCPUUsed(self, reportRequest):
    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s",
                    reportRequest['groupingFields'][1] + ['CPUTime'])

    retVal = self._getBucketData(reportRequest['startTime'],
                                 reportRequest['endTime'],
                                 selectFields,
                                 reportRequest['condDict'])
    if not retVal['OK']:
      return retVal
    dataDict = retVal['Value']
    return S_OK({'data': dataDict})

  def _plotHistogramCPUUsed(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'CPU usage by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime']}
    return self._generateHistogram(filename, [plotInfo['data']], metadata)
