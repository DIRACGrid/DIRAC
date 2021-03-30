'''A reporter class to prepare reports and network accounting plots.

Supports:

* packet loss rate (standard and magnified),
* one-way delay, jitter, jitter over one-way delay

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import numpy as np

from DIRAC import S_OK
from DIRAC.AccountingSystem.Client.Types.Network import Network
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter


class NetworkPlotter(BaseReporter):

  _typeName = "Network"
  _typeKeyFields = [dF[0] for dF in Network().definitionKeyFields]

  _reportPacketLossRateName = "Packet loss rate"

  def _reportPacketLossRate(self, reportRequest):

    selectFields = (
        self._getSelectStringForGrouping(
            reportRequest['groupingFields']) +
        ", %s, %s, 100 - SUM(%s)/SUM(%s), 100",
        reportRequest['groupingFields'][1] +
        [
            'startTime',
            'bucketLength',
            'PacketLossRate',
            'entriesInBucket'])
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'convertToGranularity': 'average'}
                                )
    if not retVal['OK']:
      return retVal

    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)

    return S_OK({'data': dataDict, 'granularity': granularity})

  def _plotPacketLossRate(self, reportRequest, plotInfo, filename):

    # prepare custom scale (10,20,...,100)
    scale_data = dict(zip(range(0, 101), range(100, -1, -1)))
    scale_ticks = list(range(0, 101, 10))

    metadata = {'title': 'Packet loss rate by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'graph_size': 'large',
                'reverse_labels': True,
                'scale_data': scale_data,
                'scale_ticks': scale_ticks}
    return self._generateQualityPlot(filename, plotInfo['data'], metadata)

  _reportMagnifiedPacketLossRateName = "Packet loss rate (magnified)"

  def _reportMagnifiedPacketLossRate(self, reportRequest):

    selectFields = (
        self._getSelectStringForGrouping(
            reportRequest['groupingFields']) +
        ", %s, %s, %s, %s",
        reportRequest['groupingFields'][1] +
        ['startTime',
         'bucketLength',
         "100 - IF(SUM(PacketLossRate)/SUM(entriesInBucket)*10 > 100, 100",
         "SUM(PacketLossRate)/SUM(entriesInBucket)*10), 100"])
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'convertToGranularity': 'average'}
                                )
    if not retVal['OK']:
      return retVal

    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)

    return S_OK({'data': dataDict, 'granularity': granularity})

  def _plotMagnifiedPacketLossRate(self, reportRequest, plotInfo, filename):

    # prepare custom scale (1..10, 100)
    boundaries = list(np.arange(0, 10, 0.1))
    boundaries.extend(range(10, 110, 10))
    values = list(np.arange(100, 0, -1))
    values.extend([0] * 10)

    scale_data = dict(zip(boundaries, values))
    scale_ticks = list(range(0, 11))
    scale_ticks.append(100)

    metadata = {'title': 'Magnified packet loss rate by %s' % reportRequest['grouping'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'span': plotInfo['granularity'],
                'reverse_labels': True,
                'graph_size': 'large',
                'scale_data': scale_data,
                'scale_ticks': scale_ticks}
    return self._generateQualityPlot(filename, plotInfo['data'], metadata)

  _reportAverageOneWayDelayName = "One-way delay (average)"

  def _reportAverageOneWayDelay(self, reportRequest):

    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)/SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', 'OneWayDelay', 'entriesInBucket']
                    )
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'convertToGranularity': 'average'}
                                )
    if not retVal['OK']:
      return retVal

    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)

    return S_OK({'data': dataDict, 'granularity': granularity, 'unit': 'ms'})

  def _plotAverageOneWayDelay(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'One-way delay by %s' % reportRequest['grouping'],
                'ylabel': plotInfo['unit'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'graph_size': 'large',
                'span': plotInfo['granularity'],
                'sort_labels': 'avg_nozeros',
                'legend_unit': plotInfo['unit']
                }
    return self._generateStackedLinePlot(filename, plotInfo['data'], metadata)

  _reportJitterName = "Jitter"

  def _reportJitter(self, reportRequest):

    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)/SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', 'Jitter', 'entriesInBucket']
                    )
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'convertToGranularity': 'average'}
                                )
    if not retVal['OK']:
      return retVal

    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)

    return S_OK({'data': dataDict, 'granularity': granularity, 'unit': 'ms'})

  def _plotJitter(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Jitter by %s' % reportRequest['grouping'],
                'ylabel': plotInfo['unit'],
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'graph_size': 'large',
                'span': plotInfo['granularity'],
                'sort_labels': 'avg_nozeros',
                'legend_unit': plotInfo['unit']}
    return self._generateStackedLinePlot(filename, plotInfo['data'], metadata)

  _reportJitterDelayRatioName = "Jitter/Delay"

  def _reportJitterDelayRatio(self, reportRequest):

    selectFields = (self._getSelectStringForGrouping(reportRequest['groupingFields']) + ", %s, %s, SUM(%s)/SUM(%s)",
                    reportRequest['groupingFields'][1] + ['startTime', 'bucketLength', 'Jitter', 'OneWayDelay']
                    )
    retVal = self._getTimedData(reportRequest['startTime'],
                                reportRequest['endTime'],
                                selectFields,
                                reportRequest['condDict'],
                                reportRequest['groupingFields'],
                                {'convertToGranularity': 'average'}
                                )
    if not retVal['OK']:
      return retVal

    dataDict, granularity = retVal['Value']
    self.stripDataField(dataDict, 0)
    dataDict = self._fillWithZero(granularity, reportRequest['startTime'], reportRequest['endTime'], dataDict)

    return S_OK({'data': dataDict, 'granularity': granularity})

  def _plotJitterDelayRatio(self, reportRequest, plotInfo, filename):
    metadata = {'title': 'Jitter over one-way delay by %s' % reportRequest['grouping'],
                'ylabel': '',
                'starttime': reportRequest['startTime'],
                'endtime': reportRequest['endTime'],
                'graph_size': 'large',
                'span': plotInfo['granularity'],
                'sort_labels': 'avg_nozeros',
                'legend_unit': ''
                }
    return self._generateStackedLinePlot(filename, plotInfo['data'], metadata)
