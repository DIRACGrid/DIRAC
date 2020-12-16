"""
It is a helper module used to create a certain plot...
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"


def _convertToSeconds(interval):
  """
  Converts number of minutes, hours, days, weeks, months, years into seconds
  """
  # unit symbols
  units = ['s', 'm', 'h', 'd', 'w', 'M', 'y']
  # this is the number of previous units in a unit
  numbers = [1, 60, 60, 24, 7, 30. / 7., 366. / 30.]
  seconds = 1.
  for unit, num in zip(units, numbers):
    seconds *= num
    if interval.endswith(unit):
      return int(float(interval[:-1]) * seconds)
  raise ValueError("Invalid time interval '%s'" % interval)


class DBUtils(object):

  """
  .. class:: DBUtils

  It implements few methods used to create the plots.

  param: list __units it is elasticsearch specific unites
  param: list __unitvalues the units in second
  param: list __esunits used to determine the buckets size

  """
  # TODO: Maybe it is better to use the same structure we have in BasePlotter

  __esbucket = {
      '1h': '1m',
      '6h': '5m',
      '12h': '10m',
      '1d': '15m',
      '2d': '30m',
      '3.5d': '1h',
      '1w': '2h',
      '2w': '4h',
      '1M': '8h',
      '2M': '12h',
      '3M': '1d',
      '6M': '2d',
      '9M': '3d',
      '1y': '4d',
      '10y': '7d',
      '100y': '1w'
  }

  def __init__(self, db, setup):
    """ c'tor

    :param self: self reference
    :param db: the database module
    :param str setup: DIRAC setup
    """
    self.__db = db
    self.__setup = setup

  def getKeyValues(self, typeName, condDict):
    """
    Get all valid key values in a type
    """
    return self.__db.getKeyValues(self.__setup, typeName, condDict)

  def _retrieveBucketedData(self, typeName, startTime, endTime, interval,
                            selectField, condDict=None, grouping='',
                            metadataDict=None):
    """
    It is a wrapper class...
    """
    return self.__db.retrieveBucketedData(typeName=typeName,
                                          startTime=startTime,
                                          endTime=endTime,
                                          interval=interval,
                                          selectField=selectField,
                                          condDict=condDict,
                                          grouping=grouping,
                                          metainfo=metadataDict)

  def _retrieveAggregatedData(self, typeName, startTime, endTime, interval,
                              selectField, condDict=None, grouping='',
                              metadataDict=None):
    """
    Retrieve data from EL
    """
    return self.__db.retrieveAggregatedData(typeName=typeName,
                                            startTime=startTime,
                                            endTime=endTime,
                                            interval=interval,
                                            selectField=selectField,
                                            condDict=condDict,
                                            grouping=grouping,
                                            metainfo=metadataDict)

  def _determineBucketSize(self, start, end):
    """ It is used to determine the bucket size using _esUnits

        :param int start: epoch time
        :param int end: epoch time
        :return: S_OK/S_ERROR with tuple of (binUnit, seconds)
    """
    diff = end - start

    error = "Can not determine the bucket size..."
    bucketSeconds = {}
    try:
      # Convert intervals into seconds
      for interval, binUnit in self.__esbucket.items():
        bucketSeconds[_convertToSeconds(interval)] = (binUnit, _convertToSeconds(binUnit))
      # Determine bin size according to time span
      for interval in sorted(bucketSeconds):
        if diff <= interval:
          return S_OK(bucketSeconds[interval])
    except ValueError as e:
      error += ': ' + repr(e)
    return S_ERROR(error)

  def _divideByFactor(self, dataDict, factor):
    """
    Divide by factor the values and get the maximum value
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    maxValue = 0.0
    for key in dataDict:
      currentDict = dataDict[key]
      for timeEpoch in currentDict:
        currentDict[timeEpoch] /= float(factor)
        maxValue = max(maxValue, currentDict[timeEpoch])
    return dataDict, maxValue

  def _getAccumulationMaxValue(self, dataDict):
    """
    Divide by factor the values and get the maximum value
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    maxValue = 0
    maxEpoch = 0
    for key in dataDict:
      currentDict = dataDict[key]
      for timeEpoch in currentDict:
        if timeEpoch > maxEpoch:
          maxEpoch = timeEpoch
          maxValue = 0
        if timeEpoch == maxEpoch:
          maxValue += currentDict[timeEpoch]
    return maxValue
