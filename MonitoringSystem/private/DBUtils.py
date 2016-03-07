
from DIRAC.Core.Utilities  import Time
from DIRAC                 import S_OK, S_ERROR

class DBUtils ( object ):
  # 86400 seconds -> 1d
  # 604800 seconds -> 1w
  # 2592000 seconds -> 1m
  # 525600 minutes -> year
  __units = ['minutes', 'day', 'week', 'month', 'year']
  __unitsvalues = {'minutes': 30, 'day':86400, 'week':604800, 'month':2592000, 'year':86400 * 365}
  __esUnits = {86400:( '30m', 60 * 30 ), 604800:( '3h', 3 * 3600 ), 2592000:( '12h', 12 * 3600 ), 86400 * 365:( '1w', 86400 * 7 ) }
  def __init__( self, db, setup ):
    self.__db = db
    self.__setup = setup
    
  def getKeyValues( self, typeName, condDict ):
    """
    Get all valid key values in a type
    """
    return self.__db.getKeyValues( self.__setup, typeName, condDict )
  
  def _retrieveBucketedData( self, typeName, startTime, endTime, interval, selectFields, condDict = None, grouping = '', metadataDict = None):
    return self.__db.retrieveBucketedData( typeName, startTime, endTime, interval, selectFields, condDict, grouping, metadataDict)
  
  def _determineBucketSize( self, start, end ):
    """
    It is used to determine the bucket size using _esUnits
    """
    diff = end - start
    unit = ''
    for i in self.__units:
      if diff <= self.__unitsvalues[i]:
        unit = self.__esUnits[self.__unitsvalues[i]]
        break
    if unit == '':
      return S_ERROR( "Can not determine the bucket size..." )
    else:
      return S_OK( unit )
  
  def _divideByFactor( self, dataDict, factor ):
    """
    Divide by factor the values and get the maximum value
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    maxValue = 0.0
    for key in dataDict:
      currentDict = dataDict[ key ]
      for timeEpoch in currentDict:
        currentDict[ timeEpoch ] /= float( factor )
        maxValue = max( maxValue, currentDict[ timeEpoch ] )
    return dataDict, maxValue