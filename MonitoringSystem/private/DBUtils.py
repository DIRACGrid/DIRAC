import types
from DIRAC.Core.Utilities import Time
from DIRAC import S_OK, S_ERROR

class DBUtils ( object ):
  # 86400 seconds -> 1d
  # 604800 seconds -> 1w
  # 2592000 seconds -> 1m
  # 525600 minutes -> year
  _units = ['minutes', 'day', 'week', 'month', 'year']
  _unitsvalues = {'minutes': 30, 'day':86400, 'week':604800, 'month':2592000, 'year':86400 * 365}
  _esUnits = {86400:( '30m', 60 * 30 ), 604800:( '3h', 3 * 3600 ), 2592000:( '12h', 12 * 3600 ), 86400 * 365:( '1w', 86400 * 7 ) }
  def __init__( self, db, setup ):
    self.__db = db
    self.__setup = setup
    
  def getKeyValues( self, typeName, condDict ):
    """
    Get all valid key values in a type
    """
    return self.__db.getKeyValues( self.__setup, typeName, condDict )
  
  def _retrieveBucketedData( self, typeName, startTime, endTime, interval, selectFields, condDict = None):
    return self.__db.retrieveBucketedData( typeName, startTime, endTime, interval, selectFields, condDict )
  
  def _determineBucketSize( self, start, end ):
    diff = end - start
    unit = ''
    for i in self._units:
      print diff, self._unitsvalues[i]
      if diff <= self._unitsvalues[i]:
        unit = self._esUnits[self._unitsvalues[i]]
        break
    if unit == '':
      return S_ERROR( "Can not determine the bucket size..." )
    else:
      return S_OK( unit )
