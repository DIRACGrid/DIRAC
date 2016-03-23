"""
It is a helper module used to create a certain plot...
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities  import Time
from DIRAC                 import S_OK, S_ERROR

class DBUtils ( object ):
  
  """ 
  .. class:: DBUtils
  
  It implements few methods used to create the plots.
  
  param: list __units it is elasticsearch specific unites
  param: list __unitvalues the units in second
  param: list __esunits used to determine the buckets size  
   
  """
  #TODO: Maybe it is better to use the same structure we have in BasePlotter
  
  # 86400 seconds -> 1d
  # 604800 seconds -> 1w
  # 2592000 seconds -> 1m
  # 525600 minutes -> year
  
  __esbucket = {86400:( '30m', 60 * 30 ),
               604800:( '3h', 3 * 3600 ),
               30 * 86400:( '12h', 12 * 3600 ),
               45 * 86400:( '1d', 24 * 3600 ),
               60 * 86400:( '2d', 2 * 24 * 3600 ),
               90 * 86400:( '3d', 3 * 24 * 3600 ),
               120 * 86400:( '4d', 4 * 24 * 3600 ),
               86400 * 367:( '1w', 86400 * 7 ) }
  
  def __init__( self, db, setup ):
    self.__db = db
    self.__setup = setup
    
    """ c'tor
    :param self: self reference
    :param object the database module
    :param str setup DIRAC setup
    """
    
  def getKeyValues( self, typeName, condDict ):
    """
    Get all valid key values in a type
    """
    return self.__db.getKeyValues( self.__setup, typeName, condDict )
  
  def _retrieveBucketedData( self, typeName, startTime, endTime, interval, selectFields, condDict = None, grouping = '', metadataDict = None):
    """
    It is a wrapper class...
    """
    return self.__db.retrieveBucketedData( typeName, startTime, endTime, interval, selectFields, condDict, grouping, metadataDict)
  
  def _determineBucketSize( self, start, end ):
    """
    It is used to determine the bucket size using _esUnits
    """
    diff = end - start
    
    unit = ''
    for i in sorted(self.__esbucket.keys()):
      if diff <= i:
        unit = self.__esbucket[i]
        break   
    if not unit:
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
