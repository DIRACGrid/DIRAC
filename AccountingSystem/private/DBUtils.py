import types
from DIRAC.Core.Utilities import Time

class DBUtils:

  def __init__( self, db, setup ):
    self._acDB = db
    self._setup = setup

  def _retrieveBucketedData( self,
                             typeName,
                             startTime,
                             endTime,
                             selectFields,
                             condDict = None,
                             groupFields = None,
                             orderFields = None ):
    """
    Get data from the DB
    Parameters:
      - typeName -> typeName
      - startTime & endTime -> datetime objects. Do I need to explain the meaning?
      - selectFields -> tuple containing a string and a list of fields:
                        ( "SUM(%s), %s/%s", ( "field1name", "field2name", "field3name" ) )
      - condDict -> conditions for the query
                    key -> name of the key field
                    value -> list of possible values
      - groupFields -> list of fields to group by, can be in form
                       ( "%s, %s", ( "field1name", "field2name", "field3name" ) )
      - orderFields -> list of fields to order by, can be in form
                       ( "%s, %s", ( "field1name", "field2name", "field3name" )
    """
    typeName = "%s_%s" % ( self._setup, typeName )
    validCondDict = {}
    if type( condDict ) == types.DictType:
      for key in condDict:
        if type( condDict[ key ] ) in ( types.ListType, types.TupleType ) and len( condDict[ key ] ) > 0:
          validCondDict[ key ] = condDict[ key ]
    retVal = self._acDB._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    return self._acDB.retrieveBucketedData( typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields, connObj = connObj )

  def _getUniqueValues( self, typeName, startTime, endTime, condDict, fieldList ):
    stringList = [ "%s" for field in fieldList ]
    return self._retrieveBucketedData( typeName,
                                       startTime,
                                       endTime,
                                       ( ",".join( stringList ), fieldList ),
                                       condDict,
                                       fieldList )

  def _groupByField( self, fieldIndex, dataList ):
    """
    From a list of lists/tuples group them into a dict of lists using as key field fieldIndex
    """
    groupDict = {}
    for row in dataList:
      groupingField = row[ fieldIndex ]
      if not groupingField in groupDict:
        groupDict[ groupingField ] = []
      if type( row ) == types.TupleType:
        rowL = list( row[ :fieldIndex ] )
        rowL.extend( row[ fieldIndex + 1: ] )
        row = rowL
      else:
        del( row[ fieldIndex ] )
      groupDict[ groupingField ].append( row )
    return groupDict

  def _getBins( self, typeName, startTime, endTime ):
    typeName = "%s_%s" % ( self._setup, typeName )
    return self._acDB.calculateBuckets( typeName, startTime, endTime )

  def _getBucketLengthForTime( self, typeName, momentEpoch ):
    nowEpoch = Time.toEpoch()
    typeName = "%s_%s" % ( self._setup, typeName )
    return self._acDB.calculateBucketLengthForTime( typeName, nowEpoch, momentEpoch )

  def _spanToGranularity( self, granularity, bucketsData ):
    """
    bucketsData must be a list of lists where each list contains
      - field 0: datetime
      - field 1: bucketLength
      - fields 2-n: numericalFields
    """
    normData = {}

    def addToNormData( bucketDate, data, proportion = 1.0 ):
      if bucketDate in normData:
        for iP in range( len( data ) ):
          val = data[ iP ]
          if val == None:
            val = 0
          normData[ bucketDate ][iP] += float( val ) * proportion
        normData[ bucketDate ][ -1 ] += proportion
      else:
        normData[ bucketDate ] = []
        for fD in data:
          if fD == None:
            fD = 0
          normData[ bucketDate ].append( float( fD ) * proportion )
        normData[ bucketDate ].append( proportion )

    for bucketData in bucketsData:
      bucketDate = bucketData[0]
      originalBucketLength = bucketData[1]
      bucketValues = bucketData[2:]
      if originalBucketLength == granularity:
        addToNormData( bucketDate, bucketValues )
      else:
        startEpoch = bucketDate
        endEpoch = bucketDate + originalBucketLength
        newBucketEpoch = startEpoch - startEpoch % granularity
        if startEpoch == endEpoch:
          addToNormData( newBucketEpoch, bucketValues )
        else:
          while newBucketEpoch < endEpoch:
            start = max( newBucketEpoch, startEpoch )
            end = min( newBucketEpoch + granularity, endEpoch )
            proportion = float( end - start ) / originalBucketLength
            addToNormData( newBucketEpoch, bucketValues, proportion )
            newBucketEpoch += granularity
    return normData

  def _sumToGranularity( self, granularity, bucketsData ):
    """
    bucketsData must be a list of lists where each list contains
      - field 0: datetime
      - field 1: bucketLength
      - fields 2-n: numericalFields
    """
    normData = self._spanToGranularity( granularity, bucketsData )
    for bDate in normData:
      del( normData[ bDate ][-1] )
    return normData

  def _averageToGranularity( self, granularity, bucketsData ):
    """
    bucketsData must be a list of lists where each list contains
      - field 0: datetime
      - field 1: bucketLength
      - fields 2-n: numericalFields
    """
    normData = self._spanToGranularity( granularity, bucketsData )
    for bDate in normData:
      for iP in range( len( normData[ bDate ] ) ):
        normData[ bDate ][iP] = float( normData[ bDate ][iP] ) / normData[ bDate ][-1]
      del( normData[ bDate ][-1] )
    return normData

  def _convertNoneToZero( self, bucketsData ):
    """
    Convert None to 0
    bucketsData must be a list of lists where each list contains
      - field 0: datetime
      - field 1: bucketLength
      - fields 2-n: numericalFields
    """
    for iPos in range( len( bucketsData ) ):
      data = bucketsData[iPos]
      for iVal in range( 2, len( data ) ):
        if data[ iVal ] == None:
          data[ iVal ] = 0
    return bucketsData

  def _fillWithZero( self, granularity, startEpoch, endEpoch, dataDict ):
    """
    Fill with zeros missing buckets
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    startBucketEpoch = startEpoch - startEpoch % granularity
    for key in dataDict:
      currentDict = dataDict[ key ]
      for timeEpoch in range( startBucketEpoch, endEpoch, granularity ):
        if timeEpoch not in currentDict:
          currentDict[ timeEpoch ] = 0
    return dataDict

  def _getAccumulationMaxValue( self, dataDict ):
    """
    Divide by factor the values and get the maximum value
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    maxValue = 0
    maxEpoch = 0
    for key in dataDict:
      currentDict = dataDict[ key ]
      for timeEpoch in currentDict:
        if timeEpoch > maxEpoch:
          maxEpoch = timeEpoch
          maxValue = 0
        if timeEpoch == maxEpoch:
          maxValue += currentDict[ timeEpoch ]
    return maxValue

  def _getMaxValue( self, dataDict ):
    """
    Divide by factor the values and get the maximum value
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    maxValues = {}
    for key in dataDict:
      currentDict = dataDict[ key ]
      for timeEpoch in currentDict:
        if timeEpoch not in maxValues:
          maxValues[ timeEpoch ] = 0
        maxValues[ timeEpoch ] += currentDict[ timeEpoch ]
    maxValue = 0
    for k in maxValues:
      maxValue = max( maxValue, k )
    return maxValue

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

  def _accumulate( self, granularity, startEpoch, endEpoch, dataDict ):
    """
    Accumulate all the values.
      - dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    startBucketEpoch = startEpoch - startEpoch % granularity
    for key in dataDict:
      currentDict = dataDict[ key ]
      lastValue = 0
      for timeEpoch in range( startBucketEpoch, endEpoch, granularity ):
        if timeEpoch in currentDict:
          lastValue += currentDict[ timeEpoch ]
        currentDict[ timeEpoch ] = lastValue
    return dataDict

  def stripDataField( self, dataDict, fieldId ):
    """
    Strip <fieldId> data and sum the rest as it was data from one key
    In:
      - dataDict : { 'key' : { <timeEpoch1>: [1, 2, 3],
                               <timeEpoch2>: [3, 4, 5].. } }
      - fieldId : 0
    Out
      - dataDict : { 'key' : { <timeEpoch1>: 1,
                               <timeEpoch2>: 3.. } }
      - return : [ { <timeEpoch1>: 2, <timeEpoch2>: 4... }
                   { <timeEpoch1>: 3, <timeEpoch2>): 5... } ]
    """
    remainingData = [{}] #Hack for empty data
    for key in dataDict:
      for timestamp in dataDict[ key ]:
        for iPos in dataDict[ key ][ timestamp ]:
          remainingData.append( {} )
        break
      break
    for key in dataDict:
      for timestamp in dataDict[ key ]:
        strippedField = float( dataDict[ key ][ timestamp ][ fieldId ] )
        del( dataDict[ key ][ timestamp ][ fieldId ] )
        for iPos in range( len( dataDict[ key ][ timestamp ] ) ):
          if timestamp in remainingData[ iPos ]:
            remainingData[ iPos ][ timestamp ] += float( dataDict[ key ][ timestamp ][ iPos ] )
          else:
            remainingData[ iPos ][ timestamp ] = float( dataDict[ key ][ timestamp ][ iPos ] )
        dataDict[ key ][ timestamp ] = strippedField

    return remainingData

  def getKeyValues( self, typeName, condDict ):
    """
    Get all valid key values in a type
    """
    retVal = self._acDB._getConnection()
    if not retVal[ 'OK' ]:
      return retVal
    connObj = retVal[ 'Value' ]
    typeName = "%s_%s" % ( self._setup, typeName )
    return self._acDB.getKeyValues( typeName, condDict, connObj )

  def _calculateProportionalGauges( self, dataDict ):
    """
    Get a dict with more than one entry per bucket and list
    """
    bucketSums = {}
    #Calculate total sums in buckets
    for key in dataDict:
      for timeKey in dataDict[ key ]:
        timeData = dataDict[ key ][ timeKey ]
        if len( timeData ) < 2:
          raise Exception( "DataDict must be of the type { <key>:{ <timeKey> : [ field1, field2, ..] } }. With at least two fields" )
        if timeKey not in bucketSums:
          bucketSums[ timeKey ] = [ 0, 0, 0]
        bucketSums[ timeKey ][0] += timeData[0]
        bucketSums[ timeKey ][1] += timeData[1]
        bucketSums[ timeKey ][2] += timeData[0] / timeData[1]
    #Calculate proportionalFactor
    for timeKey in bucketSums:
      timeData = bucketSums[ timeKey ]
      if bucketSums[ timeKey ][0] == 0:
        bucketSums[ timeKey  ] = 0
      else:
        bucketSums[ timeKey ] = ( timeData[0] / timeData[1] ) / timeData[2]
    #Calculate proportional Gauges
    for key in dataDict:
      for timeKey in dataDict[ key ]:
        timeData = dataDict[ key ][ timeKey ]
        dataDict[ key ][ timeKey ] = [ ( timeData[0] / timeData[1] ) * bucketSums[ timeKey ] ]

    return dataDict

  def _getBucketTotals( self, dataDict ):
    """
    Sum key data and get totals for each bucket
    """
    newData = {}
    for k in dataDict:
      for bt in dataDict[ k ]:
        if bt not in newData:
          newData[ bt ] = 0.0
        newData[ bt ] += dataDict[ k ][ bt ]
    return newData
