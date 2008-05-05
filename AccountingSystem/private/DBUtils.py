import types
import datetime
from DIRAC.Core.Utilities import Time
from DIRAC import S_OK, S_ERROR

class DBUtils:

  def __init__( self, db, setup ):
    self._acDB = db
    self._setup = setup

  def _retrieveBucketedData( self,
                             typeName,
                             startTime,
                             endTime,
                             selectFields,
                             condDict = {},
                             groupFields = [],
                             orderFields = [] ):
    """
    Get data from the DB
      Parameters:
        typeName -> typeName
        startTime & endTime -> datetime objects. Do I need to explain the meaning?
        selectFields -> tuple containing a string and a list of fields:
                        ( "SUM(%s), %s/%s", ( "field1name", "field2name", "field3name" ) )
        condDict -> conditions for the query
                    key -> name of the key field
                    value -> list of possible values
        groupFields -> list of fields to group by
        orderFields -> list of fields to order by
    """
    typeName = "%s_%s" % ( self._setup, typeName )
    return self._acDB.retrieveBucketedData( typeName, startTime, endTime, selectFields, condDict, groupFields, orderFields )

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
        rowL.extend( row[ fieldIndex+1: ] )
        row = rowL
      else:
        del( row[ fieldIndex ] )
      groupDict[ groupingField ].append( row )
    return groupDict

  def _getBins( self, typeName, startTime, endTime ):
    typeName = "%s_%s" % ( self._setup, typeName )
    return self._acDB.calculateBuckets( typeName )

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
          normData[ bucketDate ][iP] += data[iP]
        normData[ bucketDate ][ -1 ] += proportion
      else:
        normData[ bucketDate ] = data + [ proportion ]

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
          print "SPLIT!"
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
        normData[ bDate ][iP] /= normData[ bDate ][-1]
      del( normData[ bDate ][-1] )
    return normData


  def _fillWithZero( self, granularity, startEpoch, endEpoch, dataDict ):
    """
    Fill with zeros missing buckets
    dataDict = { 'key' : { time1 : value,  time2 : value... }, 'key2'.. }
    """
    startBucketEpoch = startEpoch - startEpoch % granularity
    for key in dataDict:
      currentDict = dataDict[ key ]
      for timeEpoch in range( startBucketEpoch, endEpoch, granularity ):
        if timeEpoch not in currentDict:
          currentDict[ timeEpoch ] = 0
    return dataDict

  def stripDataField( self, dataDict, fieldId ):
    """
    Strip <fieldId> data and sum the rest as it was data from one key
    In:
      dataDict : { 'key' : { <timeEpoch1>: [1, 2, 3],
                             <timeEpoch2>: [3, 4, 5]..
      fieldId : 0
    Out
      dataDict : { 'key' : { <timeEpoch1>: 1,
                             <timeEpoch2>: 3..
      return : [ { <timeEpoch1>: [2],
                   <timeEpoch2>: [4]... }
                 { <timeEpoch1>: [3],
                   <timeEpoch2>): [5]...
    """
    remainingData = []
    for key in dataDict:
      for timestamp in dataDict[ key ]:
        for iPos in dataDict[ key ][ timestamp ]:
          remainingData.append( {} )
        break
      break
    for key in dataDict:
      for timestamp in dataDict[ key ]:
        strippedField = dataDict[ key ][ timestamp ][ fieldId ]
        del( dataDict[ key ][ timestamp ][ fieldId ] )
        for iPos in range( len( dataDict[ key ][ timestamp ] ) ):
          if timestamp in remainingData[ iPos ]:
            remainingData[ iPos ][ timestamp ] += dataDict[ key ][ timestamp ][ iPos ]
          else:
            remainingData[ iPos ][ timestamp ] = dataDict[ key ][ timestamp ][ iPos ]
        dataDict[ key ][ timestamp ] = strippedField

    return remainingData

  def getKeyValues( self, typeName ):
    """
    Get all valid key values in a type
    """
    typeName = "%s_%s" % ( self._setup, typeName )
    retVal = self._acDB.getKeyFieldsForType( typeName )
    if not retVal[ 'OK' ]:
      return retVal
    valuesDict = {}
    for keyName in retVal[ 'Value' ]:
      retVal = self._acDB.getValuesForKeyField( typeName, keyName )
      if not retVal[ 'OK' ]:
        return retVal
      valuesDict[ keyName ] = retVal[ 'Value' ]
    return S_OK( valuesDict )
