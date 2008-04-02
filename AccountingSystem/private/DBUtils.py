import types
import datetime
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

  def _normalizeToGranularity( self, granularity, bucketsData ):
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
          normData[ bucketDate ][iP] += data[iP] * proportion
      else:
        normData[ bucketDate ] = [ value * proportion for value in  data ]

    for bucketData in bucketsData:
      originalBucketLength = bucketData[1]
      bucketDate = bucketData[0]
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

  def _fillWithZero( self, granularity, startEpoch, endEpoch, bucketsData ):
    """
    Fill with zeros missing buckets
    First field must be bucketTime in bucketsData list
    """
    startBucketEpoch = startEpoch - startEpoch % granularity
    filled = {}
    zeroList = [ 0 for field in bucketsData[ bucketsData.keys()[0] ] ]
    print ( startBucketEpoch, endEpoch, granularity )
    for bucketEpoch in range( startBucketEpoch, endEpoch, granularity ):
      if bucketEpoch in bucketsData:
        filled[ bucketEpoch ] =  bucketsData[ bucketEpoch ]
      else:
        filled[ bucketEpoch ] = list( zeroList )
    return filled

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
          print key, timestamp, iPos
          remainingData.append( {} )
        break
      break
    for key in dataDict:
      for timestamp in dataDict[ key ]:
        strippedField = dataDict[ key ][ timestamp ][ fieldId ]
        del( dataDict[ key ][ timestamp ][ fieldId ] )
        for iPos in range( len( dataDict[ key ][ timestamp ] ) ):
          if timestamp in remainingData[ iPos ]:
            remainingData[ iPos ][ timestamp ] += dataDict[ key ][ timestamp ][ fieldId ]
          else:
            remainingData[ iPos ][ timestamp ] = dataDict[ key ][ timestamp ][ fieldId ]
        dataDict[ key ][ timestamp ] = strippedField

    return remainingData