
class DBUtils:

  def __init__( self, db, setup ):
    self._acDB = db
    self._setup = setup

  def _retrieveBucketedData( self,
                             typeName,
                             startTime,
                             endTime,
                             returnList,
                             condDict = {},
                             groupFields = [],
                             orderFields = [] ):
    """
    Get data from the DB
      Parameters:
        typeName -> typeName
        startTime & endTime -> datetime objects. Do I need to explain the meaning?
        returnList -> list of value fields to retrieve. Has to contain tuples with:
                        ( <name of value field>, <function to apply> )
        condDict -> conditions for the query
                    key -> name of the key field
                    value -> list of possible values
        groupFields -> list of fields to group by
        orderFields -> list of fields to order by
    """
    typeName = "%s_%s" % ( typeName, self._setup )
    return self._acDB.retrieveBucketedData( typeName, startTime, endTime, returnList, condDict, groupFields, orderFields )

  def _getUniqueValues( self, typeName, startTime, endTime, condDict, fieldList ):
    returnList = []
    for field in fieldList:
      returnList.append( ( field, "" ) )
    return self._retrieveBucketedData( typeName,
                                       startTime,
                                       endTime,
                                       returnList,
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
      else:
        del( row[ fieldIndex ] )
      groupDict[ groupingField ].append( row )
    return groupDict

  def _getBins( self, typeName, startTime, endTime ):
    typeName = "%s_%s" % ( typeName, self._setup )
    return self._acDB.calculateBuckets( typeName )

  def _getBucketLengthForTime( self, typeName, momentTime ):
    nowEpoch = Time.toEpoch()
    typeName = "%s_%s" % ( typeName, self._setup )
    momentEpoch = Time.toEpoch( momentTime )
    return self._acDB.calculateBucketLengthForTime( typeName, nowEpoch, momentEpoch )

  def _normalizeToGranularity( self, granularity, bucketsData ):
    """
    bucketsData must be a list of lists where each list contains
      - field 0: datetime
      - field 1: bucketLength
      - fields 2-n: numericalFields
    """
    normData = {}

    def addToNormData( bucketDate, data, porportion = 1.0 ):
      if bucketDate in normData:
        for iP in range( len( data ) ):
          normData[ bucketDate ][iP] += data[iP] * proportion
      else:
        normData[ bucketDate ] = data * proportion

    for bucketData in bucketsData:
      originalBucketLength = bucketsData[1]
      bucketDate = bucketData[0]
      bucketValues = bucketData[2:]
      if originalBucketLength == granularity:
        addToNormData( bucketDate, bucketValues )
      else:
        startEpoch = Time.toEpoch( bucketStart )
        endEpoch = Time.toEpoch( bucketDate + datetime.timedelta( seconds = originalBucketLength ) )
        newBucketEpoch = startEpoch - startEpoch % granularity
        if startEpoch == endEpoch:
          addToNormData( Time.fromEpoch( newBucketEpoch ), bucketValues )
        else:
          while newBucketEpoch < endEpoch:
            start = max( newBucketEpoch, startEpoch )
            end = min( newBucketEpoch + granularity, endEpoch )
            proportion = float( end - start ) / originalBucketLength
            addToNormData( Time.fromEpoch( newBucketEpoch ), bucketValues, proportion )
            newBucketEpoch += granularity
    return normData

  def _fillWithZero( self, granularity, startTime, endTime, bucketsData ):
    """
    Fill with zeros missing buckets
    First field must be bucketTime in bucketsData list
    """
    startEpoch = Time.toEpoch( startTime )
    startBucketEpoch = startEpoch - startEpoch % granularity
    endEpoch = Time.toEpoch( endTime )
    filled = []
    zeroList = [ 0 for field in bucketsData[0][1:] ]
    for bucketEpoch in range( startBucketEpoch, endEpoch, granularity ):
      bucketTime = Time.fromEpoch( bucketEpoch )
      if bucketsData[0] == bucketTime:
        filled.append( bucketsData.pop(0) )
      else:
        filled.append( [ bucketTime ] )
        filled[-1].extend( zeroList )
    return filled

