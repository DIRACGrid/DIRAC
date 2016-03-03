########################################################################
# $Id: $
########################################################################

"""
It is a wrapper on top of Elasticsearch. It is used to manage the DIRAC monitoring types. 
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.MonitoringSystem.private.TypeLoader import TypeLoader
from DIRAC import S_OK, S_ERROR, gConfig, gLogger

########################################################################
class MonitoringDB( ElasticDB ):

  def __init__( self, name = 'Monitoring/MonitoringDB', readOnly = False ):
    ElasticDB.__init__( self, 'MonitoringDB', name )
    self.__readonly = readOnly
    self.__documents = {}
    self.__loadIndexes()
    
    
  def __loadIndexes( self ):
    """
    It loads all monitoring indexes and types.
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't get a list of setups: %s" % retVal[ 'Message' ] )
    
    setupsList = retVal[ 'Value' ]
    objectsLoaded = TypeLoader().getTypes()

    # Load the files
    for pythonClassName in sorted( objectsLoaded ):
      typeClass = objectsLoaded[ pythonClassName ]
      for setup in setupsList:
        indexName = "%s_%s" % ( setup.lower(), typeClass().getIndex() )
        doc_type = typeClass().getDocType() 
        mapping = typeClass().getMapping()
        monfields = typeClass().getMonitoringFields()
        self.__documents[doc_type] = {"indexName": indexName, "mapping":mapping, 'monitoringFields':monfields}
        if self.__readonly:
          gLogger.info( "Read only mode is okay" )
        else:
          self.registerType( doc_type, indexName, mapping )
  
  def getIndexName( self, typeName ):
    """
    :param tyeName it is a string. doc_type and type name is equivalent
    """
    indexName = None
    
    if typeName in self.__documents:
      indexName = self.__documents.get( typeName ).get( "indexName", None )
      
    if indexName:
      return S_OK( indexName )
    else:
      return S_ERROR( "The index of % not found!" % typeName )
  
  def registerType( self, mtype, index, mapping ):
    """
    It register the type and index, if does not exists
    :param str type: Type of the documents
    :param str index: name of the index
    """ 
    
    all_index = "%s-*" % index
    
    if self.checkIndex( all_index ):  
      indexes = self.getIndexes()
      if len( indexes ) > 0:
        actualindexName = self.createFullIndexName( index )
        if self.checkIndex( actualindexName ):  
          self.log.info( "The index is exists:", actualindexName )
        else:
          result = self.createIndex( index, mapping )
          if not result['OK']:
            self.log.error( result['Message'] )
          self.log.info( "The index is created", actualindexName )
    else:
      # in that case no index exists
      result = self.createIndex( index, mapping )
      if not result['OK']:
        self.log.error( result['Message'] )
      
  
    
  def getKeyValues( self, typeName, setup ):
    """
    Get all values for a given key field in a type
    """
    keyValuesDict = {}
    
    retVal = self.getIndexName( typeName )
    if not retVal['OK']:
      return retVal
    indexName = "%s*" % ( retVal['Value'] )
    retVal = self.getDocTypes( indexName )
    if not retVal['OK']:
      return retVal
    docs = retVal['Value']
    monfields = self.__documents[typeName]['monitoringFields']
    
    keys = docs[typeName]['properties'].keys() 
    for i in keys:
      if i not in monfields:
        retVal = self.getUniqueValue( indexName, i )
        if not retVal['OK']:
          return retVal
        keyValuesDict[i] = retVal['Value']
    return S_OK( keyValuesDict )                                            
    
  def retrieveBucketedData( self, typeName, startTime, endTime, interval, selectFields, condDict, grouping, metainfo ):
    """
    Get data from the DB
    
    :param str typeName name of the monitoring type
    :param int startTime  epoch objects.
    :param int endtime epoch objects.
    :param dict condDict -> conditions for the query
                  key -> name of the field
                  value -> list of possible values
     
    """
    
    if typeName not in self.__documents:
      return S_ERROR( "Type %s is not defined" % typeName )
    retVal = self.getIndexName( typeName )
    if not retVal['OK']:
      return retVal
    isAvgAgg = False
    #the data is used to fill the pie charts. This aggregation is used to average the buckets.
    if metainfo and metainfo.get('metric','sum') == 'avg':
      isAvgAgg = True
    
    indexName = "%s*" % ( retVal['Value'] )
    q = [self._Q( 'range', time = {'lte':endTime * 1000, 'gte': startTime * 1000} )]
    for cond in condDict:
      kwargs = {cond: condDict[cond][0]}
      query = self._Q( 'match', **kwargs )
      q += [query] 
    
    a1 = self._A( 'terms', field = grouping, size = 0 )
    a2 = self._A( 'terms', field = 'time' )
    a2.metric( 'total_jobs', 'sum', field = selectFields[0] )
    if isAvgAgg:
      a1.metric( 'tt', a2 ).pipeline( 'avg_monthly_sales', 'avg_bucket', buckets_path = 'tt>total_jobs' )
    else:
      a1.bucket( 'end_data', 'date_histogram', field = 'time', interval = interval ).metric( 'tt', a2 ).pipeline( 'avg_monthly_sales', 'avg_bucket', buckets_path = 'tt>total_jobs' )
    s = self._Search( indexName )
    s = s.filter( 'bool', must = q )
    s.aggs.bucket( '2', a1 )
    s.fields( ['time'] + selectFields )
    retVal = s.execute()
    print s.to_dict()
    result = {}
    for i in retVal.aggregations['2'].buckets:
      if isAvgAgg:
        result[i.key] = i.avg_monthly_sales.value
      else:
        site = i.key
        dp = {}
        for j in i.end_data.buckets:
          dp[j.key / 1000] = j.avg_monthly_sales.value
        result[site] = dp
    
    return S_OK( result )
    
