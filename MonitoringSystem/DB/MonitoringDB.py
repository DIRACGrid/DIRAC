"""
It is a wrapper on top of Elasticsearch. It is used to manage the DIRAC monitoring types.
"""
import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.Core.Utilities.ElasticSearchDB import generateFullIndexName
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals

from DIRAC.MonitoringSystem.private.TypeLoader import TypeLoader

__RCSID__ = "$Id$"

########################################################################
class MonitoringDB( ElasticDB ):

  def __init__( self, name = 'Monitoring/MonitoringDB', readOnly = False ):
    super( MonitoringDB, self ).__init__( 'MonitoringDB', name, CSGlobals.getSetup().lower() )
    self.__readonly = readOnly
    self.__documents = {}
    self.__loadIndexes()

  def __loadIndexes( self ):
    """
    It loads all monitoring indexes and types.
    """

    objectsLoaded = TypeLoader().getTypes()

    # Load the files
    for pythonClassName in sorted( objectsLoaded ):
      typeClass = objectsLoaded[ pythonClassName ]
      indexName = "%s_%s" % ( self.getIndexPrefix(), typeClass()._getIndex() )
      doc_type = typeClass()._getDocType()
      mapping = typeClass().getMapping()
      monfields = typeClass().getMonitoringFields()
      self.__documents[doc_type] = {'indexName': indexName,
                                    'mapping':mapping,
                                    'monitoringFields':monfields}
      if self.__readonly:
        gLogger.info( "Read only mode is okay" )
      else:
        self.registerType( indexName, mapping )

  def getIndexName( self, typeName ):
    """
    :param str typeName: doc_type and type name is equivalent
    """
    indexName = None

    if typeName in self.__documents:
      indexName = self.__documents.get( typeName ).get( "indexName", None )

    if indexName:
      return S_OK( indexName )
    else:
      return S_ERROR( "Type %s is not defined" % typeName )

  def registerType( self, index, mapping ):
    """
    It register the type and index, if does not exists

    :param str index: name of the index
    :param dict mapping: mapping used to create the index.
    """

    all_index = "%s-*" % index

    if self.exists( all_index ):
      indexes = self.getIndexes()
      if indexes:
        actualindexName = generateFullIndexName( index )
        if self.exists( actualindexName ):
          self.log.info( "The index is exists:", actualindexName )
        else:
          result = self.createIndex( index, mapping )
          if not result['OK']:
            self.log.error( result['Message'] )
            return result
          self.log.info( "The index is created", actualindexName )
    else:
      # in that case no index exists
      result = self.createIndex( index, mapping )
      if not result['OK']:
        self.log.error( result['Message'] )
      else:
        return result



  def getKeyValues( self, typeName ):
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
    gLogger.debug( "Doc types", docs )
    monfields = self.__documents[typeName]['monitoringFields']

    if typeName not in docs:
      #this is only happen when we the index is created and we were not able to send records to the index.
      #There is no data in the index we can not create the plot.
      return S_ERROR( "%s empty and can not retrive the Type of the index" % indexName )
    for i in docs[typeName]['properties']:
      if i not in monfields and not i.startswith( 'time' ):
        retVal = self.getUniqueValue( indexName, i )
        if not retVal['OK']:
          return retVal
        keyValuesDict[i] = retVal['Value']
    return S_OK( keyValuesDict )

  def retrieveBucketedData( self, typeName, startTime, endTime, interval, selectFields, condDict, grouping, metainfo ):
    """
    Get data from the DB

    :param str typeName: name of the monitoring type
    :param int startTime:  epoch objects.
    :param int endtime: epoch objects.
    :param dict condDict: conditions for the query

                   * key -> name of the field
                   * value -> list of possible values

    """

    retVal = self.getIndexName( typeName )
    if not retVal['OK']:
      return retVal
    isAvgAgg = False
    # the data is used to fill the pie charts. This aggregation is used to average the buckets.
    if metainfo and metainfo.get( 'metric', 'sum' ) == 'avg':
      isAvgAgg = True

    indexName = "%s*" % ( retVal['Value'] )
    q = [self._Q( 'range',
                  timestamp = {'lte':endTime * 1000,
                               'gte': startTime * 1000} )]
      
    for cond in condDict:
      query = None
      for condValue in condDict[cond]:
        kwargs = {cond: condValue}
        if query:
          query = query | self._Q( 'match', **kwargs )
        else:
          query = self._Q( 'match', **kwargs )
      q += [query]

    a1 = self._A( 'terms', field = grouping, size = self.RESULT_SIZE )
    a2 = self._A( 'terms', field = 'timestamp' )
    a2.metric( 'total_jobs', 'sum', field = selectFields[0] )
    a1.bucket( 'end_data',
               'date_histogram',
               field = 'timestamp',
               interval = interval ).metric( 'tt', a2 ).pipeline( 'avg_monthly_sales',
                                                                  'avg_bucket',
                                                                  buckets_path = 'tt>total_jobs',
                                                                  gap_policy = 'insert_zeros' )
    if isAvgAgg:
      a1.pipeline( 'avg_total_jobs',
                   'avg_bucket',
                   buckets_path = 'end_data>avg_monthly_sales',
                   gap_policy = 'insert_zeros' )

    s = self._Search( indexName )
    s = s.filter( 'bool', must = q )
    s.aggs.bucket( '2', a1 )
    #s.fields( ['timestamp'] + selectFields )
    gLogger.debug( 'Query:', s.to_dict() )
    retVal = s.execute()

    gLogger.debug( "Query result", len( retVal ) )

    result = {}
    for i in retVal.aggregations['2'].buckets:
      if isAvgAgg:
        result[i.key] = i.avg_total_jobs.value
      else:
        site = i.key
        dp = {}
        for j in i.end_data.buckets:
          dp[j.key / 1000] = j.avg_monthly_sales.value
        result[site] = dp
    # the result format is { 'grouping':{timestamp:value, timestamp:value} for example :
    # {u'Bookkeeping_BookkeepingManager': {1474300800: 4.0, 1474344000: 4.0, 1474331400: 4.0, 1
    # 474302600: 4.0, 1474365600: 4.0, 1474304400: 4.0, 1474320600: 4.0, 1474360200: 4.0,
    # 1474306200: 4.0, 1474356600: 4.0, 1474336800: 4.0, 1474326000: 4.0, 1474315200: 4.0,
    # 1474281000: 4.0, 1474309800: 4.0, 1474338600: 4.0, 1474311600: 4.0, 1474317000: 4.0,
    # 1474367400: 4.0, 1474333200: 4.0, 1474284600: 4.0, 1474362000: 4.0,
    # 1474327800: 4.0, 1474345800: 4.0, 1474286400: 4.0, 1474308000: 4.0, 1474322400: 4.0,
    # 1474288200: 4.0, 1474351200: 4.0, 1474282800: 4.0, 1474347600: 4.0,
    # 1474313400: 4.0, 1474349400: 4.0, 1474297200: 4.0, 1474340400: 4.0, 1474291800: 4.0,
    # 1474335000: 4.0, 1474293600: 4.0, 1474290000: 4.0, 1474363800: 4.0,
    # 1474329600: 4.0, 1474353000: 4.0, 1474358400: 4.0, 1474324200: 4.0, 1474354800: 4.0,
    # 1474295400: 4.0, 1474318800: 4.0, 1474299000: 4.0, 1474342200: 4.0},
    # u'Framework_SystemAdministrator': {1474300800: 8.0, 1474344000: 8.0, 1474331400: 8.0,
    # 1474302600: 8.0, 1474365600: 8.0, 1474304400: 8.0, 1474320600: 8.0,
    # 1474360200: 8.0, 1474306200: 8.0, 1474356600: 8.0, 1474336800: 8.0, 1474326000: 8.0,
    # 1474315200: 8.0, 1474281000: 8.0, 1474309800: 8.0, 1474338600: 8.0,
    # 1474311600: 8.0, 1474317000: 8.0, 1474367400: 8.0, 1474333200: 8.0, 1474284600: 8.0,
    # 1474362000: 8.0, 1474327800: 8.0, 1474345800: 8.0, 1474286400: 8.0,
    # 1474308000: 8.0, 1474322400: 8.0, 1474288200: 8.0, 1474351200: 8.0, 1474282800: 8.0,
    # 1474347600: 8.0, 1474313400: 8.0, 1474349400: 8.0, 1474297200: 8.0,
    # 1474340400: 8.0, 1474291800: 8.0, 1474335000: 8.0, 1474293600: 8.0, 1474290000: 8.0,
    # 1474363800: 8.0, 1474329600: 8.0, 1474353000: 8.0, 1474358400: 8.0,
    # 1474324200: 8.0, 1474354800: 8.0, 1474295400: 8.0, 1474318800: 8.0, 1474299000: 8.0, 1474342200: 8.0}}
    return S_OK( result )

  def retrieveAggregatedData( self, typeName, startTime, endTime, interval, selectFields, condDict, grouping, metainfo ):
    """
    Get data from the DB using simple aggregations. Note: this method is equivalent to retrieveBucketedData.
    The different is the dynamic bucketing. We do not perform dynamic bucketing on the raw data.

    :param str typeName: name of the monitoring type
    :param int startTime:  epoch objects.
    :param int endtime: epoch objects.
    :param dict condDict: conditions for the query

                   * key -> name of the field
                   * value -> list of possible values

    """
#    {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1474271462000, 'lte': 1474357862000}}}]}}]}}, 'aggs': {'end_data': {'date_histogram': {'field': 'timestamp', 'interval': '30m'}, 'aggs': {'tt': {'terms': {'field': 'component', 'size': 10000}, 'aggs': {'m1': {'avg': {'
#    field': 'threads'}}}}}}}}
#
#     query = [Q( 'range',timestamp = {'lte':1474357862000,'gte': 1474271462000} )]
#
#     a = A('terms', field = 'component', size = 10000 )
#     a.metric('m1', 'avg', field = 'threads' )
#
#     s = Search(using=cl, index = 'lhcb-certification_componentmonitoring-index-*')
#
#     s = s.filter( 'bool', must = query )
#     s = s.aggs.bucket('end_data', 'date_histogram', field='timestamp', interval='30m').metric( 'tt', a )

    retVal = self.getIndexName( typeName )
    if not retVal['OK']:
      return retVal

    # default is average
    aggregator = metainfo.get( 'metric', 'avg' )

    indexName = "%s*" % ( retVal['Value'] )
    q = [self._Q( 'range',
                  timestamp = {'lte':endTime * 1000,
                               'gte': startTime * 1000} )]
    for cond in condDict:
      query = None
      for condValue in condDict[cond]:
        kwargs = {cond: condValue}
        if query:
          query = query | self._Q( 'match', **kwargs )
        else:
          query = self._Q( 'match', **kwargs )
      q += [query]

    a1 = self._A( 'terms', field = grouping, size = self.RESULT_SIZE )
    a1.metric( 'm1', aggregator, field = selectFields[0] )

    s = self._Search( indexName )
    s = s.filter( 'bool', must = q )
    s.aggs.bucket( 'end_data',
                   'date_histogram',
                   field = 'timestamp',
                   interval = interval ).metric( 'tt', a1 )

    #s.fields( ['timestamp'] + selectFields )
    s = s.extra( size = self.RESULT_SIZE )  # do not get the hits!

    gLogger.debug( 'Query:', s.to_dict() )
    retVal = s.execute()

    result = {}
    for bucket in retVal.aggregations['end_data'].buckets:
      # each bucket key is a time (unix epoch and usual datetime
      bucketTime = bucket.key / 1000
      for value in bucket['tt'].buckets:
        # each bucket contains an agregation called tt which sum/avg of the metric.
        if value.key not in result:
          result[value.key] = {bucketTime:value.m1.value if value.m1.value else 0 } #TODO: this is kind of hack.
          #we can use a default value for pipeline aggregation. EL promised that we can use default value for simple aggregation. Later to be checked.
        else:
          result[value.key].update( {bucketTime:value.m1.value if value.m1.value else 0} )
    # the result format is { 'grouping':{timestamp:value, timestamp:value} for example : {u'Bookkeeping_BookkeepingManager': {1474300800: 4.0, 1474344000: 4.0, 1474331400: 4.0, 1
    # 474302600: 4.0, 1474365600: 4.0, 1474304400: 4.0, 1474320600: 4.0, 1474360200: 4.0, 1474306200: 4.0, 1474356600: 4.0, 1474336800: 4.0, 1474326000: 4.0, 1474315200: 4.0,
    # 1474281000: 4.0, 1474309800: 4.0, 1474338600: 4.0, 1474311600: 4.0, 1474317000: 4.0, 1474367400: 4.0, 1474333200: 4.0, 1474284600: 4.0, 1474362000: 4.0,
    # 1474327800: 4.0, 1474345800: 4.0, 1474286400: 4.0, 1474308000: 4.0, 1474322400: 4.0, 1474288200: 4.0, 1474351200: 4.0, 1474282800: 4.0, 1474347600: 4.0,
    # 1474313400: 4.0, 1474349400: 4.0, 1474297200: 4.0, 1474340400: 4.0, 1474291800: 4.0, 1474335000: 4.0, 1474293600: 4.0, 1474290000: 4.0, 1474363800: 4.0,
    # 1474329600: 4.0, 1474353000: 4.0, 1474358400: 4.0, 1474324200: 4.0, 1474354800: 4.0, 1474295400: 4.0, 1474318800: 4.0, 1474299000: 4.0, 1474342200: 4.0},
    # u'Framework_SystemAdministrator': {1474300800: 8.0, 1474344000: 8.0, 1474331400: 8.0, 1474302600: 8.0, 1474365600: 8.0, 1474304400: 8.0, 1474320600: 8.0,
    # 1474360200: 8.0, 1474306200: 8.0, 1474356600: 8.0, 1474336800: 8.0, 1474326000: 8.0, 1474315200: 8.0, 1474281000: 8.0, 1474309800: 8.0, 1474338600: 8.0,
    # 1474311600: 8.0, 1474317000: 8.0, 1474367400: 8.0, 1474333200: 8.0, 1474284600: 8.0, 1474362000: 8.0, 1474327800: 8.0, 1474345800: 8.0, 1474286400: 8.0,
    # 1474308000: 8.0, 1474322400: 8.0, 1474288200: 8.0, 1474351200: 8.0, 1474282800: 8.0, 1474347600: 8.0, 1474313400: 8.0, 1474349400: 8.0, 1474297200: 8.0,
    # 1474340400: 8.0, 1474291800: 8.0, 1474335000: 8.0, 1474293600: 8.0, 1474290000: 8.0, 1474363800: 8.0, 1474329600: 8.0, 1474353000: 8.0, 1474358400: 8.0,
    # 1474324200: 8.0, 1474354800: 8.0, 1474295400: 8.0, 1474318800: 8.0, 1474299000: 8.0, 1474342200: 8.0}}

    return S_OK( result )

  def put( self, records, monitoringType ):
    """
    It is used to insert the data to El.

    :param records: it is a list of documents (dictionary)
    :param str monitoringType: is the type of the monitoring
    :type records: python:list
    """
    mapping = self.__getMapping( monitoringType )
    gLogger.debug( "Mapping used to create an index:", mapping )
    res = self.getIndexName( monitoringType )
    if not res['OK']:
      return res
    indexName = res['Value']
    return self.bulk_index( indexName, monitoringType, records, mapping )

  def __getMapping( self, monitoringType ):
    """
    It returns the mapping of a certain monitoring type

    :param str monitoringType: the monitoring type for example WMSHistory
    :return: an empty dictionary if there is no mapping defenied.
    """
    mapping = {}
    if monitoringType in self.__documents:
      mapping = self.__documents[monitoringType].get( "mapping", {} )
    return mapping

  def __getRawData( self, typeName, condDict, size = -1 ):
    """
    It returns the last day data for a given monitoring type.
    :returns: for example

      .. code-block:: python

       {'sort': [{'timestamp': {'order': 'desc'}}],
        'query': {'bool': {'must': [{'match': {'host': 'dzmathe.cern.ch'}},
                                    {'match': {'component': 'Bookkeeping_BookkeepingManager'}}
                                   ]}}}

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                   * key -> name of the field
                   * value -> list of possible values

    :param int size: number of rows which whill be returned. By default is all
    """
    if size < 0: 
      size = self.RESULT_SIZE 
    retVal = self.getIndexName( typeName )
    if not retVal['OK']:
      return retVal
    date = datetime.datetime.utcnow()
    indexName = "%s-%s" % ( retVal['Value'], date.strftime( '%Y-%m-%d' ) )

    # going to create:
    # s = Search(using=cl, index = 'lhcb-certification_componentmonitoring-index-2016-09-16')
    # s = s.filter( 'bool', must = [Q('match', host='dzmathe.cern.ch'), Q('match', component='Bookkeeping_BookkeepingManager')])
    # s = s.query(q)
    # s = s.sort('-timestamp')


    mustClose = []
    for cond in condDict:
      kwargs = {cond: condDict[cond][0]}
      query = self._Q( 'match', **kwargs )
      mustClose.append( query )

    if condDict.get( 'startTime' ) and condDict.get( 'endTime' ):
      query = self._Q( 'range',
                timestamp = {'lte':condDict.get( 'endTime' ),
                             'gte': condDict.get( 'startTime' ) } )

      mustClose.append( query )

    s = self._Search( indexName )
    s = s.filter( 'bool', must = mustClose )
    s = s.sort( '-timestamp' )

    if size > 0:
      s = s.extra( size = size )

    retVal = s.execute()
    if not retVal['OK']:
      return retVal
    if retVal['Value']:
      records = []
      paramNames = dir( retVal['Value'][0] )
      try:
        paramNames.remove( 'meta' )
      except ValueError as e:
        gLogger.warn( "meta is not in the Result", e )
      for resObj in retVal["Value"]:
        records.append( dict( [ ( paramName, getattr( resObj, paramName ) ) for paramName in paramNames] ) )
      return S_OK( records )

  def getLastDayData( self, typeName, condDict ):
    """
    It returns the last day data for a given monitoring type.

    :returns: for example

      .. code-block:: python

       {'sort': [{'timestamp': {'order': 'desc'}}],
        'query': {'bool': {'must': [{'match': {'host': 'dzmathe.cern.ch'}},
                                    {'match': {'component': 'Bookkeeping_BookkeepingManager'}}]}}}

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                  * key -> name of the field
                  * value -> list of possible values
    """
    return self.__getRawData( typeName, condDict )


  def getLimitedData( self, typeName, condDict, size = 10 ):
    """
    Returns a list of records for a given selection.

    :param str typeName: name of the monitoring type
    :param dict condDict: -> conditions for the query

                  * key -> name of the field
                  * value -> list of possible values

    :param int size: Indicates how many entries should be retrieved from the log
    :return: Up to size entries for the given component from the database
    """
    return self.__getRawData( typeName, condDict, size )


  def getDataForAGivenPeriod( self, typeName, condDict, initialDate = '', endDate = '' ):
    """
    Retrieves the history of logging entries for the given component during a given given time period

    :param str typeName: name of the monitoring type
    :param dict condDict: conditions for the query

                  * key -> name of the field
                  * value -> list of possible values

    :param str initialDate: Indicates the start of the time period in the format 'DD/MM/YYYY hh:mm'
    :param str endDate: Indicate the end of the time period in the format 'DD/MM/YYYY hh:mm'
    :return: Entries from the database for the given component recorded between the initial and the end dates

    """
    if not initialDate and not endDate:
      return self.__getRawData( typeName, condDict, 10 )

    if initialDate:
      condDict['startTime'] = datetime.datetime.strptime( initialDate, '%d/%m/%Y %H:%M' )
    else:
      condDict['startTime'] = datetime.datetime.min
    if endDate:
      condDict['endTime'] = datetime.datetime.strptime( endDate, '%d/%m/%Y %H:%M' )
    else:
      condDict['endTime'] = datetime.datetime.utcnow()


    return self.__getRawData( typeName, condDict )
