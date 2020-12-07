"""
Wrapper on top of ElasticDB. It is used to manage the DIRAC monitoring types.

**Configuration Parameters**:

The following options can be set in ``Systems/Monitoring/<Setup>/Databases/MonitoringDB``

* *IndexPrefix*:  Prefix used to prepend to indexes created in the ES instance. If this
                  is not present in the CS, the indexes are prefixed with the setup name.

"""

__RCSID__ = "$Id$"

import time
import calendar

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.Core.Utilities.Plotting.TypeLoader import TypeLoader
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection


########################################################################
class MonitoringDB(ElasticDB):
  """ Extension of ElasticDB for Monitoring system DB
  """

  def __init__(self, name='Monitoring/MonitoringDB', readOnly=False):
    section = getDatabaseSection("Monitoring/MonitoringDB")
    indexPrefix = gConfig.getValue("%s/IndexPrefix" % section, CSGlobals.getSetup()).lower()
    super(MonitoringDB, self).__init__('MonitoringDB', name, indexPrefix)
    self.__readonly = readOnly
    self.documentTypes = {}

    # loads all monitoring indexes and types.
    objectsLoaded = TypeLoader('Monitoring').getTypes()

    # Load the files
    for pythonClassName in sorted(objectsLoaded):
      typeClass = objectsLoaded[pythonClassName]
      indexName = "%s_%s" % (self.getIndexPrefix(), typeClass()._getIndex())
      monitoringType = typeClass().__class__.__name__
      mapping = typeClass().mapping
      monfields = typeClass().monitoringFields
      period = typeClass().period
      self.documentTypes[monitoringType] = {'indexName': indexName,
                                            'mapping': mapping,
                                            'monitoringFields': monfields,
                                            'period': period}
      if self.__readonly:
        self.log.info("Read only mode is okay")
      else:
        if self.exists("%s-*" % indexName):
          indexes = self.getIndexes()
          if indexes:
            actualIndexName = self.generateFullIndexName(indexName, period)
            if self.exists(actualIndexName):
              self.log.info("The index exists:", actualIndexName)
            else:
              result = self.createIndex(indexName,
                                        self.documentTypes[monitoringType]['mapping'],
                                        period)
              if not result['OK']:
                self.log.error(result['Message'])
                raise RuntimeError(result['Message'])
              self.log.info("The index is created", actualIndexName)
        else:
          # in case the index does not exist
          result = self.createIndex(indexName,
                                    self.documentTypes[monitoringType]['mapping'],
                                    period)
          if not result['OK']:
            self.log.error(result['Message'])
            raise RuntimeError(result['Message'])
          self.log.info("The index is created", indexName)

  def getIndexName(self, typeName):
    """
    :param str typeName: monitoring type
    """
    indexName = None

    if typeName in self.documentTypes:
      indexName = self.documentTypes.get(typeName).get("indexName", None)

    if indexName:
      return S_OK(indexName)

    return S_ERROR("Monitoring type %s is not defined" % typeName)

  def getKeyValues(self, monitoringType):
    """
    Get all values for a given key field in a type
    """
    keyValuesDict = {}

    retVal = self.getIndexName(monitoringType)
    if not retVal['OK']:
      return retVal
    indexName = "%s*" % (retVal['Value'])
    retVal = self.getDocTypes(indexName)
    if not retVal['OK']:
      return retVal
    docs = retVal['Value']
    self.log.debug("Doc types", docs)
    monfields = self.documentTypes[monitoringType]['monitoringFields']

    try:
      properties = docs[monitoringType]['properties']  # "old" way, with ES types == Monitoring types
    except KeyError:
      try:
        properties = docs['_doc']['properties']  # "ES6" way, with ES types == '_doc'
      except KeyError:
        properties = docs['properties']  # "ES7" way, no types

    for i in properties:
      if i not in monfields and not i.startswith('time') and i != 'metric':
        retVal = self.getUniqueValue(indexName, i)
        if not retVal['OK']:
          return retVal
        keyValuesDict[i] = retVal['Value']
    return S_OK(keyValuesDict)

  def retrieveBucketedData(self, typeName, startTime, endTime,
			   interval, selectFields, condDict, grouping,
			   metainfo=None):
    """
    Get data from the DB

    :param str typeName: name of the monitoring type
    :param int startTime:  epoch object
    :param int endtime: epoch object
    :param dict condDict: conditions for the query

                   * key -> name of the field
                   * value -> list of possible values

    """

    isAvgAgg = False
    # the data is used to fill the pie charts.
    # This aggregation is used to average the buckets.
    if metainfo and metainfo.get('metric', 'sum') == 'avg':
      isAvgAgg = True

    # building the query incrementally
    q = [self._Q('range',
                 timestamp={'lte': endTime * 1000,
                            'gte': startTime * 1000})]
    for cond in condDict:
      query = None
      for condValue in condDict[cond]:
        kwargs = {cond: condValue}
        if query:
          query = query | self._Q('match', **kwargs)
        else:
          query = self._Q('match', **kwargs)
      q += [query]

    retVal = self.getIndexName(typeName)
    if not retVal['OK']:
      return retVal
    indexName = "%s*" % (retVal['Value'])
    s = self._Search(indexName)
    s = s.filter('bool', must=q)

    for i, field in enumerate(selectFields):
      a1 = self._A('terms', field=grouping, size=self.RESULT_SIZE)
      a2 = self._A('terms', field='timestamp')

      a2.metric('total_jobs', 'sum', field=field)
      a1.bucket('end_data',
                'date_histogram',
                field='timestamp',
                interval=interval).metric('tt', a2).pipeline('avg_monthly_sales',
                                                             'avg_bucket',
                                                             buckets_path='tt>total_jobs',
                                                             gap_policy='insert_zeros')
      if isAvgAgg:
        a1.pipeline('avg_total_jobs',
                    'avg_bucket',
                    buckets_path='end_data>avg_monthly_sales',
                    gap_policy='insert_zeros')

      s.aggs.bucket(str(i), a1)

    #  s.fields( ['timestamp'] + selectFields )
    s = s.source(False)  # don't return any fields, just the metadata
    self.log.debug('Query:', s.to_dict())
    retVal = s.execute()

    self.log.debug("Query result", len(retVal))

    result = {}
#    for i in retVal.aggregations['2'].buckets:
    for i, field in enumerate(selectFields):
      for bucket in retVal.aggregations[str(i)].buckets:
        if isAvgAgg:
	  if bucket.key not in result:
            if len(selectFields) == 1:  # for backward compatibility
	      result[bucket.key] = bucket.avg_total_jobs.value
            else:
	      result[bucket.key] = [bucket.avg_total_jobs.value]
          else:
	    result[bucket.key].append(bucket.avg_total_jobs.value)
        else:
	  site = bucket.key
          if site not in result:
            result[site] = {}
	  for k in bucket.end_data.buckets:
            if (k.key / 1000) not in result[site]:
              if len(selectFields) == 1:  # for backward compatibility
                result[site][k.key / 1000] = k.avg_monthly_sales.value
              else:
                result[site][k.key / 1000] = [k.avg_monthly_sales.value]
            else:
              result[site][k.key / 1000].append(k.avg_monthly_sales.value)

    # the result format is { 'grouping':{timestamp:value, timestamp:value}:
    # value is list if more than one value exist. for example :
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
    return S_OK(result)

  def retrieveAggregatedData(self, typeName, startTime, endTime, interval,
			     selectFields, condDict, grouping,
			     metainfo=None):
    """
    Get data from the DB using simple aggregations.
    Note: this method is equivalent to retrieveBucketedData.
    The difference is in the dynamic bucketing.
    We do not perform dynamic bucketing on the raw data.

    :param str typeName: name of the monitoring type
    :param int startTime: epoch object
    :param int endtime: epoch object
    :param interval:
    :param selectFields:
    :param dict condDict: conditions for the query
    :param str grouping: grouping requested
    :param dict metainfo: dictionary of meta info (e.g. {'metric': 'avg'})
    :returns: S_OK/S_ERROR with dictionary of key/value pairs

    """
    # {'query':
    #     {'bool':
    #         {'filter': [
    #             {'bool':
    #                 {'must': [
    #                     {'range':
    #                         {'timestamp':
    #                             {'gte': 1474271462000, 'lte': 1474357862000}}}]}}]}},
    #     'aggs':
    #         {'end_data':
    #             {'date_histogram':
    #                 {'field': 'timestamp', 'interval': '30m'},
    #              'aggs': {'tt': {'terms': {'field': 'component', 'size': 10000},
    #                       'aggs': {'m1': {'avg': {'field': 'threads'}}}}}}}}

    # query = [Q('range', timestamp={'lte':1474357862000,'gte': 1474271462000} )]

    # a = A('terms', field='component', size=10000)
    # a.metric('m1', 'avg', field='threads')

    # s = Search(using=cl, index='lhcb-certification_componentmonitoring-index-*')

    # s = s.filter( 'bool', must = query )
    # s = s.aggs.bucket('end_data', 'date_histogram', field='timestamp', interval='30m').metric( 'tt', a )

    # default is average
    aggregator = metainfo.get('metric', 'avg')

    q = [self._Q('range',
                 timestamp={'lte': endTime * 1000,
                            'gte': startTime * 1000})]
    for cond in condDict:
      query = None
      for condValue in condDict[cond]:
        kwargs = {cond: condValue}
        if query:
          query = query | self._Q('match', **kwargs)
        else:
          query = self._Q('match', **kwargs)
      q += [query]

    retVal = self.getIndexName(typeName)
    if not retVal['OK']:
      return retVal
    indexName = "%s*" % (retVal['Value'])

    s = self._Search(indexName)
    s = s.filter('bool', must=q)

    a1 = self._A('terms', field=grouping, size=self.RESULT_SIZE)
    a1.metric('m1', aggregator, field=selectFields[0])
    s.aggs.bucket('end_data',
                  'date_histogram',
                  field='timestamp',
                  interval=interval).metric('tt', a1)

    # s.fields(['timestamp'] + selectFields)
    s = s.extra(size=self.RESULT_SIZE)  # max size
    s = s.source(False)  # don't return any fields, just the metadata

    self.log.debug('Query:', s.to_dict())
    retVal = s.execute()

    result = {}
    for bucket in retVal.aggregations['end_data'].buckets:
      # each bucket key is a time (unix epoch and usual datetime
      bucketTime = bucket.key / 1000
      for value in bucket['tt'].buckets:
        # each bucket contains an agregation called tt which sum/avg of the metric.
        if value.key not in result:
          result[value.key] = {bucketTime: value.m1.value if value.m1.value else 0}  # TODO: this is kind of hack.
          # we can use a default value for pipeline aggregation. EL promised that we
          # can use default value for simple aggregation. Later to be checked.
        else:
          result[value.key].update({bucketTime: value.m1.value if value.m1.value else 0})
    # the result format is { 'grouping':{timestamp:value, timestamp:value}
    # for example : {u'Bookkeeping_BookkeepingManager': {1474300800: 4.0, 1474344000: 4.0, 1474331400: 4.0, 1
    # 474302600: 4.0, 1474365600: 4.0, 1474304400: 4.0, 1474320600: 4.0, 1474360200: 4.0, 1474306200: 4.0,
    # 1474356600: 4.0, 1474336800: 4.0, 1474326000: 4.0, 1474315200: 4.0,
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
    # 1474324200: 8.0, 1474354800: 8.0, 1474295400: 8.0, 1474318800: 8.0, 1474299000: 8.0,
    # 1474342200: 8.0}}

    return S_OK(result)

  def put(self, records, monitoringType):
    """
    It is used to insert the data to El.

    :param list records: it is a list of documents (dictionary)
    :param str monitoringType: is the type of the monitoring
    """
    mapping = self.getMapping(monitoringType)
    self.log.always("Mapping used to create an index:", mapping)
    period = self.documentTypes[monitoringType].get('period')
    res = self.getIndexName(monitoringType)
    if not res['OK']:
      return res
    indexName = res['Value']

    return self.bulk_index(indexPrefix=indexName,
                           data=records,
                           mapping=mapping,
                           period=period)

  def getMapping(self, monitoringType):
    """
    It returns the mapping of a certain monitoring type

    :param str monitoringType: the monitoring type for example WMSHistory
    :return: an empty dictionary if there is no mapping defenied.
    """
    mapping = {}
    if monitoringType in self.documentTypes:
      mapping = self.documentTypes[monitoringType].get("mapping", {})
    return mapping

  def __getRawData(self, typeName, condDict, size=-1):
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
    retVal = self.getIndexName(typeName)
    if not retVal['OK']:
      return retVal
    indexName = "%s-%s" % (retVal['Value'], time.strftime('%Y-%m-%d', time.gmtime()))

    # going to create:
    # s = Search(using=cl, index = 'lhcb-certification_componentmonitoring-index-2016-09-16')
    # s = s.filter( 'bool', must = [Q('match', host='dzmathe.cern.ch'),
    #  Q('match', component='Bookkeeping_BookkeepingManager')])
    # s = s.query(q)
    # s = s.sort('-timestamp')

    mustClose = []
    for cond in condDict:
      if cond not in ('startTime', 'endTime'):
	kwargs = {cond: condDict[cond]}
	query = self._Q('match', **kwargs)
	mustClose.append(query)

    if condDict.get('startTime') and condDict.get('endTime'):
      query = self._Q('range',
		      timestamp={'lte': condDict.get('endTime') * 1000,
				 'gte': condDict.get('startTime') * 1000})

      mustClose.append(query)

    s = self._Search(indexName)
    s = s.filter('bool', must=mustClose)
    s = s.sort('-timestamp')

    if size > 0:
      s = s.extra(size=size)

    self.log.debug('Query:', s.to_dict())
    retVal = s.execute()
    if not retVal:
      self.log.error("Error getting raw data", str(retVal))
      return S_ERROR(str(retVal))
    hits = retVal['hits']
    if hits and 'hits' in hits and hits['hits']:
      records = []
      paramNames = dir(hits['hits'][0]['_source'])
      try:
        paramNames.remove(u'metric')
      except KeyError as e:
        self.log.warn("metric is not in the Result", e)
      for resObj in hits['hits']:
        records.append(dict([(paramName, getattr(resObj['_source'], paramName)) for paramName in paramNames]))
      return S_OK(records)

  def getLastDayData(self, typeName, condDict):
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
    return self.__getRawData(typeName, condDict)

  def getLimitedData(self, typeName, condDict, size=10):
    """
    Returns a list of records for a given selection.

    :param str typeName: name of the monitoring type
    :param dict condDict: -> conditions for the query

                  * key -> name of the field
                  * value -> list of possible values

    :param int size: Indicates how many entries should be retrieved from the log
    :return: Up to size entries for the given component from the database
    """
    return self.__getRawData(typeName, condDict, size)

  def getDataForAGivenPeriod(self, typeName, condDict, initialDate='', endDate=''):
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
      return self.__getRawData(typeName, condDict, 10)

    if initialDate:
      condDict['startTime'] = calendar.timegm(time.strptime(initialDate, '%d/%m/%Y %H:%M'))
    else:
      condDict['startTime'] = 0000000000
    if endDate:
      condDict['endTime'] = calendar.timegm(time.strptime(endDate, '%d/%m/%Y %H:%M'))
    else:
      condDict['endTime'] = calendar.timegm(time.gmtime())

    return self.__getRawData(typeName, condDict)
