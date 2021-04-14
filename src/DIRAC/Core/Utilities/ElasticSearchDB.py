"""
This class a wrapper around elasticsearch-py.
It is used to query Elasticsearch instances.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six

from datetime import datetime
from datetime import timedelta

import json
import certifi
import functools

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError, RequestError
from elasticsearch.helpers import BulkIndexError, bulk

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time, DErrno
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient


sLog = gLogger.getSubLogger(__name__)


def ifConnected(method):
  """ Decorator for checking that the connection is established.
  """
  @functools.wraps(method)
  def wrapper_decorator(self, *args, **kwargs):
    if self._connected:
      return method(self, *args, **kwargs)
    else:
      sLog.error("Not connected")
      return S_ERROR("Not connected")
  return wrapper_decorator


def generateDocs(data, withTimeStamp=True):
  """ Generator for fast bulk indexing, yields docs

  :param list data: list of dictionaries
  :param bool withTimeStamp: add the timestamps to the docs

  :return: doc
  """
  for doc in data:
    if "_type" not in doc:
      doc['_type'] = "_doc"
    if withTimeStamp:
      if 'timestamp' not in doc:
        sLog.warn("timestamp is not given")

      # if the timestamp is not provided, we use the current utc time.
      timestamp = doc.get('timestamp', int(Time.toEpoch()))
      try:
        if isinstance(timestamp, datetime):
          doc['timestamp'] = int(timestamp.strftime('%s')) * 1000
        elif isinstance(timestamp, six.string_types):
          timeobj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
          doc['timestamp'] = int(timeobj.strftime('%s')) * 1000
        else:  # we assume  the timestamp is an unix epoch time (integer).
          doc['timestamp'] = timestamp * 1000
      except (TypeError, ValueError) as e:
        # in case we are not able to convert the timestamp to epoch time....
        sLog.error("Wrong timestamp", e)
        doc['timestamp'] = int(Time.toEpoch()) * 1000

    sLog.debug("yielding %s" % doc)
    yield doc

class ElasticSearchDB(object):

  """
  .. class:: ElasticSearchDB

  :param str url: the url to the database for example: el.cern.ch:9200
  :param str gDebugFile: is used to save the debug information to a file
  :param int timeout: the default time out to Elasticsearch
  :param int RESULT_SIZE: The number of data points which will be returned by the query.
  """
  __url = ""
  __timeout = 120
  clusterName = ''
  RESULT_SIZE = 10000

  ########################################################################
  def __init__(self, host, port, user=None, password=None, indexPrefix='', useSSL=True,
               useCRT=False, ca_certs=None, client_key=None, client_cert=None):
    """ c'tor

    :param self: self reference
    :param str host: name of the database for example: MonitoringDB
    :param str port: The full name of the database for example: 'Monitoring/MonitoringDB'
    :param str user: user name to access the db
    :param str password: if the db is password protected we need to provide a password
    :param str indexPrefix: it is the indexPrefix used to get all indexes
    :param bool useSSL: We can disable using secure connection. By default we use secure connection.
    :param bool useCRT: Use certificates.
    :param str ca_certs: Server certificate.
    :param str client_key: Client key.
    :param str client_cert: Client certificate.
    """

    self.__indexPrefix = indexPrefix
    self._connected = False
    if user and password:
      sLog.debug("Specified username and password")
      if port:
        self.__url = "https://%s:%s@%s:%d" % (user, password, host, port)
      else:
        self.__url = "https://%s:%s@%s" % (user, password, host)
    else:
      sLog.debug("Username and password not specified")
      if port:
        self.__url = "http://%s:%d" % (host, port)
      else:
        self.__url = "http://%s" % host

    if port:
      sLog.verbose("Connecting to %s:%s, useSSL = %s" % (host, port, useSSL))
    else:
      sLog.verbose("Connecting to %s, useSSL = %s" % (host, useSSL))

    if useSSL:
      bd = BundleDeliveryClient()
      retVal = bd.getCAs()
      casFile = None
      if not retVal['OK']:
        sLog.error("CAs file does not exists:", retVal['Message'])
        casFile = certifi.where()
      else:
        casFile = retVal['Value']

      self.client = Elasticsearch(self.__url,
                                  timeout=self.__timeout,
                                  use_ssl=True,
                                  verify_certs=True,
                                  ca_certs=casFile)
    elif useCRT:
        self.client = Elasticsearch(self.__url,
                                    timeout=self.__timeout,
                                    use_ssl=True,
                                    verify_certs=True,
                                    ca_certs=ca_certs,
                                    client_cert=client_cert,
                                    client_key=client_key)
    else:
      self.client = Elasticsearch(self.__url, timeout=self.__timeout)

    # Before we use the database we try to connect
    # and retrieve the cluster name

    try:
      if self.client.ping():
        # Returns True if the cluster is running, False otherwise
        result = self.client.info()
        self.clusterName = result.get("cluster_name", " ")  # pylint: disable=no-member
        sLog.info("Database info\n", json.dumps(result, indent=4))
        self._connected = True
      else:
        sLog.error("Cannot ping ElasticsearchDB!")
    except ConnectionError as e:
      sLog.error(repr(e))

  ########################################################################
  def getIndexPrefix(self):
    """
    It returns the DIRAC setup.
    """
    return self.__indexPrefix

  ########################################################################
  @ifConnected
  def query(self, index, query):
    """ Executes a query and returns its result (uses ES DSL language).

    :param self: self reference
    :param str index: index name
    :param dict query: It is the query in ElasticSearch DSL language

    """
    try:
      esDSLQueryResult = self.client.search(index=index, body=query)
      return S_OK(esDSLQueryResult)
    except RequestError as re:
      return S_ERROR(re)

  @ifConnected
  def update(self, index, query=None, updateByQuery=True, id=None):
    """ Executes an update of a document, and returns S_OK/S_ERROR

    :param self: self reference
    :param str index: index name
    :param dict query: It is the query in ElasticSearch DSL language
    :param bool updateByQuery: A bool to determine update by query or index values using index function.
    :param int id: ID for the document to be created.

    """

    sLog.debug("Updating %s with %s, updateByQuery=%s, id=%s" % (index, query, updateByQuery, id))

    if not index or not query:
      return S_ERROR("Missing index or query")

    try:
      if updateByQuery:
        esDSLQueryResult = self.client.update_by_query(index=index, body=query)
      else:
        esDSLQueryResult = self.client.index(index=index, doc_type='_doc', body=query, id=id)
      return S_OK(esDSLQueryResult)
    except RequestError as re:
      return S_ERROR(re)

  @ifConnected
  def _Search(self, indexname):
    """
    it returns the object which can be used for retreiving certain value from the DB
    """
    return Search(using=self.client, index=indexname)

  ########################################################################
  def _Q(self, name_or_query='match', **params):
    """
    It is a wrapper to ElasticDSL Query module used to create a query object.
    :param str name_or_query is the type of the query
    """
    return Q(name_or_query, **params)

  def _A(self, name_or_agg, aggsfilter=None, **params):
    """
    It is a wrapper to ElasticDSL aggregation module, used to create an aggregation
    """
    return A(name_or_agg, aggsfilter, **params)

  ########################################################################
  @ifConnected
  def getIndexes(self, indexName=None):
    """
    It returns the available indexes...
    """
    if not indexName:
      indexName = self.__indexPrefix
    sLog.debug("Getting indices alias of %s" % indexName)
    # we only return indexes which belong to a specific prefix for example 'lhcb-production' or 'dirac-production etc.
    return list(self.client.indices.get_alias("%s*" % indexName))

  ########################################################################
  @ifConnected
  def getDocTypes(self, indexName):
    """
    Returns mappings, by index.

    :param str indexName: is the name of the index...
    :return: S_OK or S_ERROR
    """
    result = []
    try:
      sLog.debug("Getting mappings for ", indexName)
      result = self.client.indices.get_mapping(indexName)
    except Exception as e:  # pylint: disable=broad-except
      sLog.exception()
      return S_ERROR(e)

    doctype = ''
    for indexConfig in result:
      if not result[indexConfig].get('mappings'):
        # there is a case when the mapping exits and the value is None...
        # this is usually an empty index or a corrupted index.
        sLog.warn("Index does not have mapping %s!" % indexConfig)
        continue
      if result[indexConfig].get('mappings'):
        doctype = result[indexConfig]['mappings']
        break  # we suppose the mapping of all indexes are the same...

    if not doctype:
      return S_ERROR("%s does not exists!" % indexName)

    return S_OK(doctype)

  ########################################################################
  @ifConnected
  def existingIndex(self, indexName):
    """
    Checks the existance of an index, by its name

    :param str indexName: the name of the index
    :returns: S_OK/S_ERROR if the request is successful
    """
    sLog.debug("Checking existance of index %s" % indexName)
    try:
      return S_OK(self.client.indices.exists(indexName))
    except TransportError as e:
      sLog.exception()
      return S_ERROR(e)

  ########################################################################

  @ifConnected
  def createIndex(self, indexPrefix, mapping=None, period='day'):
    """
    :param str indexPrefix: it is the index name.
    :param dict mapping: the configuration of the index.
    :param str period: We can specify, which kind of index will be created.
                       Currently only daily and monthly indexes are supported.

    """
    if period is not None:
      fullIndex = self.generateFullIndexName(indexPrefix, period)  # we have to create an index each period...
    else:
      sLog.warn("The period is not provided, so using non-periodic indexes names")
      fullIndex = indexPrefix

    res = self.existingIndex(fullIndex)
    if not res['OK']:
      return res
    elif res['Value']:
      return S_OK(fullIndex)

    try:
      sLog.info("Create index: ", fullIndex + str(mapping))
      try:
        self.client.indices.create(index=fullIndex, body={'mappings': mapping})  # ES7
      except RequestError as re:
        if re.error == 'mapper_parsing_exception':
          self.client.indices.create(index=fullIndex, body={'mappings': {'_doc': mapping}})  # ES6
      return S_OK(fullIndex)
    except Exception as e:  # pylint: disable=broad-except
      sLog.error("Can not create the index:", repr(e))
      return S_ERROR("Can not create the index")

  @ifConnected
  def deleteIndex(self, indexName):
    """
    :param str indexName: the name of the index to be deleted...
    """
    sLog.info("Deleting index", indexName)
    try:
      retVal = self.client.indices.delete(indexName)
    except NotFoundError:
      sLog.warn("Index does not exist", indexName)
      return S_OK("Noting to delete")
    except ValueError as e:
      return S_ERROR(DErrno.EVALUE, e)

    if retVal.get('acknowledged'):
      # if the value exists and the value is not None
      sLog.info("Deleted index", indexName)
      return S_OK(indexName)

    return S_ERROR(retVal)

  def index(self, indexName, body=None, docID=None):
    """
    :param str indexName: the name of the index to be used
    :param dict body: the data which will be indexed (basically the JSON)
    :param int id: optional document id
    :return: the index name in case of success.
    """

    sLog.debug("Indexing in %s body %s, id=%s" % (indexName, body, docID))

    if not indexName or not body:
      return S_ERROR("Missing index or body")

    try:
      res = self.client.index(index=indexName,
                              doc_type='_doc',
                              body=body,
                              id=docID)
    except (RequestError, TransportError) as e:
      sLog.exception()
      return S_ERROR(e)

    if res.get('created') or res.get('result') in ('created', 'updated'):
      # the created index exists but the value can be None.
      return S_OK(indexName)

    return S_ERROR(res)

  @ifConnected
  def bulk_index(self, indexPrefix, data=None, mapping=None, period='day', withTimeStamp=True):
    """
    :param str indexPrefix: index name.
    :param list data: contains a list of dictionary
    :param dict mapping: the mapping used by elasticsearch
    :param str period: Accepts 'day' and 'month'. We can specify which kind of indexes will be created.
    :param bool withTimeStamp: add timestamp to data, if not there already.

    :returns: S_OK/S_ERROR
    """
    sLog.verbose("Bulk indexing", "%d records will be inserted" % len(data))
    if mapping is None:
      mapping = {}

    if period is not None:
      indexName = self.generateFullIndexName(indexPrefix, period)
    else:
      indexName = indexPrefix
    sLog.debug("Bulk indexing into %s of %s" % (indexName, data))

    res = self.existingIndex(indexName)
    if not res['OK']:
      return res
    if not res['Value']:
      retVal = self.createIndex(indexPrefix, mapping, period)
      if not retVal['OK']:
        return retVal

    try:
      res = bulk(client=self.client, index=indexName, actions=generateDocs(data, withTimeStamp))
    except (BulkIndexError, RequestError) as e:
      sLog.exception()
      return S_ERROR(e)

    if res[0] == len(data):
      # we have inserted all documents...
      return S_OK(len(data))
    else:
      return S_ERROR(res)

  @ifConnected
  def getUniqueValue(self, indexName, key, orderBy=False):
    """
    :param str indexName: the name of the index which will be used for the query
    :param dict orderBy: it is a dictionary in case we want to order the result {key:'desc'} or {key:'asc'}
    :returns: a list of unique value for a certain key from the dictionary.
    """

    query = self._Search(indexName)

    endDate = datetime.utcnow()

    startDate = endDate - timedelta(days=30)

    timeFilter = self._Q('range',
                         timestamp={'lte': int(Time.toEpoch(endDate)) * 1000,
                                    'gte': int(Time.toEpoch(startDate)) * 1000, })
    query = query.filter('bool', must=timeFilter)
    if orderBy:
      query.aggs.bucket(key,
                        'terms',
                        field=key,
                        size=self.RESULT_SIZE,
                        order=orderBy).metric(key,
                                              'cardinality',
                                              field=key)
    else:
      query.aggs.bucket(key,
                        'terms',
                        field=key,
                        size=self.RESULT_SIZE).metric(key,
                                                      'cardinality',
                                                      field=key)

    try:
      query = query.extra(size=self.RESULT_SIZE)  # do not need the raw data.
      sLog.debug("Query", query.to_dict())
      result = query.execute()
    except TransportError as e:
      return S_ERROR(e)

    values = []
    for bucket in result.aggregations[key].buckets:
      values += [bucket['key']]
    del query
    sLog.debug("Nb of unique rows retrieved", len(values))
    return S_OK(values)

  def pingDB(self):
    """
    Try to connect to the database

    :return: S_OK(TRUE/FALSE)
    """
    connected = False
    try:
      connected = self.client.ping()
    except ConnectionError as e:
      sLog.error("Cannot connect to the db", repr(e))
    return S_OK(connected)

  @ifConnected
  def deleteByQuery(self, indexName, query):
    """
    Delete data by query (careful!)

    :param str indexName: the name of the index
    :param str query: the JSON-formatted query for which we want to issue the delete
    """
    try:
      self.client.delete_by_query(index=indexName, body=query)
    except Exception as inst:
      sLog.error("ERROR: Couldn't delete data")
      return S_ERROR(inst)
    return S_OK('Successfully deleted data from index %s' % indexName)

  @staticmethod
  def generateFullIndexName(indexName, period):
    """
    Given an index prefix we create the actual index name.

    :param str indexName: it is the name of the index
    :param str period: We can specify which kind of indexes will be created (day, week, month, year, null).
    :returns: string with full index name
    """

    # if the period is not correct, we use no-period indexes (same as "null").
    if period.lower() not in ['day', 'week', 'month', 'year', 'null']:
      sLog.error("Period is not correct: ", period)
      return indexName
    elif period.lower() == 'day':
      today = datetime.today().strftime("%Y-%m-%d")
      return "%s-%s" % (indexName, today)
    elif period.lower() == 'week':
      week = datetime.today().isocalendar()[1]
      return "%s-%s" % (indexName, week)
    elif period.lower() == 'month':
      month = datetime.today().strftime("%Y-%m")
      return "%s-%s" % (indexName, month)
    elif period.lower() == 'year':
      year = datetime.today().strftime("%Y")
      return "%s-%s" % (indexName, year)
    elif period.lower() == 'null':
      return indexName
