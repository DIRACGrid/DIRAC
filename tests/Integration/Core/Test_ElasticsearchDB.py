"""
This is used to test the ElasticSearchDB module. It is used to discover all possible changes of Elasticsearch api.
If you modify the test data, you have to update the test cases...
"""

# TODO: move to pytest

import unittest
import sys
import datetime
import time

from DIRAC import gLogger
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB

elHost = 'localhost'
elPort = 9200


class ElasticTestCase(unittest.TestCase):
  """ Test of ElasticSearchDB class, using local instance
  """

  def __init__(self, *args, **kwargs):

    super(ElasticTestCase, self).__init__(*args, **kwargs)

    self.data = [{"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 16:30:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
                 {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
                 {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
                 {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"}]

    self.moreData = [{"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 09:15:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "c", "timestamp": "2015-02-09 09:30:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "d", "timestamp": "2015-02-09 10:00:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "e", "timestamp": "2015-02-09 10:15:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "f", "timestamp": "2015-02-09 10:30:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "g", "timestamp": "2015-02-09 10:45:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "h", "timestamp": "2015-02-09 11:00:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "i", "timestamp": "2015-02-09 11:15:00.0"},
                     {"Color": "red", "quantity": 1, "Product": "l", "timestamp": "2015-02-09 11:30:00.0"}]

    self.index_name = ''

    self.maxDiff = None

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.elasticSearchDB = ElasticSearchDB(host=elHost,
                                           port=elPort,
                                           useSSL=False)

  def tearDown(self):
    pass


class ElasticBulkCreateChain(ElasticTestCase):
  """ Chain for creating indexes
  """

  def test_bulkindex(self):
    """ bulk_index test
    """
    result = self.elasticSearchDB.bulk_index('integrationtest',
                                             self.data)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 10)
    time.sleep(5)
    indexes = self.elasticSearchDB.getIndexes()
    self.assertEqual(type(indexes), list)
    for index in indexes:
      res = self.elasticSearchDB.deleteIndex(index)
      self.assertTrue(res['OK'])

  def test_bulkindexMonthly(self):
    """ bulk_index test (month)
    """
    result = self.elasticSearchDB.bulk_index(indexPrefix='integrationtestmontly',
                                             data=self.data,
                                             period='month')
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 10)
    time.sleep(5)
    indexes = self.elasticSearchDB.getIndexes()
    self.assertEqual(type(indexes), list)
    for index in indexes:
      res = self.elasticSearchDB.deleteIndex(index)
      self.assertTrue(res['OK'])


class ElasticCreateChain(ElasticTestCase):
  """ 2 simple tests on index creation and deletion
  """

  def tearDown(self):
    self.elasticSearchDB.deleteIndex(self.index_name)

  def test_index(self):
    """ create index test
    """
    result = self.elasticSearchDB.createIndex('integrationtest', {})
    self.assertTrue(result['OK'])
    self.index_name = result['Value']

    for i in self.data:
      result = self.elasticSearchDB.index(self.index_name, i)
      self.assertTrue(result['OK'])

  def test_wrongdataindex(self):
    """ create index test (wrong insertion)
    """
    result = self.elasticSearchDB.createIndex('dsh63tsdgad', {})
    self.assertTrue(result['OK'])
    index_name = result['Value']
    result = self.elasticSearchDB.index(index_name, {"Color": "red",
                                                     "quantity": 1,
                                                     "Product": "a",
                                                     "timestamp": 1458226213})
    self.assertTrue(result['OK'])
    result = self.elasticSearchDB.index(index_name, {"Color": "red",
                                                     "quantity": 1,
                                                     "Product": "a",
                                                     "timestamp": "2015-02-09T16:15:00Z"})
    self.assertFalse(result['OK'])
    self.assertTrue(result['Message'])
    result = self.elasticSearchDB.deleteIndex(index_name)
    self.assertTrue(result['OK'])


class ElasticDeleteChain(ElasticTestCase):
  """ deletion tests
  """

  def test_deleteNonExistingIndex(self):
    """ delete non-existing index
    """
    result = self.elasticSearchDB.deleteIndex('dsdssuu')
    self.assertFalse(result['OK'])
    self.assertTrue(result['Message'])


class ElasticTestChain(ElasticTestCase):
  """ various tests chained
  """

  def setUp(self):
    self.elasticSearchDB = ElasticSearchDB(host=elHost,
                                           port=elPort,
                                           useSSL=False)
    result = self.elasticSearchDB.generateFullIndexName('integrationtest', 'day')
    self.assertTrue(len(result) > len('integrationtest'))
    self.index_name = result

    result = self.elasticSearchDB.index(self.index_name, {"Color": "red",
                                                          "quantity": 1,
                                                          "Product": "a",
                                                          "timestamp": 1458226213})
    self.assertTrue(result['OK'])

  def tearDown(self):
    self.elasticSearchDB.deleteIndex(self.index_name)

  def test_getIndexes(self):
    """ test fail if no indexes are present
    """
    self.elasticSearchDB.deleteIndex(self.index_name)
    result = self.elasticSearchDB.getIndexes()
    self.assertFalse(result)  # it will be empty at this point

  def test_getDocTypes(self):
    """ test get document types
    """
    result = self.elasticSearchDB.getDocTypes(self.index_name)
    self.assertTrue(result)
    if '_doc' in result['Value']:
      self.assertEqual(list(result['Value']['_doc']['properties']), [u'Color', u'timestamp', u'Product', u'quantity'])
    else:
      self.assertEqual(list(result['Value']['properties']), [u'Color', u'timestamp', u'Product', u'quantity'])

  def test_exists(self):
    result = self.elasticSearchDB.exists(self.index_name)
    self.assertTrue(result)

  def test_generateFullIndexName(self):
    indexName = 'test'
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    expected = "%s-%s" % (indexName, today)
    result = self.elasticSearchDB.generateFullIndexName(indexName, 'day')
    self.assertEqual(result, expected)

  def test_generateFullIndexName2(self):
    indexName = 'test'
    month = datetime.datetime.today().strftime("%Y-%m")
    expected = "%s-%s" % (indexName, month)
    result = self.elasticSearchDB.generateFullIndexName(indexName, 'month')
    self.assertEqual(result, expected)

  def test_getUniqueValue(self):

    result = self.elasticSearchDB.getUniqueValue(self.index_name, 'quantity')
    self.assertTrue(result['OK'])
    self.assertTrue(result['OK'])

    # this, and the next (Product) are not run because (possibly only for ES 6+):
    # # 'Fielddata is disabled on text fields by default.
    # # Set fielddata=true on [Color] in order to load fielddata in memory by uninverting the inverted index.
    # # Note that this can however use significant memory. Alternatively use a keyword field instead.'

    # result = self.elasticSearchDB.getUniqueValue(self.index_name, 'Color', )
    # self.assertTrue(result['OK'])
    # self.assertEqual(result['Value'], [])
    # result = self.elasticSearchDB.getUniqueValue(self.index_name, 'Product')
    # self.assertTrue(result['OK'])
    # self.assertEqual(result['Value'], [])

  def test_querySimple(self):
    """ simple query test
    """

    self.elasticSearchDB.deleteIndex(self.index_name)
    # inserting 10 entries
    for i in self.moreData:
      result = self.elasticSearchDB.index(self.index_name, i)
      self.assertTrue(result['OK'])
    time.sleep(10)  # giving ES some time for indexing

    # this query returns everything, so we are expecting 10 hits
    body = {
        'query': {
            'match_all': {}
        }
    }
    result = self.elasticSearchDB.query(self.index_name, body)
    self.assertTrue(result['OK'])
    self.assertTrue(isinstance(result['Value'], dict))
    self.assertEqual(len(result['Value']['hits']['hits']), 10)

    # this query returns nothing
    body = {
        'query': {
            'match_none': {}
        }
    }
    result = self.elasticSearchDB.query(self.index_name, body)
    self.assertTrue(result['OK'])
    self.assertTrue(isinstance(result['Value'], dict))
    self.assertEqual(result['Value']['hits']['hits'], [])

    # this is a wrong query
    body = {
        'pippo': {
            'bool': {
                'must': [],
                'filter': []
            }
        }
    }
    result = self.elasticSearchDB.query(self.index_name, body)
    self.assertFalse(result['OK'])

    # this query should also return everything
    body = {
        'query': {
            'bool': {
                'must': [],
                'filter': []
            }
        }
    }
    result = self.elasticSearchDB.query(self.index_name, body)
    self.assertTrue(result['OK'])
    self.assertTrue(isinstance(result['Value'], dict))
    self.assertEqual(len(result['Value']['hits']['hits']), 10)

  # def test_query(self):
  #   body = {"size": 0,
  #           {"query": {"query_string": {"query": "*"}},
  #            "filter": {"bool":
  #                       {"must": [{"range":
  #                                  {"timestamp":
  #                                   {"gte": 1423399451544,
  #                                    "lte": 1423631917911
  #                                   }
  #                                  }
  #                                 }],
  #                        "must_not": []
  #                       }
  #                      }
  #           }
  #                    },
  #           "aggs": {
  #               "3": {
  #                   "date_histogram": {
  #                       "field": "timestamp",
  #                       "interval": "3600000ms",
  #                       "min_doc_count": 1,
  #                       "extended_bounds": {
  #                           "min": 1423399451544,
  #                           "max": 1423631917911
  #                       }
  #                   },
  #                   "aggs": {
  #                       "4": {
  #                           "terms": {
  #                               "field": "Product",
  #                               "size": 0,
  #                               "order": {
  #                                   "1": "desc"
  #                               }
  #                           },
  #                           "aggs": {
  #                               "1": {
  #                                   "sum": {
  #                                       "field": "quantity"
  #                                   }
  #                               }
  #                           }
  #                       }
  #                   }
  #               }
  #           }
  #          }
  #   result = self.elasticSearchDB.query(self.index_name, body)
  #   self.assertEqual(result['aggregations'],
  #                    {u'3': {u'buckets': [{u'4': {u'buckets': [{u'1': {u'value': 5.0},
  #                                                               u'key': u'a',
  #                                                               u'doc_count': 5}],
  #                                                 u'sum_other_doc_count': 0,
  #                                                 u'doc_count_error_upper_bound': 0},
  #                                          u'key': 1423468800000,
  #                                          u'doc_count': 5},
  #                                         {u'4': {u'buckets': [{u'1': {u'value': 8.0},
  #                                                               u'key': u'b',
  #                                                               u'doc_count': 5}],
  #                                                 u'sum_other_doc_count': 0,
  #                                                 u'doc_count_error_upper_bound': 0},
  #                                          u'key': 1423494000000,
  #                                          u'doc_count': 5}]}})

  def test_Search(self):

    self.elasticSearchDB.deleteIndex(self.index_name)
    # inserting 10 entries
    for i in self.moreData:
      result = self.elasticSearchDB.index(self.index_name, i)
      self.assertTrue(result['OK'])
    time.sleep(10)  # giving ES some time for indexing

    s = self.elasticSearchDB._Search(self.index_name)
    result = s.execute()
    self.assertEqual(len(result.hits), 10)
    self.assertEqual(dir(result.hits[0]), [u'Color', u'Product', 'meta', u'quantity', u'timestamp'])

    q = self.elasticSearchDB._Q('range', timestamp={'lte': 1423501337292, 'gte': 1423497057518})
    s = self.elasticSearchDB._Search(self.index_name)
    s = s.filter('bool', must=q)
    query = s.to_dict()
    self.assertEqual(query, {'query': {'bool': {'filter': [
                     {'bool': {'must': [{'range': {'timestamp': {'gte': 1423497057518, 'lte': 1423501337292}}}]}}]}}})
    result = s.execute()
    self.assertEqual(len(result.hits), 0)

    q = self.elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})
    s = self.elasticSearchDB._Search(self.index_name)
    s = s.filter('bool', must=q)
    query = s.to_dict()
    self.assertEqual(query, {'query': {'bool': {'filter': [
                     {'bool': {'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}})
    result = s.execute()
    self.assertEqual(len(result.hits), 0)

    # q = [
    #     self.elasticSearchDB._Q(
    #         'range',
    #         timestamp={
    #             'lte': 1423631917911,
    #             'gte': 1423399451544}),
    #     self.elasticSearchDB._Q(
    #         'match',
    #         Product='a')]
    # s = self.elasticSearchDB._Search(self.index_name)
    # s = s.filter('bool', must=q)
    # query = s.to_dict()
    # self.assertEqual(query, {'query': {'bool': {'filter': [{'bool': {
    #                  'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}},
    #                  {'match': {'Product': 'a'}}]}}]}}})
    # result = s.execute()
    # self.assertEqual(len(result.hits), 5)
    # self.assertEqual(result.hits[0].Product, 'a')
    # self.assertEqual(result.hits[4].Product, 'a')

  # def test_A1(self):
  #   q = [self.elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})]
  #   s = self.elasticSearchDB._Search(self.index_name)
  #   s = s.filter('bool', must=q)
  #   a1 = self.elasticSearchDB._A('terms', field='Product', size=0)
  #   s.aggs.bucket('2', a1)
  #   query = s.to_dict()
  #   self.assertEqual(query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {
  #                    'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}},
  #                    'aggs': {'2': {'terms': {'field': 'Product', 'size': 0}}}})
  #   result = s.execute()
  #   self.assertEqual(result.aggregations['2'].buckets, [
  #                    {u'key': u'a', u'doc_count': 5}, {u'key': u'b', u'doc_count': 5}])

  # def test_A2(self):
  #   q = [self.elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})]
  #   s = self.elasticSearchDB._Search(self.index_name)
  #   s = s.filter('bool', must=q)
  #   a1 = self.elasticSearchDB._A('terms', field='Product', size=0)
  #   a1.metric('total_quantity', 'sum', field='quantity')
  #   s.aggs.bucket('2', a1)
  #   query = s.to_dict()
  #   self.assertEqual(
  #       query, {
  #           'query': {
  #               'bool': {
  #                   'filter': [
  #                       {
  #                           'bool': {
  #                               'must': [
  #                                   {
  #                                       'range': {
  #                                           'timestamp': {
  #                                               'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {
  #               '2': {
  #                   'terms': {
  #                       'field': 'Product', 'size': 0}, 'aggs': {
  #                       'total_quantity': {
  #                           'sum': {
  #                               'field': 'quantity'}}}}}})
  #   result = s.execute()
  #   self.assertEqual(result.aggregations['2'].buckets,
  #   [{u'total_quantity': {u'value': 5.0}, u'key': u'a', u'doc_count': 5}, {
  #                    u'total_quantity': {u'value': 8.0}, u'key': u'b', u'doc_count': 5}])

  # def test_piplineaggregation(self):
  #   q = [self.elasticSearchDB._Q('range', timestamp={'lte': 1423631917911, 'gte': 1423399451544})]
  #   s = self.elasticSearchDB._Search(self.index_name)
  #   s = s.filter('bool', must=q)
  #   a1 = self.elasticSearchDB._A('terms', field='Product', size=0)
  #   a2 = self.elasticSearchDB._A('terms', field='timestamp')
  #   a2.metric('total_quantity', 'sum', field='quantity')
  #   a1.bucket(
  #       'end_data',
  #       'date_histogram',
  #       field='timestamp',
  #       interval='3600000ms').metric(
  #       'tt',
  #       a2).pipeline(
  #       'avg_buckets',
  #       'avg_bucket',
  #       buckets_path='tt>total_quantity',
  #       gap_policy='insert_zeros')
  #   s.aggs.bucket('2', a1)
  #   query = s.to_dict()
  #   self.assertEqual(
  #       query, {
  #           'query': {
  #               'bool': {
  #                   'filter': [
  #                       {
  #                           'bool': {
  #                               'must': [
  #                                   {
  #                                       'range': {
  #                                           'timestamp': {
  #                                               'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {
  #               '2': {
  #                   'terms': {
  #                       'field': 'Product', 'size': 0}, 'aggs': {
  #                       'end_data': {
  #                           'date_histogram': {
  #                               'field': 'timestamp', 'interval': '3600000ms'}, 'aggs': {
  #                               'tt': {
  #                                   'terms': {
  #                                       'field': 'timestamp'}, 'aggs': {
  #                                       'total_quantity': {
  #                                           'sum': {
  #                                               'field': 'quantity'}}}}, 'avg_buckets': {
  #                                   'avg_bucket': {
  #                                       'buckets_path': 'tt>total_quantity', 'gap_policy': 'insert_zeros'}}}}}}}})
  #   result = s.execute()
  #   self.assertEqual(len(result.aggregations['2'].buckets), 2)
  #   self.assertEqual(result.aggregations['2'].buckets[0].key, u'a')
  #   self.assertEqual(result.aggregations['2'].buckets[1].key, u'b')
  #   self.assertEqual(result.aggregations['2'].buckets[0]['end_data'].buckets[0].avg_buckets, {u'value': 2.5})
  #   self.assertEqual(result.aggregations['2'].buckets[1]['end_data'].buckets[0].avg_buckets, {u'value': 4})


if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase(ElasticTestCase)
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ElasticCreateChain))
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ElasticBulkCreateChain))
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ElasticTestChain))
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ElasticDeleteChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(testSuite)
  sys.exit(not testResult.wasSuccessful())
