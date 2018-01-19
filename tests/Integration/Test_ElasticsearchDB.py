"""
This is used to test the ElasticSearchDB module. It is used to discover all possible changes of Elasticsearch api.
If you modify the test data, you have to update the test cases...
"""

import unittest
import datetime
import time

from DIRAC                                import gLogger
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB
from DIRAC.Core.Utilities.ElasticSearchDB import generateFullIndexName

elHost = 'localhost'
elPort = 9200

class ElasticTestCase( unittest.TestCase ):

  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.el = ElasticSearchDB( host = elHost, 
                               port = elPort, 
                               useSSL = False )
    
    self.data = [{"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:00:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "b", "timestamp": "2015-02-09 16:30:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp":"2015-02-09 09:00:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
                 {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp":"2015-02-09 09:15:00.0"},
                 {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"},
                 {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09 09:15:00.0"},
                 {"Color": "red", "quantity": 2, "Product": "b", "timestamp": "2015-02-09 16:15:00.0"}]
    self.index_name = ''

  def tearDown( self ):
    pass

class ElasticBulkCreateChain( ElasticTestCase ):

  def test_bulkindex( self ):
    result = self.el.bulk_index( 'integrationtest', 'test', self.data )
    self.assertTrue(result['OK'])
    self.assertEqual( result['Value'], 10 )
    time.sleep( 10 )
  
  def test_bulkindexMonthly(self):
        
    result = self.el.bulk_index( indexprefix = 'integrationtestmontly',
                                 doc_type = 'test',
                                 data = self.data,
                                 period = 'month' )
    self.assertTrue(result['OK'])
    self.assertEqual( result['Value'], 10 )
    time.sleep( 10 )

class ElasticCreateChain( ElasticTestCase ):


  def tearDown( self ):
    self.el.deleteIndex( self.index_name )

  def test_wrongdataindex( self ):
    result = self.el.createIndex( 'dsh63tsdgad', {} )
    self.assertTrue(result['OK'])
    index_name = result['Value']
    result = self.el.index( index_name, 'test', {"Color": "red", "quantity": 1, "Product": "a", "timestamp": 1458226213})
    self.assertTrue(result['OK'])
    result = self.el.index( index_name, 'test', {"Color": "red", "quantity": 1, "Product": "a", "timestamp": "2015-02-09T16:15:00Z"})
    self.assertTrue( result['Message'] )
    result = self.el.deleteIndex( index_name )
    self.assertTrue(result['OK'])


  def test_index( self ):
    result = self.el.createIndex( 'integrationtest', {} )
    self.assertTrue(result['OK'])
    self.index_name = result['Value']
    for i in self.data:
      result = self.el.index( self.index_name, 'test', i )
      self.assertTrue(result['OK'])



class ElasticDeleteChain( ElasticTestCase ):

  def test_deleteNonExistingIndex(self):
    result = self.el.deleteIndex( 'dsdssuu' )
    self.assertTrue( result['Message'] )

  def test_deleteIndex( self ):
    result = generateFullIndexName( 'integrationtest' )
    res = self.el.deleteIndex( result )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], result )

  def test_deleteMonthlyIndex( self ):
    result = generateFullIndexName( 'integrationtestmontly', 'month' )
    res = self.el.deleteIndex( result )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], result )

class ElasticTestChain( ElasticTestCase ):

  def setUp( self ):
    self.el = ElasticSearchDB( host = elHost, 
                               port = elPort, 
                               useSSL = False )
    result = generateFullIndexName( 'integrationtest' )
    self.assertTrue( len( result ) > len( 'integrationtest' ) )
    self.index_name = result

  def test_getIndexes( self ):
    result = self.el.getIndexes()
    self.assertTrue( len( result ) > 0 )


  def test_getDocTypes( self ):
    result = self.el.getDocTypes( self.index_name )
    self.assertTrue( result )
    self.assertDictEqual( result['Value'], {u'test': {u'properties': {u'Color': {u'type': u'string'}, u'timestamp': {u'type': u'long'}, u'Product': {u'type': u'string'}, u'quantity': {u'type': u'long'}}}} )

  def test_exists( self ):
    result = self.el.exists( self.index_name )
    self.assertTrue( result )

  def test_generateFullIndexName( self ):
    indexName = 'test'
    today = datetime.datetime.today().strftime( "%Y-%m-%d" )
    expected = "%s-%s" % ( indexName, today )
    result = generateFullIndexName( indexName )
    self.assertEqual( result, expected )
  
  def test_generateFullIndexName2( self ):
    indexName = 'test'
    month = datetime.datetime.today().strftime( "%Y-%m" )
    expected = "%s-%s" % ( indexName, month )
    result = generateFullIndexName( indexName, 'month' )
    self.assertEqual( result, expected )
  
  def test_getUniqueValue( self ):
    result = self.el.getUniqueValue( self.index_name, 'Color', )
    self.assertTrue( result )
    self.assertEqual( result['Value'], [] )
    result = self.el.getUniqueValue( self.index_name, 'Product' )
    self.assertTrue( result )
    self.assertEqual( result['Value'], [] )
    result = self.el.getUniqueValue( self.index_name, 'quantity' )
    self.assertTrue( result )
    self.assertEqual( result['Value'], [] )

  def test_query( self ):
    body = { "size": 0,
             "query": { "filtered": { "query": { "query_string": { "query": "*" } },
                                      "filter": { "bool": { "must": [{ "range": {
                            "timestamp": {
                                "gte": 1423399451544,
                                "lte": 1423631917911
                            }
                        }
                    }],
                    "must_not": []
                }
            }
        }
    },
    "aggs": {
        "3": {
            "date_histogram": {
                "field": "timestamp",
                "interval": "3600000ms",
                "min_doc_count": 1,
                "extended_bounds": {
                    "min": 1423399451544,
                    "max": 1423631917911
                }
            },
            "aggs": {
                "4": {
                    "terms": {
                        "field": "Product",
                        "size": 0,
                        "order": {
                            "1": "desc"
                        }
                    },
                    "aggs": {
                        "1": {
                            "sum": {
                                "field": "quantity"
                            }
                        }
                    }
                }
            }
        }
      }
    }
    result = self.el.query( self.index_name, body )
    self.assertEqual( result['aggregations'], {u'3': {u'buckets': [{u'4': {u'buckets': [{u'1': {u'value': 5.0}, u'key': u'a', u'doc_count': 5}], u'sum_other_doc_count': 0, u'doc_count_error_upper_bound': 0}, u'key': 1423468800000, u'doc_count': 5}, {u'4': {u'buckets': [{u'1': {u'value': 8.0}, u'key': u'b', u'doc_count': 5}], u'sum_other_doc_count': 0, u'doc_count_error_upper_bound': 0}, u'key': 1423494000000, u'doc_count': 5}]}} )
  
  def test_queryMontly( self ):
    body = { "size": 0,
             "query": { "filtered": { "query": { "query_string": { "query": "*" } },
                                      "filter": { "bool": { "must": [{ "range": {
                            "timestamp": {
                                "gte": 1423399451544,
                                "lte": 1423631917911
                            }
                        }
                    }],
                    "must_not": []
                }
            }
        }
    },
    "aggs": {
        "3": {
            "date_histogram": {
                "field": "timestamp",
                "interval": "3600000ms",
                "min_doc_count": 1,
                "extended_bounds": {
                    "min": 1423399451544,
                    "max": 1423631917911
                }
            },
            "aggs": {
                "4": {
                    "terms": {
                        "field": "Product",
                        "size": 0,
                        "order": {
                            "1": "desc"
                        }
                    },
                    "aggs": {
                        "1": {
                            "sum": {
                                "field": "quantity"
                            }
                        }
                    }
                }
            }
        }
      }
    }
    result = self.el.query( 'integrationtestmontly*', body )
    self.assertEqual( result['aggregations'], {u'3': {u'buckets': [{u'4': {u'buckets': [{u'1': {u'value': 5.0}, u'key': u'a', u'doc_count': 5}], u'sum_other_doc_count': 0, u'doc_count_error_upper_bound': 0}, u'key': 1423468800000, u'doc_count': 5}, {u'4': {u'buckets': [{u'1': {u'value': 8.0}, u'key': u'b', u'doc_count': 5}], u'sum_other_doc_count': 0, u'doc_count_error_upper_bound': 0}, u'key': 1423494000000, u'doc_count': 5}]}} )

  def test_Search( self ):
    s = self.el._Search( self.index_name )
    result = s.execute()
    self.assertEqual( len( result.hits ), 10 )
    self.assertEqual( dir( result.hits[0] ), [u'Color', u'Product', 'meta', u'quantity', u'timestamp'] )

  def test_Q1( self ):
    q = self.el._Q( 'range', timestamp = {'lte':1423501337292, 'gte': 1423497057518} )
    s = self.el._Search( self.index_name )
    s = s.filter( 'bool', must = q )
    query = s.to_dict()
    self.assertEqual( query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1423497057518, 'lte': 1423501337292}}}]}}]}}} )
    result = s.execute()
    self.assertEqual( len( result.hits ), 0 )
    
    q = self.el._Q( 'range', timestamp = {'lte':1423631917911, 'gte': 1423399451544} )
    s = self.el._Search( self.index_name )
    s = s.filter( 'bool', must = q )
    query = s.to_dict()
    self.assertEqual( query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}} )
    result = s.execute()
    self.assertEqual( len( result.hits ), 10 )
    self.assertEqual( dir( result.hits[0] ), [u'Color', u'Product', 'meta', u'quantity', u'timestamp'] )

  def test_Q2( self ):
    q = [self.el._Q( 'range', timestamp = {'lte':1423631917911, 'gte': 1423399451544} ), self.el._Q( 'match', Product = 'a' )]
    s = self.el._Search( self.index_name )
    s = s.filter( 'bool', must = q )
    query = s.to_dict()
    self.assertEqual( query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}}, {'match': {'Product': 'a'}}]}}]}}} )
    result = s.execute()
    self.assertEqual( len( result.hits ), 5 )
    self.assertEqual( result.hits[0].Product, 'a' )
    self.assertEqual( result.hits[4].Product, 'a' )

  def test_A1( self ):
    q = [self.el._Q( 'range', timestamp = {'lte':1423631917911, 'gte': 1423399451544} )]
    s = self.el._Search( self.index_name )
    s = s.filter( 'bool', must = q )
    a1 = self.el._A( 'terms', field = 'Product', size = 0 )
    s.aggs.bucket( '2', a1 )
    query = s.to_dict()
    self.assertEqual( query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {'2': {'terms': {'field': 'Product', 'size': 0}}}} )
    result = s.execute()
    self.assertEqual( result.aggregations['2'].buckets, [{u'key': u'a', u'doc_count': 5}, {u'key': u'b', u'doc_count': 5}] )

  def test_A2( self ):
    q = [self.el._Q( 'range', timestamp = {'lte':1423631917911, 'gte': 1423399451544} )]
    s = self.el._Search( self.index_name )
    s = s.filter( 'bool', must = q )
    a1 = self.el._A( 'terms', field = 'Product', size = 0 )
    a1.metric( 'total_quantity', 'sum', field = 'quantity' )
    s.aggs.bucket( '2', a1 )
    query = s.to_dict()
    self.assertEqual( query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {'2': {'terms': {'field': 'Product', 'size': 0}, 'aggs': {'total_quantity': {'sum': {'field': 'quantity'}}}}}} )
    result = s.execute()
    self.assertEqual( result.aggregations['2'].buckets, [{u'total_quantity': {u'value': 5.0}, u'key': u'a', u'doc_count': 5}, {u'total_quantity': {u'value': 8.0}, u'key': u'b', u'doc_count': 5}] )

  def test_piplineaggregation( self ):
    q = [self.el._Q( 'range', timestamp = {'lte':1423631917911, 'gte': 1423399451544} )]
    s = self.el._Search( self.index_name )
    s = s.filter( 'bool', must = q )
    a1 = self.el._A( 'terms', field = 'Product', size = 0 )
    a2 = self.el._A( 'terms', field = 'timestamp' )
    a2.metric( 'total_quantity', 'sum', field = 'quantity' )
    a1.bucket( 'end_data', 'date_histogram', field = 'timestamp', interval = '3600000ms' ).metric( 'tt', a2 ).pipeline( 'avg_buckets', 'avg_bucket', buckets_path = 'tt>total_quantity', gap_policy = 'insert_zeros' )
    s.aggs.bucket( '2', a1 )
    query = s.to_dict()
    self.assertEqual( query, {'query': {'bool': {'filter': [{'bool': {'must': [{'range': {'timestamp': {'gte': 1423399451544, 'lte': 1423631917911}}}]}}]}}, 'aggs': {'2': {'terms': {'field': 'Product', 'size': 0}, 'aggs': {'end_data': {'date_histogram': {'field': 'timestamp', 'interval': '3600000ms'}, 'aggs': {'tt': {'terms': {'field': 'timestamp'}, 'aggs': {'total_quantity': {'sum': {'field': 'quantity'}}}}, 'avg_buckets': {'avg_bucket': {'buckets_path': 'tt>total_quantity', 'gap_policy': 'insert_zeros'}}}}}}}} )
    result = s.execute()
    self.assertEqual( len( result.aggregations['2'].buckets ), 2 )
    self.assertEqual( result.aggregations['2'].buckets[0].key, u'a' )
    self.assertEqual( result.aggregations['2'].buckets[1].key, u'b' )
    self.assertEqual( result.aggregations['2'].buckets[0]['end_data'].buckets[0].avg_buckets, {u'value': 2.5} )
    self.assertEqual( result.aggregations['2'].buckets[1]['end_data'].buckets[0].avg_buckets, {u'value': 4} )

if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase( ElasticTestCase )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticCreateChain ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticBulkCreateChain ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticTestChain ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticDeleteChain ) )
  unittest.TextTestRunner( verbosity = 2 ).run( testSuite )
