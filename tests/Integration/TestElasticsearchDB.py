
from DIRAC.Core.Utilities.ElasticSearchDB        import ElasticSearchDB
from DIRAC                                       import gLogger
import unittest

elHost = 'elastic1.cern.ch'
elPort = '9200'

class ElasticTestCase( unittest.TestCase ):
  
  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.el = ElasticSearchDB( elHost, elPort )
    self.data = [{"Color": "red", "quantity": 1, "Product": "a", "time": "2015-02-09T09:00:00Z"},
                 {"Color": "red", "quantity": 1, "Product": "b", "time": "2015-02-09T16:15:00Z"},
                 {"Color": "red", "quantity": 1, "Product": "b", "time": "2015-02-09T16:30:00Z"},
                 {"Color": "red", "quantity": 1, "Product": "a", "time":"2015-02-09T09:00:00Z"},
                 {"Color": "red", "quantity": 1, "Product": "a", "time": "2015-02-09T09:15:00Z"},
                 {"Color": "red", "quantity": 2, "Product": "b", "time": "2015-02-09T16:15:00Z"},
                 {"Color": "red", "quantity": 1, "Product": "a", "time":"2015-02-09T09:15:00Z"},
                 {"Color": "red", "quantity": 2, "Product": "b", "time": "2015-02-09T16:15:00Z"},
                 {"Color": "red", "quantity": 1, "Product": "a", "time": "2015-02-09T09:15:00Z"},
                 {"Color": "red", "quantity": 2, "Product": "b", "time": "2015-02-09T16:15:00Z"}]
    self.index_name = ''

  def tearDown( self ):
    pass
    # if self.index_name != '':
    #  self.el.deleteIndex( self.index_name )

class ElasticCreateChain( ElasticTestCase ):
    
  def tearDown( self ):
    self.el.deleteIndex( self.index_name )
    
  def test_index( self ):
    result = self.el.createIndex( 'integrationtest', {} )
    self.assert_( result['OK'] )
    self.index_name = result['Value']
    for i in self.data:
      result = self.el.index( self.index_name, 'test', i )
      self.assert_( result['created'] )
    

class ElasticBulkCreateChain( ElasticTestCase ):
  
  def test_bulkindex( self ):
    result = self.el.createIndex( 'integrationtest', {} )
    self.assert_( result['OK'] )
    self.index_name = result['Value']
    result = self.el.bulk_index( self.index_name, 'test', self.data )
    self.assertEqual( result[0], 10 )

class ElasticDeleteChain( ElasticTestCase ):
  
  def test_deleteIndex( self ):
    result = self.el.generateFullIndexName( 'integrationtest' )    
    self.el.deleteIndex( result )
  
class ElasticTestChain( ElasticTestCase ):
  
  def setUp( self ):
    self.el = ElasticSearchDB( elHost, elPort )
    result = self.el.generateFullIndexName( 'integrationtest' )    
    self.assert_( len( result ) > len( 'integrationtest' ) )
    self.index_name = result     
  
  def test_getIndexes( self ):
    result = self.el.getIndexes()
    self.assert_( len( result ) > 0 )
  
  def test_getDocTypes( self ):
    result = self.el.getDocTypes( self.index_name )
    self.assert_( result )
    self.assertDictEqual( result['Value'], {u'test': {u'properties': {u'Color': {u'type': u'string'}, u'Product': {u'type': u'string'}, u'time': {u'type': u'date', u'format': u'strict_date_optional_time||epoch_millis'}, u'quantity': {u'type': u'long'}}}} )

if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase( ElasticTestCase )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticCreateChain ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticBulkCreateChain ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticTestChain ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ElasticDeleteChain ) )
  unittest.TextTestRunner( verbosity = 2 ).run( testSuite )
