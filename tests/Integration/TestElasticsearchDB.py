
from DIRAC.Core.Utilities.ElasticSearchDB        import ElasticSearchDB
from DIRAC                                       import gLogger
import unittest

class ElasticTestCase( unittest.TestCase ):
  
  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.el = ElasticSearchDB('elastic1.cern.ch', '9200')
    #self.el = ElasticSearchDB('localhost', '9200')
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
    self.es.indices.delete(self.index_name)
    del self.el
  
  def test_addRecords(self):
    result = self.el.createIndex('integrationtest', {})
    print 'rrrr', result
    self.assert_( result['OK'] )
    self.index_name = result['Value']
    
if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase( ElasticTestCase )
  unittest.TextTestRunner( verbosity = 2 ).run( testSuite )
