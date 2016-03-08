
from DIRAC.Core.Utilities.ElasticSearchDB        import ElasticSearchDB
from DIRAC.Core.Base.ElasticDB                   import ElasticDB
from DIRAC                                       import gLogger
import unittest

class ElasticTestCase( unittest.TestCase ):
  
  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.el = ElasticDB('TestDb', 'Test/TestDb')

  def tearDown( self ):
    del self.el