# imports
import unittest
# sut
from DIRAC.Core.DISET.RPCClient import RPCClient

class TestHelloHandler( unittest.TestCase ):

  def setUp( self ):
    self.helloService = RPCClient('Framework/Hello')

  def tearDown( self ):
    pass

class TestHelloHandlerSuccess( TestHelloHandler ):

  def test_success( self ):
    res = self.helloService.sayHello("Tester")
    return res['OK']


class TestHelloHandlerFailure( TestHelloHandler ):

  def test_failure( self ):
    pass


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestHelloHandler )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestHelloHandlerSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestHelloHandlerFailure ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
