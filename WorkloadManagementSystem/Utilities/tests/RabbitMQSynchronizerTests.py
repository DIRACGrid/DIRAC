""" Unit tests for RabbitMQSync
"""

import unittest
from DIRAC.WorkloadManagementSystem.Utilities.RabbitMQSynchronizer import RabbitMQSynchronizer
#from DIRAC.ConfigurationSystem.Client.Helpers.Registry import

class TestRabbitMQSync( unittest.TestCase ):

  def setUp( self ):
    self.synchronizer =RabbitMQSynchronizer()
    pass
  def tearDown( self ):
    pass

class TestA( TestRabbitMQSync ):

  def test_success( self ):
    self.synchronizer.sync( _eventName = None, _params =None)
    pass
  def test_failureBadFile( self ):
    pass
if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestRabbitMQSync  )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestA ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


