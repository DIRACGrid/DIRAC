""" Unit tests for PathFinder only for functions that I added
"""

import unittest
from DIRAC.ConfigurationSystem.Client.PathFinder import getEntitySection
from DIRAC.ConfigurationSystem.Client.PathFinder import getConsumerSection

class TestPathFinder( unittest.TestCase ):
  def setUp( self ):
    pass
  def tearDown( self ):
    pass

class TestGetEntitySection( TestPathFinder ):

  def test_success( self ):
    result = getEntitySection("WorkloadManagement/SimpleLogConsumer",False, False,"Consumers")
    self.assertEqual(result, "/Systems/WorkloadManagement/DevInstance/Consumers/SimpleLogConsumer")

  def test_success_2( self ):
    result = getEntitySection("WorkloadManagement/SandboxStoreHandler",False, False,"Services")
    self.assertEqual(result, "/Systems/WorkloadManagement/DevInstance/Services/SandboxStoreHandler")

  def test_successConsumerDoesNotExist( self ):
    """ tricky case one could expect that if the consumer module does exist
        than some kind of error will be returned, but it is not the case
    """
    result = getEntitySection("WorkloadManagement/NoExistantConsumer",False, False,"Consumers")
    self.assertEqual(result,"/Systems/WorkloadManagement/DevInstance/Consumers/NoExistantConsumer")

  def test_sucessEntityStringDoesNotExist( self ):
    """ tricky case one could expect that if entity string is wrong
        than some kind of error will be returned, but it is not the case
    """
    result = getEntitySection("WorkloadManagement/SimpleLogConsumer",False, False,"NonRonsumersNon")
    self.assertEqual(result,"/Systems/WorkloadManagement/DevInstance/NonRonsumersNon/SimpleLogConsumer")

  def test_failureSystemDoesNotExist( self ):
    self.assertRaises(RuntimeError,lambda:getEntitySection("NullSystem/SimpleLogConsumer",False, False,"Consumers"))


class TestGetConsumerSection( TestPathFinder ):

  def test_success( self ):
    result = getConsumerSection("WorkloadManagement/SimpleLogConsumer",False, False)
    self.assertEqual(result, "/Systems/WorkloadManagement/DevInstance/Consumers/SimpleLogConsumer")

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPathFinder )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestGetEntitySection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestGetConsumerSection ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
