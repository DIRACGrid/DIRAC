"""Unit tests for ConsumerTools
"""
import unittest
from DIRAC.WorkloadManagementSystem.Consumer.ConsumerTools import getOption
from DIRAC.WorkloadManagementSystem.Consumer.ConsumerTools import getConsumerOption
from DIRAC.ConfigurationSystem.Client import PathFinder

#from DIRAC import gConfig

class TestConsumerTools( unittest.TestCase ):

  def setUp( self ):
    self.consumerSection = PathFinder.getConsumerSection( 'WorkloadManagement/AnotherTestConsumer' )
  def tearDown( self ):
    pass

class TestConsumerToolsGetOption( TestConsumerTools ):
  def test_successHost( self ):
    res = getConsumerOption( optionName = 'Host', consumerSection = self.consumerSection )
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], '127.0.0.1')

  def test_successPort( self ):
    res = getConsumerOption( optionName = 'Port', consumerSection = self.consumerSection )
    self.assertTrue(res['OK'])
    self.assertEqual(int(res['Value']), 61613)

  def test_successUser( self ):
    res = getConsumerOption( optionName = 'User', consumerSection = self.consumerSection )
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'ala')
  def test_successVirtualHost( self ):
    res = getConsumerOption( optionName = 'VH', consumerSection = self.consumerSection )
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], '/')
  def test_successExchangeName( self ):
    res = getConsumerOption( optionName = 'ExchangeName', consumerSection = self.consumerSection )
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'test')
  def test_successType( self ):
    res = getConsumerOption( optionName = 'Type', consumerSection = self.consumerSection )
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'aa')
  def test_failure( self ):
    pass

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestConsumerTools )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestConsumerToolsGetOption ))
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
