"""
Tests the DynamicMonitoring Service and db.
This program assumes that the service Framework/DynamicMonitoring service is running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import datetime

from DIRAC.FrameworkSystem.Client.DynamicMonitoringClient import DynamicMonitoringClient

class TestDynamicMonitoringClient( unittest.TestCase ):
  """
  TestCase-inheriting class with setUp and tearDown methods
  """

  def setUp( self ):
    """
    Initialize the client on every test
    """
    self.client = DynamicMonitoringClient()

  
class DynamicMonitoringClientChain( TestDynamicMonitoringClient ):
  """
  Contains methods for testing of separate elements
  """

  def test_getLastLog( self ):
    result = self.client.getLastLog( 'localhost', 'Framework/SystemAdministrator' )
    self.assert_( result[ 'OK' ] )

  def test_getLogHistory( self ):
    result = self.client.getLogHistory( 'localhost', 'SystemAdministrator', 2 )
    self.assert_( result[ 'OK' ] )

  def test_getLogsPeriod( self ):
    start = datetime.datetime.now() - datetime.timedelta( days = 1 ).strftime( "%Y/%m/%d %H:%M" )
    end = datetime.datetime.now().strftime( "%Y/%m/%d %H:%M" ) 
    result = self.client.getLogsPeriod( 'localhost', 'SystemAdministrator', start, end )
    self.assert_( result[ 'OK' ] )

if __name__ == '__main__':
  
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DynamicMonitoringClientChain ) 
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
