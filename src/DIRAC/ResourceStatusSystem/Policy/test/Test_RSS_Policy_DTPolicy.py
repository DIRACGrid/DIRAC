""" Test_RSS_Policy_DTPolicy
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id: $'

from mock import MagicMock
import unittest

from DIRAC import gLogger
import DIRAC.ResourceStatusSystem.Policy.DowntimePolicy as moduleTested


################################################################################

class DTPolicy_TestCase( unittest.TestCase ):

  def setUp( self ):
    """ Setup
    """
    gLogger.setLevel( 'DEBUG' )
    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.DowntimePolicy

    self.DTCommand = MagicMock()

  def tearDown( self ):
    """ TearDown
    """
    del self.testClass
    del self.moduleTested

################################################################################
# Tests

class DTPolicy_Success( DTPolicy_TestCase ):

  def test_instantiate( self ):
    """ tests that we can instantiate one object of the tested class
    """

    policy = self.testClass()
    self.assertEqual( 'DowntimePolicy', policy.__class__.__name__ )

  def test_evaluate( self ):
    """ tests the evaluate method
    """

    policy = self.testClass()

    # command failing
    self.DTCommand.doCommand.return_value = { 'OK' : False, 'Message' : 'Grumpy command' }
    policy.setCommand( self.DTCommand )
    res = policy.evaluate()
    self.assertTrue(res['OK'])
    self.assertEqual( 'Grumpy command', res['Value']['Reason'] )
    self.assertEqual( 'Error', res['Value']['Status'] )

    # command failing /2
    self.DTCommand.doCommand.return_value = { 'OK' : True, 'Value' : {'Severity': 'XYZ',
                                                                      'EndDate' : 'Y',
                                                                      'DowntimeID': '123',
                                                                      'Description': 'blah' } }
    self.assertEqual( 'Error', res['Value']['Status'] )

    res = policy.evaluate()
    self.assertTrue( res[ 'OK' ] )
    # command result empty
    self.DTCommand.doCommand.return_value = {'OK': True, 'Value': None}
    res = policy.evaluate()
    self.assertTrue( res[ 'OK' ] )
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'No DownTime announced', res[ 'Value' ][ 'Reason' ] )

    # command result with a DT
    self.DTCommand.doCommand.return_value = { 'OK' : True, 'Value' : {'Severity':'OUTAGE',
                                                                      'EndDate':'Y',
                                                                      'DowntimeID': '123',
                                                                      'Description': 'blah' }}
    policy.command = self.DTCommand

    res = policy.evaluate()
    self.assertTrue(res['OK'])
    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( '123 blah', res[ 'Value' ][ 'Reason' ] )

    # command mock
    self.DTCommand.doCommand.return_value = { 'OK' : True, 'Value' : {'Severity': 'WARNING',
                                                                      'EndDate': 'Y',
                                                                      'DowntimeID': '123',
                                                                      'Description': 'blah' }}
    policy.command = self.DTCommand

    res = policy.evaluate()
    self.assertTrue(res['OK'])
    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( '123 blah', res[ 'Value' ][ 'Reason' ] )


################################################################################
################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DTPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DTPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
