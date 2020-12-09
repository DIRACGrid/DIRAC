""" Test_RSS_Policy_AlwaysActivePolicy
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id: $'

import unittest

import DIRAC.ResourceStatusSystem.Policy.AlwaysActivePolicy as moduleTested


################################################################################

class AlwaysActivePolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    """ Setup
    """

    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.AlwaysActivePolicy

  def tearDown( self ):
    """ TearDown
    """
    del self.testClass
    del self.moduleTested

################################################################################
# Tests

class AlwaysActivePolicy_Success( AlwaysActivePolicy_TestCase ):

  def test_instantiate( self ):
    """ tests that we can instantiate one object of the tested class
    """

    policy = self.testClass()
    self.assertEqual( 'AlwaysActivePolicy', policy.__class__.__name__ )  
  
  def test_evaluate( self ):
    """ tests the evaluate method
    """

    policy = self.testClass()
    res = policy.evaluate()
    
    self.assertTrue(res['OK'])
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )

################################################################################
################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AlwaysActivePolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( AlwaysActivePolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
