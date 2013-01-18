# $HeadURL:  $
''' Test_RSS_Policy_AlwaysActivePolicy
'''

import unittest

import DIRAC.ResourceStatusSystem.Policy.AlwaysActivePolicy as moduleTested

__RCSID__ = '$Id: $'

################################################################################

class AlwaysActivePolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
        
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.AlwaysActivePolicy
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested
        
################################################################################
# Tests

class AlwaysActivePolicy_Success( AlwaysActivePolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    policy = self.testClass()
    self.assertEqual( 'AlwaysActivePolicy', policy.__class__.__name__ )  
  
  def test_evaluate( self ):
    ''' tests the evaluate method
    '''
    
    policy = self.testClass()
    res = policy.evaluate()
    
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF