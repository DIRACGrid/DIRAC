# $HeadURL:  $
''' Test_RSS_Policy_DTPolicy
'''

import mock
import unittest

import DIRAC.ResourceStatusSystem.Policy.DTPolicy as moduleTested

__RCSID__ = '$Id: $'

################################################################################

class DTPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
        
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.DTPolicy
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested
        
################################################################################
# Tests

class DTPolicy_Success( DTPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    policy = self.testClass()
    self.assertEqual( 'DTPolicy', policy.__class__.__name__ )  
  
  def test_evaluate( self ):
    ''' tests the evaluate method
    '''
    
    policy = self.testClass()
    res = policy.evaluate()
    self.assertEqual( False, res[ 'OK' ] )
    
    # command mock
    mock_command = mock.Mock()
    mock_command.doCommand.return_value = { 'OK' : False, 'Message' : 'Grumpy command' }
    
    policy.command = mock_command
    
    res = policy.evaluate()
    self.assertEqual( False, res[ 'OK' ] )
    self.assertEqual( 'Grumpy command', res[ 'Message' ] )

    # command mock
    mock_command.doCommand.return_value = { 'OK' : True, 'Value' : None }
    policy.command = mock_command
    
    res = policy.evaluate()
    self.assertEqual( False, res[ 'OK' ] )
   
    # command mock
    mock_command.doCommand.return_value = { 'OK' : True, 'Value' : { 'DT' : None } }
    policy.command = mock_command
    
    res = policy.evaluate()
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'No DownTime announced', res[ 'Value' ][ 'Reason' ] )
    #self.assertEqual( False, 'EndDate' in res[ 'Value' ] )
    
    # command mock
    mock_command.doCommand.return_value = { 'OK' : True, 'Value' : { 'DT'      : 'OUTAGE',
                                                                     'EndDate' : 'Y' } }
    policy.command = mock_command
    
    res = policy.evaluate()
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'DownTime found: OUTAGE', res[ 'Value' ][ 'Reason' ] )
    #self.assertEqual( 'Y', res[ 'Value' ][ 'EndDate' ] )

    # command mock
    mock_command.doCommand.return_value = { 'OK' : True, 'Value' : { 'DT'      : 'WARNING',
                                                                     'EndDate' : 'Y' } }
    policy.command = mock_command
    
    res = policy.evaluate()
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'Bad', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'DownTime found: WARNING', res[ 'Value' ][ 'Reason' ] )
    #self.assertEqual( 'Y', res[ 'Value' ][ 'EndDate' ] )

    # command mock
    mock_command.doCommand.return_value = { 'OK' : True, 'Value' : { 'DT'      : 'XYZ',
                                                                     'EndDate' : 'Y' } }
    policy.command = mock_command
    
    res = policy.evaluate()
    self.assertEqual( False, res[ 'OK' ] )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF