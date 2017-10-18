#''' Test_RSS_Policy_JobRunningMatchedRatioPolicy
#'''
#
#import mock
#import unittest
#
#import LHCbDIRAC.ResourceStatusSystem.Policy.JobRunningMatchedRatioPolicy as moduleTested
#
#################################################################################
#
#class JobRunningMatchedRatioPolicy_TestCase( unittest.TestCase ):
#
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    self.moduleTested = moduleTested
#    self.testClass    = self.moduleTested.JobRunningMatchedRatioPolicy
#
#  def tearDown( self ):
#    '''
#    Tear down
#    '''
#
#    del self.moduleTested
#    del self.testClass
#
#
#################################################################################
#
#class JobRunningMatchedRatioPolicy_Success( JobRunningMatchedRatioPolicy_TestCase ):
#
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''
#
#    module = self.testClass()
#    self.assertEqual( 'JobRunningMatchedRatioPolicy', module.__class__.__name__ )
#
#  def test_evaluate( self ):
#    ''' tests the method _evaluate
#    '''
#
#    module = self.testClass()
#
#    res = module._evaluate( { 'OK' : False, 'Message' : 'Bo!' } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Bo!', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : None } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{}] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 0, 'Matched' : 0,
#                                                         'Received': 0, 'Checking' : 0 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No jobs take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 1, 'Matched' : 1,
#                                                         'Received': 0, 'Checking' : 0 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Job Running / Matched ratio of 0.50', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 7, 'Matched' : 1,
#                                                         'Received': 1, 'Checking' : 1 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Job Running / Matched ratio of 0.70', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 7, 'Matched' : 0,
#                                                         'Received': 0, 'Checking' : 0 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Job Running / Matched ratio of 1.00', res[ 'Value' ][ 'Reason' ] )
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
