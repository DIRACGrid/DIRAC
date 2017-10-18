#''' Test_RSS_Policy_PilotEfficiencyPolicy
#'''
#
#import mock
#import unittest
#
#import LHCbDIRAC.ResourceStatusSystem.Policy.PilotEfficiencyPolicy as moduleTested
#
#################################################################################
#
#class PilotEfficiencyPolicy_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#                  
#    self.moduleTested = moduleTested
#    self.testClass    = self.moduleTested.PilotEfficiencyPolicy
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
#class PilotEfficiencyPolicy_Success( PilotEfficiencyPolicy_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#   
#    module = self.testClass()
#    self.assertEqual( 'PilotEfficiencyPolicy', module.__class__.__name__ )
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
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
#                                                         'Done' : 0, 'Failed' : 0 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No pilots to take a decision', res[ 'Value' ][ 'Reason' ] )    
#    
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 1, 'Deleted' : 0,
#                                                         'Done' : 1, 'Failed' : 0 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency of 0.50', res[ 'Value' ][ 'Reason' ] )    
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
#                                                         'Done' : 3, 'Failed' : 1 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency of 0.75', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
#                                                         'Done' : 19, 'Failed' : 1 }] } )
#    self.assertEqual( True, res[ 'OK' ] )
#    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency of 0.95', res[ 'Value' ][ 'Reason' ] )
#
#################################################################################    
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF