''' Test_RSS_Policy_PilotEfficiencyPolicy
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import DIRAC.ResourceStatusSystem.Policy.PilotEfficiencyPolicy as moduleTested

################################################################################

class PilotEfficiencyPolicy_TestCase( unittest.TestCase ):
 
  def setUp( self ):
    '''
    Setup
    '''
                 
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.PilotEfficiencyPolicy

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass
 

################################################################################

class PilotEfficiencyPolicy_Success( PilotEfficiencyPolicy_TestCase ):
 
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'PilotEfficiencyPolicy', module.__class__.__name__ )

  def test_evaluate( self ):
    ''' tests the method _evaluate
    '''

    module = self.testClass()

    res = module._evaluate( { 'OK' : False, 'Message' : 'Bo!' } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Bo!', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : None } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{}] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
                                                        'Done' : 0, 'Failed' : 0 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Not enough pilots to take a decision', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{'Aborted' : 10, 'Deleted' : 0,
                                                        'Done' : 10, 'Failed' : 0 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Pilots Efficiency of 0.50', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{'Aborted' : 0, 'Deleted' : 0,
                                                        'Done' : 30, 'Failed' : 10 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Pilots Efficiency of 0.75', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{'Aborted' : 0, 'Deleted' : 0,
                                                        'Done' : 19, 'Failed' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Pilots Efficiency of 0.95', res[ 'Value' ][ 'Reason' ] )

################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( PilotEfficiencyPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PilotEfficiencyPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF