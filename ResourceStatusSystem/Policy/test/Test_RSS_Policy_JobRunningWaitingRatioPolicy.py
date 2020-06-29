''' Test_RSS_Policy_JobRunningWaitingRatioPolicy
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import DIRAC.ResourceStatusSystem.Policy.JobRunningWaitingRatioPolicy as moduleTested

################################################################################

class JobRunningWaitingRatioPolicy_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.JobRunningWaitingRatioPolicy

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass


################################################################################

class JobRunningWaitingRatioPolicy_Success( JobRunningWaitingRatioPolicy_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'JobRunningWaitingRatioPolicy', module.__class__.__name__ )

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

    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 0, 'Waiting' : 0, 'Staging' : 0 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Not enough jobs to take a decision', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 10, 'Waiting' : 10, 'Staging' : 10 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Job Running / Waiting ratio of 0.33', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 10, 'Waiting' : 10, 'Staging' : 0 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Job Running / Waiting ratio of 0.50', res[ 'Value' ][ 'Reason' ] )

    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 20, 'Waiting' : 10, 'Staging' : 0 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Job Running / Waiting ratio of 0.67', res[ 'Value' ][ 'Reason' ] )

################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobRunningWaitingRatioPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobRunningWaitingRatioPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
