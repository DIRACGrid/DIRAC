''' Test_RSS_Policy_SpaceTokenOccupancyPolicy
'''

import unittest

import DIRAC.ResourceStatusSystem.Policy.SpaceTokenOccupancyPolicy as moduleTested

################################################################################

class SpaceTokenOccupancyPolicy_TestCase( unittest.TestCase ):
 
  def setUp( self ):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.SpaceTokenOccupancyPolicy

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass

################################################################################

class SpaceTokenOccupancyPolicy_Success( SpaceTokenOccupancyPolicy_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'SpaceTokenOccupancyPolicy', module.__class__.__name__ )

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

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'A' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Key total missing', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Total' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Key free missing', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{'Total' : 100, 'Free' : 0.0 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Key guaranteed missing', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{'Total' : 100, 'Free' : 0.0,
                                                       'Guaranteed' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Free space < 100GB', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{'Total' : 100, 'Free' : 4.0,
                                                       'Guaranteed' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Free space < 5TB',
                      res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{'Total' : 100, 'Free' : 100,
                                                       'Guaranteed' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEqual( 'Free space > 5TB',
                      res[ 'Value' ][ 'Reason' ] )


################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( SpaceTokenOccupancyPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SpaceTokenOccupancyPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF