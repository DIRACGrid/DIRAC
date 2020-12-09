''' Test_RSS_Policy_AlwaysActivePolicy
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id: $'

import unittest

import DIRAC.ResourceStatusSystem.Policy.CEAvailabilityPolicy as moduleTested


################################################################################

class CEAvailabilityPolicy_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.CEAvailabilityPolicy
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested

################################################################################
# Tests

class CEAvailabilityPolicy_Success( CEAvailabilityPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  

    policy = self.testClass()
    self.assertEqual( 'CEAvailabilityPolicy', policy.__class__.__name__ )  
  
  def test_evaluate( self ):
    ''' tests the evaluate method
    '''

    policy = self.testClass()
    commandResult = {'OK': True,
                     'Value': {'Reason': "All queues in 'Production'",
                               'Status': 'Production',
                               'cccreamceli05.in2p3.fr:8443/cream-sge-long': 'Production',
                               'cccreamceli05.in2p3.fr:8443/cream-sge-verylong': 'Production'
                              }
                    }
    res = policy._evaluate(commandResult)
    
    self.assertTrue(res['OK'])
    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
    
    commandResult = {'OK': True,
                    'Value': {
                      'Reason': "All queues in 'Production'",
                      'Status': 'Degraded',
                      'cccreamceli05.in2p3.fr:8443/cream-sge-long': 'Production',
                      'cccreamceli05.in2p3.fr:8443/cream-sge-verylong': 'Production'
                      }
                    }
    res = policy._evaluate(commandResult)

    self.assertTrue(res['OK'])
    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )

################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( CEAvailabilityPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( CEAvailabilityPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF