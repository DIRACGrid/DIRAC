# $HeadURL:  $
''' Test_RSS_Policy_Configurations
'''

import unittest

import DIRAC.ResourceStatusSystem.Policy.Configurations as moduleTested

__RCSID__ = '$Id:  $'

################################################################################

class Configurations_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
        
    self.moduleTested = moduleTested
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.moduleTested
    
################################################################################
# Tests

class Configurations_Success( Configurations_TestCase ):
  
  def test_policiesMeta( self ):
    ''' tests that the configuration does not have any funny key
    '''
    
    self.assertEqual( True, hasattr( self.moduleTested, 'POLICIESMETA' ) )
    
    policiesMeta = self.moduleTested.POLICIESMETA
    
    for _policyName, policyMeta in policiesMeta.items():
      
      self.assertEqual( [ 'args', 'command', 'description', 'module' ], policyMeta.keys() )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF