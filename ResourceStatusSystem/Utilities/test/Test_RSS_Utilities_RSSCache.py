# $HeadURL: $
''' Test_RSS_Utilities_RSSCache

'''

import unittest

__RCSID__ = '$Id: $'

class Dummy():
    
  def dummyFunc( self, *args, **kwargs ):
    pass
    
  def __getattr__( self, name ):
    return dummyFunc 

################################################################################

class RSSCache_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import DIRAC.ResourceStatusSystem.Utilities.RSSCache as moduleTested   
    moduleTested.DictCache = dict
    moduleTested.gLogger   = Dummy()
      
    self.cache = moduleTested.RSSCache
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.cache
    
################################################################################    
    
class RSSCache_Success( RSSCache_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    cache = self.cache( 1, 1 )
    self.assertEqual( 'RSSCache', cache.__class__.__name__ )       
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      