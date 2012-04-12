# $HeadURL: $
''' Test_RSS_Utilities_RSSCache

'''

import thread
import time
import unittest

__RCSID__ = '$Id: $'

class Dummy( object ):
    
  def dummyFunc( self, *args, **kwargs ):
    pass
    
  def __getattr__( self, name ):
    return dummyFunc 

class DummyCache( object ):

  def __init__( self ):
    self.cache = dict()
    
  def getKeys( self ):
    return self.cache.keys()
  
  def get( self, key ):
    return self.cache.get( key )  

  def add( self, key, lifeTime, value = None ):
    self.cache[ key ] = value
    
  def purgeAll( self ):
    self.cache = {}  
    
################################################################################

class RSSCache_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import DIRAC.ResourceStatusSystem.Utilities.RSSCache as moduleTested   
    moduleTested.DictCache = DummyCache
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
    cache = self.cache( 1 )
    self.assertEqual( 'RSSCache', cache.__class__.__name__ )    
    cache = self.cache( 1, updateFunc = 1 )
    self.assertEqual( 'RSSCache', cache.__class__.__name__ )
    
  def test_stopRefreshThread( self ):
    ''' test that we can stop the refreshing thread
    '''     
    cache = self.cache( 1 )
    cache.stopRefreshThread()
    self.assertEqual( cache._RSSCache__refreshStop, True )

  def test__isCacheAlive( self ):
    ''' test that we can get CacheStatus when it does not run
    '''  
    cache = self.cache( 1 )
    res   = cache.isCacheAlive()
    self.assertEqual( res, False )

  def test_setLifeTime( self ):
    ''' test that we update lifeTime
    '''     
    cache = self.cache( 1 )
    cache.setLifeTime( 2 )
    self.assertEqual( cache._RSSCache__lifeTime, 2 )    
  
  def test_startStopRefreshThread( self ):
    ''' test that we can start and stop the refreshing thread
    '''     
    cache = self.cache( 1 )
    cache.startRefreshThread()
    self.assertEqual( cache.isCacheAlive(), True )
    cache.stopRefreshThread()
    time.sleep( 2 )
    self.assertEqual( cache.isCacheAlive(), False )
    self.assertEqual( cache._RSSCache__refreshStop, False )    

  def test_reStartRefreshThread( self ):
    ''' test that we can restart the refreshing thread
    '''     
    cache = self.cache( 1 )
    cache.startRefreshThread()
    self.assertEqual( cache.isCacheAlive(), True )
    cache.stopRefreshThread()
    time.sleep( 2 )
    self.assertEqual( cache.isCacheAlive(), False )
    self.assertEqual( cache._RSSCache__refreshStop, False )
    cache.startRefreshThread()
    self.assertEqual( cache.isCacheAlive(), True )
    cache.stopRefreshThread()
    time.sleep( 2 )
    self.assertEqual( cache.isCacheAlive(), False )
    self.assertEqual( cache._RSSCache__refreshStop, False )
    
  def test_getCacheKeys( self ):  
    ''' test that we can get the cache keys
    '''
    cache = self.cache( 1 )
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [] )
    cache._RSSCache__rssCache = { 'A' : 1, 'B' : 2 }
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [ 'A', 'B' ] )
    
  def test_resetCache( self ):
    ''' test that we can reset the cache
    '''  
    cache = self.cache( 1 )
    cache._RSSCache__rssCache = { 'A' : 1, 'B' : 2 }
    cache.resetCache()
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [] )
  
  def test_acquireReleaseLock( self ):
    
    cache = self.cache( 1 )
    self.assertRaises( thread.error, l.releaseLock )
    cache.acquireLock()
    cache.releaseLock()
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      