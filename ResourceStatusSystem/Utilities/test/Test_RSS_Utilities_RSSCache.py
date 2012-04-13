# $HeadURL: $
''' Test_RSS_Utilities_RSSCache

'''

import thread
import time
import unittest

__RCSID__ = '$Id: $'

forcedResult = None
def dummyFunction():
  return forcedResult

class Dummy( object ):
      
  def dummyMethod( self, *args, **kwargs ):
    pass
    
  def __getattr__( self, name ):
    return self.dummyMethod 

class DummyCache( object ):

  def __init__( self, cache = None ):
    self.cache = ( 1 and cache ) or dict()
    
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
    cache = self.cache( 1, updateFunc = 1, cacheHistoryLifeTime = 1 )
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

  def test_setCacheHistoryLifeTime( self ):
    ''' test that we update cacheHistoryLifeTime
    '''     
    cache = self.cache( 1 )
    cache.setCacheHistoryLifeTime( 2 )
    self.assertEqual( cache._RSSCache__cacheHistoryLifeTime, 2 )
    
  def test_getCacheStatus( self ):  
    ''' test that we can extract latest record from the cache history 
    '''
    cache = self.cache( 1 )
    res = cache.getCacheStatus()
    self.assertEqual( res, {} )
    cache._RSSCache__rssCacheStatus = [ ( 1, 2 ), ( 3, 4 ) ]
    res = cache.getCacheStatus()
    self.assertEqual( res, { 1 : 2 } )

  def test_getCacheHistory( self ):  
    ''' test that we can extract information from the cache history 
    '''
    cache = self.cache( 1 )
    res = cache.getCacheHistory()
    self.assertEqual( res, {} )
    cache._RSSCache__rssCacheStatus = [ ( 1, 2 ), ( 3, 4 ) ]
    res = cache.getCacheHistory()
    self.assertEqual( res, { 1 : 2, 3 : 4 } )
  
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
    self.assertRaises( RuntimeError, cache.startRefreshThread )
    
  def test_getCacheKeys( self ):  
    ''' test that we can get the cache keys
    '''
    cache = self.cache( 1 )
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [] )
    cache._RSSCache__rssCache = DummyCache( { 'A' : 1, 'B' : 2 } )
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [ 'A', 'B' ] )
    
  def test_resetCache( self ):
    ''' test that we can reset the cache
    '''  
    cache = self.cache( 1 )
    cache._RSSCache__rssCache = DummyCache( { 'A' : 1, 'B' : 2 } )
    cache.resetCache()
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [] )
  
  def test_acquireReleaseLock( self ):
    ''' test that we can instantiate a lock
    '''
    cache = self.cache( 1 )
    self.assertRaises( thread.error, cache.releaseLock )
    cache.acquireLock()
    cache.releaseLock()
    
  def test_refreshCache( self ):
    ''' test that we can refresh the cache
    '''  
    cache = self.cache( 1 )
    res = cache.refreshCache()
    self.assertEqual( res, False )
    
    global forcedResult
    cache = self.cache( 1, dummyFunction )
    forcedResult = { 'OK' : False, 'Message' : 'forcedMessage' }
    res = cache.refreshCache()
    self.assertEqual( res, False )
    
    forcedResult = { 'OK' : True, 'Value' : { 'A' : 1, 'B' : 2 } }
    res = cache.refreshCache()
    self.assertEqual( res, True )
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [ 'A', 'B' ] )
    
    forcedResult = { 'OK' : True, 'Value' : { 'A' : 2, 'C' : 3 } }
    res = cache.refreshCache()
    self.assertEqual( res, True )
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [ 'A', 'C' ] )
    
  def test_refreshThreadRefreshCache( self ):
    ''' test that the refreshThread can refresh the cache.
    '''
    
    global forcedResult
    cache = self.cache( 1, updateFunc = dummyFunction )
    forcedResult = { 'OK' : True, 'Value' : { 'A' : 1, 'B' : 2 } }
    cache.startRefreshThread()
    self.assertEqual( cache.isCacheAlive(), True )
    time.sleep( 1 )
    cache.stopRefreshThread()
    time.sleep( 2 )
    self.assertEqual( cache.isCacheAlive(), False )
    self.assertEqual( cache._RSSCache__refreshStop, False )
    keys = cache.getCacheKeys()
    self.assertEqual( keys, [ 'A', 'B' ] )        
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      