# $HeadURL:  $
''' RSSCache

  Extension of DictCache to be used within RSS

'''

import datetime
import threading
import time

from DIRAC.Core.Utilities.DictCache import DictCache

class RSSCache( object ):
  '''
    Cache with purgeThread integrated
  '''
  
  def __init__( self, lifeTime, updateFunc = None, cacheHistoryLifeTime = None ):
    '''
    Constructor
    '''
    
    self.__lifeTime             = lifeTime
    # lifetime of the history on hours
    self.__cacheHistoryLifeTime = ( 1 and cacheHistoryLifeTime ) or 24 
    self.__updateFunc           = updateFunc
    
    # RSSCache
    self.__rssCache       = DictCache()
    self.__rssCacheStatus = [] # ( updateTime, message )
    self.__rssCacheLock   = threading.Lock()
    
    # Create purgeThread
    self.__refreshStop    = False
    self.__refreshThread  = threading.Thread( target = self.__refreshCache )
    self.__refreshThread.setDaemon( True )
    
  def startRefreshThread( self ):  
    '''
      Run refresh thread.
    '''
    self.__refreshThread.start()
    
  def stopRefreshThread( self ):  
    '''
      Stop refresh thread.
    '''
    self.__refreshStop = True  
    
  def isCacheAlive( self ):
    '''
      Returns status of the cache refreshing thread 
    '''  
    return self.__refreshThread.isAlive()
    
  def setLifeTime( self, lifeTime ):
    '''
      Set cache life time
    '''  
    self.__lifeTime = lifeTime

  def setCacheHistoryLifeTime( self, cacheHistoryLifeTime ):
    '''
      Set cache life time
    '''  
    self.__cacheHistoryLifeTime = cacheHistoryLifeTime
  
  def getCacheKeys( self ):
    '''
      List all the keys stored in the cache.
    '''
    self.__rssCacheLock.acquire()
    keys = self.__rssCache.getKeys()
    self.__rssCacheLock.release()
    
    return keys
  
  def acquireLock( self ):
    '''
      Acquires RSSCache lock
    '''
    self.__rssCacheLock.acquire()

  def releaseLock( self ):
    '''
      Releases RSSCache lock
    '''
    self.__rssCacheLock.release()
  
  def getCacheStatus( self ):
    '''
      Return the latest cache status
    '''
    self.__rssCacheLock.acquire()
    if self.__rssCacheStatus:
      res = dict( [ self.__rssCacheStatus[ 0 ] ] )
    else:
      res = {}  
    self.__rssCacheLock.release()
    return res
    
  def getCacheHistory( self ):
    '''
      Return the cache updates history
    '''
    self.__rssCacheLock.acquire()
    res = dict( self.__rssCacheStatus )
    self.__rssCacheLock.release()
    return res
    
  def get( self, resourceKey ):
    '''
      Gets the resource(s) status(es). Every resource can have multiple statuses, 
      so in order to speed up things, we store them on the cache as follows:
      
      { <resourceName>#<resourceStatusType0> : whatever0,
        <resourceName>#<resourceStatusType1> : whatever1,
      }
    '''
    
    #cacheKey = '%s#%s' % ( resourceName, resourceStatusType )
        
    self.__rssCacheLock.acquire()
    resourceStatus = self.__rssCache.get( resourceKey )
    self.__rssCacheLock.release()
    
    return { resourceKey : resourceStatus }

  def resetCache( self ):
    '''
      Reset cache.
    '''
    self.__rssCacheLock.acquire()
    self.__rssCache.purgeAll()
    self.__rssCacheLock.release()
    
    return True

  def refreshCache( self ):
    '''
      Clears the cache and gets its latest version, not Thread safe !
      Acquire a lock before using it ! ( and release it afterwards ! )
    '''
    
    if self.__updateFunc is None:
      return 'RSSCache has no updateFunction'
    newCache = self.__updateFunc()
    if not newCache[ 'OK' ]:
      return 'RSSCache %s' % newCache[ 'Message' ]
    else:  
      self.__rssCache.purgeAll()
      self.__updateCache( newCache[ 'Value' ] )
         
    return 'Ok'

################################################################################
# Private methods    
     
  def __updateCache( self, newCache ):
    '''
      The new cache must be a dictionary, which should look like:
      { <resourceName>#<resourceStatusType0> : whatever0,
        <resourceName>#<resourceStatusType1> : whatever1,
      }
    '''
    
    for cacheKey, cacheValue in newCache.items():
      self.__rssCache.add( cacheKey, self.__lifeTime, value = cacheValue )
         
    return True

  def __refreshCache( self ):
    '''
      Method that refreshes periodically the cache.
    '''
    
    while not self.__refreshStop:
    
      self.__rssCacheLock.acquire()  
      refreshResult = self.refreshCache()
      
      now = datetime.datetime.utcnow()
      
      if self.__rssCacheStatus:
        dateInserted, _message = self.__rssCacheStatus[ 0 ]
        
        if dateInserted < now - datetime.timedelta( hours = self.__cacheHistoryLifeTime ):
          self.__rssCacheStatus.pop()

      self.__rssCacheStatus.insert( 0, ( now, refreshResult ) )
      
      self.__rssCacheLock.release()
            
      time.sleep( self.__lifeTime )  
            
    self.__refreshStop = False
           
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    