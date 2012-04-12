# $HeadURL:  $
''' RSSCache

  Extension of DictCache to be used within RSS

'''

import threading
import time

from DIRAC                          import gLogger
from DIRAC.Core.Utilities.DictCache import DictCache

class RSSCache( object ):
  '''
    Cache with purgeThread integrated
  '''
  
  def __init__( self, lifeTime, updateFunc ):
    '''
    Constructor
    '''
    
    self.__lifeTime   = lifeTime
    self.__updateFunc = updateFunc
    
    # RSSCache
    self.__rssCache     = DictCache()
    self.__rssCacheLock = threading.Lock()
    
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
    
  def setLifeTime( self, lifeTime ):
    '''
      Set cache life time
    '''  
    self.__lifeTime = lifeTime
  
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
  
  def getResourceStatus( self, resourceKey ):
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
       
    newCache = self.__updateFunc()
    if not newCache[ 'OK' ]:
      gLogger.warn( 'RSSCache %s' % newCache[ 'Message' ] )
    else:  
      self.__rssCache.purgeAll()
      self.__updateCache( newCache[ 'Value' ] )
         
    return True

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
      self.refreshCache()
      self.__rssCacheLock.release()
            
      time.sleep( self.__lifeTime )  
            
    self.__refreshStop = True
           
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    