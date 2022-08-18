"""
:mod: RSSCache

Extension of DictCache to be used within RSS

"""
import datetime
import threading
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.DictCache import DictCache


class RSSCache:
    """
    Cache with purgeThread integrated
    """

    def __init__(self, lifeTime, updateFunc=None, cacheHistoryLifeTime=None):
        """
        Constructor
        """

        self.__lifeTime = lifeTime
        # lifetime of the history on hours
        self.__cacheHistoryLifeTime = (1 and cacheHistoryLifeTime) or 24
        self.__updateFunc = updateFunc

        # RSSCache
        self.__rssCache = DictCache()
        self.__rssCacheStatus = []  # ( updateTime, message )
        self.__rssCacheLock = threading.Lock()

        # Create purgeThread
        self.__refreshStop = False
        self.__refreshThread = threading.Thread(target=self.__refreshCacheThreadRun)
        self.__refreshThread.daemon = True

    def startRefreshThread(self):
        """
        Run refresh thread.
        """
        self.__refreshThread.start()

    def stopRefreshThread(self):
        """
        Stop refresh thread.
        """
        self.__refreshStop = True

    def isCacheAlive(self):
        """
        Returns status of the cache refreshing thread
        """
        return S_OK(self.__refreshThread.is_alive())

    def setLifeTime(self, lifeTime):
        """
        Set cache life time
        """
        self.__lifeTime = lifeTime

    def setCacheHistoryLifeTime(self, cacheHistoryLifeTime):
        """
        Set cache life time
        """
        self.__cacheHistoryLifeTime = cacheHistoryLifeTime

    def getCacheKeys(self):
        """
        List all the keys stored in the cache.
        """
        self.__rssCacheLock.acquire()
        keys = self.__rssCache.getKeys()
        self.__rssCacheLock.release()

        return S_OK(keys)

    def acquireLock(self):
        """
        Acquires RSSCache lock
        """
        self.__rssCacheLock.acquire()

    def releaseLock(self):
        """
        Releases RSSCache lock
        """
        self.__rssCacheLock.release()

    def getCacheStatus(self):
        """
        Return the latest cache status
        """
        self.__rssCacheLock.acquire()
        if self.__rssCacheStatus:
            res = dict([self.__rssCacheStatus[0]])
        else:
            res = {}
        self.__rssCacheLock.release()
        return S_OK(res)

    def getCacheHistory(self):
        """
        Return the cache updates history
        """
        self.__rssCacheLock.acquire()
        res = dict(self.__rssCacheStatus)
        self.__rssCacheLock.release()
        return S_OK(res)

    def get(self, resourceKey):
        """
        Gets the resource(s) status(es). Every resource can have multiple statuses,
        so in order to speed up things, we store them on the cache as follows::

          { (<resourceName>,<resourceStatusType0>) : whatever0,
            (<resourceName>,<resourceStatusType1>) : whatever1,
          }

        """

        # cacheKey = '%s#%s' % ( resourceName, resourceStatusType )

        self.__rssCacheLock.acquire()
        resourceStatus = self.__rssCache.get(resourceKey)
        self.__rssCacheLock.release()

        if resourceStatus:
            return S_OK({resourceKey: resourceStatus})
        return S_ERROR("Cannot get %s" % resourceKey)

    def getBulk(self, resourceKeys):
        """
        Gets values for resourceKeys in one ATOMIC operation.
        """

        result = {}
        self.__rssCacheLock.acquire()

        for resourceKey in resourceKeys:

            resourceRow = self.__rssCache.get(resourceKey)
            if not resourceRow:
                return S_ERROR("Cannot get %s" % resourceKey)
            result.update({resourceKey: resourceRow})

        self.__rssCacheLock.release()
        return S_OK(result)

    def resetCache(self):
        """
        Reset cache.
        """
        self.__rssCacheLock.acquire()
        self.__rssCache.purgeAll()
        self.__rssCacheLock.release()

        return S_OK()

    def refreshCache(self):
        """
        Clears the cache and gets its latest version, not Thread safe !
        Acquire a lock before using it ! ( and release it afterwards ! )
        """

        self.__rssCache.purgeAll()

        if self.__updateFunc is None:
            return S_ERROR("RSSCache has no updateFunction")
        newCache = self.__updateFunc()
        if not newCache["OK"]:
            return newCache

        itemsAdded = self.__updateCache(newCache["Value"])

        return itemsAdded

    def refreshCacheAndHistory(self):
        """
        Method that refreshes the cache and updates the history. Not thread safe,
        you must acquire a lock before using it, and release it right after !
        """

        refreshResult = self.refreshCache()

        now = datetime.datetime.utcnow()

        if self.__rssCacheStatus:
            # Check oldest record
            dateInserted, _message = self.__rssCacheStatus[-1]
            if dateInserted < now - datetime.timedelta(hours=self.__cacheHistoryLifeTime):
                self.__rssCacheStatus.pop()

        self.__rssCacheStatus.insert(0, (now, refreshResult))

    ################################################################################
    # Private methods

    def __updateCache(self, newCache):
        """
        The new cache must be a dictionary, which should look like::

          { ( <resourceName>,<resourceStatusType0>) : whatever0,
            ( <resourceName>,<resourceStatusType1>) : whatever1,
          }

        """

        itemsCounter = 0

        for cacheKey, cacheValue in newCache.items():
            self.__rssCache.add(cacheKey, self.__lifeTime, value=cacheValue)
            itemsCounter += 1

        return S_OK(itemsCounter)

    def __refreshCacheThreadRun(self):
        """
        Method that refreshes periodically the cache.
        """

        while not self.__refreshStop:

            self.__rssCacheLock.acquire()
            self.refreshCacheAndHistory()
            self.__rssCacheLock.release()

            time.sleep(self.__lifeTime)

        self.__refreshStop = False
