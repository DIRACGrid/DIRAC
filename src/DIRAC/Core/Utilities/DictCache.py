"""
DictCache and TwoLevelCache
"""
import datetime
import threading
import weakref
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any

from cachetools import TTLCache

# DIRAC
from DIRAC.Core.Utilities.LockRing import LockRing


class ThreadLocalDict(threading.local):
    """This class is just useful to have a mutable object (in this case, a dict) as a thread local
    Read the _threading_local docstring for more details.

    Its purpose is to have a different cache per thread
    """

    def __init__(self):  # pylint: disable=super-init-not-called
        """c'tor"""
        # Note: it is on purpose that the threading.local constructor is not called
        # Dictionary, local to a thread, that will be used as such
        self.cache = {}


class MockLockRing:
    """This mock class is just used to expose the acquire and release method"""

    def doNothing(self, *args, **kwargs):
        """Really does nothing !"""

    acquire = release = doNothing


class DictCache:
    """DictCache is a generic cache implementation.
    The user can decide whether this cache should be shared among the threads or not, but it is always thread safe
    Note that when shared, the access to the cache is protected by a lock, but not necessarily the
    object you are retrieving from it.
    """

    def __init__(self, deleteFunction=False, threadLocal=False):
        """Initialize the dict cache.

        :param deleteFunction: if not False, invoked when deleting a cached object
        :param threadLocal: if False, the cache will be shared among all the threads, otherwise,
                            each thread gets its own cache.
        """

        self.__threadLocal = threadLocal

        # Placeholder either for a LockRing if the cache is shared,
        # or a mock class if not.
        self.__lock = None

        # One of the following two objects is returned
        # by the __cache property, depending on the threadLocal strategy

        # This is the Placeholder for a shared cache
        self.__sharedCache = {}
        # This is the Placeholder for a shared cache
        self.__threadLocalCache = ThreadLocalDict()

        # Function to clean the elements
        self.__deleteFunction = deleteFunction

        # Called when this object is deleted or the program ends
        self.__finalizer = weakref.finalize(self, _purgeAll, None, self.__cache, self.__deleteFunction)

    @property
    def lock(self):
        """Return the lock.
        In practice, if the cache is shared among threads, it is a LockRing.
        Otherwise, it is just a mock object.
        """

        if not self.__lock:
            if not self.__threadLocal:
                self.__lock = LockRing().getLock(self.__class__.__name__, recursive=True)
            else:
                self.__lock = MockLockRing()

        return self.__lock

    @property
    def __cache(self):
        """Returns either a shared or a thread local cache.
        In any case, the returned object is a dictionary
        """
        if self.__threadLocal:
            return self.__threadLocalCache.cache

        return self.__sharedCache

    def exists(self, cKey, validSeconds=0):
        """Returns True/False if the key exists for the given number of seconds

        :param cKey: identification key of the record
        :param int validSeconds: The amount of seconds the key has to be valid for

        :return: bool
        """
        self.lock.acquire()
        try:
            # Is the key in the cache?
            if cKey in self.__cache:
                expTime = self.__cache[cKey]["expirationTime"]
                # If it's valid return True!
                if expTime > datetime.datetime.now() + datetime.timedelta(seconds=validSeconds):
                    return True

                # Delete expired
                self.delete(cKey)
            return False
        finally:
            self.lock.release()

    def delete(self, cKey):
        """Delete a key from the cache

        :param cKey: identification key of the record
        """
        self.lock.acquire()
        try:
            if cKey not in self.__cache:
                return
            if self.__deleteFunction:
                self.__deleteFunction(self.__cache[cKey]["value"])
            del self.__cache[cKey]
        finally:
            self.lock.release()

    def add(self, cKey, validSeconds, value=None):
        """Add a record to the cache

        :param cKey: identification key of the record
        :param int validSeconds: valid seconds of this record
        :param value: value of the record
        """
        if max(0, validSeconds) == 0:
            return
        self.lock.acquire()
        try:
            vD = {"expirationTime": datetime.datetime.now() + datetime.timedelta(seconds=validSeconds), "value": value}
            self.__cache[cKey] = vD
        finally:
            self.lock.release()

    def get(self, cKey, validSeconds=0):
        """Get a record from the cache

        :param cKey: identification key of the record
        :param int validSeconds: The amount of seconds the key has to be valid for

        :return: None or value of key
        """
        self.lock.acquire()
        try:
            # Is the key in the cache?
            if cKey in self.__cache:
                expTime = self.__cache[cKey]["expirationTime"]
                # If it's valid return True!
                if expTime > datetime.datetime.now() + datetime.timedelta(seconds=validSeconds):
                    return self.__cache[cKey]["value"]

                # Delete expired
                self.delete(cKey)
            return None
        finally:
            self.lock.release()

    def showContentsInString(self):
        """Return a human readable string to represent the contents

        :return: str
        """
        self.lock.acquire()
        try:
            data = []
            for cKey, cValue in self.__cache.items():
                data.append(f"{cKey}:")
                data.append(f"\tExp: {cValue['expirationTime']}")
                if cValue["value"]:
                    data.append(f"\tVal: {cValue['Value']}")
            return "\n".join(data)
        finally:
            self.lock.release()

    def getKeys(self, validSeconds=0):
        """Get keys for all contents

        :param int validSeconds: valid time in seconds

        :return: list
        """
        self.lock.acquire()
        try:
            keys = []
            limitTime = datetime.datetime.now() + datetime.timedelta(seconds=validSeconds)
            for cKey, cValue in self.__cache.items():
                if cValue["expirationTime"] > limitTime:
                    keys.append(cKey)
            return keys
        finally:
            self.lock.release()

    def purgeExpired(self, expiredInSeconds=0):
        """Purge all entries that are expired or will be expired in <expiredInSeconds>

        :param int expiredInSeconds: expired time in a seconds
        """
        self.lock.acquire()
        try:
            keys = []
            limitTime = datetime.datetime.now() + datetime.timedelta(seconds=expiredInSeconds)
            for cKey, cValue in self.__cache.items():
                if cValue["expirationTime"] < limitTime:
                    keys.append(cKey)
            for key in keys:
                if self.__deleteFunction:
                    self.__deleteFunction(self.__cache[key]["value"])
                del self.__cache[key]
        finally:
            self.lock.release()

    def purgeAll(self, useLock=True):
        """Purge all entries
        CAUTION: useLock parameter should ALWAYS be True

        :param bool useLock: use lock
        """
        _purgeAll(self.lock if useLock else None, self.__cache, self.__deleteFunction)


def _purgeAll(lock, cache, deleteFunction):
    """Purge all entries

    This is split in to a helper function to be used by the finalizer without
    needing to add a reference to the DictCache object itself.
    """
    if lock:
        lock.acquire()
    try:
        for cKey in list(cache):
            if deleteFunction:
                deleteFunction(cache[cKey]["value"])
            del cache[cKey]
    finally:
        if lock:
            lock.release()


class TwoLevelCache:
    """A two-level caching system with soft and hard time-to-live (TTL) expiration.

    This cache implements a two-tier caching mechanism to allow for background refresh
    of cached values. It uses a soft TTL for quick access and a hard TTL as a fallback,
    which helps in reducing latency and maintaining data freshness.

    Attributes:
        soft_cache (TTLCache): A cache with a shorter TTL for quick access.
        hard_cache (TTLCache): A cache with a longer TTL as a fallback.
        locks (defaultdict): Thread-safe locks for each cache key.
        futures (dict): Stores ongoing asynchronous population tasks.
        pool (ThreadPoolExecutor): Thread pool for executing cache population tasks.

    Args:
        soft_ttl (int): Time-to-live in seconds for the soft cache.
        hard_ttl (int): Time-to-live in seconds for the hard cache.
        max_workers (int): Maximum number of workers in the thread pool.
        max_items (int): Maximum number of items in the cache.

    Example:
        >>> cache = TwoLevelCache(soft_ttl=60, hard_ttl=300)
        >>> def populate_func():
        ...     return "cached_value"
        >>> value = cache.get("key", populate_func)

    Note:
        The cache uses a ThreadPoolExecutor with a maximum of 10 workers to
        handle concurrent cache population requests.
    """

    def __init__(self, soft_ttl: int, hard_ttl: int, *, max_workers: int = 10, max_items: int = 1_000_000):
        """Initialize the TwoLevelCache with specified TTLs."""
        self.soft_cache = TTLCache(max_items, soft_ttl)
        self.hard_cache = TTLCache(max_items, hard_ttl)
        self.locks = defaultdict(threading.Lock)
        self.futures: dict[str, Future] = {}
        self.pool = ThreadPoolExecutor(max_workers=max_workers)

    def get(self, key: str, populate_func: Callable[[], Any]) -> dict:
        """Retrieve a value from the cache, populating it if necessary.

        This method first checks the soft cache for the key. If not found,
        it checks the hard cache while initiating a background refresh.
        If the key is not in either cache, it waits for the populate_func
        to complete and stores the result in both caches.

        Locks are used to ensure there is never more than one concurrent
        population task for a given key.

        Args:
            key (str): The cache key to retrieve or populate.
            populate_func (Callable[[], Any]): A function to call to populate the cache
                                               if the key is not found.

        Returns:
            Any: The cached value associated with the key.

        Note:
            This method is thread-safe and handles concurrent requests for the same key.
        """
        if result := self.soft_cache.get(key):
            return result
        with self.locks[key]:
            if key not in self.futures:
                self.futures[key] = self.pool.submit(self._work, key, populate_func)
            if result := self.hard_cache.get(key):
                self.soft_cache[key] = result
                return result
            # It is critical that ``future`` is waited for outside of the lock as
            # _work aquires the lock before filling the caches. This also means
            # we can guarantee that the future has not yet been removed from the
            # futures dict.
            future = self.futures[key]
        wait([future])
        return self.hard_cache[key]

    def _work(self, key: str, populate_func: Callable[[], Any]) -> None:
        """Internal method to execute the populate_func and update caches.

        This method is intended to be run in a separate thread. It calls the
        populate_func, stores the result in both caches, and cleans up the
        associated future.

        Args:
            key (str): The cache key to populate.
            populate_func (Callable[[], Any]): The function to call to get the value.

        Note:
            This method is not intended to be called directly by users of the class.
        """
        result = populate_func()
        with self.locks[key]:
            self.futures.pop(key)
            self.hard_cache[key] = result
            self.soft_cache[key] = result
