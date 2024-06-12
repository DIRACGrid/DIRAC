"""
  DictCache.
"""
import datetime
import threading
import weakref

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
