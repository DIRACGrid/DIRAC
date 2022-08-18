"""
  DictCache.
"""
import datetime
import threading

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
        pass

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
                else:
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
                else:
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
            for cKey in self.__cache:
                data.append("%s:" % str(cKey))
                data.append("\tExp: %s" % self.__cache[cKey]["expirationTime"])
                if self.__cache[cKey]["value"]:
                    data.append("\tVal: %s" % self.__cache[cKey]["value"])
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
            for cKey in self.__cache:
                if self.__cache[cKey]["expirationTime"] > limitTime:
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
            for cKey in self.__cache:
                if self.__cache[cKey]["expirationTime"] < limitTime:
                    keys.append(cKey)
            for cKey in keys:
                if self.__deleteFunction:
                    self.__deleteFunction(self.__cache[cKey]["value"])
                del self.__cache[cKey]
        finally:
            self.lock.release()

    def purgeAll(self, useLock=True):
        """Purge all entries
        CAUTION: useLock parameter should ALWAYS be True except when called from __del__

        :param bool useLock: use lock
        """
        if useLock:
            self.lock.acquire()
        try:
            for cKey in list(self.__cache):
                if self.__deleteFunction:
                    self.__deleteFunction(self.__cache[cKey]["value"])
                del self.__cache[cKey]
        finally:
            if useLock:
                self.lock.release()

    def __del__(self):
        """When the DictCache is deleted, all the entries should be purged.
        This is particularly useful when the DictCache manages files
        CAUTION: if you carefully read the python doc, you will see all the
        caveat of __del__. In particular, no guaranty that it is called...
        (https://docs.python.org/2/reference/datamodel.html#object.__del__)
        """
        self.purgeAll(useLock=False)
        del self.__lock
        if self.__threadLocal:
            del self.__threadLocalCache
        else:
            del self.__sharedCache
