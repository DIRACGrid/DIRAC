""" Cache

This module provides a generic Cache extended to be used on RSS, RSSCache.
This cache features a lazy update method. It will only be updated if it is
empty and there is a new query. If not, it will remain in its previous state.
However, Cache class internal cache: DictCache sets a validity to its entries.
After that, the cache is empty.

"""
import itertools
import random

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration


class Cache:
    """
    Cache basic class.

    WARNING: None of its methods is thread safe. Acquire / Release lock when
    using them !
    """

    def __init__(self, lifeTime, updateFunc):
        """
        Constructor

        :Parameters:
          **lifeTime** - `int`
            Lifetime of the elements in the cache ( seconds ! )
          **updateFunc** - `function`
            This function MUST return a S_OK | S_ERROR object. In the case of the first,
            its value must be a dictionary.

        """

        # We set a 20% of the lifetime randomly, so that if we have thousands of jobs
        # starting at the same time, all the caches will not end at the same time.
        randomLifeTimeBias = 0.2 * random.random()

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        self.__lifeTime = int(lifeTime * (1 + randomLifeTimeBias))
        self.__updateFunc = updateFunc
        # The records returned from the cache must be valid at least 30 seconds.
        self.__validSeconds = 30

        # Cache
        self.__cache = DictCache()
        self.__cacheLock = LockRing()
        self.__cacheLock.getLock(self.__class__.__name__)

    # internal cache object getter

    def cacheKeys(self):
        """
        Cache keys getter

        :returns: list with keys in the cache valid for at least twice the validity period of the element
        """

        # Here we need to have more than the validity period because of the logic of the matching:
        # * get all the keys with validity T
        # * for each key K, get the element K with validity T
        # This logic fails for elements just at the limit of the required time
        return self.__cache.getKeys(validSeconds=self.__validSeconds * 2)

    # acquire / release Locks

    def acquireLock(self):
        """
        Acquires Cache lock
        """
        self.__cacheLock.acquire(self.__class__.__name__)

    def releaseLock(self):
        """
        Releases Cache lock
        """
        self.__cacheLock.release(self.__class__.__name__)

    # Cache getters

    def get(self, cacheKeys):
        """
        Gets values for cacheKeys given, if all are found ( present on the cache and
        valid ), returns S_OK with the results. If any is not neither present not
        valid, returns S_ERROR.

        :Parameters:
          **cacheKeys** - `list`
            list of keys to be extracted from the cache

        :return: S_OK | S_ERROR
        """

        result = {}

        for cacheKey in cacheKeys:
            cacheRow = self.__cache.get(cacheKey, validSeconds=self.__validSeconds)

            if not cacheRow:
                return S_ERROR(f"Cannot get {str(cacheKey)}")
            result.update({cacheKey: cacheRow})

        return S_OK(result)

    def check(self, cacheKeys, vO):
        """
        Modified get() method. Attempts to find keys with a vO value appended or 'all'
        value appended. The cacheKeys passed in are 'flattened' cache keys (no vO)
        Gets values for cacheKeys given, if all are found ( present on the cache and
        valid ), returns S_OK with the results. If any is not neither present not
        valid, returns S_ERROR.

        :Parameters:
          **cacheKeys** - `list`
            list of keys to be extracted from the cache

        :return: S_OK | S_ERROR
        """

        result = {}

        for cacheKey in cacheKeys:
            longCacheKey = cacheKey + ("all",)
            cacheRow = self.__cache.get(longCacheKey, validSeconds=self.__validSeconds)
            if not cacheRow:
                longCacheKey = cacheKey + (vO,)
                cacheRow = self.__cache.get(longCacheKey, validSeconds=self.__validSeconds)
                if not cacheRow:
                    return S_ERROR(f'Cannot get extended {str(cacheKey)} (neither for VO = {vO} nor for "all" Vos)')
            result.update({longCacheKey: cacheRow})

        return S_OK(result)

    # Cache refreshers

    def refreshCache(self):
        """
        Purges the cache and gets fresh data from the update function.

        :return: S_OK | S_ERROR. If the first, its content is the new cache.
        """

        self.log.verbose("refreshing...")

        self.__cache.purgeAll()

        newCache = self.__updateFunc()
        if not newCache["OK"]:
            self.log.error(newCache["Message"])
            return newCache

        newCache = self.__updateCache(newCache["Value"])

        self.log.verbose("refreshed")

        return newCache

    # Private methods

    def __updateCache(self, newCache):
        """
        Given the new cache dictionary, updates the internal cache with it. It sets
        a duration to the entries of <self.__lifeTime> seconds.

        :Parameters:
          **newCache** - `dict`
            dictionary containing a new cache

        :return: dictionary. It is newCache argument.
        """

        for cacheKey, cacheValue in newCache.items():
            self.__cache.add(cacheKey, self.__lifeTime, value=cacheValue)

        # We are assuming nothing will fail while inserting in the cache. There is
        # no apparent reason to suspect from that piece of code.
        return S_OK(newCache)


class RSSCache(Cache):
    """
    The RSSCache is an extension of Cache in which the cache keys are pairs of the
    form: ( elementName, statusType ).

    When instantiating one object of RSSCache, we need to specify the RSS elementType
    it applies, e.g. : StorageElement, CE, Queue, ...

    It provides a unique public method `match` which is thread safe. All other
    methods are not !!
    """

    def __init__(self, lifeTime, updateFunc):
        """
        Constructor

        :Parameters:
          **elementType** - `string`
            RSS elementType, e.g.: StorageElement, CE, Queue... note that one RSSCache
            can only hold elements of a single elementType to avoid issues while doing
            the Cartesian product.
          **lifeTime** - `int`
            Lifetime of the elements in the cache ( seconds ! )
          **updateFunc** - `function`
            This function MUST return a S_OK | S_ERROR object. In the case of the first,
            its value must follow the dict format: ( key, value ) being key ( elementName,
            statusType ) and value status.

        """

        super().__init__(lifeTime, updateFunc)

        self.allStatusTypes = RssConfiguration().getConfigStatusType()

    def match(self, elementNames, elementType, statusTypes, vO):
        """
        In first instance, if the cache is invalid, it will request a new one from
        the server.
        It make the Cartesian product of elementNames x statusTypes to generate a key
        set that will be compared against the cache set. If the first is included in
        the second, we have a positive match and a dictionary will be returned. Otherwise,
        we have a cache miss.

        However, arguments ( elementNames or statusTypes ) can have a None value. If
        that is the case, they are considered wildcards.

        :Parameters:
          **elementNames** - [ None, `string`, `list` ]
            name(s) of the elements to be matched
          **elementType** - [ `string` ]
            type of the elements to be matched
          **statusTypes** - [ None, `string`, `list` ]
            name(s) of the statusTypes to be matched

        :return: S_OK() || S_ERROR()
        """

        self.acquireLock()
        try:
            return self._match(elementNames, elementType, statusTypes, vO)
        finally:
            # Release lock, no matter what !
            self.releaseLock()

    # Private methods: NOT THREAD SAFE !!

    def _match(self, elementNames, elementType, statusTypes, vO):
        """
        Method doing the actual work. It must be wrapped around locks to ensure no
        disaster happens.

        :Parameters:
          **elementNames** - [ None, `string`, `list` ]
            name(s) of the elements to be matched
          **elementType** - [ `string` ]
            type of the elements to be matched
          **statusTypes** - [ None, `string`, `list` ]
            name(s) of the statusTypes to be matched

        :return: S_OK() || S_ERROR()
        """

        # Gets the entire cache or a new one if it is empty / invalid
        validCache = self.__getValidCache()
        if not validCache["OK"]:
            return validCache
        validCache = validCache["Value"]

        # Gets matched keys
        try:
            matchKeys = self.__match(validCache, elementNames, elementType, statusTypes, vO)
        except IndexError:
            return S_ERROR("RSS cache empty?")

        if not matchKeys["OK"]:
            return matchKeys

        # Gets objects for matched keys. It will return S_ERROR if the cache value
        # has expired in between. It has 30 valid seconds, which means something was
        # extremely slow above.
        if matchKeys["CheckVO"]:
            cacheMatches = self.check(matchKeys["Value"], vO)  # add an appropriate VO to the keys
        else:
            cacheMatches = self.get(matchKeys["Value"])
        if not cacheMatches["OK"]:
            return cacheMatches

        cacheMatches = cacheMatches["Value"]
        if not cacheMatches:
            return S_ERROR(f"Empty cache for: {elementNames}, {elementType}")

        # We undo the key into <elementName> and <statusType>
        try:
            cacheMatchesDict = self.__getDictFromCacheMatches(cacheMatches)
        except ValueError:
            cacheMatchesDict = cacheMatches

        return S_OK(cacheMatchesDict)

    def __getValidCache(self):
        """
        Obtains the keys on the cache which are valid. If any, returns the complete
        valid dictionary. If the list is empty, we assume the cache is invalid or
        not filled, so we issue a cache refresh and return its data.

        :return: { ( elementName, statusType, vO ) : status, ... }
        """

        cacheKeys = self.cacheKeys()
        # If cache is empty, we refresh it.
        if not cacheKeys:
            cache = self.refreshCache()
        else:
            cache = self.get(cacheKeys)

        return cache

    def __match(self, validCache, elementNames, elementType, statusTypes, vO):
        """
        Obtains all keys on the cache ( should not be empty ! ).

        Gets the sets ( no duplicates ) of elementNames and statusTypes. There is a
        slight distinction. A priori we cannot know which are all the elementNames.
        So, if elementNames is None, we will consider all elementNames in the cacheKeys.
        However, if statusTypes is None, we will get the standard list from the
        ResourceStatus configuration in the CS.

        If the cartesian product of our sets is on the cacheKeys set, we have a
        positive match.

        :Parameters:
          **validCache** - `dict`
            cache dictionary
          **elementNames** - [ None, `string`, `list` ]
            name(s) of the elements to be matched
          **elementType** - [ `string` ]
            type of the elements to be matched
          **statusTypes** - [ None, `string`, `list` ]
            name(s) of the statusTypes to be matched

        :return: S_OK() with a Vo check marker || S_ERROR()
        """

        cacheKeys = list(validCache)
        # flatten the cache. From our VO perspective we are only want to keep:
        # 1) keys with vO tuple element equal to our vO,
        # 2) keys with vO tuple element equal to 'all', but only if no element described in 1) exists.
        # a resource key is set to have 3 elements to allow a comparison with a cartesian product.
        checkVo = False
        if len(cacheKeys[0]) == 4:  # resource
            checkVo = True
            flattenedCache = {(key[0], key[1], key[2]): value for key, value in validCache.items() if key[3] == "all"}
            flattenedCache.update(
                {(key[0], key[1], key[2]): value for key, value in validCache.items() if key[3] == vO}
            )
            validCache = flattenedCache
        else:  # site, not VO specific in SiteStatus, eventually to be upgraded there to include the VO
            pass

        if isinstance(elementNames, str):
            elementNames = [elementNames]
        elif elementNames is None:
            if isinstance(cacheKeys[0], (tuple, list)):
                elementNames = [cacheKey[0] for cacheKey in cacheKeys]
            else:
                elementNames = cacheKeys
        # Remove duplicates, makes Cartesian product faster
        elementNamesSet = set(elementNames)

        if isinstance(elementType, str):
            if not elementType or elementType == "Site":
                elementType = []
            else:
                elementType = [elementType]
        elif elementType is None:
            elementType = [cacheKey[1] for cacheKey in cacheKeys]
        # Remove duplicates, makes Cartesian product faster
        elementTypeSet = set(elementType)

        if isinstance(statusTypes, str):
            if not statusTypes:
                statusTypes = []
            else:
                statusTypes = [statusTypes]
        elif statusTypes is None:
            statusTypes = self.allStatusTypes
        # Remove duplicates, makes Cartesian product faster
        statusTypesSet = set(statusTypes)

        if not elementTypeSet and not statusTypesSet:
            cartesianProduct = elementNamesSet
        else:
            cartesianProduct = set(itertools.product(elementNamesSet, elementTypeSet, statusTypesSet))

        # Some users find funny sending empty lists, which will make the cartesianProduct
        # be []. Problem: [] is always subset, no matter what !

        if not cartesianProduct:
            self.log.warn("Empty cartesian product")
            return S_ERROR("Empty cartesian product")

        notInCache = list(cartesianProduct.difference(set(validCache)))
        if notInCache:
            self.log.warn(f"Cache misses: {notInCache}")
            return S_ERROR(f"Cache misses: {notInCache}")

        result = S_OK(cartesianProduct)
        result["CheckVO"] = checkVo
        return result

    @staticmethod
    def __getDictFromCacheMatches(cacheMatches):
        """
        Formats the cacheMatches to a format expected by the RSS helpers clients.

        :Parameters:
          **cacheMatches** - `dict`
            cache dictionary of the form { ( elementName, elementType, statusType, vO ) : status, ... }


        :return: dict of the form { elementName : { statusType: status, ... }, ... }
        """

        result = {}

        for cacheKey, cacheValue in cacheMatches.items():
            elementName, _elementType, statusType, vO = cacheKey
            result.setdefault(elementName, {})[statusType] = cacheValue

        return result
