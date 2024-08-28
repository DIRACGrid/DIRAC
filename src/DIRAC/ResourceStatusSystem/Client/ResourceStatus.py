""" ResourceStatus

Module that acts as a helper for knowing the status of a resource.
It takes care of switching between the CS and the RSS.
The status is kept in the RSSCache object, which is a small wrapper on top of DictCache

"""

import math
from datetime import datetime, timedelta
from time import sleep

from DIRAC import S_ERROR, S_OK, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import getPoliciesThatApply
from DIRAC.ResourceStatusSystem.Utilities.RSSCacheNoThread import RSSCache
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration


class ResourceStatus(metaclass=DIRACSingleton):
    """
    ResourceStatus helper keeps the connection to the db / server as an object member,
    to avoid creating a new connection every time we need to do one.
    """

    def __init__(self):
        """
        Constructor, initializes the rssClient.
        """
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.rssConfig = RssConfiguration()
        self.rssClient = ResourceStatusClient()

        cacheLifeTime = int(self.rssConfig.getConfigCache())

        # RSSCache only affects the calls directed to RSS, if using the CS it is not used.
        self.rssCache = RSSCache(cacheLifeTime, self.__updateRssCache)

    def getElementStatus(self, elementName, elementType, statusType=None, default=None, vO=None):
        """
        Helper function, tries to get information from the RSS for the given
        Element, otherwise, it gets it from the CS.

        :param elementName: name of the element or list of element names
        :type elementName: str, list
        :param elementType: type of the element (StorageElement, ComputingElement, FTS, Catalog)
        :type elementType: str
        :param statusType: type of the status (meaningful only when elementType==StorageElement)
        :type statusType: None, str, list
        :param default: defult value (meaningful only when rss is InActive)
        :type default: str
        :return: S_OK/S_ERROR
        :rtype: dict

        :Example:

        >>> getElementStatus('CE42', 'ComputingElement')
            S_OK( { 'CE42': { 'all': 'Active' } } } )
        >>> getElementStatus('SE1', 'StorageElement', 'ReadAccess')
            S_OK( { 'SE1': { 'ReadAccess': 'Banned' } } } )
        >>> getElementStatus('SE1', 'ThisIsAWrongElementType', 'ReadAccess')
            S_ERROR( xyz.. )
        >>> getElementStatus('ThisIsAWrongName', 'StorageElement', 'WriteAccess')
            S_ERROR( xyz.. )
        >>> getElementStatus('A_file_catalog', 'FileCatalog')
            S_OK( { 'A_file_catalog': { 'all': 'Active' } } } )
        >>> getElementStatus('SE1', 'StorageElement', ['ReadAccess', 'WriteAccess'])
            S_OK( { 'SE1': { 'ReadAccess': 'Banned' , 'WriteAccess': 'Active'} } } )
        >>> getElementStatus('SE1', 'StorageElement')
            S_OK( { 'SE1': { 'ReadAccess': 'Probing' ,
                             'WriteAccess': 'Active',
                             'CheckAccess': 'Degraded',
                             'RemoveAccess': 'Banned'} } } )
        >>> getElementStatus(['CE1', 'CE2'], 'ComputingElement')
            S_OK( {'CE1': {'all': 'Active'},
                   'CE2': {'all': 'Probing'}}}
        """

        allowedParameters = ["StorageElement", "ComputingElement", "FTS", "Catalog"]

        if elementType not in allowedParameters:
            return S_ERROR(f"{elementType} in not in the list of the allowed parameters: {allowedParameters}")

        # Apply defaults
        if not statusType:
            if elementType == "StorageElement":
                statusType = ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
            elif elementType == "ComputingElement":
                statusType = ["all"]
            elif elementType == "FTS":
                statusType = ["all"]
            elif elementType == "Catalog":
                statusType = ["all"]

        return self.__getRSSElementStatus(elementName, elementType, statusType, vO)

    def setElementStatus(self, elementName, elementType, statusType, status, reason=None, tokenOwner=None):
        """Tries set information in RSS and in CS.

        :param elementName: name of the element
        :type elementName: str
        :param elementType: type of the element (StorageElement, ComputingElement, FTS, Catalog)
        :type elementType: str
        :param statusType: type of the status (meaningful only when elementType==StorageElement)
        :type statusType: str
        :param reason: reason for setting the status
        :type reason: str
        :param tokenOwner: owner of the token (meaningful only when rss is Active)
        :type tokenOwner: str
        :return: S_OK/S_ERROR
        :rtype: dict

        :Example:

        >>> setElementStatus('CE42', 'ComputingElement', 'all', 'Active')
            S_OK(  xyz.. )
        >>> setElementStatus('SE1', 'StorageElement', 'ReadAccess', 'Banned')
            S_OK(  xyz.. )
        """

        return self.__setRSSElementStatus(elementName, elementType, statusType, status, reason, tokenOwner)

    ################################################################################

    def __updateRssCache(self):
        """Method used to update the rssCache.

        It will try 5 times to contact the RSS before giving up
        """

        meta = {"columns": ["Name", "ElementType", "StatusType", "Status", "VO"]}

        for ti in range(5):
            rawCache = self.rssClient.selectStatusElement("Resource", "Status", meta=meta)
            if rawCache["OK"]:
                break
            self.log.warn("Can't get resource's status", rawCache["Message"] + "; trial %d" % ti)
            sleep(math.pow(ti, 2))
            self.rssClient = ResourceStatusClient()

        if not rawCache["OK"]:
            return rawCache
        return S_OK(getCacheDictFromRawData(rawCache["Value"]))

    ################################################################################

    def __getRSSElementStatus(self, elementName, elementType, statusType, vO):
        """Gets from the cache or the RSS the Elements status. The cache is a
            copy of the DB table. If it is not on the cache, most likely is not going
            to be on the DB.

            There is one exception: item just added to the CS, e.g. new Element.
            The period between it is added to the DB and the changes are propagated
            to the cache will be inconsistent, but not dangerous. Just wait <cacheLifeTime>
            minutes.

        :param elementName: name of the element or list of element names
        :type elementName: str, list
        :param elementType: type of the element (StorageElement, ComputingElement, FTS, Catalog)
        :type elementType: str
        :param statusType: type of the status (meaningful only when elementType==StorageElement,
                           otherwise it is 'all' or ['all'])
        :type statusType: str, list
        """

        cacheMatch = self.rssCache.match(elementName, elementType, statusType, vO)

        self.log.debug("__getRSSElementStatus")
        self.log.debug(cacheMatch)

        return cacheMatch

    def __getCSElementStatus(self, elementName, elementType, statusType, default):
        """Gets from the CS the Element status

        :param elementName: name of the element
        :type elementName: str
        :param elementType: type of the element (StorageElement, ComputingElement, FTS, Catalog)
        :type elementType: str
        :param statusType: type of the status (meaningful only when elementType==StorageElement)
        :type statusType: str, list
        :param default: defult value
        :type default: None, str
        """
        cs_path = None
        # DIRAC doesn't store the status of ComputingElements nor FTS in the CS, so here we can just return 'Active'
        if elementType in ("ComputingElement", "FTS"):
            return S_OK({elementName: {"all": "Active"}})

        # If we are here it is because elementType is either 'StorageElement' or 'Catalog'
        if elementType == "StorageElement":
            cs_path = "/Resources/StorageElements"
        elif elementType == "Catalog":
            cs_path = "/Resources/FileCatalogs"
            statusType = ["Status"]

        if not isinstance(elementName, list):
            elementName = [elementName]

        if not isinstance(statusType, list):
            statusType = [statusType]

        result = {}
        for element in elementName:
            for sType in statusType:
                # Look in standard location, 'Active' by default
                res = gConfig.getValue(f"{cs_path}/{element}/{sType}", "Active")
                result.setdefault(element, {})[sType] = res

        if result:
            return S_OK(result)

        if default is not None:
            defList = [[el, statusType, default] for el in elementName]
            return S_OK(getDictFromList(defList))

        _msg = "Element '%s', with statusType '%s' is unknown for CS."
        return S_ERROR(DErrno.ERESUNK, _msg % (elementName, statusType))

    def __setRSSElementStatus(self, elementName, elementType, statusType, status, reason, tokenOwner):
        """
        Sets on the RSS the Elements status
        """

        expiration = datetime.utcnow() + timedelta(days=1)

        self.rssCache.acquireLock()
        try:
            res = self.rssClient.addOrModifyStatusElement(
                "Resource",
                "Status",
                name=elementName,
                elementType=elementType,
                status=status,
                statusType=statusType,
                reason=reason,
                tokenOwner=tokenOwner,
                tokenExpiration=expiration,
            )

            if res["OK"]:
                self.rssCache.refreshCache()

            if not res["OK"]:
                _msg = f"Error updating Element ({elementName},{statusType},{status})"
                gLogger.warn(f"RSS: {_msg}")

            return res

        finally:
            # Release lock, no matter what.
            self.rssCache.releaseLock()

    def __setCSElementStatus(self, elementName, elementType, statusType, status):
        """
        Sets on the CS the Elements status
        """
        cs_path = None
        # DIRAC doesn't store the status of ComputingElements nor FTS in the CS, so here we can just do nothing
        if elementType in ("ComputingElement", "FTS"):
            return S_OK()

        # If we are here it is because elementType is either 'StorageElement' or 'Catalog'
        statuses = self.rssConfig.getConfigStatusType(elementType)
        if statusType not in statuses:
            gLogger.error(f"{statusType} is not a valid statusType")
            return S_ERROR(f"{statusType} is not a valid statusType: {statuses}")

        if elementType == "StorageElement":
            cs_path = "/Resources/StorageElements"
        elif elementType == "Catalog":
            cs_path = "/Resources/FileCatalogs"
            # FIXME: This a probably outdated location (new one is in /Operations/[]/Services/Catalogs)
            # but needs to be VO-aware
            statusType = "Status"

        csAPI = CSAPI()
        csAPI.setOption(f"{cs_path}/{elementName}/{elementType}/{statusType}", status)

        res = csAPI.commitChanges()
        if not res["OK"]:
            gLogger.warn(f"CS: {res['Message']}")

        return res

    def isStorageElementAlwaysBanned(self, seName, statusType):
        """Checks if the AlwaysBanned policy is applied to the SE given as parameter

        :param seName: string, name of the SE
        :param statusType: ReadAcces, WriteAccess, RemoveAccess, CheckAccess

        :returns: S_OK(True/False)
        """

        res = getPoliciesThatApply({"name": seName, "statusType": statusType})
        if not res["OK"]:
            self.log.error("isStorageElementAlwaysBanned: unable to get the information", res["Message"])
            return res

        isAlwaysBanned = "AlwaysBanned" in [policy["type"] for policy in res["Value"]]

        return S_OK(isAlwaysBanned)


################################################################################


def getDictFromList(fromList):
    """
    Auxiliary method that given a list returns a dictionary of dictionaries:
    { site1 : { statusType1 : st1, statusType2 : st2 }, ... }
    """

    res = {}
    for listElement in fromList:
        site, sType, status = listElement
        if site not in res:
            res[site] = {}
        res[site][sType] = status
    return res


def getCacheDictFromRawData(rawList):
    """
    Formats the raw data list, which we know it must have tuples of five elements.
    ( element1, element2, element3, elementt4, element5 ) into a dictionary of tuples with the format
    { ( element1, element2, element3, element5 ): element4 )}.
    The resulting dictionary will be the new Cache.

    It happens that element1 is elementName,
                    element2 is elementType,
                    element3 is statusType,
                    element4 is status.
                    element5 is vO

    :Parameters:
      **rawList** - `list`
        list of three element tuples [( element1, element2, element3, element4, element5 ),... ]

    :return: dict of the form { ( elementName, elementType, statusType, vO ) : status, ... }
    """

    res = {}
    for entry in rawList:
        res.update({(entry[0], entry[1], entry[2], entry[4]): entry[3]})

    return res
