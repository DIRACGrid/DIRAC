""" ProxyManagerClient has the function to "talk" to the ProxyManager service
    (:mod:`~DIRAC.FrameworkSystem.Service.ProxyManagerHandler`).
    This inherits the DIRAC base Client for direct execution of server functionality.
    Client also contain caching of the requested proxy information.
"""

import datetime
import os

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.DiracX import addTokenToPEM
from DIRAC.Core.Security.ProxyFile import deleteMultiProxy, multiProxyArgument
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.X509Request import X509Request  # pylint: disable=import-error
from DIRAC.Core.Utilities import DIRACSingleton, ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache

gUsersSync = ThreadSafe.Synchronizer()
gProxiesSync = ThreadSafe.Synchronizer()
gVOMSProxiesSync = ThreadSafe.Synchronizer()


class ProxyManagerClient(metaclass=DIRACSingleton.DIRACSingleton):
    def __init__(self):
        self.__usersCache = DictCache()
        self.__proxiesCache = DictCache()
        self.__vomsProxiesCache = DictCache()
        self.__pilotProxiesCache = DictCache()
        self.__filesCache = DictCache(self.__deleteTemporalFile)

    @staticmethod
    def __deleteTemporalFile(filename):
        """Delete temporal file

        :param str filename: path to file
        """
        try:
            os.remove(filename)
        except Exception:
            pass

    def clearCaches(self):
        """Clear caches"""
        self.__usersCache.purgeAll()
        self.__proxiesCache.purgeAll()
        self.__vomsProxiesCache.purgeAll()
        self.__pilotProxiesCache.purgeAll()

    def __getSecondsLeftToExpiration(self, expiration, utc=True):
        """Get time left to expiration in a seconds

        :param datetime expiration:
        :param boolean utc: time in utc

        :return: datetime
        """
        if utc:
            td = expiration - datetime.datetime.utcnow()
        else:
            td = expiration - datetime.datetime.now()
        return td.days * 86400 + td.seconds

    def __refreshUserCache(self, validSeconds=0):
        """Refresh user cache

        :param int validSeconds: required seconds the proxy is valid for

        :return: S_OK()/S_ERROR()
        """
        rpcClient = Client(url="Framework/ProxyManager", timeout=120)
        retVal = rpcClient.getRegisteredUsers(validSeconds)
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        # Update the cache
        for record in data:
            cacheKey = record["DN"]
            self.__usersCache.add(cacheKey, self.__getSecondsLeftToExpiration(record["expirationtime"]), record)
        return S_OK()

    @gUsersSync
    def userHasProxy(self, userDN, userGroup, validSeconds=0):
        """Check if a user(DN-group) has a proxy in the proxy management
        Updates internal cache if needed to minimize queries to the service

        :param str userDN: user DN
        :param str userGroup: user group
        :param int validSeconds: proxy valid time in a seconds

        :return: S_OK()/S_ERROR()
        """

        cacheKeys = (userDN, "")
        if self.__usersCache.exists(cacheKeys, validSeconds):
            return S_OK(True)

        # Get list of users from the DB with proxys at least 300 seconds
        gLogger.verbose("Updating list of users in proxy management")
        retVal = self.__refreshUserCache(validSeconds)
        if not retVal["OK"]:
            return retVal

        for cacheKey in cacheKeys:
            if self.__usersCache.exists(cacheKey, validSeconds):
                return S_OK(True)

        return S_OK(False)

    def uploadProxy(self, proxy=None, restrictLifeTime: int = 0, rfcIfPossible=None):
        """Upload a proxy to the proxy management service using delegation

        :param X509Chain proxy: proxy as a chain
        :param restrictLifeTime: proxy live time in a seconds

        :return: S_OK(dict)/S_ERROR() -- dict contain proxies
        """
        if rfcIfPossible is not None:
            if os.environ.get("DIRAC_DEPRECATED_FAIL", None):
                raise NotImplementedError("'rfcIfPossible' argument is deprecated.")
            gLogger.warn("'rfcIfPossible' argument is deprecated.")

        # Discover proxy location
        if isinstance(proxy, X509Chain):
            chain = proxy
            proxyLocation = ""
        else:
            if not proxy:
                proxyLocation = Locations.getProxyLocation()
                if not proxyLocation:
                    return S_ERROR("Can't find a valid proxy")
            elif isinstance(proxy, str):
                proxyLocation = proxy
            else:
                return S_ERROR("Can't find a valid proxy")
            chain = X509Chain()
            result = chain.loadProxyFromFile(proxyLocation)
            if not result["OK"]:
                return S_ERROR(f"Can't load {proxyLocation}: {result['Message']}")

        # Make sure it's valid
        if chain.hasExpired().get("Value"):
            return S_ERROR(f"Proxy {proxyLocation} has expired")
        if chain.getDIRACGroup(ignoreDefault=True).get("Value") or chain.isVOMS().get("Value"):
            return S_ERROR("Cannot upload proxy with DIRAC group or VOMS extensions")

        rpcClient = Client(url="Framework/ProxyManager", timeout=120)
        # Get a delegation request
        result = rpcClient.requestDelegationUpload()
        if not result["OK"]:
            return result
        reqDict = result["Value"]
        # Generate delegated chain
        chainLifeTime = chain.getRemainingSecs()["Value"] - 60
        if restrictLifeTime and restrictLifeTime < chainLifeTime:
            chainLifeTime = restrictLifeTime
        result = chain.generateChainFromRequestString(reqDict["request"], lifetime=chainLifeTime)
        if result["OK"]:
            result = rpcClient.completeDelegationUpload(reqDict["id"], pemChain := result["Value"])
        return result

    @gProxiesSync
    def downloadProxy(
        self, userDN, userGroup, limited=False, requiredTimeLeft=1200, cacheTime=14400, proxyToConnect=None
    ):
        """Get a proxy Chain from the proxy management

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        cacheKey = (userDN, userGroup)
        if self.__proxiesCache.exists(cacheKey, requiredTimeLeft):
            return S_OK(self.__proxiesCache.get(cacheKey))

        if proxyToConnect:
            rpcClient = Client(url="Framework/ProxyManager", proxyChain=proxyToConnect, timeout=120)
        else:
            rpcClient = Client(url="Framework/ProxyManager", timeout=120)

        generateProxyArgs = {"limited": limited}
        res = rpcClient.getStoredProxyStrength(userDN, userGroup, None)
        if not res["OK"]:
            gLogger.warn(
                "Could not get stored proxy strength",
                f"{userDN}, {userGroup}: {res}",
            )
        else:
            generateProxyArgs["bitStrength"] = res["Value"]

        req = X509Request()
        req.generateProxyRequest(**generateProxyArgs)

        retVal = rpcClient.getProxy(userDN, userGroup, req.dumpRequest()["Value"], int(cacheTime + requiredTimeLeft))
        if not retVal["OK"]:
            return retVal
        chain = X509Chain(keyObj=req.getPKey())
        retVal = chain.loadChainFromString(retVal["Value"])
        if not retVal["OK"]:
            return retVal
        self.__proxiesCache.add(cacheKey, chain.getRemainingSecs()["Value"], chain)
        return S_OK(chain)

    def downloadProxyToFile(
        self,
        userDN,
        userGroup,
        limited=False,
        requiredTimeLeft=1200,
        cacheTime=14400,
        filePath=None,
        proxyToConnect=None,
    ):
        """Get a proxy Chain from the proxy management and write it to file

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str filePath: path to save proxy
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        retVal = self.downloadProxy(userDN, userGroup, limited, requiredTimeLeft, cacheTime, proxyToConnect)
        if not retVal["OK"]:
            return retVal
        chain = retVal["Value"]
        retVal = self.dumpProxyToFile(chain, filePath)
        if not retVal["OK"]:
            return retVal
        retVal["chain"] = chain
        return retVal

    @gVOMSProxiesSync
    def downloadVOMSProxy(
        self,
        userDN,
        userGroup,
        limited=False,
        requiredTimeLeft=1200,
        cacheTime=14400,
        requiredVOMSAttribute=None,
        proxyToConnect=None,
    ):
        """Download a proxy if needed and transform it into a VOMS one

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str requiredVOMSAttribute: VOMS attr to add to the proxy
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        cacheKey = (userDN, userGroup, requiredVOMSAttribute, limited)
        if self.__vomsProxiesCache.exists(cacheKey, requiredTimeLeft):
            return S_OK(self.__vomsProxiesCache.get(cacheKey))

        if proxyToConnect:
            rpcClient = Client(url="Framework/ProxyManager", proxyChain=proxyToConnect, timeout=120)
        else:
            rpcClient = Client(url="Framework/ProxyManager", timeout=120)

        generateProxyArgs = {"limited": limited}
        res = rpcClient.getStoredProxyStrength(userDN, userGroup, requiredVOMSAttribute)
        if not res["OK"]:
            gLogger.warn(
                "Could not get stored proxy strength",
                f"{userDN}, {userGroup}, {requiredVOMSAttribute}: {res}",
            )
        else:
            generateProxyArgs["bitStrength"] = res["Value"]

        req = X509Request()
        req.generateProxyRequest(**generateProxyArgs)
        retVal = rpcClient.getVOMSProxy(
            userDN, userGroup, req.dumpRequest()["Value"], int(cacheTime + requiredTimeLeft), requiredVOMSAttribute
        )
        if not retVal["OK"]:
            return retVal
        chain = X509Chain(keyObj=req.getPKey())
        retVal = chain.loadChainFromString(retVal["Value"])
        if not retVal["OK"]:
            return retVal
        self.__vomsProxiesCache.add(cacheKey, chain.getRemainingSecs()["Value"], chain)
        return S_OK(chain)

    def downloadVOMSProxyToFile(
        self,
        userDN,
        userGroup,
        limited=False,
        requiredTimeLeft=1200,
        cacheTime=14400,
        requiredVOMSAttribute=None,
        filePath=None,
        proxyToConnect=None,
    ):
        """Download a proxy if needed, transform it into a VOMS one and write it to file

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str requiredVOMSAttribute: VOMS attr to add to the proxy
        :param str filePath: path to save proxy
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        retVal = self.downloadVOMSProxy(
            userDN, userGroup, limited, requiredTimeLeft, cacheTime, requiredVOMSAttribute, proxyToConnect
        )
        if not retVal["OK"]:
            return retVal
        chain = retVal["Value"]
        retVal = self.dumpProxyToFile(chain, filePath)
        if not retVal["OK"]:
            return retVal
        retVal["chain"] = chain
        return retVal

    def getPilotProxyFromDIRACGroup(self, userDN, userGroup, requiredTimeLeft=43200, proxyToConnect=None):
        """Download a pilot proxy with VOMS extensions depending on the group

        :param str userDN: user DN
        :param str userGroup: user group
        :param int requiredTimeLeft: required proxy live time in seconds
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        # Assign VOMS attribute
        vomsAttr = Registry.getVOMSAttributeForGroup(userGroup)
        if not vomsAttr:
            gLogger.warn(f"No voms attribute assigned to group {userGroup} when requested pilot proxy")
            return self.downloadProxy(
                userDN, userGroup, limited=False, requiredTimeLeft=requiredTimeLeft, proxyToConnect=proxyToConnect
            )
        else:
            return self.downloadVOMSProxy(
                userDN,
                userGroup,
                limited=False,
                requiredTimeLeft=requiredTimeLeft,
                requiredVOMSAttribute=vomsAttr,
                proxyToConnect=proxyToConnect,
            )

    def getPilotProxyFromVOMSGroup(self, userDN, vomsAttr, requiredTimeLeft=43200, proxyToConnect=None):
        """Download a pilot proxy with VOMS extensions depending on the group

        :param str userDN: user DN
        :param str vomsAttr: VOMS attribute
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        groups = Registry.getGroupsWithVOMSAttribute(vomsAttr)
        if not groups:
            return S_ERROR(f"No group found that has {vomsAttr} as voms attrs")

        for userGroup in groups:
            result = self.downloadVOMSProxy(
                userDN,
                userGroup,
                limited=False,
                requiredTimeLeft=requiredTimeLeft,
                requiredVOMSAttribute=vomsAttr,
                proxyToConnect=proxyToConnect,
            )
            if result["OK"]:
                return result
        return result

    def getPayloadProxyFromDIRACGroup(self, userDN, userGroup, requiredTimeLeft, proxyToConnect=None):
        """Download a payload proxy with VOMS extensions depending on the group

        :param str userDN: user DN
        :param str userGroup: user group
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        # Assign VOMS attribute
        vomsAttr = Registry.getVOMSAttributeForGroup(userGroup)
        if not vomsAttr:
            gLogger.verbose(f"No voms attribute assigned to group {userGroup} when requested payload proxy")
            return self.downloadProxy(
                userDN,
                userGroup,
                limited=True,
                requiredTimeLeft=requiredTimeLeft,
                proxyToConnect=proxyToConnect,
            )
        else:
            return self.downloadVOMSProxy(
                userDN,
                userGroup,
                limited=True,
                requiredTimeLeft=requiredTimeLeft,
                requiredVOMSAttribute=vomsAttr,
                proxyToConnect=proxyToConnect,
            )

    def dumpProxyToFile(self, chain, destinationFile=None, requiredTimeLeft=600, includeToken=True):
        """Dump a proxy to a file. It's cached so multiple calls won't generate extra files

        :param X509Chain chain: proxy as a chain
        :param str destinationFile: path to store proxy
        :param int requiredTimeLeft: required proxy live time in a seconds

        :return: S_OK(str)/S_ERROR()
        """
        result = chain.hash()
        if not result["OK"]:
            return result
        cHash = result["Value"]
        if self.__filesCache.exists(cHash, requiredTimeLeft):
            filepath = self.__filesCache.get(cHash)
            if filepath and os.path.isfile(filepath):
                return S_OK(filepath)
            self.__filesCache.delete(cHash)
        retVal = chain.dumpAllToFile(destinationFile)
        if not retVal["OK"]:
            return retVal
        filename = retVal["Value"]
        if not (result := chain.getDIRACGroup())["OK"]:
            return result
        if (
            includeToken
            and not (result := addTokenToPEM(filename, result["Value"]))["OK"]  # pylint: disable=unsubscriptable-object
        ):
            return result
        self.__filesCache.add(cHash, chain.getRemainingSecs()["Value"], filename)
        return S_OK(filename)

    def deleteGeneratedProxyFile(self, chain):
        """Delete a file generated by a dump

        :param X509Chain chain: proxy as a chain

        :return: S_OK()
        """
        self.__filesCache.delete(chain)
        return S_OK()

    def deleteProxyBundle(self, idList):
        """delete a list of id's

        :param list,tuple idList: list of identity numbers

        :return: S_OK(int)/S_ERROR()
        """
        rpcClient = Client(url="Framework/ProxyManager", timeout=120)
        return rpcClient.deleteProxyBundle(idList)

    def renewProxy(self, proxyToBeRenewed=None, minLifeTime=3600, newProxyLifeTime=43200, proxyToConnect=None):
        """Renew a proxy using the ProxyManager

        :param X509Chain proxyToBeRenewed: proxy to renew
        :param int minLifeTime: if proxy life time is less than this, renew. Skip otherwise
        :param int newProxyLifeTime: life time of new proxy
        :param X509Chain proxyToConnect: proxy to use for connecting to the service

        :return: S_OK(X509Chain)/S_ERROR()
        """
        retVal = multiProxyArgument(proxyToBeRenewed)
        if not retVal["Value"]:
            return retVal
        proxyToRenewDict = retVal["Value"]

        secs = proxyToRenewDict["chain"].getRemainingSecs()["Value"]
        if secs > minLifeTime:
            deleteMultiProxy(proxyToRenewDict)
            return S_OK()

        if not proxyToConnect:
            proxyToConnectDict = {"chain": False, "tempFile": False}
        else:
            retVal = multiProxyArgument(proxyToConnect)
            if not retVal["Value"]:
                deleteMultiProxy(proxyToRenewDict)
                return retVal
            proxyToConnectDict = retVal["Value"]

        userDN = proxyToRenewDict["chain"].getIssuerCert()["Value"].getSubjectDN()["Value"]
        retVal = proxyToRenewDict["chain"].getDIRACGroup()
        if not retVal["OK"]:
            deleteMultiProxy(proxyToRenewDict)
            deleteMultiProxy(proxyToConnectDict)
            return retVal
        userGroup = retVal["Value"]
        limited = proxyToRenewDict["chain"].isLimitedProxy()["Value"]

        voms = VOMS()
        retVal = voms.getVOMSAttributes(proxyToRenewDict["chain"])
        if not retVal["OK"]:
            deleteMultiProxy(proxyToRenewDict)
            deleteMultiProxy(proxyToConnectDict)
            return retVal
        vomsAttrs = retVal["Value"]
        if vomsAttrs:
            retVal = self.downloadVOMSProxy(
                userDN,
                userGroup,
                limited=limited,
                requiredTimeLeft=newProxyLifeTime,
                requiredVOMSAttribute=vomsAttrs[0],
                proxyToConnect=proxyToConnectDict["chain"],
            )
        else:
            retVal = self.downloadProxy(
                userDN,
                userGroup,
                limited=limited,
                requiredTimeLeft=newProxyLifeTime,
                proxyToConnect=proxyToConnectDict["chain"],
            )

        deleteMultiProxy(proxyToRenewDict)
        deleteMultiProxy(proxyToConnectDict)

        if not retVal["OK"]:
            return retVal

        chain = retVal["Value"]

        if not proxyToRenewDict["tempFile"]:
            filename = proxyToRenewDict["file"]
            if not (result := chain.dumpAllToFile(filename))["OK"]:
                return result
            if not (result := chain.getDIRACGroup())["OK"]:
                return result
            if not (result := addTokenToPEM(filename, result["Value"]))["OK"]:  # pylint: disable=unsubscriptable-object
                return result
            return S_OK(filename)

        return S_OK(chain)

    def getDBContents(self, condDict={}, sorting=[["UserDN", "DESC"]], start=0, limit=0):
        """Get the contents of the db

        :param dict condDict: search condition

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
        """
        rpcClient = Client(url="Framework/ProxyManager", timeout=120)
        return rpcClient.getContents(condDict, sorting, start, limit)

    def getVOMSAttributes(self, chain):
        """Get the voms attributes for a chain

        :param X509Chain chain: proxy as a chain

        :return: S_OK(str)/S_ERROR()
        """
        return VOMS().getVOMSAttributes(chain)

    def getUploadedProxyLifeTime(self, DN):
        """Get the remaining seconds for an uploaded proxy

        :param str DN: user DN

        :return: S_OK(int)/S_ERROR()
        """
        parameters = dict(UserDN=[DN])
        result = self.getDBContents(parameters)
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data["Records"]) == 0:
            return S_OK(0)
        pNames = list(data["ParameterNames"])
        dnPos = pNames.index("UserDN")
        expiryPos = pNames.index("ExpirationTime")
        for row in data["Records"]:
            if DN == row[dnPos]:
                td = row[expiryPos] - datetime.datetime.utcnow()
                secondsLeft = td.days * 86400 + td.seconds
                return S_OK(max(0, secondsLeft))
        return S_OK(0)

    def getUserProxiesInfo(self):
        """Get the user proxies uploaded info

        :return: S_OK(dict)/S_ERROR()
        """
        result = Client(url="Framework/ProxyManager", timeout=120).getUserProxiesInfo()
        if "rpcStub" in result:
            result.pop("rpcStub")
        return result


gProxyManager = ProxyManagerClient()
