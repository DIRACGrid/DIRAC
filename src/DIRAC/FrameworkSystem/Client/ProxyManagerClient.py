""" ProxyManagerClient has the function to "talk" to the ProxyManager service
    (:mod:`~DIRAC.FrameworkSystem.Service.ProxyManagerHandler`).
    This inherits the DIRAC base Client for direct execution of server functionality.
    Client also contain caching of the requested proxy information.
"""
import os
import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Utilities import ThreadSafe, DIRACSingleton
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Security.ProxyFile import multiProxyArgument, deleteMultiProxy
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.X509Request import X509Request  # pylint: disable=import-error
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security import Locations
from DIRAC.Core.Base.Client import Client

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

    def __deleteTemporalFile(self, filename):
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
            cacheKey = (record["DN"], record["group"])
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

        # For backward compatibility reasons with versions prior to v7r1
        # we need to check for proxy with a group
        # AND for groupless proxy even if not specified

        cacheKeys = ((userDN, userGroup), (userDN, ""))
        for cacheKey in cacheKeys:
            if self.__usersCache.exists(cacheKey, validSeconds):
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

    @gUsersSync
    def getUserPersistence(self, userDN, userGroup, validSeconds=0):
        """Check if a user(DN-group) has a proxy in the proxy management
        Updates internal cache if needed to minimize queries to the service

        :param str userDN: user DN
        :param str userGroup: user group
        :param int validSeconds: proxy valid time in a seconds

        :return: S_OK()/S_ERROR()
        """
        cacheKey = (userDN, userGroup)
        userData = self.__usersCache.get(cacheKey, validSeconds)
        if userData:
            if userData["persistent"]:
                return S_OK(True)
        # Get list of users from the DB with proxys at least 300 seconds
        gLogger.verbose("Updating list of users in proxy management")
        retVal = self.__refreshUserCache(validSeconds)
        if not retVal["OK"]:
            return retVal
        userData = self.__usersCache.get(cacheKey, validSeconds)
        if userData:
            return S_OK(userData["persistent"])
        return S_OK(False)

    def setPersistency(self, userDN, userGroup, persistent):
        """Set the persistency for user/group

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean persistent: presistent flag

        :return: S_OK()/S_ERROR()
        """
        # Hack to ensure bool in the rpc call
        persistentFlag = True
        if not persistent:
            persistentFlag = False
        rpcClient = Client(url="Framework/ProxyManager", timeout=120)
        retVal = rpcClient.setPersistency(userDN, userGroup, persistentFlag)
        if not retVal["OK"]:
            return retVal
        # Update internal persistency cache
        cacheKey = (userDN, userGroup)
        record = self.__usersCache.get(cacheKey, 0)
        if record:
            record["persistent"] = persistentFlag
            self.__usersCache.add(cacheKey, self.__getSecondsLeftToExpiration(record["expirationtime"]), record)
        return retVal

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
        self, userDN, userGroup, limited=False, requiredTimeLeft=1200, cacheTime=14400, proxyToConnect=None, token=None
    ):
        """Get a proxy Chain from the proxy management

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy

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

        if token:
            retVal = rpcClient.getProxyWithToken(
                userDN, userGroup, req.dumpRequest()["Value"], int(cacheTime + requiredTimeLeft), token
            )
        else:
            retVal = rpcClient.getProxy(
                userDN, userGroup, req.dumpRequest()["Value"], int(cacheTime + requiredTimeLeft)
            )
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
        token=None,
    ):
        """Get a proxy Chain from the proxy management and write it to file

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str filePath: path to save proxy
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy

        :return: S_OK(X509Chain)/S_ERROR()
        """
        retVal = self.downloadProxy(userDN, userGroup, limited, requiredTimeLeft, cacheTime, proxyToConnect, token)
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
        token=None,
    ):
        """Download a proxy if needed and transform it into a VOMS one

        :param str userDN: user DN
        :param str userGroup: user group
        :param boolean limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str requiredVOMSAttribute: VOMS attr to add to the proxy
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy

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
        if token:
            retVal = rpcClient.getVOMSProxyWithToken(
                userDN,
                userGroup,
                req.dumpRequest()["Value"],
                int(cacheTime + requiredTimeLeft),
                token,
                requiredVOMSAttribute,
            )

        else:
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
        token=None,
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
        :param str token: valid token to get a proxy

        :return: S_OK(X509Chain)/S_ERROR()
        """
        retVal = self.downloadVOMSProxy(
            userDN, userGroup, limited, requiredTimeLeft, cacheTime, requiredVOMSAttribute, proxyToConnect, token
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
            gLogger.warn("No voms attribute assigned to group %s when requested pilot proxy" % userGroup)
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
            return S_ERROR("No group found that has %s as voms attrs" % vomsAttr)

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

    def getPayloadProxyFromDIRACGroup(self, userDN, userGroup, requiredTimeLeft, token=None, proxyToConnect=None):
        """Download a payload proxy with VOMS extensions depending on the group

        :param str userDN: user DN
        :param str userGroup: user group
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param str token: valid token to get a proxy
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        # Assign VOMS attribute
        vomsAttr = Registry.getVOMSAttributeForGroup(userGroup)
        if not vomsAttr:
            gLogger.verbose("No voms attribute assigned to group %s when requested payload proxy" % userGroup)
            return self.downloadProxy(
                userDN,
                userGroup,
                limited=True,
                requiredTimeLeft=requiredTimeLeft,
                proxyToConnect=proxyToConnect,
                token=token,
            )
        else:
            return self.downloadVOMSProxy(
                userDN,
                userGroup,
                limited=True,
                requiredTimeLeft=requiredTimeLeft,
                requiredVOMSAttribute=vomsAttr,
                proxyToConnect=proxyToConnect,
                token=token,
            )

    def getPayloadProxyFromVOMSGroup(self, userDN, vomsAttr, token, requiredTimeLeft, proxyToConnect=None):
        """Download a payload proxy with VOMS extensions depending on the VOMS attr

        :param str userDN: user DN
        :param str vomsAttr: VOMS attribute
        :param str token: valid token to get a proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain

        :return: S_OK(X509Chain)/S_ERROR()
        """
        groups = Registry.getGroupsWithVOMSAttribute(vomsAttr)
        if not groups:
            return S_ERROR("No group found that has %s as voms attrs" % vomsAttr)
        userGroup = groups[0]

        return self.downloadVOMSProxy(
            userDN,
            userGroup,
            limited=True,
            requiredTimeLeft=requiredTimeLeft,
            requiredVOMSAttribute=vomsAttr,
            proxyToConnect=proxyToConnect,
            token=token,
        )

    def dumpProxyToFile(self, chain, destinationFile=None, requiredTimeLeft=600):
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

    def requestToken(self, requesterDN, requesterGroup, numUses=1):
        """Request a number of tokens. usesList must be a list of integers and each integer is the number of uses a token
        must have

        :param str requesterDN: user DN
        :param str requesterGroup: user group
        :param int numUses: number of uses

        :return: S_OK(tuple)/S_ERROR() -- tuple contain token, number uses
        """
        rpcClient = Client(url="Framework/ProxyManager", timeout=120)
        return rpcClient.generateToken(requesterDN, requesterGroup, numUses)

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
            return chain.dumpAllToFile(proxyToRenewDict["file"])

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

    def getUploadedProxyLifeTime(self, DN, group=None):
        """Get the remaining seconds for an uploaded proxy

        :param str DN: user DN
        :param str group: group

        :return: S_OK(int)/S_ERROR()
        """
        parameters = dict(UserDN=[DN])
        if group:
            parameters["UserGroup"] = [group]
        result = self.getDBContents(parameters)
        if not result["OK"]:
            return result
        data = result["Value"]
        if len(data["Records"]) == 0:
            return S_OK(0)
        pNames = list(data["ParameterNames"])
        dnPos = pNames.index("UserDN")
        groupPos = pNames.index("UserGroup")
        expiryPos = pNames.index("ExpirationTime")
        for row in data["Records"]:
            if DN == row[dnPos] and group == row[groupPos]:
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
