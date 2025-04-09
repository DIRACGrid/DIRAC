"""ProxyManager is the implementation of the ProxyManagement service in the DISET framework

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ProxyManager:
  :end-before: ##END
  :dedent: 2
  :caption: ProxyManager options
"""

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue
from DIRAC.FrameworkSystem.Utilities.diracx import get_token

DEFAULT_MAIL_FROM = "proxymanager@diracgrid.org"


class ProxyManagerHandlerMixin:
    __maxExtraLifeFactor = 1.5
    __proxyDB = None

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        mailFrom = getServiceOption(serviceInfoDict, "MailFrom", DEFAULT_MAIL_FROM)

        try:
            result = ObjectLoader().loadObject("FrameworkSystem.DB.ProxyDB")
            if not result["OK"]:
                gLogger.error(f"Failed to load ProxyDB class: {result['Message']}")
                return result
            dbClass = result["Value"]

            cls.__proxyDB = dbClass(mailFrom=mailFrom, parentLogger=cls.log)

        except RuntimeError as excp:
            return S_ERROR("Can't connect to ProxyDB", repr(excp))
        return S_OK()

    def __generateUserProxiesInfo(self):
        """Generate information dict about user proxies

        :return: dict
        """
        proxiesInfo = {}
        credDict = self.getRemoteCredentials()
        result = Registry.getDNForUsername(credDict["username"])
        if not result["OK"]:
            return result
        selDict = {"UserDN": result["Value"]}
        result = self.__proxyDB.getProxiesContent(selDict, {})
        if not result["OK"]:
            return result
        contents = result["Value"]
        userDNIndex = contents["ParameterNames"].index("UserDN")
        expirationIndex = contents["ParameterNames"].index("ExpirationTime")
        for record in contents["Records"]:
            userDN = record[userDNIndex]
            if userDN not in proxiesInfo:
                proxiesInfo[userDN] = {}
            proxiesInfo[userDN] = record[expirationIndex]
        return proxiesInfo

    auth_getUserProxiesInfo = ["authenticated"]
    types_getUserProxiesInfo = []

    def export_getUserProxiesInfo(self):
        """Get the info about the user proxies in the system

        :return: S_OK(dict)
        """
        return S_OK(self.__generateUserProxiesInfo())

    # WARN: Since 8.0 requestDelegationUpload method do not use arguments!
    # WARN:   requestedUploadTime argument for compatibility with older versions
    types_requestDelegationUpload = []

    def export_requestDelegationUpload(self, requestedUploadTime=None):
        """Request a delegation. Send a delegation request to client

        :return: S_OK(dict)/S_ERROR() -- dict contain id and proxy as string of the request
        """
        credDict = self.getRemoteCredentials()
        user = f"{credDict['username']}:{credDict['group']}"
        result = self.__proxyDB.generateDelegationRequest(credDict["x509Chain"], credDict["DN"])
        if result["OK"]:
            gLogger.info(f"Upload request by {user} given id {result['Value']['id']}")
        else:
            gLogger.error("Upload request failed", f"by {user} : {result['Message']}")
        return result

    types_completeDelegationUpload = [int, str]

    def export_completeDelegationUpload(self, requestId, pemChain):
        """Upload result of delegation

        :param int requestId: identity number
        :param str pemChain: certificate as string

        :return: S_OK(dict)/S_ERROR() -- dict contain proxies
        """
        credDict = self.getRemoteCredentials()
        userId = f'{credDict["username"]}:{credDict["group"]}'
        retVal = self.__proxyDB.completeDelegation(requestId, credDict["DN"], pemChain)
        if not retVal["OK"]:
            gLogger.error("Upload proxy failed", f"id: {requestId} user: {userId} message: {retVal['Message']}")
            return retVal
        gLogger.info(f"Upload {requestId} by {userId} completed")
        return S_OK(self.__generateUserProxiesInfo())

    types_getRegisteredUsers = []

    def export_getRegisteredUsers(self, validSecondsRequired=0):
        """Get the list of users who have a valid proxy in the system

        :param int validSecondsRequired: required seconds the proxy is valid for

        :return: S_OK(list)/S_ERROR() -- list contain dicts with user name, DN, group
                                         expiration time
        """
        credDict = self.getRemoteCredentials()
        if Properties.PROXY_MANAGEMENT not in credDict["properties"]:
            return self.__proxyDB.getUsers(validSecondsRequired, userMask=credDict["username"])
        return self.__proxyDB.getUsers(validSecondsRequired)

    def __checkProperties(self, requestedUserDN, requestedUserGroup):
        """Check the properties and return if they can only download limited proxies if authorized

        :param str requestedUserDN: user DN
        :param str requestedUserGroup: DIRAC group

        :return: S_OK(boolean)/S_ERROR()
        """
        credDict = self.getRemoteCredentials()
        if Properties.FULL_DELEGATION in credDict["properties"]:
            return S_OK(False)
        if Properties.LIMITED_DELEGATION in credDict["properties"]:
            return S_OK(True)
        if Properties.PRIVATE_LIMITED_DELEGATION in credDict["properties"]:
            if credDict["DN"] != requestedUserDN:
                return S_ERROR("You are not allowed to download any proxy")
            if Properties.PRIVATE_LIMITED_DELEGATION not in Registry.getPropertiesForGroup(requestedUserGroup):
                return S_ERROR("You can't download proxies for that group")
            return S_OK(True)
        # Not authorized!
        return S_ERROR("You can't get proxies!")

    types_getStoredProxyStrength = [str, str, [str, type(None), bool]]

    def export_getStoredProxyStrength(self, userDN, userGroup=None, vomsAttr=None):
        """Return the strength in bit of the stored proxy

        :param userDN: DN of the user
        :param userGroup: group of the user
        :param vomsAttr: VOMS attr we plan to add on the proxy
        """
        return self.__proxyDB.getProxyStrength(userDN, userGroup=userGroup, vomsAttr=vomsAttr)

    types_getProxy = [str, str, str, int]

    def export_getProxy(self, userDN, userGroup, requestPem, requiredLifetime):
        """Get a proxy for a userDN/userGroup

        :param requestPem: PEM encoded request object for delegation
        :param requiredLifetime: Argument for length of proxy

          * Properties:
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self
        """
        credDict = self.getRemoteCredentials()

        result = self.__checkProperties(userDN, userGroup)
        if not result["OK"]:
            return result
        forceLimited = result["Value"]

        self.__proxyDB.logAction("download proxy", credDict["DN"], credDict["group"], userDN, userGroup)
        return self.__getProxy(userDN, userGroup, requestPem, requiredLifetime, forceLimited)

    def __getProxy(self, userDN, userGroup, requestPem, requiredLifetime, forceLimited):
        """Internal to get a proxy

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param str requestPem: dump of request certificate
        :param int requiredLifetime: requested live time of proxy
        :param boolean forceLimited: limited proxy

        :return: S_OK(str)/S_ERROR()
        """
        retVal = self.__proxyDB.getProxy(userDN, userGroup, requiredLifeTime=requiredLifetime)
        if not retVal["OK"]:
            return retVal
        chain, secsLeft = retVal["Value"]
        # If possible we return a proxy 1.5 longer than requested
        requiredLifetime = int(min(secsLeft, requiredLifetime * self.__maxExtraLifeFactor))
        retVal = chain.generateChainFromRequestString(
            requestPem, lifetime=requiredLifetime, requireLimited=forceLimited
        )
        if not retVal["OK"]:
            return retVal
        return S_OK(retVal["Value"])

    types_getVOMSProxy = [str, str, str, int, [str, type(None), bool]]

    def export_getVOMSProxy(self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute=None):
        """Get a proxy for a userDN/userGroup

        :param requestPem: PEM encoded request object for delegation
        :param requiredLifetime: Argument for length of proxy
        :param vomsAttribute: VOMS attr to add to the proxy

          * Properties :
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self
        """
        credDict = self.getRemoteCredentials()

        result = self.__checkProperties(userDN, userGroup)
        if not result["OK"]:
            return result
        forceLimited = result["Value"]

        self.__proxyDB.logAction("download voms proxy", credDict["DN"], credDict["group"], userDN, userGroup)
        return self.__getVOMSProxy(userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, forceLimited)

    def __getVOMSProxy(self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, forceLimited):
        retVal = self.__proxyDB.getVOMSProxy(
            userDN, userGroup, requiredLifeTime=requiredLifetime, requestedVOMSAttr=vomsAttribute
        )
        if not retVal["OK"]:
            return retVal
        chain, secsLeft = retVal["Value"]
        # If possible we return a proxy 1.5 longer than requested
        requiredLifetime = int(min(secsLeft, requiredLifetime * self.__maxExtraLifeFactor))
        return chain.generateChainFromRequestString(requestPem, lifetime=requiredLifetime, requireLimited=forceLimited)

    types_deleteProxyBundle = [(list, tuple)]

    def export_deleteProxyBundle(self, idList):
        """delete a list of id's

        :param list,tuple idList: list of identity numbers

        :return: S_OK(int)/S_ERROR()
        """
        errorInDelete = []
        deleted = 0
        for _id in idList:
            if len(_id) != 2:
                errorInDelete.append(f"{str(_id)} doesn't have two fields")
            retVal = self.export_deleteProxy(_id[0], _id[1])
            if not retVal["OK"]:
                errorInDelete.append(f"{str(_id)} : {retVal['Message']}")
            else:
                deleted += 1
        if errorInDelete:
            return S_ERROR(f"Could not delete some proxies: {','.join(errorInDelete)}")
        return S_OK(deleted)

    types_deleteProxy = [(list, tuple)]

    def export_deleteProxy(self, userDN, userGroup):
        """Delete a proxy from the DB

        :param str userDN: user DN
        :param str userGroup: DIRAC group

        :return: S_OK()/S_ERROR()
        """
        credDict = self.getRemoteCredentials()
        if Properties.PROXY_MANAGEMENT not in credDict["properties"]:
            if userDN != credDict["DN"]:
                return S_ERROR("You aren't allowed!")
        retVal = self.__proxyDB.deleteProxy(userDN, userGroup)
        if not retVal["OK"]:
            return retVal
        self.__proxyDB.logAction("delete proxy", credDict["DN"], credDict["group"], userDN, userGroup)
        return S_OK()

    types_getContents = [dict, (list, tuple), int, int]

    def export_getContents(self, selDict, sortDict, start, limit):
        """Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list,tuple sortDict: sorting fields
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
        """
        credDict = self.getRemoteCredentials()
        if Properties.PROXY_MANAGEMENT not in credDict["properties"]:
            selDict["UserName"] = credDict["username"]
        return self.__proxyDB.getProxiesContent(selDict, sortDict, start, limit)

    types_getLogContents = [dict, (list, tuple), int, int]

    def export_getLogContents(self, selDict, sortDict, start, limit):
        """Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list,tuple sortDict: search filter
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
        """
        return self.__proxyDB.getLogsContent(selDict, sortDict, start, limit)

    types_exchangeProxyForToken = []

    @convertToReturnValue
    def export_exchangeProxyForToken(self):
        """Exchange a proxy for an equivalent token to be used with diracx"""

        credDict = self.getRemoteCredentials()
        return get_token(
            credDict["username"],
            credDict["group"],
            set(credDict.get("groupProperties", []) + credDict.get("properties", [])),
            expires_minutes=credDict["secondsLeft"] // 60 + 1,
            source="ProxyManager",
        )


class ProxyManagerHandler(ProxyManagerHandlerMixin, RequestHandler):
    pass
