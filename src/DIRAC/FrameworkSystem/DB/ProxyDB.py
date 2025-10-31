""" ProxyDB class is a front-end to the ProxyDB MySQL database.

    Database contains the following tables:

    * ProxyDB_Requests -- a delegation requests storage table for a given proxy Chain
    * ProxyDB_CleanProxies -- table for storing proxies in "clean" form, ie without
      the presence of DIRAC and VOMS extensions.
    * ProxyDB_VOMSProxies -- proxy storage table with VOMS extension already added.
    * ProxyDB_Log -- table with logs.
"""
import textwrap
from threading import Lock

from cachetools import TTLCache, cachedmethod

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.X509Request import X509Request  # pylint: disable=import-error
from DIRAC.Core.Utilities import DErrno
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory

DEFAULT_MAIL_FROM = "proxymanager@diracgrid.org"

# Module-level cache for getProxyStrength method (shared across ProxyDB instances)
_get_proxy_strength_cache = TTLCache(maxsize=1000, ttl=600)
_get_proxy_strength_lock = Lock()


class ProxyDB(DB):
    NOTIFICATION_TIMES = [2592000, 1296000]

    def __init__(self, mailFrom=None, parentLogger=None):
        """
        :param str mailFrom: address to use as sender for the expiration reminder emails
        """
        DB.__init__(self, "ProxyDB", "Framework/ProxyDB", parentLogger=parentLogger)
        self._mailFrom = mailFrom if mailFrom else DEFAULT_MAIL_FROM
        self.__defaultRequestLifetime = 300  # 5min
        self._minSecsToAllowStore = 3600
        self.__notifClient = NotificationClient()
        retVal = self.__initializeDB()
        if not retVal["OK"]:
            raise Exception(f"Can't create tables: {retVal['Message']}")
        self.purgeExpiredProxies(sendNotifications=False)

    def __initializeDB(self):
        """Create the tables

        :result: S_OK()/S_ERROR()
        """
        retVal = self._query("show tables")
        if not retVal["OK"]:
            return retVal

        tablesInDB = [t[0] for t in retVal["Value"]]
        tablesD = {}

        if "ProxyDB_Requests" not in tablesInDB:
            tablesD["ProxyDB_Requests"] = {
                "Fields": {
                    "Id": "INTEGER AUTO_INCREMENT NOT NULL",
                    "UserDN": "VARCHAR(255) NOT NULL",
                    "Pem": "BLOB",
                    "ExpirationTime": "DATETIME",
                },
                "PrimaryKey": "Id",
            }

        if "ProxyDB_CleanProxies" not in tablesInDB:
            tablesD["ProxyDB_CleanProxies"] = {
                "Fields": {
                    "UserName": "VARCHAR(64) NOT NULL",
                    "UserDN": "VARCHAR(255) NOT NULL",
                    "ProxyProvider": 'VARCHAR(64) DEFAULT "Certificate"',
                    "Pem": "BLOB",
                    "ExpirationTime": "DATETIME",
                },
                "PrimaryKey": ["UserDN", "ProxyProvider"],
            }

        if "ProxyDB_VOMSProxies" not in tablesInDB:
            tablesD["ProxyDB_VOMSProxies"] = {
                "Fields": {
                    "UserName": "VARCHAR(64) NOT NULL",
                    "UserDN": "VARCHAR(255) NOT NULL",
                    "UserGroup": "VARCHAR(255) NOT NULL",
                    "VOMSAttr": "VARCHAR(255) NOT NULL",
                    "Pem": "BLOB",
                    "ExpirationTime": "DATETIME",
                },
                "PrimaryKey": ["UserDN", "UserGroup", "vomsAttr"],
            }

        if "ProxyDB_Log" not in tablesInDB:
            tablesD["ProxyDB_Log"] = {
                "Fields": {
                    "ID": "BIGINT NOT NULL AUTO_INCREMENT",
                    "IssuerDN": "VARCHAR(255) NOT NULL",
                    "IssuerGroup": "VARCHAR(255) NOT NULL",
                    "TargetDN": "VARCHAR(255) NOT NULL",
                    "TargetGroup": "VARCHAR(255) NOT NULL",
                    "Action": "VARCHAR(128) NOT NULL",
                    "Timestamp": "DATETIME",
                },
                "PrimaryKey": "ID",
                "Indexes": {"Timestamp": ["Timestamp"]},
            }

        return self._createTables(tablesD)

    def generateDelegationRequest(self, proxyChain, userDN):
        """Generate a request and store it for a given proxy Chain

        :param X509Chain() proxyChain: proxy as chain
        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dict contain id and proxy as string of the request
        """
        retVal = self._getConnection()
        if not retVal["OK"]:
            return retVal
        connObj = retVal["Value"]

        # Generate a proxy Request with the same length as the existing proxy
        retVal = proxyChain.getStrength()
        if not retVal["OK"]:
            return retVal
        bitStrength = retVal["Value"]

        retVal = proxyChain.generateProxyRequest(bitStrength=bitStrength)
        if not retVal["OK"]:
            return retVal
        request = retVal["Value"]
        retVal = request.dumpRequest()
        if not retVal["OK"]:
            return retVal
        reqStr = retVal["Value"]
        retVal = request.dumpPKey()
        if not retVal["OK"]:
            return retVal
        allStr = reqStr + retVal["Value"]
        try:
            sUserDN = self._escapeString(userDN)["Value"]
            sAllStr = self._escapeString(allStr)["Value"]
        except KeyError:
            return S_ERROR("Cannot escape DN")
        cmd = "INSERT INTO `ProxyDB_Requests` ( Id, UserDN, Pem, ExpirationTime )"
        cmd += " VALUES ( 0, %s, %s, TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() ) )" % (
            sUserDN,
            sAllStr,
            int(self.__defaultRequestLifetime),
        )
        retVal = self._update(cmd, conn=connObj)
        if not retVal["OK"]:
            return retVal
        # 99% of the times we will stop here
        if "lastRowId" in retVal:
            return S_OK({"id": retVal["lastRowId"], "request": reqStr})
        # If the lastRowId hack does not work. Get it by hand
        retVal = self._query(f"SELECT Id FROM `ProxyDB_Requests` WHERE Pem='{reqStr}'")
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        if not data:
            return S_ERROR("Insertion of the request in the db didn't work as expected")
        userGroup = proxyChain.getDIRACGroup().get("Value") or "unset"
        self.logAction("request upload", userDN, userGroup, userDN, "any")
        # Here we go!
        return S_OK({"id": data[0][0], "request": reqStr})

    def __retrieveDelegationRequest(self, requestId, userDN):
        """Retrieve a request from the DB

        :param int requestId: id of the request
        :param str userDN: user DN

        :return: S_OK(str)/S_ERROR()
        """
        try:
            sUserDN = self._escapeString(userDN)["Value"]
        except KeyError:
            return S_ERROR("Cannot escape DN")
        cmd = f"SELECT Pem FROM `ProxyDB_Requests` WHERE Id = {requestId} AND UserDN = {sUserDN}"
        retVal = self._query(cmd)
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        if len(data) == 0:
            return S_ERROR(f"No requests with id {requestId}")
        request = X509Request()
        retVal = request.loadAllFromString(data[0][0].decode("ascii"))
        if not retVal["OK"]:
            return retVal
        return S_OK(request)

    def purgeExpiredRequests(self):
        """Purge expired requests from the db

        :return: S_OK()/S_ERROR()
        """
        cmd = "DELETE FROM `ProxyDB_Requests` WHERE ExpirationTime < UTC_TIMESTAMP()"
        return self._update(cmd)

    def deleteRequest(self, requestId):
        """Delete a request from the db

        :param int requestId: id of the request

        :return: S_OK()/S_ERROR()
        """
        cmd = f"DELETE FROM `ProxyDB_Requests` WHERE Id={requestId}"
        return self._update(cmd)

    def completeDelegation(self, requestId, userDN, delegatedPem):
        """Complete a delegation and store it in the db

        :param int requestId: id of the request
        :param str userDN: user DN
        :param str delegatedPem: delegated proxy as string

        :return: S_OK()/S_ERROR()
        """
        retVal = self.__retrieveDelegationRequest(requestId, userDN)
        if not retVal["OK"]:
            return retVal
        request = retVal["Value"]
        chain = X509Chain(keyObj=request.getPKey())
        retVal = chain.loadChainFromString(delegatedPem)
        if not retVal["OK"]:
            return retVal
        retVal = chain.isValidProxy()
        if not retVal["OK"]:
            return retVal

        result = chain.isVOMS()
        if result["OK"] and result.get("Value"):
            return S_ERROR("Proxies with VOMS extensions are not allowed to be uploaded")

        retVal = chain.getDIRACGroup(ignoreDefault=True)
        if not retVal["OK"]:
            return retVal
        if retVal["Value"]:
            return S_ERROR("Proxies with DIRAC group extensions not allowed to be uploaded")
        retVal = self.__storeProxy(userDN, chain)
        return self.deleteRequest(requestId) if retVal["OK"] else retVal

    def __storeProxy(self, userDN, chain, proxyProvider=None):
        """Store user proxy into the Proxy repository for a user specified by his
        DN and group or proxy provider.

        :param str userDN: user DN from proxy
        :param X509Chain() chain: proxy chain
        :param str proxyProvider: proxy provider name

        :return: S_OK()/S_ERROR()
        """
        retVal = Registry.getUsernameForDN(userDN)
        if not retVal["OK"]:
            return retVal
        userName = retVal["Value"]

        if not proxyProvider:
            result = Registry.getProxyProvidersForDN(userDN)
            if not result["OK"]:
                return result
            proxyProvider = result["Value"][0] if result.get("Value") else "Certificate"

        # Get remaining secs
        retVal = chain.getRemainingSecs()
        if not retVal["OK"]:
            return retVal
        remainingSecs = retVal["Value"]
        if remainingSecs < self._minSecsToAllowStore:
            return S_ERROR(
                f"Cannot store proxy, remaining secs {remainingSecs} is less than {self._minSecsToAllowStore}"
            )

        # Compare the DNs
        retVal = chain.getIssuerCert()
        if not retVal["OK"]:
            return retVal
        proxyIdentityDN = retVal["Value"].getSubjectDN()["Value"]
        if userDN != proxyIdentityDN:
            msg = "Mismatch in the user DN"
            vMsg = f"Proxy says {proxyIdentityDN} and credentials are {userDN}"
            self.log.error(msg, vMsg)
            return S_ERROR(f"{msg}. {vMsg}")

        # Check if its limited
        if chain.isLimitedProxy()["Value"]:
            return S_ERROR("Limited proxies are not allowed to be stored")
        dLeft = int(remainingSecs / 86400)
        hLeft = int(remainingSecs / 3600 - dLeft * 24)
        mLeft = int(remainingSecs / 60 - hLeft * 60 - dLeft * 1440)
        sLeft = int(remainingSecs - hLeft * 3600 - mLeft * 60 - dLeft * 86400)
        self.log.info(
            "Storing proxy for credentials %s (%d:%02d:%02d:%02d left)" % (proxyIdentityDN, dLeft, hLeft, mLeft, sLeft)
        )

        try:
            sUserDN = self._escapeString(userDN)["Value"]
            sTable = "ProxyDB_CleanProxies"
        except KeyError:
            return S_ERROR("Cannot escape DN")
        # Check what we have already got in the repository
        cmd = "SELECT TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ), Pem "
        cmd += f"FROM `{sTable}` WHERE UserDN={sUserDN} "
        result = self._query(cmd)
        if not result["OK"]:
            return result

        # Check if there is a previous ticket for the DN
        data = result["Value"]
        sqlInsert = True
        if data:
            sqlInsert = False
            pem = data[0][1].decode("ascii")
            if pem:
                remainingSecsInDB = data[0][0]
                if remainingSecs <= remainingSecsInDB:
                    self.log.info(
                        "Proxy stored is longer than uploaded, omitting.",
                        f"{remainingSecs} in uploaded, {remainingSecsInDB} in db",
                    )
                    return S_OK()

        pemChain = chain.dumpAllToString()["Value"]
        dValues = {
            "UserName": self._escapeString(userName)["Value"],
            "UserDN": sUserDN,
            "Pem": self._escapeString(pemChain)["Value"],
            "ExpirationTime": "TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() )" % int(remainingSecs),
        }
        dValues["ProxyProvider"] = f"'{proxyProvider}'"
        if sqlInsert:
            sqlFields = []
            sqlValues = []
            for key in dValues:
                sqlFields.append(key)
                sqlValues.append(dValues[key])
            cmd = f"INSERT INTO `{sTable}` ( {', '.join(sqlFields)} ) VALUES ( {', '.join(sqlValues)} )"
        else:
            sqlSet = []
            sqlWhere = []
            for k in dValues:
                if k in ("UserDN", "ProxyProvider"):
                    sqlWhere.append(f"{k} = {dValues[k]}")
                else:
                    sqlSet.append(f"{k} = {dValues[k]}")
            cmd = f"UPDATE `{sTable}` SET {', '.join(sqlSet)} WHERE {' AND '.join(sqlWhere)}"

        self.logAction("store proxy", userDN, proxyProvider, userDN, proxyProvider)
        return self._update(cmd)

    def purgeExpiredProxies(self, sendNotifications=True):
        """Purge expired requests from the db

        :param boolean sendNotifications: if need to send notification

        :return: S_OK(int)/S_ERROR() -- int is number of purged expired proxies
        """
        purged = 0
        for tableName in ("ProxyDB_CleanProxies", "ProxyDB_VOMSProxies"):
            cmd = f"DELETE FROM `{tableName}` WHERE ExpirationTime < UTC_TIMESTAMP()"
            result = self._update(cmd)
            if not result["OK"]:
                return result
            purged += result["Value"]
            self.log.info(f"Purged {result['Value']} expired proxies from {tableName}")
        if sendNotifications:
            if not (result := self.sendExpirationNotifications())["OK"]:
                return result
        return S_OK(purged)

    def deleteProxy(self, userDN, userGroup=None, proxyProvider=None):
        """Remove proxy of the given user from the repository

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param str proxyProvider: proxy provider name

        :return: S_OK()/S_ERROR()
        """
        try:
            userDN = self._escapeString(userDN)["Value"]
            if userGroup:
                userGroup = self._escapeString(userGroup)["Value"]
            if proxyProvider:
                proxyProvider = self._escapeString(proxyProvider)["Value"]
        except KeyError:
            return S_ERROR("Invalid DN or group or proxy provider")
        errMsgs = []
        req = f"DELETE FROM `ProxyDB_CleanProxies` WHERE UserDN={userDN}"
        result = self._update(req)
        if not result["OK"]:
            errMsgs.append(result["Message"])
        if errMsgs:
            return S_ERROR(", ".join(errMsgs))
        return result

    @cachedmethod(lambda self: _get_proxy_strength_cache, lock=lambda self: _get_proxy_strength_lock)
    def getProxyStrength(self, userDN, userGroup=None, vomsAttr=None):
        """Load the proxy in cache corresponding to the criteria, and check its strength

        :param userDN: DN of the user
        :param userGroup: group of the user
        :param vomsAttr: VOMS attr we plan to add on the proxy
        """
        # Look in the cache
        retVal = Registry.getProxyProvidersForDN(userDN)

        if retVal["OK"]:
            providers = retVal["Value"]
            providers.append("Certificate")
            for proxyProvider in providers:
                retVal = self.__getPemAndTimeLeft(userDN, userGroup, vomsAttr=vomsAttr, proxyProvider=proxyProvider)
                if retVal["OK"]:
                    pemData = retVal["Value"][0]
                    chain = X509Chain()
                    retVal = chain.loadProxyFromString(pemData)
                    if retVal["OK"]:
                        return chain.getStrength()

        return retVal

    def __getPemAndTimeLeft(self, userDN, userGroup=None, vomsAttr=None, proxyProvider=None):
        """Get proxy from database

        :param str userDN: user DN
        :param str userGroup: requested DIRAC group
        :param str vomsAttr: VOMS name
        :param str proxyProvider: proxy provider name

        :return: S_OK(tuple)/S_ERROR() -- tuple contain proxy as string and remaining seconds
        """
        try:
            sUserDN = self._escapeString(userDN)["Value"]
            if userGroup:
                sUserGroup = self._escapeString(userGroup)["Value"]
            if vomsAttr:
                sVomsAttr = self._escapeString(vomsAttr)["Value"]
        except KeyError:
            return S_ERROR("Invalid DN or Group")
        if proxyProvider:
            sTable = "`ProxyDB_CleanProxies`"
        else:
            sTable = "`ProxyDB_VOMSProxies`"
        cmd = f"SELECT Pem, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) from {sTable} "
        cmd += f"WHERE UserDN={sUserDN} AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > 0"
        if proxyProvider:
            cmd += f' AND ProxyProvider="{proxyProvider}"'
        else:
            if userGroup:
                cmd += f" AND UserGroup={sUserGroup}"
            if vomsAttr:
                cmd += f" AND VOMSAttr={sVomsAttr}"
        retVal = self._query(cmd)
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        for pemData, secondsRemaining in data:
            pemData = pemData.decode("ascii")
            if not pemData:
                continue
            if proxyProvider:
                chain = X509Chain()
                result = chain.loadProxyFromString(pemData)
                if not result["OK"]:
                    return result
                result = chain.getStrength()
                if not result["OK"]:
                    return result
                strength = result["Value"]
                result = chain.generateProxyToString(secondsRemaining, diracGroup=userGroup, strength=strength)
                if not result["OK"]:
                    return result
                return S_OK((result["Value"], secondsRemaining))
            return S_OK((pemData, secondsRemaining))
        if userGroup:
            userMask = f"{userDN}@{userGroup}"
        else:
            userMask = userDN
        return S_ERROR(DErrno.EPROXYFIND, f"{userMask} has no proxy registered")

    def __generateProxyFromProxyProvider(self, userDN, proxyProvider):
        """Get proxy from proxy provider

        :param str userDN: user DN for what need to create proxy
        :param str proxyProvider: proxy provider name that will ganarete proxy

        :return: S_OK(dict)/S_ERROR() -- dict with remaining seconds, proxy as a string and as a chain
        """
        gLogger.info("Getting proxy from proxyProvider", f'(for "{userDN}" DN by "{proxyProvider}")')
        result = ProxyProviderFactory().getProxyProvider(proxyProvider)
        if not result["OK"]:
            return result
        pp = result["Value"]
        result = pp.getProxy(userDN)
        if not result["OK"]:
            return result
        proxyStr = result["Value"]
        chain = X509Chain()
        result = chain.loadProxyFromString(proxyStr)
        if not result["OK"]:
            return result
        result = chain.getRemainingSecs()
        if not result["OK"]:
            return result
        remainingSecs = result["Value"]
        result = self.__storeProxy(userDN, chain, proxyProvider)
        if result["OK"]:
            return S_OK({"proxy": proxyStr, "chain": chain, "remainingSecs": remainingSecs})
        return result

    def __getProxyFromProxyProviders(self, userDN, userGroup, requiredLifeTime):
        """Generate new proxy from exist clean proxy or from proxy provider
        for use with userDN in the userGroup

        :param str userDN: user DN
        :param str userGroup: required group name
        :param int requiredLifeTime: required proxy live time in a seconds

        :return: S_OK(tuple)/S_ERROR() -- tuple contain proxy as string and remainig seconds
        """
        result = Registry.getGroupsForDN(userDN)
        if not result["OK"]:
            return S_ERROR(f"Cannot generate proxy: {result['Message']}")
        if userGroup not in result["Value"]:
            return S_ERROR(f"Cannot generate proxy: Invalid group {userGroup} for user")
        result = Registry.getProxyProvidersForDN(userDN)

        errMsgs = []
        if result["OK"]:
            providers = result["Value"]
            providers.append("Certificate")
            for proxyProvider in providers:
                self.log.verbose("Try to get proxy from ProxyDB_CleanProxies")
                result = self.__getPemAndTimeLeft(userDN, userGroup, proxyProvider=proxyProvider)
                if result["OK"] and (not requiredLifeTime or result["Value"][1] > requiredLifeTime):
                    return result
                if len(providers) == 1:
                    return S_ERROR(f'Cannot generate proxy: No proxy providers found for "{userDN}"')
                self.log.verbose(f"Try to generate proxy from {proxyProvider} proxy provider")
                result = self.__generateProxyFromProxyProvider(userDN, proxyProvider)
                if result["OK"]:
                    chain = result["Value"]["chain"]
                    remainingSecs = result["Value"]["remainingSecs"]
                    result = chain.generateProxyToString(remainingSecs, diracGroup=userGroup)
                    if result["OK"]:
                        return S_OK((result["Value"], remainingSecs))
                errMsgs.append(f"\"{proxyProvider}\": {result['Message']}")

        return S_ERROR("Cannot generate proxy%s" % (errMsgs and ": " + ", ".join(errMsgs) or ""))

    def getProxy(self, userDN, userGroup, requiredLifeTime=None):
        """Get proxy string from the Proxy Repository for use with userDN
        in the userGroup

        :param str userDN: user DN
        :param str userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
        """
        # Test that group enable to download
        if not Registry.isDownloadableGroup(userGroup):
            return S_ERROR(f'"{userGroup}" group is disable to download.')

        # Standard proxy is requested
        self.log.verbose("Try to get proxy from ProxyDB_CleanProxies")
        retVal = self.__getPemAndTimeLeft(userDN, userGroup)
        errMsg = "Can't get proxy%s: " % (requiredLifeTime and " for %s seconds" % requiredLifeTime or "")
        if not retVal["OK"]:
            errMsg += f"{retVal['Message']}, try to generate new"
            retVal = self.__getProxyFromProxyProviders(userDN, userGroup, requiredLifeTime=requiredLifeTime)
        elif requiredLifeTime:
            if retVal["Value"][1] < requiredLifeTime:
                errMsg += "Stored proxy is not long lived enough, try to generate new"
                retVal = self.__getProxyFromProxyProviders(userDN, userGroup, requiredLifeTime=requiredLifeTime)
        if not retVal["OK"]:
            return S_ERROR(DErrno.EPROXYFIND, f"{errMsg}; {retVal['Message']}")
        pemData = retVal["Value"][0]
        timeLeft = retVal["Value"][1]
        chain = X509Chain()
        retVal = chain.loadProxyFromString(pemData)
        if not retVal["OK"]:
            return S_ERROR(f"{errMsg}; {retVal['Message']}")

        # Proxy is invalid for some reason, let's delete it
        if not chain.isValidProxy()["OK"]:
            self.deleteProxy(userDN, userGroup)
            return S_ERROR(DErrno.EPROXYFIND, f"{userDN}@{userGroup} has no proxy registered")
        return S_OK((chain, timeLeft))

    def __getVOMSAttribute(self, userGroup, requiredVOMSAttribute=False):
        """Get VOMS attribute for DIRAC group

        :param str userGroup: DIRAC group
        :param boolean requiredVOMSAttribute: VOMS attribute

        :return: S_OK(dict)/S_ERROR() -- dict contain attribute and VOMS VO
        """
        if requiredVOMSAttribute:
            return S_OK({"attribute": requiredVOMSAttribute, "VO": Registry.getVOForGroup(userGroup)})

        csVOMSMapping = Registry.getVOMSAttributeForGroup(userGroup)
        if not csVOMSMapping:
            return S_ERROR(f"No mapping defined for group {userGroup} in the CS")

        return S_OK({"attribute": csVOMSMapping, "VO": Registry.getVOForGroup(userGroup)})

    def getVOMSProxy(self, userDN, userGroup, requiredLifeTime=None, requestedVOMSAttr=None):
        """Get proxy string from the Proxy Repository for use with userDN
        in the userGroup

        :param str userDN: user DN
        :param str userGroup: required DIRAC group
        :param int requiredLifeTime: required proxy live time in a seconds
        :param str requestedVOMSAttr: VOMS attribute

        :return: S_OK(tuple)/S_ERROR() -- tuple with proxy as chain and proxy live time in a seconds
        """
        retVal = self.__getVOMSAttribute(userGroup, requestedVOMSAttr)
        if not retVal["OK"]:
            return retVal
        vomsAttr = retVal["Value"]["attribute"]
        vomsVO = retVal["Value"]["VO"]

        # Look in the cache
        retVal = self.__getPemAndTimeLeft(userDN, userGroup, vomsAttr)
        if retVal["OK"]:
            pemData = retVal["Value"][0]
            vomsTime = retVal["Value"][1]
            chain = X509Chain()
            retVal = chain.loadProxyFromString(pemData)
            if retVal["OK"]:
                retVal = chain.getRemainingSecs()
                if retVal["OK"]:
                    remainingSecs = retVal["Value"]
                    if requiredLifeTime and requiredLifeTime <= vomsTime and requiredLifeTime <= remainingSecs:
                        return S_OK((chain, min(vomsTime, remainingSecs)))

        # Get the stored proxy and dress it with the VOMS extension
        retVal = self.getProxy(userDN, userGroup, requiredLifeTime)
        if not retVal["OK"]:
            return retVal
        chain, _secsLeft = retVal["Value"]

        vomsMgr = VOMS()
        attrs = vomsMgr.getVOMSAttributes(chain).get("Value") or [""]
        if attrs[0]:
            if vomsAttr != attrs[0]:
                return S_ERROR(
                    f"Stored proxy has already a different VOMS attribute {attrs[0]} than requested {vomsAttr}"
                )
        else:
            retVal = vomsMgr.setVOMSAttributes(chain, vomsAttr, vo=vomsVO)
            if not retVal["OK"]:
                return retVal
            chain = retVal["Value"]

        # We have got the VOMS proxy, store it into the cache
        result = self.__storeVOMSProxy(userDN, userGroup, vomsAttr, chain)
        if not result["OK"]:
            return result
        return S_OK((chain, result["Value"]))

    def __storeVOMSProxy(self, userDN, userGroup, vomsAttr, chain):
        """Store VOMS proxy

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param str vomsAttr: VOMS attribute
        :param X509Chain() chain: proxy as chain

        :return: S_OK(str)/S_ERROR()
        """
        retVal = self._getConnection()
        if not retVal["OK"]:
            return retVal
        connObj = retVal["Value"]
        retVal1 = VOMS().getVOMSProxyInfo(chain, "actimeleft")
        retVal2 = VOMS().getVOMSProxyInfo(chain, "timeleft")
        if not retVal1["OK"]:
            return retVal1
        if not retVal2["OK"]:
            return retVal2
        try:
            vomsSecsLeft1 = int(retVal1["Value"].strip())
            vomsSecsLeft2 = int(retVal2["Value"].strip())
            vomsSecsLeft = min(vomsSecsLeft1, vomsSecsLeft2)
        except Exception as e:
            return S_ERROR(f"Can't parse VOMS time left: {str(e)}")
        secsLeft = min(vomsSecsLeft, chain.getRemainingSecs()["Value"])
        pemData = chain.dumpAllToString()["Value"]
        result = Registry.getUsernameForDN(userDN)
        if not result["OK"]:
            userName = ""
        else:
            userName = result["Value"]
        try:
            sUserName = self._escapeString(userName)["Value"]
            sUserDN = self._escapeString(userDN)["Value"]
            sUserGroup = self._escapeString(userGroup)["Value"]
            sVomsAttr = self._escapeString(vomsAttr)["Value"]
            sPemData = self._escapeString(pemData)["Value"]
        except KeyError:
            return S_ERROR("Could not escape some data")
        cmd = (
            "REPLACE INTO `ProxyDB_VOMSProxies` ( UserName, UserDN, UserGroup, VOMSAttr, Pem, ExpirationTime ) VALUES "
        )
        cmd += "( %s, %s, %s, %s, %s, TIMESTAMPADD( SECOND, %d, UTC_TIMESTAMP() ) )" % (
            sUserName,
            sUserDN,
            sUserGroup,
            sVomsAttr,
            sPemData,
            secsLeft,
        )
        result = self._update(cmd, conn=connObj)
        if not result["OK"]:
            return result
        return S_OK(secsLeft)

    def getUsers(self, validSecondsLeft=0, userMask=None):
        """Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds

        :param int validSecondsLeft: validity period expressed in seconds
        :param str userMask: user name that need to add to search filter

        :return: S_OK(list)/S_ERROR() -- list contain dicts with user name, DN, expiration time
        """
        data = []
        sqlCond = []
        if validSecondsLeft:
            try:
                validSecondsLeft = int(validSecondsLeft)
            except ValueError:
                return S_ERROR("Seconds left has to be an integer")
            sqlCond.append("TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > %d" % validSecondsLeft)

        if userMask:
            try:
                sUserName = self._escapeString(userMask)["Value"]
            except KeyError:
                return S_ERROR("Can't escape user name")
            sqlCond.append(f"UserName = {sUserName}")

        cmd = "SELECT UserName, UserDN, ExpirationTime FROM `ProxyDB_CleanProxies`"
        if sqlCond:
            cmd += f" WHERE {' AND '.join(sqlCond)}"
        retVal = self._query(cmd)
        if not retVal["OK"]:
            return retVal
        for record in retVal["Value"]:
            record = list(record)
            data.append(
                {
                    "Name": record[0],
                    "DN": record[1],
                    "expirationtime": record[2],
                }
            )
        return S_OK(data)

    def getProxiesContent(self, selDict, sortList, start=0, limit=0):
        """Get the contents of the db, parameters are a filter to the db

        :param dict selDict: selection dict that contain fields and their posible values
        :param dict sortList: dict with sorting fields
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
        """
        data = []
        sqlWhere = ["Pem is not NULL"]
        fields = ("UserName", "UserDN", "ExpirationTime")
        cmd = f"SELECT {', '.join(fields)} FROM `ProxyDB_CleanProxies`"
        for field in selDict:
            if field not in fields:
                continue
            fVal = selDict[field]
            if isinstance(fVal, (dict, tuple, list)):
                sqlWhere.append(
                    f"{field} in ({', '.join([self._escapeString(str(value))['Value'] for value in fVal])})"
                )
            else:
                sqlWhere.append(f"{field} = {self._escapeString(str(fVal))['Value']}")
        sqlOrder = []
        if sortList:
            for sort in sortList:
                if len(sort) == 1:
                    sort = (sort, "DESC")
                elif len(sort) > 2:
                    return S_ERROR(f"Invalid sort {sort}")
                if sort[0] not in fields:
                    continue
                if sort[1].upper() not in ("ASC", "DESC"):
                    return S_ERROR(f"Invalid sorting order {sort[1]}")
                sqlOrder.append(f"{sort[0]} {sort[1]}")
        if sqlWhere:
            cmd = f"{cmd} WHERE {' AND '.join(sqlWhere)}"
        if sqlOrder:
            cmd = f"{cmd} ORDER BY {', '.join(sqlOrder)}"
        if limit:
            try:
                start = int(start)
                limit = int(limit)
            except ValueError:
                return S_ERROR("start and limit have to be integers")
            cmd += " LIMIT %d,%d" % (start, limit)
        retVal = self._query(cmd)
        if not retVal["OK"]:
            return retVal
        for record in retVal["Value"]:
            record = list(record)
            data.append(record)
        totalRecords = len(data)
        return S_OK({"ParameterNames": fields, "Records": data, "TotalRecords": totalRecords})

    def logAction(self, action, issuerDN, issuerGroup, targetDN, targetGroup):
        """Add an action to the log

        :param str action: proxy action
        :param str issuerDN: user DN of issuer
        :param str issuerGroup: DIRAC group of issuer
        :param str targetDN: user DN of target
        :param str targetGroup: DIRAC group of target

        :return: S_ERROR()
        """
        try:
            sAction = self._escapeString(action)["Value"]
            sIssuerDN = self._escapeString(issuerDN)["Value"]
            sIssuerGroup = self._escapeString(issuerGroup)["Value"]
            sTargetDN = self._escapeString(targetDN)["Value"]
            sTargetGroup = self._escapeString(targetGroup)["Value"]
        except KeyError:
            return S_ERROR("Can't escape from death")
        cmd = "INSERT INTO `ProxyDB_Log` ( Action, IssuerDN, IssuerGroup, TargetDN, TargetGroup, Timestamp ) VALUES "
        cmd += f"( {sAction}, {sIssuerDN}, {sIssuerGroup}, {sTargetDN}, {sTargetGroup}, UTC_TIMESTAMP() )"
        retVal = self._update(cmd)
        if not retVal["OK"]:
            self.log.error("Can't add a proxy action log: ", retVal["Message"])

    def purgeLogs(self):
        """Purge expired requests from the db

        :return: S_OK()/S_ERROR()
        """
        return self._update(
            "DELETE FROM `ProxyDB_Log` WHERE TIMESTAMPDIFF( SECOND, Timestamp, UTC_TIMESTAMP() ) > 15552000"
        )

    def getLogsContent(self, selDict, sortList, start=0, limit=0):
        """
        Function to get the contents of the logs table
          parameters are a filter to the db
        """
        fields = ("Action", "IssuerDN", "IssuerGroup", "TargetDN", "TargetGroup", "Timestamp")
        cmd = f"SELECT {', '.join(fields)} FROM `ProxyDB_Log`"
        if selDict:
            qr = []
            if "beforeDate" in selDict:
                qr.append(f"Timestamp < {self._escapeString(selDict['beforeDate'])['Value']}")
                del selDict["beforeDate"]
            if "afterDate" in selDict:
                qr.append(f"Timestamp > {self._escapeString(selDict['afterDate'])['Value']}")
                del selDict["afterDate"]
            for field in selDict:
                qr.append(
                    "(%s)"
                    % " OR ".join(
                        ["{}={}".format(field, self._escapeString(str(value))["Value"]) for value in selDict[field]]
                    )
                )
            whereStr = f" WHERE {' AND '.join(qr)}"
            cmd += whereStr
        else:
            whereStr = ""
        if sortList:
            cmd += f" ORDER BY {', '.join([f'{sort[0]} {sort[1]}' for sort in sortList])}"
        if limit:
            cmd += " LIMIT %d,%d" % (start, limit)
        retVal = self._query(cmd)
        if not retVal["OK"]:
            return retVal
        data = retVal["Value"]
        totalRecords = len(data)
        cmd = "SELECT COUNT( Timestamp ) FROM `ProxyDB_Log`"
        cmd += whereStr
        retVal = self._query(cmd)
        if retVal["OK"]:
            totalRecords = retVal["Value"][0][0]
        return S_OK({"ParameterNames": fields, "Records": data, "TotalRecords": totalRecords})

    def sendExpirationNotifications(self):
        """Send notification about expiration

        :return: S_OK(list)/S_ERROR() -- tuple list of user DN, group and proxy left time
        """
        notifLimits = sorted(int(x) for x in self.getCSOption("NotificationTimes", ProxyDB.NOTIFICATION_TIMES))

        for notifLimit in notifLimits:
            sqlSel = "UserName, TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime )"
            sqlCond = "TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) < %d" % notifLimit
            sqlCond += " AND TIMESTAMPDIFF( SECOND, UTC_TIMESTAMP(), ExpirationTime ) > %d" % (notifLimit - 86400)

            if not (result := self._query(f"SELECT {sqlSel} FROM `ProxyDB_CleanProxies` WHERE {sqlCond}"))["OK"]:
                return result
            for userName, lTime in result["Value"]:
                if not self._notifyProxyAboutToExpire(userName, lTime):
                    self.log.warn("Cannot send notification, will retry later")
                    break

        return S_OK()

    def _notifyProxyAboutToExpire(self, userName, lTime):
        """Send notification mail about to expire

        :param str userName: user name
        :param int lTime: left proxy live time in seconds

        :return: boolean
        """
        if not (userEMail := Registry.getUserOption(userName, "Email")):
            gLogger.error("Could not discover user email", userName)
            return False
        if not (userDN := Registry.getUserOption(userName, "DN")):
            gLogger.error("Could not discover user DN", userName)
            return False
        daysLeft = int(lTime / 86400)
        if daysLeft <= 0:
            return True  # Don't annoy users with -1 messages
        msgSubject = "Your proxy uploaded to DIRAC will expire in %d days" % daysLeft
        msgBody = textwrap.dedent(
            """\
                Dear %s,

                The proxy you uploaded to DIRAC will expire in aproximately %d days. The proxy
                information is:

                DN: %s

                If you plan on keep using this credentials, please upload a newer proxy by executing:

                $ dirac-proxy-init

                If you have been issued different certificate, please make sure you have a
                proxy uploaded with that certificate.

                Cheers,
                DIRAC's Proxy Manager
            """
            % (
                userName,
                daysLeft,
                userDN,
            )
        )
        fromAddr = self._mailFrom
        if not (
            result := self.__notifClient.sendMail(
                userEMail, msgSubject, msgBody, fromAddress=fromAddr, localAttempt=False
            )
        )["OK"]:
            gLogger.error("Could not send email", result["Message"])
            return False
        return True
