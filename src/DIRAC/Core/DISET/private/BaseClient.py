""" This module exposes the BaseClient class,
    which serves as base for InnerRPCClient and TransferClient.
"""
import time

import _thread

import DIRAC
from DIRAC.Core.DISET.private.Protocols import gProtocolDict
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL, getServiceFailoverURL
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import skipCACheck
from DIRAC.Core.DISET.private.TransportPool import getGlobalTransportPool
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig


class BaseClient:
    """Glues together stubs with threading, credentials, and URLs discovery (by DIRAC vo and setup).
    Basically what needs to be done to enable RPC calls, and transfer, to find a URL.
    """

    VAL_EXTRA_CREDENTIALS_HOST = "hosts"

    KW_USE_CERTIFICATES = "useCertificates"
    KW_EXTRA_CREDENTIALS = "extraCredentials"
    KW_TIMEOUT = "timeout"
    KW_SETUP = "setup"
    KW_VO = "VO"
    KW_DELEGATED_DN = "delegatedDN"
    KW_DELEGATED_GROUP = "delegatedGroup"
    KW_IGNORE_GATEWAYS = "ignoreGateways"
    KW_PROXY_LOCATION = "proxyLocation"
    KW_PROXY_STRING = "proxyString"
    KW_PROXY_CHAIN = "proxyChain"
    KW_SKIP_CA_CHECK = "skipCACheck"
    KW_KEEP_ALIVE_LAPSE = "keepAliveLapse"

    __threadConfig = ThreadConfig()

    def __init__(self, serviceName, **kwargs):
        """Constructor

        :param serviceName: URL of the service (proper uri or just System/Component)
        :param useCertificates: If set to True, use the server certificate
        :param extraCredentials:
        :param timeout: Timeout of the call (default 600 s)
        :param setup: Specify the Setup
        :param VO: Specify the VO
        :param delegatedDN: Not clear what it can be used for.
        :param delegatedGroup: Not clear what it can be used for.
        :param ignoreGateways: Ignore the DIRAC Gatways settings
        :param proxyLocation: Specify the location of the proxy
        :param proxyString: Specify the proxy string
        :param proxyChain: Specify the proxy chain
        :param skipCACheck: Do not check the CA
        :param keepAliveLapse: Duration for keepAliveLapse (heartbeat like)
        """

        if not isinstance(serviceName, str):
            raise TypeError(
                f"Service name expected to be a string. Received {str(serviceName)} type {type(serviceName)}"
            )
        # Explicitly convert to a str to avoid Python 2 M2Crypto issues with unicode objects
        self._destinationSrv = str(serviceName)
        self._serviceName = str(serviceName)
        self.kwargs = kwargs
        self.__useCertificates = None
        # The CS useServerCertificate option can be overridden by explicit argument
        self.__forceUseCertificates = self.kwargs.get(self.KW_USE_CERTIFICATES)
        self.__initStatus = S_OK()
        self.__idDict = {}
        self.__extraCredentials = ""
        self.__enableThreadCheck = False
        self.__retry = 0
        self.__retryDelay = 0
        # by default we always have 1 url for example:
        # RPCClient('dips://volhcb38.cern.ch:9162/Framework/SystemAdministrator')
        self.__nbOfUrls = 1
        self.__nbOfRetry = 3  # by default we try try times
        self.__retryCounter = 1
        self.__bannedUrls = []
        for initFunc in (
            self.__discoverVO,
            self.__discoverTimeout,
            self.__discoverURL,
            self.__discoverCredentialsToUse,
            self.__checkTransportSanity,
            self.__setKeepAliveLapse,
        ):
            result = initFunc()
            if not result["OK"] and self.__initStatus["OK"]:
                self.__initStatus = result
        self.numberOfURLs = 0
        self._initialize()
        # HACK for thread-safety:
        self.__allowedThreadID = False

    def _initialize(self):
        pass

    def getDestinationService(self):
        """Return service destination

        :return: str
        """
        return self._destinationSrv

    def getServiceName(self):
        """Return service name

        :return: str
        """
        return self._serviceName

    def __discoverVO(self):
        """Discover which VO to use and stores it in self.vo
        The VO is looked for:
           * kwargs of the constructor (see KW_VO)
           * in the CS /DIRAC/VirtualOrganization
           * default to 'unknown'

        :return: S_OK()/S_ERROR()
        """
        if self.KW_VO in self.kwargs and self.kwargs[self.KW_VO]:
            self.vo = str(self.kwargs[self.KW_VO])
        else:
            self.vo = gConfig.getValue("/DIRAC/VirtualOrganization", "unknown")
        return S_OK()

    def __discoverURL(self):
        """Calculate the final URL. It is called at initialization and in connect in case of issue

        It sets:
          * self.serviceURL: the url (dips) selected as target using __findServiceURL
          * self.__URLTuple: a split of serviceURL obtained by Network.splitURL
          * self._serviceName: the last part of URLTuple (typically System/Component)

        :return: S_OK()/S_ERROR()
        """
        # Calculate final URL
        try:
            result = self.__findServiceURL()
        except Exception as e:
            return S_ERROR(repr(e))
        if not result["OK"]:
            return result
        self.serviceURL = result["Value"]
        retVal = Network.splitURL(self.serviceURL)
        if not retVal["OK"]:
            return retVal
        self.__URLTuple = retVal["Value"]
        self._serviceName = self.__URLTuple[-1]
        res = gConfig.getOptionsDict("/DIRAC/ConnConf/%s:%s" % self.__URLTuple[1:3])
        if res["OK"]:
            opts = res["Value"]
            for k in opts:
                if k not in self.kwargs:
                    self.kwargs[k] = opts[k]
        return S_OK()

    def __discoverTimeout(self):
        """Discover which timeout to use and stores it in self.timeout
        The timeout can be specified kwargs of the constructor (see KW_TIMEOUT),
        with a minimum of 120 seconds.
        If unspecified, the timeout will be 600 seconds.
        The value is set in self.timeout, as well as in self.kwargs[KW_TIMEOUT]

        :return: S_OK()/S_ERROR()
        """
        if self.KW_TIMEOUT in self.kwargs:
            self.timeout = self.kwargs[self.KW_TIMEOUT]
        else:
            self.timeout = False
        if self.timeout:
            self.timeout = max(120, self.timeout)
        else:
            self.timeout = 600
        self.kwargs[self.KW_TIMEOUT] = self.timeout
        return S_OK()

    def __discoverCredentialsToUse(self):
        """Discovers which credentials to use for connection.

        * Server certificate:
          -> If KW_USE_CERTIFICATES in kwargs, sets it in self.__useCertificates
          -> If not, check gConfig.useServerCertificate(),
             and sets it in self.__useCertificates and kwargs[KW_USE_CERTIFICATES]

        * Certification Authorities check:
           -> if KW_SKIP_CA_CHECK is not in kwargs and we are using the certificates,
              set KW_SKIP_CA_CHECK to false in kwargs
           -> if KW_SKIP_CA_CHECK is not in kwargs and we are not using the certificate, check the CS.skipCACheck

        * Proxy Chain
           -> if KW_PROXY_CHAIN in kwargs, we remove it and dump its string form into kwargs[KW_PROXY_STRING]

        :return: S_OK()/S_ERROR()
        """
        # Use certificates?
        if self.KW_USE_CERTIFICATES in self.kwargs:
            self.__useCertificates = self.kwargs[self.KW_USE_CERTIFICATES]
        else:
            self.__useCertificates = gConfig.useServerCertificate()
            self.kwargs[self.KW_USE_CERTIFICATES] = self.__useCertificates
        if self.KW_SKIP_CA_CHECK not in self.kwargs:
            if self.__useCertificates:
                self.kwargs[self.KW_SKIP_CA_CHECK] = False
            else:
                self.kwargs[self.KW_SKIP_CA_CHECK] = skipCACheck()
        if self.KW_PROXY_CHAIN in self.kwargs:
            try:
                self.kwargs[self.KW_PROXY_STRING] = self.kwargs[self.KW_PROXY_CHAIN].dumpAllToString()["Value"]
                del self.kwargs[self.KW_PROXY_CHAIN]
            except Exception:
                return S_ERROR("Invalid proxy chain specified on instantiation")
        return S_OK()

    def __discoverExtraCredentials(self):
        """Add extra credentials informations.
        * self.__extraCredentials
          -> if KW_EXTRA_CREDENTIALS in kwargs, we set it
          -> Otherwise, if we use the server certificate, we set it to VAL_EXTRA_CREDENTIALS_HOST
          -> If we have a delegation (see below), we set it to (delegatedDN, delegatedGroup)
          -> otherwise it is an empty string
        * delegation:
          -> if KW_DELEGATED_DN in kwargs, or delegatedDN in threadConfig, put in in self.kwargs
          -> if KW_DELEGATED_GROUP in kwargs or delegatedGroup in threadConfig, put it in self.kwargs
          -> If we have a delegated DN but not group, we find the corresponding group in the CS

        :return: S_OK()/S_ERROR()
        """
        # which extra credentials to use?
        self.__extraCredentials = (
            self.VAL_EXTRA_CREDENTIALS_HOST
            if (self.__useCertificates and not self.kwargs.get(self.KW_PROXY_LOCATION))
            else ""
        )
        if self.KW_EXTRA_CREDENTIALS in self.kwargs:
            self.__extraCredentials = self.kwargs[self.KW_EXTRA_CREDENTIALS]

        # Are we delegating something?
        delegatedDN = self.kwargs.get(self.KW_DELEGATED_DN) or self.__threadConfig.getDN()
        delegatedGroup = self.kwargs.get(self.KW_DELEGATED_GROUP) or self.__threadConfig.getGroup()
        if delegatedDN:
            self.kwargs[self.KW_DELEGATED_DN] = delegatedDN
            if not delegatedGroup:
                result = Registry.findDefaultGroupForDN(delegatedDN)
                if not result["OK"]:
                    return result
                delegatedGroup = result["Value"]
            self.kwargs[self.KW_DELEGATED_GROUP] = delegatedGroup
            self.__extraCredentials = (delegatedDN, delegatedGroup)
        return S_OK()

    def __findServiceURL(self):
        """Discovers the URL of a service, taking into account gateways, multiple URLs, banned URLs


        If the site on which we run is configured to use gateways (/DIRAC/Gateways/<siteName>),
        these URLs will be used. To ignore the gateway, it is possible to set KW_IGNORE_GATEWAYS
        to False in kwargs.

        If self._destinationSrv (given as constructor attribute) is a properly formed URL,
        we just return this one. If we have to use a gateway, we just replace the server name in the url.

        The list of URLs defined in the CS (<System>/URLs/<Component>) is randomized

        This method also sets some attributes:
          * self.__nbOfUrls = number of URLs
          * self.__nbOfRetry = 2 if we have more than 2 urls, otherwise 3
          * self.__bannedUrls is reinitialized if all the URLs are banned

        :return: S_OK(str)/S_ERROR() -- the selected URL
        """
        if not self.__initStatus["OK"]:
            return self.__initStatus

        # Load the Gateways URLs for the current site Name
        gatewayURL = False
        if not self.kwargs.get(self.KW_IGNORE_GATEWAYS):
            dRetVal = gConfig.getOption(f"/DIRAC/Gateways/{DIRAC.siteName()}")
            if dRetVal["OK"]:
                rawGatewayURL = List.randomize(List.fromChar(dRetVal["Value"], ","))[0]
                gatewayURL = "/".join(rawGatewayURL.split("/")[:3])

        # If what was given as constructor attribute is a properly formed URL,
        # we just return this one.
        # If we have to use a gateway, we just replace the server name in it
        for protocol in gProtocolDict:
            if self._destinationSrv.find(f"{protocol}://") == 0:
                gLogger.debug("Already given a valid url", self._destinationSrv)
                if not gatewayURL:
                    return S_OK(self._destinationSrv)
                gLogger.debug("Reconstructing given URL to pass through gateway")
                path = "/".join(self._destinationSrv.split("/")[3:])
                finalURL = f"{gatewayURL}/{path}"
                gLogger.debug(f"Gateway URL conversion:\n {self._destinationSrv} -> {finalURL}")
                return S_OK(finalURL)

        if gatewayURL:
            gLogger.debug("Using gateway", gatewayURL)
            return S_OK(f"{gatewayURL}/{self._destinationSrv}")

        # We extract the list of URLs from the CS (System/URLs/Component)
        try:
            urls = getServiceURL(self._destinationSrv)
        except Exception as e:
            return S_ERROR(f"Cannot get URL for {self._destinationSrv} : {repr(e)}")
        if not urls:
            return S_ERROR(f"URL for service {self._destinationSrv} not found")

        failoverUrls = []
        # Try if there are some failover URLs to use as last resort
        try:
            failoverUrlsStr = getServiceFailoverURL(self._destinationSrv)
            if failoverUrlsStr:
                failoverUrls = failoverUrlsStr.split(",")
        except Exception:
            pass

        # We randomize the list, and add at the end the failover URLs (System/FailoverURLs/Component)
        urlsList = List.fromChar(urls, ",") + failoverUrls
        self.__nbOfUrls = len(urlsList)
        self.__nbOfRetry = (
            2 if self.__nbOfUrls > 2 else 3
        )  # we retry 2 times all services, if we run more than 2 services
        if self.__nbOfUrls == len(self.__bannedUrls):
            self.__bannedUrls = []  # retry all urls
            gLogger.debug("Retrying again all URLs")

        if len(self.__bannedUrls) > 0 and len(urlsList) > 1:
            # we have host which is not accessible. We remove that host from the list.
            # We only remove if we have more than one instance
            for i in self.__bannedUrls:
                gLogger.debug("Removing banned URL", f"{i}")
                urlsList.remove(i)

        # Take the first URL from the list
        # randUrls = List.randomize( urlsList ) + failoverUrls

        sURL = urlsList[0]

        # If we have banned URLs, and several URLs at disposals, we make sure that the selected sURL
        # is not on a host which is banned. If it is, we take the next one in the list using __selectUrl
        # If we have banned URLs, and several URLs at disposals, we make sure that the selected sURL
        # is not on a host which is banned. If it is, we take the next one in the list using __selectUrl

        if len(self.__bannedUrls) > 0 and self.__nbOfUrls > 2:  # when we have multiple services then we can
            # have a situation when two services are running on the same machine with different ports...
            retVal = Network.splitURL(sURL)
            nexturl = None
            if retVal["OK"]:
                nexturl = retVal["Value"]

                found = False
                for i in self.__bannedUrls:
                    retVal = Network.splitURL(i)
                    if retVal["OK"]:
                        bannedurl = retVal["Value"]
                    else:
                        break
                    # We found a banned URL on the same host as the one we are running on
                    if nexturl[1] == bannedurl[1]:
                        found = True
                        break
                if found:
                    nexturl = self.__selectUrl(nexturl, urlsList[1:])
                    if nexturl:  # an url found which is in different host
                        sURL = nexturl
        gLogger.debug("Discovering URL for service", f"{self._destinationSrv} -> {sURL}")
        return S_OK(sURL)

    def __selectUrl(self, notselect, urls):
        """In case when multiple services are running in the same host, a new url has to be in a different host
        Note: If we do not have different host we will use the selected url...

        :param notselect: URL that should NOT be selected
        :param list urls: list of potential URLs

        :return: str -- selected URL
        """
        url = None
        for i in urls:
            retVal = Network.splitURL(i)
            if retVal["OK"]:
                if retVal["Value"][1] != notselect[1]:  # the hosts are different
                    url = i
                    break
                else:
                    gLogger.error(retVal["Message"])
        return url

    def __checkThreadID(self):
        """
        ..warning:: just guessing....
        This seems to check that we are not creating a client and then using it
        in a multithreaded environment.
        However, it is triggered only if self.__enableThreadCheck is to True, but it is
        hardcoded to False, and does not seem to be modified anywhere in the code.
        """
        if not self.__initStatus["OK"]:
            return self.__initStatus
        cThID = _thread.get_ident()
        if not self.__allowedThreadID:
            self.__allowedThreadID = cThID
        elif cThID != self.__allowedThreadID:
            msgTxt = f"""
=======DISET client thread safety error========================
Client {str(self)}
can only run on thread {self.__allowedThreadID}
and this is thread {cThID}
==============================================================="""
            gLogger.error("DISET client thread safety error", msgTxt)
            # raise Exception( msgTxt )

    def _connect(self):
        """Establish the connection.
        It uses the URL discovered in __discoverURL.
        In case the connection cannot be established, __discoverURL
        is called again, and _connect calls itself.
        We stop after trying self.__nbOfRetry * self.__nbOfUrls

        :return: S_OK()/S_ERROR()
        """
        # Check if the useServerCertificate configuration changed
        # Note: I am not really sure that  all this block makes
        # any sense at all since all these variables are
        # evaluated in __discoverCredentialsToUse
        if gConfig.useServerCertificate() != self.__useCertificates:
            if self.__forceUseCertificates is None:
                self.__useCertificates = gConfig.useServerCertificate()
                self.kwargs[self.KW_USE_CERTIFICATES] = self.__useCertificates
                # The server certificate use context changed, rechecking the transport sanity
                result = self.__checkTransportSanity()
                if not result["OK"]:
                    return result

        # Take all the extra credentials
        self.__discoverExtraCredentials()
        if not self.__initStatus["OK"]:
            return self.__initStatus
        if self.__enableThreadCheck:
            self.__checkThreadID()
        gLogger.debug(f"Trying to connect to: {self.serviceURL}")
        try:
            # Calls the transport method of the apropriate protocol.
            # self.__URLTuple[1:3] = [server name, port, System/Component]
            transport = gProtocolDict[self.__URLTuple[0]]["transport"](self.__URLTuple[1:3], **self.kwargs)
            # the socket timeout is the default value which is 1.
            # later we increase to 5
            retVal = transport.initAsClient()
            # We try at most __nbOfRetry each URLs
            if not retVal["OK"]:
                gLogger.warn("Issue getting socket:", f"{transport} : {self.__URLTuple} : {retVal['Message']}")
                # We try at most __nbOfRetry each URLs
                if self.__retry < self.__nbOfRetry * self.__nbOfUrls - 1:
                    # Recompose the URL (why not using self.serviceURL ? )
                    url = "%s://%s:%d/%s" % (
                        self.__URLTuple[0],
                        self.__URLTuple[1],
                        int(self.__URLTuple[2]),
                        self.__URLTuple[3],
                    )
                    # Add the url to the list of banned URLs if it is not already there. (Can it happen ? I don't think so)
                    if url not in self.__bannedUrls:
                        gLogger.warn("Non-responding URL temporarily banned", f"{url}")
                        self.__bannedUrls += [url]
                    # Increment the retry counter
                    self.__retry += 1
                    # 16.07.20 CHRIS: I guess this setSocketTimeout does not behave as expected.
                    # If the initasClient did not work, we anyway re-enter the whole method,
                    # so a new transport object is created.
                    # However, it migh be that this timeout value was propagated down to the
                    # SocketInfoFactory singleton, and thus used, but that means that the timeout
                    # specified in parameter was then void.

                    # If it is our last attempt for each URL, we increase the timeout
                    if self.__retryCounter == self.__nbOfRetry - 1:
                        transport.setSocketTimeout(5)  # we increase the socket timeout in case the network is not good
                    gLogger.info("Retry connection", ": %d to %s" % (self.__retry, self.serviceURL))
                    # If we tried all the URL, we increase the global counter (__retryCounter), and sleep
                    if len(self.__bannedUrls) == self.__nbOfUrls:
                        self.__retryCounter += 1
                        # we run only one service! In that case we increase the retry delay.
                        self.__retryDelay = 3.0 / self.__nbOfUrls if self.__nbOfUrls > 1 else 2
                        gLogger.info(f"Waiting {self.__retryDelay:f} seconds before retry all service(s)")
                        time.sleep(self.__retryDelay)
                    # rediscover the URL
                    self.__discoverURL()
                    # try to reconnect
                    return self._connect()
                else:
                    return retVal
        except Exception as e:
            gLogger.exception(lException=True, lExcInfo=True)
            return S_ERROR(f"Can't connect to {self.serviceURL}: {repr(e)}")
        # We add the connection to the transport pool
        gLogger.debug(f"Connected to: {self.serviceURL}")
        trid = getGlobalTransportPool().add(transport)

        return S_OK((trid, transport))

    def _disconnect(self, trid):
        """Disconnect the connection.

        :param str trid: Transport ID in the transportPool
        """
        getGlobalTransportPool().close(trid)

    @staticmethod
    def _serializeStConnectionInfo(stConnectionInfo):
        """We want to send tuple but we need to convert
        into a list
        """
        serializedTuple = [list(x) if isinstance(x, tuple) else x for x in stConnectionInfo]

        return serializedTuple

    def _proposeAction(self, transport, action):
        """Proposes an action by sending a tuple containing

          * System/Component
          * Setup
          * VO
          * action
          * extraCredentials

        It is kind of a handshake.

        The server might ask for a delegation, in which case it is done here.
        The result of the delegation is then returned.

        :param transport: the Transport object returned by _connect
        :param action: tuple (<action type>, <action name>). It depends on the
                       subclasses of BaseClient. <action type> can be for example
                       'RPC' or 'FileTransfer'

        :return: whatever the server sent back

        """
        if not self.__initStatus["OK"]:
            return self.__initStatus
        stConnectionInfo = ((self.__URLTuple[3], "None", self.vo), action, self.__extraCredentials, DIRAC.version)

        # Send the connection info and get the answer back
        retVal = transport.sendData(S_OK(BaseClient._serializeStConnectionInfo(stConnectionInfo)))
        if not retVal["OK"]:
            return retVal
        serverReturn = transport.receiveData()

        # TODO: Check if delegation is required. This seems to be used only for the GatewayService
        if serverReturn["OK"] and "Value" in serverReturn and isinstance(serverReturn["Value"], dict):
            gLogger.debug("There is a server requirement")
            serverRequirements = serverReturn["Value"]
            if "delegate" in serverRequirements:
                gLogger.debug("A delegation is requested")
                serverReturn = self.__delegateCredentials(transport, serverRequirements["delegate"])
        return serverReturn

    def __delegateCredentials(self, transport, delegationRequest):
        """Perform a credential delegation. This seems to be used only for the GatewayService.
        It calls the delegation mechanism of the Transport class. Note that it is not used when
        delegating credentials to the ProxyDB

        :param transport: the Transport object returned by _connect
        :param delegationRequest: delegation request

        :return: S_OK()/S_ERROR()
        """
        retVal = gProtocolDict[self.__URLTuple[0]]["delegation"](delegationRequest, self.kwargs)
        if not retVal["OK"]:
            return retVal
        retVal = transport.sendData(retVal["Value"])
        if not retVal["OK"]:
            return retVal
        return transport.receiveData()

    def __checkTransportSanity(self):
        """Calls the sanity check of the underlying Transport object
        and stores the result in self.__idDict.
        It is checked at the creation of the BaseClient, and when connecting
        if the use of the certificate has changed.

        :return: S_OK()/S_ERROR()
        """
        if not self.__initStatus["OK"]:
            return self.__initStatus
        retVal = gProtocolDict[self.__URLTuple[0]]["sanity"](self.__URLTuple[1:3], self.kwargs)
        if not retVal["OK"]:
            return retVal
        idDict = retVal["Value"]
        for key in idDict:
            self.__idDict[key] = idDict[key]
        return S_OK()

    def __setKeepAliveLapse(self):
        """Select the maximum Keep alive lapse between
        150 seconds and what is specifind in kwargs[KW_KEEP_ALIVE_LAPSE],
        and sets it in kwargs[KW_KEEP_ALIVE_LAPSE]

        :return: S_OK()/S_ERROR()
        """
        kaa = 1
        if self.KW_KEEP_ALIVE_LAPSE in self.kwargs:
            try:
                kaa = max(0, int(self.kwargs[self.KW_KEEP_ALIVE_LAPSE]))
            except Exception:
                pass
        if kaa:
            kaa = max(150, kaa)
        self.kwargs[self.KW_KEEP_ALIVE_LAPSE] = kaa
        return S_OK()

    def _getBaseStub(self):
        """Returns a list with [self._destinationSrv, newKwargs]
        self._destinationSrv is what was given as first parameter of the init serviceName

        newKwargs is an updated copy of kwargs:
          * if set, we remove the useCertificates (KW_USE_CERTIFICATES) in newKwargs

        This method is just used to return information in case of error in the InnerRPCClient

        :return: tuple
        """
        newKwargs = dict(self.kwargs)
        # Remove useCertificates as the forwarder of the call will have to
        # independently decide whether to use their cert or not anyway.
        if "useCertificates" in newKwargs:
            del newKwargs["useCertificates"]
        return [self._destinationSrv, newKwargs]

    def __bool__(self):
        return True

    def __str__(self):
        return f"<DISET Client {self.serviceURL} {self.__extraCredentials}>"
