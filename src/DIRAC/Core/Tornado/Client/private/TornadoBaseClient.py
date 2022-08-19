"""
    TornadoBaseClient contains all the low-levels functionalities and initilization methods
    It must be instantiated from :py:class:`~DIRAC.Core.Tornado.Client.TornadoClient`

    Requests library manage itself retry when connection failed, so the __nbOfRetry attribute is removed from DIRAC
    (For each URL requests manage retries himself, if it still fail, we try next url)
    KeepAlive lapse is also removed because managed by request,
    see https://requests.readthedocs.io/en/latest/user/advanced/#keep-alive

    If necessary this class can be modified to define number of retry in requests, documentation does not give
    lot of informations but you can see this simple solution from StackOverflow.
    After some tests request seems to retry 3 times by default.
    https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request

    .. warning::
      If you use your own certificates, it's like in dips, please take a look at :ref:`using_own_CA`

    .. warning::
      Lots of method are copy-paste from :py:class:`~DIRAC.Core.DISET.private.BaseClient`.
      And some methods are copy-paste AND modifications, for now it permit to fully separate DISET and HTTPS.


"""

# pylint: disable=broad-except
import io
import errno
import os
import requests
import ssl
import tempfile
from http import HTTPStatus


from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue

from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import skipCACheck
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import findDefaultGroupForDN
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURLs, getGatewayURLs

from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities import Network
from DIRAC.Core.Utilities.JEncode import decode, encode


# TODO CHRIS: refactor all the messy `discover` methods
# I do not do it now because I want first to decide
# whether we go with code copy of fatorization


class TornadoBaseClient:
    """
    This class contain initialization method and all utilities method used for RPC
    """

    __threadConfig = ThreadConfig()
    VAL_EXTRA_CREDENTIALS_HOST = "hosts"

    KW_USE_ACCESS_TOKEN = "useAccessToken"
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

    def __init__(self, serviceName, **kwargs):
        """
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
        :param keepAliveLapse: Duration for keepAliveLapse (heartbeat like)  (now managed by requests)
        """

        if not isinstance(serviceName, str):
            raise TypeError(
                f"Service name expected to be a string. Received {str(serviceName)} type {type(serviceName)}"
            )

        self._destinationSrv = serviceName
        self._serviceName = serviceName
        self.__session = None

        self.kwargs = kwargs
        self.__idp = None
        self.__useAccessToken = None
        self.__useCertificates = None
        # The CS useServerCertificate option can be overridden by explicit argument
        self.__forceUseCertificates = self.kwargs.get(self.KW_USE_CERTIFICATES)
        self.__initStatus = S_OK()
        self.__idDict = {}
        self.__extraCredentials = ""
        # by default we always have 1 url for example:
        # RPCClient('dips://volhcb38.cern.ch:9162/Framework/SystemAdministrator')
        self.__nbOfUrls = 1
        self.__bannedUrls = []

        # For pylint...
        self.setup = None
        self.vo = None
        self.serviceURL = None

        for initFunc in (
            self.__discoverTimeout,
            self.__discoverSetup,
            self.__discoverVO,
            self.__discoverCredentialsToUse,
            self.__discoverExtraCredentials,
            self.__discoverURL,
        ):

            result = initFunc()
            if not result["OK"] and self.__initStatus["OK"]:
                self.__initStatus = result

    def __discoverSetup(self):
        """Discover which setup to use and stores it in self.setup
        The setup is looked for:
           * kwargs of the constructor (see KW_SETUP)
           * in the CS /DIRAC/Setup
           * default to 'Test'
        """
        if self.KW_SETUP in self.kwargs and self.kwargs[self.KW_SETUP]:
            self.setup = str(self.kwargs[self.KW_SETUP])
        else:
            self.setup = self.__threadConfig.getSetup()
            if not self.setup:
                self.setup = gConfig.getValue("/DIRAC/Setup", "Test")
        return S_OK()

    def __discoverURL(self):
        """Calculate the final URL. It is called at initialization and in connect in case of issue

          It sets:
            * self.serviceURL: the url (dips) selected as target using __findServiceURL
            * self.__URLTuple: a split of serviceURL obtained by Network.splitURL
            * self._serviceName: the last part of URLTuple (typically System/Component)

        WARNING: COPY PASTE FROM BaseClient
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

    def __discoverVO(self):
        """Discover which VO to use and stores it in self.vo
        The VO is looked for:
           * kwargs of the constructor (see KW_VO)
           * in the CS /DIRAC/VirtualOrganization
           * default to 'unknown'

        WARNING: COPY/PASTE FROM Core/Diset/private/BaseClient FOR NOW
        """
        if self.KW_VO in self.kwargs and self.kwargs[self.KW_VO]:
            self.vo = str(self.kwargs[self.KW_VO])
        else:
            self.vo = gConfig.getValue("/DIRAC/VirtualOrganization", "unknown")
        return S_OK()

    def __discoverCredentialsToUse(self):
        """Discovers which credentials to use for connection.
        * Server certificate:
          -> If KW_USE_CERTIFICATES in kwargs, sets it in self.__useCertificates
          -> If not, check gConfig.useServerCertificate(), and sets it in self.__useCertificates
              and kwargs[KW_USE_CERTIFICATES]
        * Certification Authorities check:
           -> if KW_SKIP_CA_CHECK is not in kwargs and we are using the certificates,
                set KW_SKIP_CA_CHECK to false in kwargs
           -> if KW_SKIP_CA_CHECK is not in kwargs and we are not using the certificate, check the skipCACheck
        * Bearer token:
          -> If KW_USE_ACCESS_TOKEN in kwargs, sets it in self.__useAccessToken
          -> If not, check "/DIRAC/Security/UseTokens", and sets it in self.__useAccessToken
              and kwargs[KW_USE_ACCESS_TOKEN]
          -> If not, check 'DIRAC_USE_ACCESS_TOKEN' in os.environ, sets it in self.__useAccessToken
              and kwargs[KW_USE_ACCESS_TOKEN]
        * Proxy Chain

        WARNING: MOSTLY COPY/PASTE FROM Core/Diset/private/BaseClient

        """
        # Use certificates?
        if self.KW_USE_CERTIFICATES in self.kwargs:
            self.__useCertificates = self.kwargs[self.KW_USE_CERTIFICATES]
        else:
            self.__useCertificates = gConfig.useServerCertificate()
            self.kwargs[self.KW_USE_CERTIFICATES] = self.__useCertificates

        # Prepare the session
        skip_ca_check = self.kwargs.get(self.KW_SKIP_CA_CHECK, False if self.__useCertificates else skipCACheck())
        retVal = _create_session(verified=not skip_ca_check)
        if not retVal["OK"]:  # pylint: disable=unsubscriptable-object
            return retVal
        self.__session = retVal["Value"]  # pylint: disable=unsubscriptable-object

        # Use tokens?
        if self.KW_USE_ACCESS_TOKEN in self.kwargs:
            self.__useAccessToken = self.kwargs[self.KW_USE_ACCESS_TOKEN]
        elif "DIRAC_USE_ACCESS_TOKEN" in os.environ:
            self.__useAccessToken = os.environ.get("DIRAC_USE_ACCESS_TOKEN", "false").lower() in ("y", "yes", "true")
        else:
            self.__useAccessToken = gConfig.getValue("/DIRAC/Security/UseTokens", "false").lower() in (
                "y",
                "yes",
                "true",
            )
        self.kwargs[self.KW_USE_ACCESS_TOKEN] = self.__useAccessToken

        if self.__useAccessToken:
            from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

            result = IdProviderFactory().getIdProvider("DIRACCLI")
            if not result["OK"]:
                return result
            self.__idp = result["Value"]

        # Rewrite a little bit from here: don't need the proxy string, we use the file
        if self.KW_PROXY_CHAIN in self.kwargs:
            try:
                self.kwargs[self.KW_PROXY_STRING] = self.kwargs[self.KW_PROXY_CHAIN].dumpAllToString()["Value"]
                del self.kwargs[self.KW_PROXY_CHAIN]
            except Exception:
                return S_ERROR("Invalid proxy chain specified on instantiation")

        # ==== REWRITED FROM HERE ====

        # For certs always check CA's. For clients skipServerIdentityCheck

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
              -> If we have a delegated DN but not group, we find the corresponding group in the CS

        WARNING: COPY/PASTE FROM Core/Diset/private/BaseClient
        """
        # which extra credentials to use?
        if self.__useCertificates:
            self.__extraCredentials = self.VAL_EXTRA_CREDENTIALS_HOST
        else:
            self.__extraCredentials = ""
        if self.KW_EXTRA_CREDENTIALS in self.kwargs:
            self.__extraCredentials = self.kwargs[self.KW_EXTRA_CREDENTIALS]
        # Are we delegating something?
        delegatedDN, delegatedGroup = self.__threadConfig.getID()
        if self.KW_DELEGATED_DN in self.kwargs and self.kwargs[self.KW_DELEGATED_DN]:
            delegatedDN = self.kwargs[self.KW_DELEGATED_DN]
        elif delegatedDN:
            self.kwargs[self.KW_DELEGATED_DN] = delegatedDN
        if self.KW_DELEGATED_GROUP in self.kwargs and self.kwargs[self.KW_DELEGATED_GROUP]:
            delegatedGroup = self.kwargs[self.KW_DELEGATED_GROUP]
        elif delegatedGroup:
            self.kwargs[self.KW_DELEGATED_GROUP] = delegatedGroup
        if delegatedDN:
            if not delegatedGroup:
                result = findDefaultGroupForDN(self.kwargs[self.KW_DELEGATED_DN])
                if not result["OK"]:
                    return result
            self.__extraCredentials = (delegatedDN, delegatedGroup)
        return S_OK()

    def __discoverTimeout(self):
        """Discover which timeout to use and stores it in self.timeout
        The timeout can be specified kwargs of the constructor (see KW_TIMEOUT),
        with a minimum of 120 seconds.
        If unspecified, the timeout will be 600 seconds.
        The value is set in self.timeout, as well as in self.kwargs[KW_TIMEOUT]

        WARNING: COPY/PASTE FROM Core/Diset/private/BaseClient
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

    def __findServiceURL(self):
        """
        Discovers the URL of a service, taking into account gateways, multiple URLs, banned URLs


        If the site on which we run is configured to use gateways (/DIRAC/Gateways/<siteName>),
        these URLs will be used. To ignore the gateway, it is possible to set KW_IGNORE_GATEWAYS
        to False in kwargs.

        If self._destinationSrv (given as constructor attribute) is a properly formed URL,
        we just return this one. If we have to use a gateway, we just replace the server name in the url.

        The list of URLs defined in the CS (<System>/URLs/<Component>) is randomized

        This method also sets some attributes:
          * self.__nbOfUrls = number of URLs
          * self.__nbOfRetry removed in HTTPS (Managed by requests)
          * self.__bannedUrls is reinitialized if all the URLs are banned

        :return: the selected URL

        WARNING (Mostly) COPY PASTE FROM BaseClient (protocols list is changed to https)

        """
        if not self.__initStatus["OK"]:
            return self.__initStatus

        # Load the Gateways URLs for the current site Name
        gatewayURL = False
        if not self.kwargs.get(self.KW_IGNORE_GATEWAYS):
            gatewayURLs = getGatewayURLs()
            if gatewayURLs:
                gatewayURL = "/".join(gatewayURLs[0].split("/")[:3])

        # If what was given as constructor attribute is a properly formed URL,
        # we just return this one.
        # If we have to use a gateway, we just replace the server name in it
        if self._destinationSrv.startswith("https://"):
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

        # If nor url is given as constructor, we extract the list of URLs from the CS (System/URLs/Component)
        try:
            # We randomize the list, and add at the end the failover URLs (System/FailoverURLs/Component)
            urlsList = getServiceURLs(self._destinationSrv, setup=self.setup, failover=True)
        except Exception as e:
            return S_ERROR(f"Cannot get URL for {self._destinationSrv} in setup {self.setup}: {repr(e)}")
        if not urlsList:
            return S_ERROR("URL for service %s not found" % self._destinationSrv)

        self.__nbOfUrls = len(urlsList)
        # __nbOfRetry removed in HTTPS (managed by requests)
        if self.__nbOfUrls == len(self.__bannedUrls):
            self.__bannedUrls = []  # retry all urls
            gLogger.debug("Retrying again all URLs")

        if self.__bannedUrls and len(urlsList) > 1:
            # we have host which is not accessible. We remove that host from the list.
            # We only remove if we have more than one instance
            for i in self.__bannedUrls:
                gLogger.debug("Removing banned URL", "%s" % i)
                urlsList.remove(i)

        sURL = urlsList[0]

        # If we have banned URLs, and several URLs at disposals, we make sure that the selected sURL
        # is not on a host which is banned. If it is, we take the next one in the list using __selectUrl

        if self.__bannedUrls and self.__nbOfUrls > 2:  # when we have multiple services then we can
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
        :param urls: list of potential URLs

        :return: selected URL

        WARNING: COPY/PASTE FROM Core/Diset/private/BaseClient
        """
        url = None
        for i in urls:
            retVal = Network.splitURL(i)
            if retVal["OK"]:
                if retVal["Value"][1] != notselect[1]:  # the hots are different
                    url = i
                    break
                else:
                    gLogger.error(retVal["Message"])
        return url

    def getServiceName(self):
        """
        Returns the name of the service, if you had given a url at init, returns the URL.
        """
        return self._serviceName

    def getDestinationService(self):
        """
        Returns the url the service.
        """
        urls = getServiceURLs(self._serviceName)
        return urls[0] if urls else ""

    def _getBaseStub(self):
        """Returns a tuple with (self._destinationSrv, newKwargs)
        self._destinationSrv is what was given as first parameter of the init serviceName

        newKwargs is an updated copy of kwargs:
          * if set, we remove the useCertificates (KW_USE_CERTIFICATES) in newKwargs

        This method is just used to return information in case of error in the InnerRPCClient

        WARNING: COPY/PASTE FROM Core/Diset/private/BaseClient
        """
        newKwargs = dict(self.kwargs)
        # Remove useCertificates as the forwarder of the call will have to
        # independently decide whether to use their cert or not anyway.
        if "useCertificates" in newKwargs:
            del newKwargs["useCertificates"]
        return (self._destinationSrv, newKwargs)

    def _request(self, retry=0, outputFile=None, **kwargs):
        """
        Sends the request to server

        :param retry: internal parameters for recursive call. TODO: remove ?
        :param outputFile: (default None) can be the path to a file, or the file itself where to store the received data.
                          If set, the server response will be streamed for optimization
                          purposes, and the response data will not go through the
                          JDecode process
        :param **kwargs: Any argument there is used as a post parameter. They are detailed bellow.
        :param method: (mandatory) name of the distant method
        :param args: (mandatory) json serialized list of argument for the procedure



        :returns: The received data. If outputFile is set, return always S_OK

        """

        # Adding some informations to send
        if self.__extraCredentials:
            kwargs[self.KW_EXTRA_CREDENTIALS] = encode(self.__extraCredentials)
        kwargs["clientVO"] = self.vo
        kwargs["clientSetup"] = self.setup

        # Getting URL
        url = self.__findServiceURL()
        if not url["OK"]:
            return url
        url = url["Value"]

        # getting certificate
        # Do we use the server certificate ?
        if self.kwargs[self.KW_USE_CERTIFICATES]:
            auth = {"cert": Locations.getHostCertificateAndKeyLocation()}

        # Use access token?
        elif self.__useAccessToken:
            from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (
                getLocalTokenDict,
                writeTokenDictToTokenFile,
            )

            # Read token from token environ variable or from token file
            result = getLocalTokenDict()
            if not result["OK"]:
                return result
            token = result["Value"]

            # Check if access token expired
            if token.is_expired():
                if not token.get("refresh_token"):
                    return S_ERROR("Access token expired.")

                # Try to refresh token
                self.__idp.scope = None
                result = self.__idp.refreshToken(token["refresh_token"])
                if result["OK"]:
                    token = result["Value"]
                    result = writeTokenDictToTokenFile(token)
                if not result["OK"]:
                    return result
                gLogger.notice("Token is saved in %s." % result["Value"])

            auth = {"headers": {"Authorization": "Bearer %s" % token["access_token"]}}
        elif self.kwargs.get(self.KW_PROXY_STRING):
            tmpHandle, cert = tempfile.mkstemp()
            fp = os.fdopen(tmpHandle, "w")
            fp.write(self.kwargs[self.KW_PROXY_STRING])
            fp.close()

        # CHRIS 04.02.21
        # TODO: add proxyLocation check ?
        else:
            auth = {"cert": Locations.getProxyLocation()}
            if not auth["cert"]:
                gLogger.error("No proxy found")
                return S_ERROR("No proxy found")

        # We have a try/except for all the exceptions
        # whose default behavior is to try again,
        # maybe to different server
        try:
            # And we have a second block to handle specific exceptions
            # which makes it not worth retrying
            try:
                rawText = None

                # Default case, just return the result
                if not outputFile:
                    call = self.__session.post(url, data=kwargs, timeout=self.timeout, **auth)
                    # raising the exception for status here
                    # means essentialy that we are losing here the information of what is returned by the server
                    # as error message, since it is not passed to the exception
                    # However, we can store the text and return it raw as an error,
                    # since there is no guarantee that it is any JEncoded text
                    # Note that we would get an exception only if there is an exception on the server side which
                    # is not handled.
                    # Any standard S_ERROR will be transfered as an S_ERROR with a correct code.
                    rawText = call.text
                    call.raise_for_status()
                    return decode(rawText)[0]
                else:
                    # Instruct the server not to encode the response
                    kwargs["rawContent"] = True

                    rawText = None
                    # Stream download
                    # https://requests.readthedocs.io/en/latest/user/advanced/#body-content-workflow
                    with self.__session.post(url, data=kwargs, timeout=self.timeout, stream=True, **auth) as r:
                        rawText = r.text
                        r.raise_for_status()

                        if isinstance(outputFile, io.IOBase):
                            for chunk in r.iter_content(4096):
                                # if chunk:  # filter out keep-alive new chuncks
                                outputFile.write(chunk)
                        else:
                            with open(outputFile, "wb") as f:
                                for chunk in r.iter_content(4096):
                                    # if chunk:  # filter out keep-alive new chuncks
                                    f.write(chunk)

                        return S_OK()

            # Some HTTPError are not worth retrying
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == HTTPStatus.NOT_IMPLEMENTED:
                    return S_ERROR(errno.ENOSYS, "%s is not implemented" % kwargs.get("method"))
                elif status_code in (HTTPStatus.FORBIDDEN, HTTPStatus.UNAUTHORIZED):
                    return S_ERROR(errno.EACCES, "No access to %s" % url)
                elif status_code == HTTPStatus.NOT_FOUND:
                    rawText = "%s is not found" % url

                # if it is something else, retry
                raise

        # Whatever exception we have here, we deem worth retrying
        except Exception as e:
            # CHRIS TODO review this part: retry logic is fishy
            # self.__bannedUrls is emptied in findServiceURLs
            if url not in self.__bannedUrls:
                self.__bannedUrls += [url]
            if retry < self.__nbOfUrls - 1:
                self._request(retry=retry + 1, outputFile=outputFile, **kwargs)

            errStr = f"{str(e)}: {rawText}"
            return S_ERROR(errStr)


# --- TODO ----
# Rewrite this method if needed:
#  /Core/DISET/private/BaseClient.py
# __delegateCredentials


class _ContextAdapter(requests.adapters.HTTPAdapter):
    """Allows to override the default context."""

    def __init__(self, *args, **kwargs):
        self.ssl_context = kwargs.pop("ssl_context", None)
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs.setdefault("ssl_context", self.ssl_context)
        return super().init_poolmanager(*args, **kwargs)


@convertToReturnValue
def _create_session(verified=True):
    ctx = ssl.create_default_context()
    # Python 3.10+ sets DEFAULT:@SECLEVEL=2 which prevents the use of 1024 bit RSA for proxies.
    # In DIRAC 8.0 the default proxy length has been increased to 2048 bits however we need to
    # downgrade to DEFAULT:@SECLEVEL=1 until all users have uploaded a new proxy.
    ctx.set_ciphers(os.environ.get("DIRAC_HTTPS_SSL_CIPHERS", "DEFAULT:@SECLEVEL=1"))
    if minimum_tls_version := os.environ.get("DIRAC_HTTPS_SSL_METHOD_MIN"):
        ctx.minimum_version = getattr(ssl.TLSVersion, minimum_tls_version)
    if maximum_tls_version := os.environ.get("DIRAC_HTTPS_SSL_METHOD_MAX"):
        ctx.maximum_version = getattr(ssl.TLSVersion, maximum_tls_version)
    session = requests.Session()
    session.mount("https://", _ContextAdapter(ssl_context=ctx))
    if verified:
        ca_location = Locations.getCAsLocation()
        if not ca_location:
            raise ValueError("No CAs found!")
        session.verify = ca_location
    else:
        ctx.check_hostname = False
        session.verify = False
    return session
