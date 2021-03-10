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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from io import open
import errno
import requests
import six
from six.moves import http_client


import DIRAC

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import skipCACheck
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import findDefaultGroupForDN
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL, getServiceFailoverURL

from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.Security import Locations
from DIRAC.Core.Utilities import List, Network
from DIRAC.Core.Utilities.JEncode import decode, encode


# TODO CHRIS: refactor all the messy `discover` methods
# I do not do it now because I want first to decide
# whether we go with code copy of fatorization


class TornadoBaseClient(object):
  """
    This class contain initialization method and all utilities method used for RPC
  """
  __threadConfig = ThreadConfig()
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

    if not isinstance(serviceName, six.string_types):
      raise TypeError("Service name expected to be a string. Received %s type %s" %
                      (str(serviceName), type(serviceName)))

    self._destinationSrv = serviceName
    self._serviceName = serviceName
    self.__ca_location = False

    self.kwargs = kwargs
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
            self.__discoverURL):

      result = initFunc()
      if not result['OK'] and self.__initStatus['OK']:
        self.__initStatus = result

  def __discoverSetup(self):
    """ Discover which setup to use and stores it in self.setup
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
    """ Calculate the final URL. It is called at initialization and in connect in case of issue

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
    if not result['OK']:
      return result
    self.serviceURL = result['Value']
    retVal = Network.splitURL(self.serviceURL)
    if not retVal['OK']:
      return retVal
    self.__URLTuple = retVal['Value']
    self._serviceName = self.__URLTuple[-1]
    res = gConfig.getOptionsDict("/DIRAC/ConnConf/%s:%s" % self.__URLTuple[1:3])
    if res['OK']:
      opts = res['Value']
      for k in opts:
        if k not in self.kwargs:
          self.kwargs[k] = opts[k]
    return S_OK()

  def __discoverVO(self):
    """ Discover which VO to use and stores it in self.vo
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
    """ Discovers which credentials to use for connection.
        * Server certificate:
          -> If KW_USE_CERTIFICATES in kwargs, sets it in self.__useCertificates
          -> If not, check gConfig.useServerCertificate(), and sets it in self.__useCertificates
              and kwargs[KW_USE_CERTIFICATES]
        * Certification Authorities check:
           -> if KW_SKIP_CA_CHECK is not in kwargs and we are using the certificates,
                set KW_SKIP_CA_CHECK to false in kwargs
           -> if KW_SKIP_CA_CHECK is not in kwargs and we are not using the certificate, check the skipCACheck
        * Proxy Chain

        WARNING: MOSTLY COPY/PASTE FROM Core/Diset/private/BaseClient

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

    # Rewrite a little bit from here: don't need the proxy string, we use the file
    if self.KW_PROXY_CHAIN in self.kwargs:
      try:
        self.kwargs[self.KW_PROXY_STRING] = self.kwargs[self.KW_PROXY_CHAIN].dumpAllToString()['Value']
        del self.kwargs[self.KW_PROXY_CHAIN]
      except Exception:
        return S_ERROR("Invalid proxy chain specified on instantiation")

    # ==== REWRITED FROM HERE ====

    # For certs always check CA's. For clients skipServerIdentityCheck

    return S_OK()

  def __discoverExtraCredentials(self):
    """ Add extra credentials informations.
        * self.__extraCredentials
          -> if KW_EXTRA_CREDENTIALS in kwargs, we set it
          -> Otherwise, if we use the server certificate, we set it to VAL_EXTRA_CREDENTIALS_HOST
          -> If we have a delegation (see bellow), we set it to (delegatedDN, delegatedGroup)
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
        if not result['OK']:
          return result
      self.__extraCredentials = (delegatedDN, delegatedGroup)
    return S_OK()

  def __discoverTimeout(self):
    """ Discover which timeout to use and stores it in self.timeout
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
    if not self.__initStatus['OK']:
      return self.__initStatus

    # Load the Gateways URLs for the current site Name
    gatewayURL = False
    if not self.kwargs.get(self.KW_IGNORE_GATEWAYS):
      dRetVal = gConfig.getOption("/DIRAC/Gateways/%s" % DIRAC.siteName())
      if dRetVal['OK']:
        rawGatewayURL = List.randomize(List.fromChar(dRetVal['Value'], ","))[0]
        gatewayURL = "/".join(rawGatewayURL.split("/")[:3])

    # If what was given as constructor attribute is a properly formed URL,
    # we just return this one.
    # If we have to use a gateway, we just replace the server name in it
    if self._destinationSrv.startswith("https://"):
      gLogger.debug("Already given a valid url", self._destinationSrv)
      if not gatewayURL:
        return S_OK(self._destinationSrv)
      gLogger.debug("Reconstructing given URL to pass through gateway")
      path = "/".join(self._destinationSrv.split("/")[3:])
      finalURL = "%s/%s" % (gatewayURL, path)
      gLogger.debug("Gateway URL conversion:\n %s -> %s" % (self._destinationSrv, finalURL))
      return S_OK(finalURL)

    if gatewayURL:
      gLogger.debug("Using gateway", gatewayURL)
      return S_OK("%s/%s" % (gatewayURL, self._destinationSrv))

    # If nor url is given as constructor, we extract the list of URLs from the CS (System/URLs/Component)
    try:
      urls = getServiceURL(self._destinationSrv, setup=self.setup)
    except Exception as e:
      return S_ERROR("Cannot get URL for %s in setup %s: %s" % (self._destinationSrv, self.setup, repr(e)))
    if not urls:
      return S_ERROR("URL for service %s not found" % self._destinationSrv)

    failoverUrls = []
    # Try if there are some failover URLs to use as last resort
    try:
      failoverUrlsStr = getServiceFailoverURL(self._destinationSrv, setup=self.setup)
      if failoverUrlsStr:
        failoverUrls = failoverUrlsStr.split(',')
    except Exception as e:
      pass

    # We randomize the list, and add at the end the failover URLs (System/FailoverURLs/Component)
    urlsList = List.randomize(List.fromChar(urls, ",")) + failoverUrls
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

    # Take the first URL from the list
    # randUrls = List.randomize( urlsList ) + failoverUrls

    sURL = urlsList[0]

    # If we have banned URLs, and several URLs at disposals, we make sure that the selected sURL
    # is not on a host which is banned. If it is, we take the next one in the list using __selectUrl

    if self.__bannedUrls and self.__nbOfUrls > 2:  # when we have multiple services then we can
      # have a situation when two services are running on the same machine with different ports...
      retVal = Network.splitURL(sURL)
      nexturl = None
      if retVal['OK']:
        nexturl = retVal['Value']

        found = False
        for i in self.__bannedUrls:
          retVal = Network.splitURL(i)
          if retVal['OK']:
            bannedurl = retVal['Value']
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
    gLogger.debug("Discovering URL for service", "%s -> %s" % (self._destinationSrv, sURL))
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
      if retVal['OK']:
        if retVal['Value'][1] != notselect[1]:  # the hots are different
          url = i
          break
        else:
          gLogger.error(retVal['Message'])
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
    return getServiceURL(self._serviceName)

  def _getBaseStub(self):
    """ Returns a tuple with (self._destinationSrv, newKwargs)
        self._destinationSrv is what was given as first parameter of the init serviceName

        newKwargs is an updated copy of kwargs:
          * if set, we remove the useCertificates (KW_USE_CERTIFICATES) in newKwargs

        This method is just used to return information in case of error in the InnerRPCClient

        WARNING: COPY/PASTE FROM Core/Diset/private/BaseClient
    """
    newKwargs = dict(self.kwargs)
    # Remove useCertificates as the forwarder of the call will have to
    # independently decide whether to use their cert or not anyway.
    if 'useCertificates' in newKwargs:
      del newKwargs['useCertificates']
    return (self._destinationSrv, newKwargs)

  def _request(self, retry=0, outputFile=None, **kwargs):
    """
      Sends the request to server

      :param retry: internal parameters for recursive call. TODO: remove ?
      :param outputFile: (default None) path to a file where to store the received data.
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

    # Getting URL
    url = self.__findServiceURL()
    if not url['OK']:
      return url
    url = url['Value']

    # Getting CA file (or skip verification)
    verify = (not self.kwargs.get(self.KW_SKIP_CA_CHECK))
    if verify:
      cafile = Locations.getCAsLocation()
      if not cafile:
        gLogger.error("No CAs found!")
        return S_ERROR("No CAs found!")
      verify = self.__ca_location

    # getting certificate
    # Do we use the server certificate ?
    if self.kwargs[self.KW_USE_CERTIFICATES]:
      cert = Locations.getHostCertificateAndKeyLocation()
    # CHRIS 04.02.21
    # TODO: add proxyLocation check ?
    else:
      cert = Locations.getProxyLocation()
      if not cert:
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
          call = requests.post(url, data=kwargs,
                               timeout=self.timeout, verify=verify,
                               cert=cert)
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
          kwargs['rawContent'] = True

          rawText = None
          # Stream download
          # https://requests.readthedocs.io/en/latest/user/advanced/#body-content-workflow
          with requests.post(url, data=kwargs, timeout=self.timeout, verify=verify,
                             cert=cert, stream=True) as r:
            rawText = r.text
            r.raise_for_status()

            with open(outputFile, 'wb') as f:
              for chunk in r.iter_content(4096):
                # if chunk:  # filter out keep-alive new chuncks
                f.write(chunk)

            return S_OK()

      # Some HTTPError are not worth retrying
      except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code == http_client.NOT_IMPLEMENTED:
          return S_ERROR(errno.ENOSYS, "%s is not implemented" % kwargs.get('method'))
        elif status_code in (http_client.FORBIDDEN, http_client.UNAUTHORIZED):
          return S_ERROR(errno.EACCES, "No access to %s" % url)

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

      errStr = "%s: %s" % (str(e), rawText)
      return S_ERROR(errStr)


# --- TODO ----
# Rewrite this method if needed:
#  /Core/DISET/private/BaseClient.py
# __delegateCredentials
