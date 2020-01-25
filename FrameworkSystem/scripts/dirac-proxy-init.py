#!/usr/bin/env python
########################################################################
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
########################################################################

import os
import sys
import stat
import glob
import time
import pickle
import datetime

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Security import X509Chain, ProxyInfo, Properties, VOMS  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client import ProxyGeneration, ProxyUpload
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

__RCSID__ = "$Id$"


class Params(ProxyGeneration.CLIParams):

  session = None
  provider = ''
  addEmail = False
  addQRcode = False
  addVOMSExt = False
  addProvider = False
  uploadProxy = False
  uploadPilot = False

  def setEmail(self, arg):
    """ Set email

        :param str arg: email

        :return: S_OK()
    """
    self.Email = arg
    self.addEmail = True
    return S_OK()

  def setQRcode(self, _arg):
    """ Use QRcode

        :param _arg: unuse

        :return: S_OK()
    """
    self.addQRcode = True
    return S_OK()

  def setProvider(self, arg):
    """ Set provider

        :param str arg: provider

        :return: S_OK()
    """
    self.provider = arg
    self.addProvider = True
    return S_OK()

  def setVOMSExt(self, _arg):
    """ Set VOMS extention

        :param _arg: unuse

        :return: S_OK()
    """
    self.addVOMSExt = True
    return S_OK()

  def setUploadProxy(self, _arg):
    """ Set upload proxy

        :param _arg: unuse

        :return: S_OK()
    """
    self.uploadProxy = True
    return S_OK()

  def registerCLISwitches(self):
    """ Register CLI switches """
    ProxyGeneration.CLIParams.registerCLISwitches(self)
    Script.registerSwitch("U", "upload", "Upload a long lived proxy to the ProxyManager", self.setUploadProxy)
    Script.registerSwitch("e:", "email=", "Send oauth authentification url on email", self.setEmail)
    Script.registerSwitch("P:", "provider=", "Set provider name for authentification", self.setProvider)
    Script.registerSwitch("Q", "qrcode", "Print link as QR code", self.setQRcode)
    Script.registerSwitch("M", "VOMS", "Add voms extension", self.setVOMSExt)


class ProxyInit(object):

  def __init__(self, piParams):
    """ Constructor """
    self.__piParams = piParams
    self.__issuerCert = False
    self.__proxyGenerated = False
    self.__uploadedInfo = {}

  def getIssuerCert(self):
    """ Get certificate issuer

        :return: str
    """
    if self.__issuerCert:
      return self.__issuerCert
    proxyChain = X509Chain.X509Chain()
    resultProxyChainFromFile = proxyChain.loadChainFromFile(self.__piParams.certLoc)
    if not resultProxyChainFromFile['OK']:
      gLogger.error("Could not load the proxy: %s" % resultProxyChainFromFile['Message'])
      sys.exit(1)
    resultIssuerCert = proxyChain.getIssuerCert()
    if not resultIssuerCert['OK']:
      gLogger.error("Could not load the proxy: %s" % resultIssuerCert['Message'])
      sys.exit(1)
    self.__issuerCert = resultIssuerCert['Value']
    return self.__issuerCert

  def certLifeTimeCheck(self):
    """ Check certificate live time
    """
    minLife = Registry.getGroupOption(self.__piParams.diracGroup, "SafeCertificateLifeTime", 2592000)
    resultIssuerCert = self.getIssuerCert()
    resultRemainingSecs = resultIssuerCert.getRemainingSecs()  # pylint: disable=no-member
    if not resultRemainingSecs['OK']:
      gLogger.error("Could not retrieve certificate expiration time", resultRemainingSecs['Message'])
      return
    lifeLeft = resultRemainingSecs['Value']
    if minLife > lifeLeft:
      daysLeft = int(lifeLeft / 86400)
      msg = "Your certificate will expire in less than %d days. Please renew it!" % daysLeft
      sep = "=" * (len(msg) + 4)
      gLogger.notice("%s\n  %s  \n%s" % (sep, msg, sep))

  def addVOMSExtIfNeeded(self):
    """ Add VOMS extension if needed

        :return: S_OK()/S_ERROR()
    """
    addVOMS = self.__piParams.addVOMSExt or Registry.getGroupOption(self.__piParams.diracGroup, "AutoAddVOMS", False)
    if not addVOMS:
      return S_OK()

    vomsAttr = Registry.getVOMSAttributeForGroup(self.__piParams.diracGroup)
    if not vomsAttr:
      return S_ERROR("Requested adding a VOMS extension but no VOMS attribute defined for group %s" %
                     self.__piParams.diracGroup)

    resultVomsAttributes = VOMS.VOMS().setVOMSAttributes(self.__proxyGenerated, attribute=vomsAttr,
                                                         vo=Registry.getVOMSVOForGroup(self.__piParams.diracGroup))
    if not resultVomsAttributes['OK']:
      return S_ERROR("Could not add VOMS extensions to the proxy\nFailed adding VOMS attribute: %s" %
                     resultVomsAttributes['Message'])

    gLogger.notice("Added VOMS attribute %s" % vomsAttr)
    chain = resultVomsAttributes['Value']
    return chain.dumpAllToFile(self.__proxyGenerated)

  def createProxy(self):
    """ Creates the proxy on disk

        :return: S_OK()/S_ERROR()
    """
    gLogger.notice("Generating proxy...")
    resultProxyGenerated = ProxyGeneration.generateProxy(piParams)
    if not resultProxyGenerated['OK']:
      gLogger.error(resultProxyGenerated['Message'])
      sys.exit(1)
    self.__proxyGenerated = resultProxyGenerated['Value']
    return resultProxyGenerated

  def uploadProxy(self):
    """ Upload the proxy to the proxyManager service

        :return: S_OK()/S_ERROR()
    """
    issuerCert = self.getIssuerCert()
    resultUserDN = issuerCert.getSubjectDN()  # pylint: disable=no-member
    if not resultUserDN['OK']:
      return resultUserDN
    userDN = resultUserDN['Value']

    gLogger.notice("Uploading proxy..")
    if userDN in self.__uploadedInfo:
      expiry = self.__uploadedInfo[userDN].get('')
      if expiry:
        if issuerCert.getNotAfterDate()['Value'] - datetime.timedelta(minutes=10) < expiry:  # pylint: disable=no-member
          gLogger.info('Proxy with DN "%s" already uploaded' % userDN)
          return S_OK()
    gLogger.info("Uploading %s proxy to ProxyManager..." % userDN)
    upParams = ProxyUpload.CLIParams()
    upParams.onTheFly = True
    upParams.proxyLifeTime = issuerCert.getRemainingSecs()['Value'] - 300  # pylint: disable=no-member
    upParams.rfcIfPossible = self.__piParams.rfc
    for k in ('certLoc', 'keyLoc', 'userPasswd'):
      setattr(upParams, k, getattr(self.__piParams, k))
    resultProxyUpload = ProxyUpload.uploadProxy(upParams)
    if not resultProxyUpload['OK']:
      gLogger.error(resultProxyUpload['Message'])
      sys.exit(1)
    self.__uploadedInfo = resultProxyUpload['Value']
    gLogger.info("Proxy uploaded")
    return S_OK()

  def printInfo(self):
    """ Printing utilities
    """
    resultProxyInfoAsAString = ProxyInfo.getProxyInfoAsString(self.__proxyGenerated)
    if not resultProxyInfoAsAString['OK']:
      gLogger.error('Failed to get the new proxy info: %s' % resultProxyInfoAsAString['Message'])
    else:
      gLogger.notice("Proxy generated:")
      gLogger.notice(resultProxyInfoAsAString['Value'])
    if self.__uploadedInfo:
      gLogger.notice("\nProxies uploaded:")
      maxDNLen = 0
      maxGroupLen = 0
      for userDN in self.__uploadedInfo:
        maxDNLen = max(maxDNLen, len(userDN))
        for group in self.__uploadedInfo[userDN]:
          maxGroupLen = max(maxGroupLen, len(group))
      gLogger.notice(" %s | %s | Until (GMT)" % ("DN".ljust(maxDNLen), "Group".ljust(maxGroupLen)))
      for userDN in self.__uploadedInfo:
        for group in self.__uploadedInfo[userDN]:
          gLogger.notice(" %s | %s | %s" % (userDN.ljust(maxDNLen),
                                            group.ljust(maxGroupLen),
                                            self.__uploadedInfo[userDN][group].strftime("%Y/%m/%d %H:%M")))

  def checkCAs(self):
    """ Check CAs

        :return: S_OK()
    """
    if "X509_CERT_DIR" not in os.environ:
      gLogger.warn("X509_CERT_DIR is unset. Abort check of CAs")
      return
    caDir = os.environ["X509_CERT_DIR"]
    # In globus standards .r0 files are CRLs. They have the same names of the CAs but diffent file extension
    searchExp = os.path.join(caDir, "*.r0")
    crlList = glob.glob(searchExp)
    if not crlList:
      gLogger.warn("No CRL files found for %s. Abort check of CAs" % searchExp)
      return
    newestFPath = max(crlList, key=os.path.getmtime)
    newestFTime = os.path.getmtime(newestFPath)
    if newestFTime > (time.time() - (2 * 24 * 3600)):
      # At least one of the files has been updated in the last 2 days
      return S_OK()
    if not os.access(caDir, os.W_OK):
      gLogger.error("Your CRLs appear to be outdated, but you have no access to update them.")
      # Try to continue anyway...
      return S_OK()
    # Update the CAs & CRLs
    gLogger.notice("Your CRLs appear to be outdated; attempting to update them...")
    bdc = BundleDeliveryClient()
    res = bdc.syncCAs()
    if not res['OK']:
      gLogger.error("Failed to update CAs", res['Message'])
    res = bdc.syncCRLs()
    if not res['OK']:
      gLogger.error("Failed to update CRLs", res['Message'])
    # Continue even if the update failed...
    return S_OK()

  def doTheMagic(self):
    """ Magig method

        :return: S_OK()/S_ERROR()
    """
    proxy = self.createProxy()
    if not proxy['OK']:
      return proxy

    self.checkCAs()
    pI.certLifeTimeCheck()
    resultProxyWithVOMS = pI.addVOMSExtIfNeeded()
    if not resultProxyWithVOMS['OK']:
      if "returning a valid AC for the user" in resultProxyWithVOMS['Message']:
        gLogger.error(resultProxyWithVOMS['Message'])
        gLogger.error("\n Are you sure you are properly registered in the VO?")
      elif "Missing voms-proxy" in resultProxyWithVOMS['Message']:
        gLogger.notice("Failed to add VOMS extension: no standard grid interface available")
      else:
        gLogger.error(resultProxyWithVOMS['Message'])
      if self.__piParams.strict:
        return resultProxyWithVOMS

    if self.__piParams.uploadProxy:
      resultProxyUpload = pI.uploadProxy()
      if not resultProxyUpload['OK']:
        if self.__piParams.strict:
          return resultProxyUpload

    return S_OK()

  def doOAuthMagic(self):
    """ Magic method

        :return: S_OK()/S_ERROR()
    """
    import urllib3
    import requests
    import threading
    import webbrowser

    from DIRAC.FrameworkSystem.Utilities.halo import Halo
    from DIRAC.Core.Utilities.JEncode import decode, encode

    authAPI = None
    proxyAPI = None
    spinner = Halo()
    s = requests.Session()

    # Search and load sessions cache
    try:
      with open('/tmp/cache_u%d' % os.getuid(), 'rb') as f:
        s.cookies.update(pickle.load(f))
      if self.__piParams.provider:
        s.cookies.set("TypeAuth", self.__piParams.provider, domain=s.cookies.list_domains()[0])
    except Exception:
      pass

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def restRequest(url, endpoint='', metod='GET', **kwargs):
      """ Method to do http requests

          :param str url: root path of request URL
          :param str endpoint: DIRAC rest endpoint
          :param str method: HTTP method
          :param `**kwargs`: options that need to add to request

          :return: S_OK(Responce)/S_ERROR()
      """
      # Collect options
      __opts = ''
      for key in kwargs:
        if kwargs[key]:
          if not __opts:
            __opts = '?%s=%s' % (key, kwargs[key])
          else:
            __opts += '&%s=%s' % (key, kwargs[key])

      # Make request
      try:
        r = s.get('%s/%s%s' % (url.strip('/'), endpoint.strip('/'), __opts), verify=False)
        r.raise_for_status()
        # Save cookies
        with open('/tmp/cache_u%d' % os.getuid(), 'wb+') as f:
          pickle.dump(s.cookies, f)
        return S_OK(decode(r.text)[0])
      except requests.exceptions.Timeout:
        return S_ERROR('Time out')
      except requests.exceptions.RequestException as ex:
        return S_ERROR(r.content or ex)
      except Exception as ex:
        return S_ERROR('Cannot read response: %s' % ex)

    def qrterminal(url):
      """ Show QR code

          :param str url: URL to convert to QRCode

          :return: S_OK(str)/S_ERROR()
      """
      try:
        import pyqrcode  # pylint: disable=import-error
      except Exception as ex:
        return S_ERROR('pyqrcode library is not installed.')
      __qr = '\n'
      qrA = pyqrcode.create(url).code
      qrA.insert(0, [0 for i in range(0, len(qrA[0]))])
      qrA.append([0 for i in range(0, len(qrA[0]))])
      if not (len(qrA) % 2) == 0:
        qrA.append([0 for i in range(0, len(qrA[0]))])
      for i in range(0, len(qrA)):
        if not (i % 2) == 0:
          continue
        __qr += '\033[0;30;47m '
        for j in range(0, len(qrA[0])):
          p = str(qrA[i][j]) + str(qrA[i + 1][j])
          if p == '11':  # black bg
            __qr += '\033[0;30;40m \033[0;30;47m'
          if p == '10':  # upblock
            __qr += u'\u2580'
          if p == '01':  # downblock
            __qr += u'\u2584'
          if p == '00':  # white bg
            __qr += ' '
        __qr += ' \033[0m\n'
      return S_OK(__qr)

    with Halo('Authentification from %s.' % self.__piParams.provider) as spin:
      # Get https endpoint of OAuthService API from http API of ConfigurationService
      confUrl = gConfig.getValue("/LocalInstallation/ConfigurationServerAPI")
      if not confUrl:
        sys.exit('Cannot get http url of configuration server.')
      result = restRequest(confUrl, '/get', option='/Systems/Framework/Production/URLs/AuthAPI')
      if not result['OK']:
        sys.exit('Cannot get URL of authentication server:\n %s' % result['Message'])
      authAPI = result['Value']
      result = restRequest(confUrl, '/get', option='/Systems/Framework/Production/URLs/ProxyAPI')
      if not result['OK']:
        sys.exit('Cannot get URL of proxy server:\n %s' % result['Message'])
      proxyAPI = result['Value']
      result = restRequest(confUrl, '/get', option='/DIRAC/Setup')
      if not result['OK']:
        sys.exit('Cannot get DIRAC setup name:\n %s' % result['Message'])
      setup = result['Value']

      # Submit authorization session
      params = {}
      if self.__piParams.addEmail:
        params['email'] = self.__piParams.Email
      result = restRequest(authAPI, '/auth/%s' % self.__piParams.provider, **params)
      if not result['OK']:
        sys.exit(result['Message'])
      authDict = result['Value']

      if authDict.get('Comment'):
        spinner.info(authDict['Comment'].strip())

      spin.result = None

    if authDict['Status'] == 'needToAuth':
      session = authDict['Session']
      if not authDict.get('URL'):
        sys.exit('Cannot get link for authentication.')
      # Show QR code
      if self.__piParams.addQRcode:
        result = qrterminal(authDict['URL'])
        if not result['OK']:
          spinner.info(authDict['URL'])
          spinner.color = 'red'
          spinner.text = 'QRCode is crash: %s Please use upper link.' % result['Message']
        else:
          spinner.info('Scan QR code to continue: %s' % result['Value'])
          spinner.text = 'Or use link: %s' % authDict['URL']
      else:
        spinner.info(authDict['URL'])
        spinner.text = 'Use upper link to continue'

      # Try to open in default browser
      if webbrowser.open_new_tab(authDict['URL']):
        spinner.text = '%s opening in default browser..' % authDict['URL']

      comment = ''
      with spinner:
        # Loop: waiting status of request
        __start = time.time()
        __eNum = 0
        while True:
          time.sleep(5)
          if time.time() - __start > 300:
            sys.exit('Time out.')

          result = restRequest(authAPI, '/auth/%s/status' % session)
          if not result['OK']:
            if __eNum < 3:
              __eNum += 1
              spinner.color = 'red'
              spinner.text = result['Message']
              continue
            sys.exit(result['Message'])
          authDict = result['Value']
          if authDict['Status'] in ['prepared', 'in progress', 'finishing', 'redirect']:
            if spinner.color != 'green':
              spinner.text = '"%s" session %s' % (session, authDict['Status'])
            spinner.color = 'green'
            continue
          break

        comment = authDict['Comment'].strip()
        if authDict['Status'] != 'authed':
          if authDict['Status'] == 'authed and reported':
            spinner.warn('Authenticated success. Administrators was notified about you.')
            sys.exit(0)
          elif authDict['Status'] == 'visitor':
            spinner.warn('Authenticated success. You have permissions as Visitor.')
            sys.exit(0)
          sys.exit('Authentication failed.')
        spinner.text = 'Authenticated success.'

      if comment:
        spinner.info(comment)

    username = authDict['UserName']

    with Halo(text='Downloading proxy') as spin:
      # Get group status
      result = restRequest(confUrl, '/getGroupsStatusByUsername', username=username)
      if not result['OK']:
        sys.exit('Cannot get status of groups: %s' % result['Message'])
      groupsStatusDict = result['Value']
      if self.__piParams.diracGroup not in groupsStatusDict:
        sys.exit('%s is uncorrect.' % self.__piParams.diracGroup)
      if groupsStatusDict[self.__piParams.diracGroup]['Status'] != 'ready':
        sys.exit('Cannot get proxy: %s' % groupsStatusDict[self.__piParams.diracGroup]['Comment'])

      addVOMS = self.__piParams.addVOMSExt or Registry.getGroupOption(self.__piParams.diracGroup, "AutoAddVOMS", False)
      result = restRequest(proxyAPI, 's:%s/g:%s/proxy' % (setup, self.__piParams.diracGroup),
                           lifetime=self.__piParams.proxyLifeTime, voms=addVOMS)
      if not result['OK']:
        sys.exit(result['Message'])
      proxy = result['Value']
      if not proxy:
        sys.exit("Result is empty.")

    if not self.__piParams.proxyLoc:
      self.__piParams.proxyLoc = '/tmp/x509up_u%s' % os.getuid()

    with Halo(text='Saving proxy to %s' % self.__piParams.proxyLoc):
      try:
        with open(self.__piParams.proxyLoc, 'w+') as fd:
          fd.write(proxy.encode("UTF-8"))
        os.chmod(self.__piParams.proxyLoc, stat.S_IRUSR | stat.S_IWUSR)
      except Exception as e:
        return S_ERROR("%s :%s" % (self.__piParams.proxyLoc, repr(e).replace(',)', ')')))
      self.__piParams.certLoc = self.__piParams.proxyLoc

    result = Script.enableCS()
    if not result['OK']:
      return S_ERROR("Cannot contact CS to get user list")
    threading.Thread(target=self.checkCAs).start()
    gConfig.forceRefresh(fromMaster=True)
    return S_OK(self.__piParams.proxyLoc)


if __name__ == "__main__":
  piParams = Params()
  piParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine(ignoreErrors=True)
  DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseServerCertificate", "False")

  pI = ProxyInit(piParams)
  if piParams.addProvider:
    resultDoMagic = pI.doOAuthMagic()
  else:
    resultDoMagic = pI.doTheMagic()
  if not resultDoMagic['OK']:
    gLogger.fatal(resultDoMagic['Message'])
    sys.exit(1)

  pI.printInfo()

  sys.exit(0)
