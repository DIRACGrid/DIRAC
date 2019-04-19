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
import datetime

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Security import X509Chain, ProxyInfo, Properties, VOMS
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client import ProxyGeneration, ProxyUpload
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

__RCSID__ = "$Id$"


class Params(ProxyGeneration.CLIParams):

  IdP = ''
  IdPproxy = True
  addEmail = False
  addOAuth = False
  addQRcode = False
  addVOMSExt = False
  uploadProxy = False
  uploadPilot = False

  def setUploadProxy(self, _arg):
    self.uploadProxy = True
    return S_OK()

  def setUploadPilotProxy(self, _arg):
    self.uploadPilot = True
    return S_OK()

  def setVOMSExt(self, _arg):
    self.addVOMSExt = True
    return S_OK()

  def setOAuth(self, arg):
    self.IdP = arg
    self.addOAuth = True
    return S_OK()

  def setEmail(self, arg):
    self.Email = arg
    self.addEmail = True
    return S_OK()

  def setQRcode(self, _arg):
    self.addQRcode = True
    return S_OK()

  def setOAuthProxy(self, _arg):
    self.IdPproxy = 'proxy_from_oauth_endpoint'
    return S_OK()

  def registerCLISwitches(self):
    ProxyGeneration.CLIParams.registerCLISwitches(self)
    Script.registerSwitch("U", "upload", "Upload a long lived proxy to the ProxyManager", self.setUploadProxy)
    Script.registerSwitch("P", "uploadPilot", "Upload a long lived pilot proxy to the ProxyManager",
                          self.setUploadPilotProxy)
    Script.registerSwitch("M", "VOMS", "Add voms extension", self.setVOMSExt)
    Script.registerSwitch("O:", "OAuth:", "Set OAuth2 IdP for authentification", self.setOAuth)
    Script.registerSwitch("e:", "email:", "Send oauth authentification url on email", self.setEmail)
    Script.registerSwitch("G", "get_oauth_proxy", "Get proxy throught OAuth2", self.setOAuthProxy)
    Script.registerSwitch("q", "qrcode", "Print link as QR code", self.setQRcode)

class ProxyInit(object):

  def __init__(self, piParams):
    self.__piParams = piParams
    self.__issuerCert = False
    self.__proxyGenerated = False
    self.__uploadedInfo = {}

  def getIssuerCert(self):
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
      msg = "%s\n  %s  \n%s" % (sep, msg, sep)
      gLogger.notice(msg)

  def getGroupsToUpload(self):
    uploadGroups = []

    if self.__piParams.uploadProxy or Registry.getGroupOption(self.__piParams.diracGroup, "AutoUploadProxy", False):
      uploadGroups.append(self.__piParams.diracGroup)

    if not self.__piParams.uploadPilot:
      if not Registry.getGroupOption(self.__piParams.diracGroup, "AutoUploadPilotProxy", False):
        return uploadGroups

    issuerCert = self.getIssuerCert()
    resultUserDN = issuerCert.getSubjectDN()  # pylint: disable=no-member
    if not resultUserDN['OK']:
      return resultUserDN
    userDN = resultUserDN['Value']

    resultGroups = Registry.getGroupsForDN(userDN)
    if not resultGroups['OK']:
      gLogger.error("No groups defined for DN %s" % userDN)
      return []
    availableGroups = resultGroups['Value']

    for group in availableGroups:
      groupProps = Registry.getPropertiesForGroup(group)
      if Properties.PILOT in groupProps or Properties.GENERIC_PILOT in groupProps:
        uploadGroups.append(group)
    return uploadGroups

  def addVOMSExtIfNeeded(self):
    addVOMS = self.__piParams.addVOMSExt or Registry.getGroupOption(self.__piParams.diracGroup, "AutoAddVOMS", False)
    if not addVOMS:
      return S_OK()

    vomsAttr = Registry.getVOMSAttributeForGroup(self.__piParams.diracGroup)
    if not vomsAttr:
      return S_ERROR(
          "Requested adding a VOMS extension but no VOMS attribute defined for group %s" %
          self.__piParams.diracGroup)

    resultVomsAttributes = VOMS.VOMS().setVOMSAttributes(self.__proxyGenerated, attribute=vomsAttr,
                                                         vo=Registry.getVOMSVOForGroup(self.__piParams.diracGroup))
    if not resultVomsAttributes['OK']:
      return S_ERROR(
          "Could not add VOMS extensions to the proxy\nFailed adding VOMS attribute: %s" %
          resultVomsAttributes['Message'])

    gLogger.notice("Added VOMS attribute %s" % vomsAttr)
    chain = resultVomsAttributes['Value']
    retDump = chain.dumpAllToFile(self.__proxyGenerated)
    if not retDump['OK']:
      return retDump
    return S_OK()

  def createProxy(self):
    """ Creates the proxy on disk
    """
    gLogger.notice("Generating proxy...")
    resultProxyGenerated = ProxyGeneration.generateProxy(piParams)
    if not resultProxyGenerated['OK']:
      gLogger.error(resultProxyGenerated['Message'])
      sys.exit(1)
    self.__proxyGenerated = resultProxyGenerated['Value']
    return resultProxyGenerated

  def uploadProxy(self, userGroup=False):
    """ Upload the proxy to the proxyManager service
    """
    issuerCert = self.getIssuerCert()
    resultUserDN = issuerCert.getSubjectDN()  # pylint: disable=no-member
    if not resultUserDN['OK']:
      return resultUserDN
    userDN = resultUserDN['Value']
    if not userGroup:
      userGroup = self.__piParams.diracGroup
    gLogger.notice("Uploading proxy for %s..." % userGroup)
    if userDN in self.__uploadedInfo:
      expiry = self.__uploadedInfo[userDN].get(userGroup)
      if expiry:
        if issuerCert.getNotAfterDate()['Value'] - datetime.timedelta(minutes=10) < expiry:  # pylint: disable=no-member
          gLogger.info("SKipping upload for group %s. Already uploaded" % userGroup)
          return S_OK()
    gLogger.info("Uploading %s proxy to ProxyManager..." % self.__piParams.diracGroup)
    upParams = ProxyUpload.CLIParams()
    upParams.onTheFly = True
    upParams.proxyLifeTime = issuerCert.getRemainingSecs()['Value'] - 300  # pylint: disable=no-member
    upParams.rfcIfPossible = self.__piParams.rfc
    upParams.diracGroup = userGroup
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
    if not "X509_CERT_DIR" in os.environ:
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
      # At least one of the files has been updated in the last 28 days
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

    for pilotGroup in pI.getGroupsToUpload():
      resultProxyUpload = pI.uploadProxy(userGroup=pilotGroup)
      if not resultProxyUpload['OK']:
        if self.__piParams.strict:
          return resultProxyUpload

    return S_OK()

  def doOAuthMagic(self):
    import urllib3
    import requests
    import itertools
    import threading
    import webbrowser
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def restRequest(url=None, endpoint='', metod='GET', **kwargs):
      """ Method to do http requests """
      if not url or not kwargs:
        return S_ERROR('Not arguments present.')
      __opts = None
      for key in kwargs:
        if kwargs[key]:
          if not __opts:
            __opts = '%s=%s' % (key, kwargs[key])
          else:
            __opts += '&%s=%s' % (key, kwargs[key])
      try:
        r = requests.get('%s%s?%s' % (url, endpoint, __opts), verify=False)
      except Exception as ex:
        return S_ERROR(ex)
      if not r.status_code == 200:
        return S_ERROR('Http request error: %s' % r.status_code)
      try:
        return S_OK(r.json())
      except Exception as ex:
        return S_ERROR('Cannot read response: %s' % ex)

    time_out = 300
    done = False

    def loading():
      """ Show loading string """
      __start = time.time()
      __runtime = 0
      for c in itertools.cycle(['.*.            ', '..*            ', ' ..*           ',
                                '  ..*          ', '   ..*         ', '    ..*        ',
                                '     ..*       ', '      ..*      ', '       ..*     ',
                                '        ..*    ', '         ..*   ', '          ..*  ',
                                '           ..* ', '            ..*', '            .*.',
                                '            *..', '           *.. ', '          *..  ',
                                '         *..   ', '        *..    ', '       *..     ',
                                '      *..      ', '     *..       ', '    *..        ',
                                '   *..         ', '  *..          ', ' *..           ',
                                '*..            ']):
          __runtime = time.time() - __start
          if done or __runtime > time_out:
            sys.stdout.write('\r                                                                   \n')
            break
          lefttime = (time_out - __runtime) // 60
          sys.stdout.write('\r Waiting %s minutes when you authenticated..' % lefttime + c)
          sys.stdout.flush()
          time.sleep(0.1)

    def qrterminal(url):
      """ Show QR code """
      try:
        import pyqrcode
      except Exception as ex:
        pass
      else:
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
        gLogger.notice(__qr)

    gLogger.notice('OAuth authentification from %s.' % self.__piParams.IdP)

    # Get https endpoint of OAuthService API from http API of ConfigurationService
    confUrl = gConfig.getValue("/LocalInstallation/ConfigurationServerAPI")
    if not confUrl:
      gLogger.fatal('Cannot get http url of configuration server.')
      sys.exit(1)
    res = restRequest(confUrl, '/get', **{'option': '/Systems/Framework/Production/URLs/OAuthAPI'})
    if not res['OK']:
      gLogger.fatal('Cannot get http url of oauth server:\n %s' % res['Message'])
      sys.exit(1)
    oauthUrl = res['Value']
    params = {'IdP': self.__piParams.IdP}
    if self.__piParams.addEmail:
      params['email'] = self.__piParams.Email
    res = restRequest(oauthUrl, '/oauth', **params)
    if not res['OK']:
      gLogger.fatal(res['Message'])
    result = res['Value']
    if not result['OK']:
      # Print link in output if it was not sent by email
      if not self.__piParams.addEmail:
        gLogger.fatal(result['Message'])
        sys.exit(1)
      gLogger.notice('Cannot not sent link to your email: %s' % result['Message'])
    elif 'url' not in result['Value'] or 'state' not in result['Value']:
      gLogger.fatal('Authentification url with state was not genereted by OAuth2service. Sorry sheet happends.')
      sys.exit(1)
    url = result['Value']['url']
    state = result['Value']['state']
    if not url or not state:
      gLogger.fatal('Cannot get link for authentication.')
      sys.exit(1)
    url = '%s/oauth?getlink=%s' % (oauthUrl, state)

    gLogger.notice('Use link to authentication..')

    if self.__piParams.addEmail:
      gLogger.notice('Likn was sent to your email(%s)!' % self.__piParams.Email)
    else:
      if not webbrowser.open_new_tab(url):
        gLogger.notice('%s\n' % url)
        if self.__piParams.addQRcode:
          qrterminal(url)
    # Loop: waiting status of request
    threading.Thread(target=loading).start()
    addVOMS = self.__piParams.addVOMSExt or Registry.getGroupOption(self.__piParams.diracGroup, "AutoAddVOMS", False)
    res = restRequest(oauthUrl, '/redirect', status=state, group=self.__piParams.diracGroup,
                      proxy=self.__piParams.IdPproxy, time_out=time_out,
                      proxyLifeTime=self.__piParams.proxyLifeTime, voms=addVOMS)
    done = True
    time.sleep(1)
    # End loop
    if not res['OK']:
      gLogger.error(res['Message'])
      sys.exit(1)
    result = res['Value']
    if not result['OK']:
      gLogger.error(result['Message'])
      sys.exit(1)
    if result['Value']['Status'] == 'visitor':
      gLogger.notice(result['Value']['Message'])
      sys.exit(1)
    if not self.__piParams.proxyLoc:
      self.__piParams.proxyLoc = '/tmp/x509up_u%s' % os.getuid()
    try:
      with open(self.__piParams.proxyLoc, 'w') as fd:
        fd.write(result['Value']['proxy'].encode("UTF-8"))
    except Exception as e:
      return S_ERROR("%s :%s" % (self.__piParams.proxyLoc, repr(e).replace(',)', ')')))
    try:
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
  if piParams.addOAuth:
    resultDoMagic = pI.doOAuthMagic()
  else:
    resultDoMagic = pI.doTheMagic()
  if not resultDoMagic['OK']:
    gLogger.fatal(resultDoMagic['Message'])
    sys.exit(1)

  pI.printInfo()

  sys.exit(0)
