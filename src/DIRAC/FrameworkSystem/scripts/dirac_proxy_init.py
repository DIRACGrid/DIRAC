#!/usr/bin/env python
########################################################################
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
########################################################################
"""
Creating a proxy.

Example:
  $ dirac-proxy-init -g dirac_user -t --rfc
  Enter Certificate password:
"""
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

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
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
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
  uploadProxy = True
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

  def disableProxyUpload(self, _arg):
    """ Do not upload proxy

        :param _arg: unuse

        :return: S_OK()
    """
    self.uploadProxy = False
    return S_OK()

  def registerCLISwitches(self):
    """ Register CLI switches """
    ProxyGeneration.CLIParams.registerCLISwitches(self)
    Script.registerSwitch(
        "U",
        "upload",
        "Upload a long lived proxy to the ProxyManager (deprecated, see --no-upload)")
    Script.registerSwitch(
        "N",
        "no-upload",
        "Do not upload a long lived proxy to the ProxyManager",
        self.disableProxyUpload)
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
      return resultProxyUpload
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
      maxProviderLen = len('ProxyProvider')
      for userDN, data in self.__uploadedInfo.items():
        maxDNLen = max(maxDNLen, len(userDN))
        maxProviderLen = max(maxProviderLen, len(data['provider']))
      gLogger.notice(" %s | %s | %s | SupportedGroups" % ("DN".ljust(maxDNLen), "ProxyProvider".ljust(maxProviderLen),
                                                          "Until (GMT)".ljust(16)))
      for userDN, data in self.__uploadedInfo.items():
        gLogger.notice(" %s | %s | %s | " % (userDN.ljust(maxDNLen), data['provider'].ljust(maxProviderLen),
                                             data['expirationtime'].strftime("%Y/%m/%d %H:%M").ljust(16)),
                       ",".join(data['groups']))

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
    """ Magic method

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
    """ Magic method with tokens

        :return: S_OK()/S_ERROR()
    """
    import urllib3
    import threading
    import webbrowser
    import requests
    import json
    from authlib.integrations.requests_client import OAuth2Session

    from DIRAC.Core.Utilities.JEncode import encode
    from DIRAC.ConfigurationSystem.Client.Utilities import getProxyAPI, getDIRACClientID
    from DIRAC.FrameworkSystem.Utilities.halo import Halo, qrterminal
    from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import submitUserAuthorizationFlow
    from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import waitFinalStatusOfUserAuthorizationFlow
    from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
    from DIRAC.ConfigurationSystem.Client.Utilities import getAuthAPI, getDIRACClientID

    spinner = Halo()
    proxyAPI = getProxyAPI()

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Get IdP
    result = IdProviderFactory().getIdProvider(self.__piParams.provider + '_public')
    if not result['OK']:
      return result

    idpObj = result['Value']

    # Submit Device authorisation flow
    with Halo('Authentification from %s.' % self.__piParams.provider) as spin:
      if Script.enableCS()['OK']:
        result = idpObj.submitDeviceCodeAuthorizationFlow(self.__piParams.diracGroup)
        if not result['OK']:
          sys.exit(result['Message'])
        response = result['Value']
      else:
        try:
          r = requests.post('{api}/device?{group}'.format(
              api=getAuthAPI(),
              group = ('group=%s' % self.__piParams.diracGroup) if self.__piParams.diracGroup else ''
          ), verify=False)
          r.raise_for_status()
          response = r.json()
          # Check if all main keys are present here
          for k in ['user_code', 'device_code', 'verification_uri']:
            if not response.get(k):
              sys.exit('Mandatory %s key is absent in authentication response.' % k)
        except requests.exceptions.Timeout:
          sys.exit('Authentication server is not answer, timeout.')
        except requests.exceptions.RequestException as ex:
          sys.exit(r.content or repr(ex))
        except Exception as ex:
          sys.exit('Cannot read authentication response: %s' % repr(ex))
      
    deviceCode = response['device_code']
    userCode = response['user_code']
    verURL = response['verification_uri']
    verURLComplete = response.get('verification_uri_complete')
    interval = response.get('interval', 5)

    # Notify user to go to authorization endpoint
    showURL = 'Use next link to continue, your user code is "%s"\n%s' % (userCode, verURL)
    if self.__piParams.addQRcode:
      if not verURLComplete:
        spinner.warn('Cannot get verification_uri_complete for authentication.')
        spinner.info(showURL)
      else:
        result = qrterminal(verURLComplete)
        if not result['OK']:
          spinner.fail(result['Message'])
          spinner.info(showURL)
        else:
          # Show QR code
          spinner.info('Scan QR code to continue: %s' % result['Value'])
    else:
      spinner.info(showURL)

    # Try to open in default browser
    if webbrowser.open_new_tab(verURL):
      spinner.text = '%s opening in default browser..' % verURL

    with Halo('Waiting authorization status..') as spin:
      result = idpObj.waitFinalStatusOfDeviceCodeAuthorizationFlow(deviceCode)
      if not result['OK']:
        sys.exit(result['Message'])
      idpObj.token = result['Value']

      spin.color = 'green'
      spin.text = 'Saving token.. to env DIRAC_TOKEN..'

      os.environ["DIRAC_TOKEN"] = json.dumps(idpObj.token)

      spin.text = 'Download proxy..'
      url = '%s?lifetime=%s' % (proxyAPI, self.__piParams.proxyLifeTime)
      addVOMS = self.__piParams.addVOMSExt or Registry.getGroupOption(self.__piParams.diracGroup, "AutoAddVOMS", False)
      if addVOMS:
        url += '&voms=%s' % addVOMS
      if not idpObj.token.get('refresh_token'):
        sys.exit('Refresh token is absent in response.')
      url += '&refresh_token=%s' % idpObj.token['refresh_token']
      r = idpObj.get(url)
      r.raise_for_status()
      proxy = r.text
      if not proxy:
        sys.exit("Something went wrong, the proxy is empty.")

      if not self.__piParams.proxyLoc:
        self.__piParams.proxyLoc = '/tmp/x509up_u%s' % os.getuid()

      spin.color = 'green'
      spin.text = 'Saving proxy.. to %s..' % self.__piParams.proxyLoc
      try:
        with open(self.__piParams.proxyLoc, 'w+') as fd:
          fd.write(proxy.encode("UTF-8"))
        os.chmod(self.__piParams.proxyLoc, stat.S_IRUSR | stat.S_IWUSR)
      except Exception as e:
        return S_ERROR("%s :%s" % (self.__piParams.proxyLoc, repr(e).replace(',)', ')')))
      self.__piParams.certLoc = self.__piParams.proxyLoc
      spin.text = 'Proxy is saved to %s.' % self.__piParams.proxyLoc

    result = Script.enableCS()
    if not result['OK']:
      return S_ERROR("Cannot contact CS to get user list")
    threading.Thread(target=self.checkCAs).start()
    gConfig.forceRefresh(fromMaster=True)
    return S_OK(self.__piParams.proxyLoc)


@DIRACScript()
def main():
  global piParams, pI
  piParams = Params()
  piParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine(ignoreErrors=True)
  DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseServerCertificate", "False")

  pI = ProxyInit(piParams)
  gLogger.info(gConfig.getConfigurationTree())
  if piParams.addProvider:
    resultDoMagic = pI.doOAuthMagic()
  else:
    resultDoMagic = pI.doTheMagic()
  if not resultDoMagic['OK']:
    gLogger.fatal(resultDoMagic['Message'])
    sys.exit(1)

  pI.printInfo()

  sys.exit(0)


if __name__ == "__main__":
  main()
