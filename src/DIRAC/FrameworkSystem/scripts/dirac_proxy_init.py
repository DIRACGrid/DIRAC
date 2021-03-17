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
import glob
import time
import datetime

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import X509Chain, ProxyInfo, Properties, VOMS
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.FrameworkSystem.Client import ProxyGeneration, ProxyUpload
from DIRAC.Core.Security import X509Chain, ProxyInfo, Properties, VOMS
from DIRAC.Core.Security.Locations import getCAsLocation
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

__RCSID__ = "$Id$"


class ProxyInit(ProxyGeneration, ProxyUpload):

  def initParameters(self):
    ProxyUpload.initParameters(self)
    ProxyGeneration.initParameters(self)
    self.addVOMSExt = False
    self.uploadProxy = True
    self.uploadPilot = False
    self.__issuerCert = False
    self.__proxyGenerated = False
    self.__uploadedInfo = {}
    self.switches = [
        ("U", "upload",
         "Upload a long lived proxy to the ProxyManager (deprecated, see --no-upload)"),
        ("N", "no-upload",
         "Do not upload a long lived proxy to the ProxyManager", self.disableProxyUpload),
        ("M", "VOMS", "Add voms extension", self.setVOMSExt)
    ]
    self.switches += self.proxyGenerationSwitches
    for upSwitch in self.proxyUploadSwitches:
      if upSwitch[0] not in [u[0] for u in self.switches]:
        self.switches.append(upSwitch)

  def setVOMSExt(self, _arg):
    self.addVOMSExt = True
    return S_OK()

  def disableProxyUpload(self, _arg):
    self.uploadProxy = False
    return S_OK()

  def getIssuerCert(self):
    if self.__issuerCert:
      return self.__issuerCert
    proxyChain = X509Chain.X509Chain()
    resultProxyChainFromFile = proxyChain.loadChainFromFile(self.certLoc)
    if not resultProxyChainFromFile['OK']:
      gLogger.error("Could not load the proxy: %s" % resultProxyChainFromFile['Message'])
      sys.exit(1)
    resultIssuerCert = proxyChain.getIssuerCert()
    if not resultIssuerCert['OK']:
      gLogger.error("Could not load the proxy: %s" % resultIssuerCert['Message'])
      sys.exit(1)
    self.__issuerCert = resultIssuerCert['Value']

  def certLifeTimeCheck(self):
    minLife = Registry.getGroupOption(self.diracGroup, "SafeCertificateLifeTime", 2592000)
    resultRemainingSecs = self.__issuerCert.getRemainingSecs()  # pylint: disable=no-member
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

  def addVOMSExtIfNeeded(self):
    addVOMS = self.addVOMSExt or Registry.getGroupOption(self.diracGroup, "AutoAddVOMS", False)
    if not addVOMS:
      return S_OK()

    vomsAttr = Registry.getVOMSAttributeForGroup(self.diracGroup)
    if not vomsAttr:
      return S_ERROR("Requested adding a VOMS extension but no VOMS attribute defined for group %s" %
                     self.diracGroup)

    resultVomsAttributes = VOMS.VOMS().setVOMSAttributes(self.__proxyGenerated, attribute=vomsAttr,
                                                         vo=Registry.getVOMSVOForGroup(self.diracGroup))
    if not resultVomsAttributes['OK']:
      return S_ERROR("Could not add VOMS extensions to the proxy\nFailed adding VOMS attribute: %s" %
                     resultVomsAttributes['Message'])

    gLogger.notice("Added VOMS attribute %s" % vomsAttr)
    chain = resultVomsAttributes['Value']
    result = chain.dumpAllToFile(self.__proxyGenerated)
    if not result["OK"]:
      return result
    return S_OK()

  def _uploadProxy(self):
    """ Upload the proxy to the proxyManager service
    """
    resultUserDN = self.__issuerCert.getSubjectDN()  # pylint: disable=no-member
    if not resultUserDN['OK']:
      return resultUserDN
    userDN = resultUserDN['Value']

    gLogger.notice("Uploading proxy..")
    if userDN in self.__uploadedInfo:
      expiry = self.__uploadedInfo[userDN].get('')
      if expiry:
        # pylint: disable=no-member
        if self.__issuerCert.getNotAfterDate()['Value'] - datetime.timedelta(minutes=10) < expiry:
          gLogger.info('Proxy with DN "%s" already uploaded' % userDN)
          return S_OK()

    gLogger.info("Uploading %s proxy to ProxyManager..." % userDN)
    self.onTheFly = True
    self.proxyLifeTime = self.__issuerCert.getRemainingSecs()['Value'] - 300  # pylint: disable=no-member
    resultProxyUpload = self.uploadProxy(self)
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
    caDir = getCAsLocation()
    if not caDir:
      gLogger.warn("No valid CA dir found.")
      return
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
    gLogger.notice("Generating proxy...")
    result = self.generateProxy()
    if not result['OK']:
      gLogger.error(result['Message'])
      sys.exit(1)
    self.__proxyGenerated = result['Value']

    self.checkCAs()
    self.getIssuerCert()
    self.certLifeTimeCheck()

    resultProxyWithVOMS = self.addVOMSExtIfNeeded()
    if not resultProxyWithVOMS['OK']:
      if "returning a valid AC for the user" in resultProxyWithVOMS['Message']:
        gLogger.error(resultProxyWithVOMS['Message'])
        gLogger.error("\n Are you sure you are properly registered in the VO?")
      elif "Missing voms-proxy" in resultProxyWithVOMS['Message']:
        gLogger.notice("Failed to add VOMS extension: no standard grid interface available")
      else:
        gLogger.error(resultProxyWithVOMS['Message'])
      if self.strict:
        return resultProxyWithVOMS

    if self.uploadProxy:
      resultProxyUpload = self._uploadProxy()
      if not resultProxyUpload['OK']:
        if self.strict:
          return resultProxyUpload

    return S_OK()


@ProxyInit()
def main(self):
  self.disableCS()
  self.registerSwitches(self.switches)
  self.parseCommandLine(ignoreErrors=True)

  DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseServerCertificate", "False")

  resultDoTheMagic = self.doTheMagic()
  if not resultDoTheMagic['OK']:
    gLogger.fatal(resultDoTheMagic['Message'])
    sys.exit(1)

  self.printInfo()

  sys.exit(0)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
