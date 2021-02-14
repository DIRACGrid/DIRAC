#!/usr/bin/env python

from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import glob
import time
import threading

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Security import ProxyInfo  # pylint: disable=import-error
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.FrameworkSystem.Client import ProxyGeneration
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient
from DIRAC.FrameworkSystem.private.testNotebookAuth import notebookAuth

__RCSID__ = "$Id$"


class Params(ProxyGeneration.CLIParams):

  addVOMSExt = False

  def setVOMSExt(self, _arg):
    """ Set VOMS extention

        :param _arg: unuse

        :return: S_OK()
    """
    self.addVOMSExt = True
    return S_OK()

  def registerCLISwitches(self):
    """ Register CLI switches """
    ProxyGeneration.CLIParams.registerCLISwitches(self)
    Script.registerSwitch("M", "VOMS", "Add voms extension", self.setVOMSExt)


class ProxyInit(object):

  def __init__(self, piParams):
    """ Constructor """
    self.__piParams = piParams
    self.__issuerCert = False
    self.__proxyGenerated = False
    self.__uploadedInfo = {}

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

  def doOAuthMagic(self):
    """ Magic method

        :return: S_OK()/S_ERROR()
    """
    if not self.__piParams.diracGroup:
      return S_ERROR('Need to set user group.')
    nAuth = notebookAuth(self.__piParams.diracGroup, voms=self.__piParams.addVOMSExt, proxyPath=self.__piParams.proxyLoc)
    result = nAuth.getToken()
    if not result['OK']:
      return result
    aToken = result['Value'].get('access_token')
    if not aToken:
      return S_ERROR('Access token is absent in resporse.')
    result = nAuth.getProxyWithToken(aToken)
    if not result['OK']:
      return result

    result = Script.enableCS()
    if not result['OK']:
      return S_ERROR("Cannot contact CS to get user list")
    threading.Thread(target=self.checkCAs).start()
    gConfig.forceRefresh(fromMaster=True)
    return S_OK(self.__piParams.proxyLoc)


@DIRACScript()
def main():
  piParams = Params()
  piParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine(ignoreErrors=True)
  DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseServerCertificate", "False")

  pI = ProxyInit(piParams)
  gLogger.info(gConfig.getConfigurationTree())
  resultDoMagic = pI.doOAuthMagic()
  if not resultDoMagic['OK']:
    gLogger.fatal(resultDoMagic['Message'])
    sys.exit(1)

  pI.printInfo()

  sys.exit(0)


if __name__ == "__main__":
  main()
