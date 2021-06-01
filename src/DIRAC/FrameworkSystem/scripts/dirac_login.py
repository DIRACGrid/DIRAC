#!/usr/bin/env python
########################################################################
# File :    dirac-login.py
# Author :  Andrii Lytovchenko
########################################################################
"""
Login to DIRAC.

Example:
  $ dirac-login -g dirac_user
"""
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

import os
import sys

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import writeTokenDictToTokenFile, readTokenFromFile

__RCSID__ = "$Id$"


class Params(object):

  def __init__(self):
    self.proxy = False
    self.group = None
    self.lifetime = None
    self.provider = 'DIRACCLI'
    self.issuer = None
    self.proxyLoc = '/tmp/x509up_u%s' % os.getuid()
    self.tokenLoc = None

  def returnProxy(self, _arg):
    """ To return proxy

        :return: S_OK()
    """
    self.proxy = True
    return S_OK()

  def setGroup(self, arg):
    """ Set group

        :param str arg: group

        :return: S_OK()
    """
    self.group = arg
    return S_OK()

  def setProvider(self, arg):
    """ Set provider name

        :param str arg: provider

        :return: S_OK()
    """
    self.provider = arg
    return S_OK()

  def setIssuer(self, arg):
    """ Set issuer

        :param str arg: issuer

        :return: S_OK()
    """
    self.issuer = arg
    return S_OK()

  def setTokenFile(self, arg):
    """ Set token file

        :param str arg: token file

        :return: S_OK()
    """
    self.tokenLoc = arg
    return S_OK()

  def setLivetime(self, arg):
    """ Set email

        :param str arg: lifetime

        :return: S_OK()
    """
    self.lifetime = arg
    return S_OK()

  def registerCLISwitches(self):
    """ Register CLI switches """
    Script.registerSwitch(
        "P",
        "proxy",
        "return with an access token also a proxy certificate with DIRAC group extension",
        self.returnProxy)
    Script.registerSwitch(
        "g:",
        "group=",
        "set DIRAC group",
        self.setGroup)
    Script.registerSwitch(
        "O:",
        "provider=",
        "set identity provider",
        self.setProvider)
    Script.registerSwitch(
        "I:",
        "issuer=",
        "set issuer",
        self.setIssuer)
    Script.registerSwitch(
        "T:",
        "lifetime=",
        "set proxy lifetime in a hours",
        self.setLivetime)
    Script.registerSwitch(
        "F:",
        "file=",
        "set token file location",
        self.setTokenFile)

  def doOAuthMagic(self):
    """ Magic method with tokens

        :return: S_OK()/S_ERROR()
    """
    params = {}
    if self.issuer:
      params['issuer'] = self.issuer
    result = IdProviderFactory().getIdProvider(self.provider, **params)
    if not result['OK']:
      return result
    idpObj = result['Value']
    if self.group:
      idpObj.scope += '+g:%s' % self.group
    if self.proxy:
      idpObj.scope += '+proxy'
    if self.lifetime:
      idpObj.scope += '+lifetime:%s' % (int(self.lifetime) * 3600)

    # Submit Device authorisation flow
    result = idpObj.deviceAuthorization()
    if not result['OK']:
      return result

    if self.proxy:
      result = writeToProxyFile(idpObj.token['proxy'].encode("UTF-8"), self.proxyLoc)
      if not result['OK']:
        return result
      gLogger.notice('Proxy is saved to %s.' % self.proxyLoc)
    else:
      result = writeTokenDictToTokenFile(idpObj.token, self.tokenLoc)
      if not result['OK']:
        return result
      self.tokenLoc = result['Value']
      gLogger.notice('Token is saved in %s.' % self.tokenLoc)

    result = Script.enableCS()
    if not result['OK']:
      return S_ERROR("Cannot contact CS to get user list")
    DIRAC.gConfig.forceRefresh()

    if self.proxy:
      result = getProxyInfo(self.proxyLoc)
      if not result['OK']:
        return result['Message']
      gLogger.notice(formatProxyInfoAsString(result['Value']))
    else:
      result = readTokenFromFile(self.tokenLoc)
      if not result['OK']:
        return result
      gLogger.notice(result['Value'].getInfoAsString())

    return S_OK()


@DIRACScript()
def main():
  piParams = Params()
  piParams.registerCLISwitches()

  Script.disableCS()
  Script.parseCommandLine(ignoreErrors=True)
  DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseServerCertificate", "False")

  resultDoMagic = piParams.doOAuthMagic()
  if not resultDoMagic['OK']:
    gLogger.fatal(resultDoMagic['Message'])
    sys.exit(1)

  sys.exit(0)


if __name__ == "__main__":
  main()
