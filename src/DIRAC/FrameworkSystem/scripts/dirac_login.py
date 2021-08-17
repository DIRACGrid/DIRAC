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
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (writeTokenDictToTokenFile, readTokenFromFile,
                                                                      getTokenFileLocation)

__RCSID__ = "$Id$"


class Params(object):

  def __init__(self):
    self.info = False
    self.provider = 'DIRACCLI'
    self.proxy = False
    self.group = None
    self.lifetime = None
    self.issuer = None
    self.proxyLoc = '/tmp/x509up_u%s' % os.getuid()
    self.tokenLoc = None

  def getInfo(self, _arg):
    """ To return user info

        :return: S_OK()
    """
    self.info = True
    return S_OK()

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
    Script.registerSwitch("", "info", "output current user authorization status", self.getInfo)
    Script.registerSwitch("P", "proxy", "request a proxy certificate with DIRAC group extension", self.returnProxy)
    Script.registerSwitch("g:", "group=", "set DIRAC group", self.setGroup)
    Script.registerSwitch("I:", "issuer=", "set issuer", self.setIssuer)
    Script.registerSwitch("T:", "lifetime=", "set proxy lifetime in a hours", self.setLivetime)
    Script.registerSwitch("F:", "file=", "set token file location", self.setTokenFile)

  def doOAuthMagic(self):
    """ Magic method with tokens

        :return: S_OK()/S_ERROR()
    """
    tokenFile = getTokenFileLocation(self.tokenLoc)

    if self.info:
      # Try to get user information
      result = Script.enableCS()
      if not result['OK']:
        return S_ERROR("Cannot contact CS.")
      useTokens = DIRAC.gConfig.getValue('/DIRAC/Security/UseTokens', 'false').lower() in ("y", "yes", "true")
      if 'DIRAC_USE_ACCESS_TOKEN' in os.environ:
        useTokens = os.environ.get('DIRAC_USE_ACCESS_TOKEN', 'false').lower() in ("y", "yes", "true")
      if useTokens:
        gLogger.notice('You use proxy, to use access token set "DIRAC_USE_ACCESS_TOKEN=True" env.\n')
        result = getProxyInfo(self.proxyLoc)
        if not result['OK']:
          return result['Message']
        gLogger.notice(formatProxyInfoAsString(result['Value']))
      else:
        gLogger.notice('You use access token, to use proxy set "DIRAC_USE_ACCESS_TOKEN=False" env.\n')
        result = readTokenFromFile(tokenFile)
        if not result['OK']:
          return result
        gLogger.notice(result['Value'].getInfoAsString())
      return S_OK()
    params = {}
    if self.issuer:
      params['issuer'] = self.issuer
    result = IdProviderFactory().getIdProvider(self.provider, **params)
    if not result['OK']:
      return result
    idpObj = result['Value']
    scope = []
    if self.group:
      scope.append('g:%s' % self.group)
    if self.proxy:
      scope.append('proxy')
    if self.lifetime:
      scope.append('lifetime:%s' % (int(self.lifetime) * 3600))
    idpObj.scope = '+'.join(scope) if scope else ''

    # Submit Device authorisation flow
    result = idpObj.deviceAuthorization()
    if not result['OK']:
      return result

    if self.proxy:
      os.environ['DIRAC_USE_ACCESS_TOKEN'] = 'False'
      # Save new proxy certificate
      result = writeToProxyFile(idpObj.token['proxy'].encode("UTF-8"), self.proxyLoc)
      if not result['OK']:
        return result
      gLogger.notice('Proxy is saved to %s.' % self.proxyLoc)
    else:
      os.environ['DIRAC_USE_ACCESS_TOKEN'] = 'True'
      # Revoke old tokens from token file
      if os.path.isfile(tokenFile):
        result = readTokenFromFile(tokenFile)
        if not result['OK']:
          gLogger.error(result['Message'])
        elif result['Value']:
          oldToken = result['Value']
          for tokenType in ['access_token', 'refresh_token']:
            result = idpObj.revokeToken(oldToken[tokenType], tokenType)
            if result['OK']:
              gLogger.notice('%s is revoked from' % tokenType, tokenFile)
            else:
              gLogger.error(result['Message'])

      # Save new tokens to token file
      result = writeTokenDictToTokenFile(idpObj.token, tokenFile)
      if not result['OK']:
        return result
      tokenFile = result['Value']
      gLogger.notice('New token is saved to %s.' % tokenFile)

      if not DIRAC.gConfig.getValue('/DIRAC/Security/Authorization/issuer'):
        gLogger.notice('To continue use token you need to add /DIRAC/Security/Authorization/issuer option.')
        if not self.issuer:
          DIRAC.exit(1)
        DIRAC.gConfig.setOptionValue('/DIRAC/Security/Authorization/issuer', self.issuer)

    # Try to get user information
    result = Script.enableCS()
    if not result['OK']:
      return S_ERROR("Cannot contact CS.")
    DIRAC.gConfig.forceRefresh()

    if self.proxy:
      result = getProxyInfo(self.proxyLoc)
      if not result['OK']:
        return result['Message']
      gLogger.notice(formatProxyInfoAsString(result['Value']))
    else:
      result = readTokenFromFile(tokenFile)
      if not result['OK']:
        return result
      gLogger.notice(result['Value'].getInfoAsString())

    return S_OK()


@Script()
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
