#!/usr/bin/env python
########################################################################
# File :    dirac-login.py
# Author :  Adrian Casajus
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
import urllib3
import requests
import threading

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.TokenFile import writeTokenDictToTokenFile
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.Core.Security.TokenInfo import getTokenInfo, formatTokenInfoAsString
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

__RCSID__ = "$Id$"


class Params(object):

  def __init__(self):
    self.proxy = False
    self.group = None
    self.lifetime = None
    self.provider = 'DIRACCLI'
    self.issuer = None
    self.proxyLoc = '/tmp/x509up_u%s' % os.getuid()

  def returnProxy(self, _arg):
    """ Set email

        :return: S_OK()
    """
    self.proxy = True
    return S_OK()
  
  def setGroup(self, arg):
    """ Set email

        :param str arg: group

        :return: S_OK()
    """
    self.group = arg
    return S_OK()
  
  def setProvider(self, arg):
    """ Set email

        :param str arg: provider

        :return: S_OK()
    """
    self.provider = arg
    return S_OK()
  
  def setIssuer(self, arg):
    """ Set email

        :param str arg: issuer

        :return: S_OK()
    """
    self.issuer = arg
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
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Submit Device authorisation flow
    try:
      result = idpObj.authorization()
    except KeyboardInterrupt as e:
      return S_ERROR(repr(e))
    if not result['OK']:
      return result
    
    if self.proxy:
      result = writeToProxyFile(idpObj.token['proxy'].encode("UTF-8"), self.proxyLoc)
      if not result['OK']:
        return result
      gLogger.notice('Proxy is saved to %s.' % self.proxyLoc)
    else:
      result = writeTokenDictToTokenFile(idpObj.token)
      if not result['OK']:
        return result
      gLogger.notice('Token is saved in %s.' % result['Value'])

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
      result = getTokenInfo(self.proxyLoc)
      if not result['OK']:
        return result['Message']
      gLogger.notice(formatTokenInfoAsString(result['Value']))

    return S_OK(self.proxyLoc)


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
