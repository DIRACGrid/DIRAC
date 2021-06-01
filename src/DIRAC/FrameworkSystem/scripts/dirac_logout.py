#!/usr/bin/env python
########################################################################
# File :    dirac-logout.py
# Author :  Andrii Lytovchenko
########################################################################
"""
Logout

Example:
  $ dirac-logout
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
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import readTokenFromFile, getTokenLocation

__RCSID__ = "$Id$"


class Params(object):

  def __init__(self):
    self.provider = 'DIRACCLI'
    self.issuer = None
    self.tokenLoc = None

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

  def registerCLISwitches(self):
    """ Register CLI switches """
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
    self.tokenLoc = self.tokenLoc or getTokenLocation()
    result = readTokenFromFile(self.tokenLoc)
    if not result['OK']:
      return result
    token = result['Value']
    # Revoke token
    for tokenType in ['access_token', 'refresh_token']:
      if token.get(tokenType):
        result = idpObj.revokeToken(token[tokenType], tokenType)
        if not result['OK']:
          gLogger.error(result['Message'])
    os.unlink(self.tokenLoc)
    gLogger.notice('Token is removed from %s.' % self.tokenLoc)

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
