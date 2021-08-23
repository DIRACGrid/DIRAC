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
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (readTokenFromFile, readTokenFromEnv,
                                                                      getTokenFileLocation, BEARER_TOKEN_ENV)

__RCSID__ = "$Id$"


class Params(object):

  def __init__(self):
    self.issuer = None
    self.tokenFileLoc = None

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
    self.tokenFileLoc = arg
    return S_OK()

  def registerCLISwitches(self):
    """ Register CLI switches """
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
    tokens = []
    params = {}
    if self.issuer:
      params['issuer'] = self.issuer
    result = IdProviderFactory().getIdProvider('DIRACCLI', **params)
    if not result['OK']:
      return result
    idpObj = result['Value']
    tokenFile = getTokenFileLocation(self.tokenFileLoc)

    # Try to find token in environ and in a token file and revoke it
    for result, location in [(readTokenFromEnv(), BEARER_TOKEN_ENV),
                             (readTokenFromFile(tokenFile), tokenFile)]:
      if not result['OK']:
        gLogger.error(result['Message'])
      elif result['Value']:
        token = result['Value']
        for tokenType in ['access_token', 'refresh_token']:
          result = idpObj.revokeToken(token[tokenType], tokenType)
          if result['OK']:
            gLogger.notice('%s is revoked from' % tokenType, location)
          else:
            gLogger.error(result['Message'])

    # After remove token file
    if os.path.isfile(tokenFile):
      os.unlink(tokenFile)
      gLogger.notice('%s token file is removed.' % tokenFile)

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
