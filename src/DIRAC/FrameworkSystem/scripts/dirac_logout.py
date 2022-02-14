#!/usr/bin/env python
"""
Logout

Example:
  $ dirac-logout
"""
import os
import sys

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Security import Locations
from DIRAC.Core.Base.Script import Script
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (
    readTokenFromFile,
    readTokenFromEnv,
    getTokenFileLocation,
    BEARER_TOKEN_ENV,
)


class Params:
    def __init__(self):
        self.issuer = None
        self.targetFile = None

    def setIssuer(self, arg: str) -> dict:
        """Set issuer

        :param arg: issuer
        """
        self.issuer = arg
        return S_OK()

    def setFile(self, arg: str) -> dict:
        """Set token/proxy file

        :param arg: token file
        """
        self.targetFile = arg
        return S_OK()

    def registerCLISwitches(self):
        """Register CLI switches"""
        Script.registerSwitch("I:", "issuer=", "set issuer", self.setIssuer)
        Script.registerSwitch("F:", "file=", "set target file location", self.setFile)

    def removeProxy(self):
        """Log out with proxy

        :return: S_OK()/S_ERROR()
        """
        proxyFile = self.targetFile or Locations.getDefaultProxyLocation()
        if os.path.isfile(proxyFile):
            os.unlink(proxyFile)
            gLogger.notice(f"{proxyFile} proxy file is removed.")
        return S_OK()

    def revokeTokens(self):
        """Log out with tokens

        :return: S_OK()/S_ERROR()
        """
        params = {}
        if self.issuer:
            params["issuer"] = self.issuer
        result = IdProviderFactory().getIdProvider("DIRACCLI", **params)
        if not result["OK"]:
            return result
        idpObj = result["Value"]
        tokenFile = getTokenFileLocation(self.targetFile)

        # Try to find token in environ and in a token file and revoke it
        for result, location in [(readTokenFromEnv(), BEARER_TOKEN_ENV), (readTokenFromFile(tokenFile), tokenFile)]:
            if not result["OK"]:
                gLogger.error(result["Message"])
            elif result["Value"]:
                token = result["Value"]
                for tokenType in ["access_token", "refresh_token"]:
                    result = idpObj.revokeToken(token[tokenType], tokenType)
                    if result["OK"]:
                        gLogger.notice("%s is revoked from" % tokenType, location)
                    else:
                        gLogger.error(result["Message"])

        # After remove token file
        if os.path.isfile(tokenFile):
            os.unlink(tokenFile)
            gLogger.notice(f"{tokenFile} token file is removed.")

        return S_OK()


@Script()
def main():
    p = Params()
    p.registerCLISwitches()

    Script.parseCommandLine()
    # It's server installation?
    if gConfig.useServerCertificate():
        # In this case you do not need to login.
        gLogger.notice(
            "You have run the command in a DIRAC server installation environment, which eliminates the need for login."
        )
        DIRAC.exit(1)

    # What the user has set up to use?
    useTokens = gConfig.getValue("/DIRAC/Security/UseTokens", "false").lower() in ("y", "yes", "true")
    if "DIRAC_USE_ACCESS_TOKEN" in os.environ:
        useTokens = os.environ.get("DIRAC_USE_ACCESS_TOKEN", "false").lower() in ("y", "yes", "true")

    result = p.revokeTokens() if useTokens else p.removeProxy()
    if not result["OK"]:
        gLogger.fatal(result["Message"])
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
