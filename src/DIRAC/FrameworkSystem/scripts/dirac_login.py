#!/usr/bin/env python
"""
With this command you can log in to DIRAC.

There are two options:

  - using a user certificate, creating a proxy.
  - go through DIRAC Authorization Server by selecting your Identity Provider.


Example:
  # Login with default group
  $ dirac-login
  # Choose another group
  $ dirac-login dirac_user
  # Return token
  $ dirac-login dirac_user --token
"""
import os
import sys
import copy
import getpass

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.NTP import getClockDeviation
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (
    writeTokenDictToTokenFile,
    readTokenFromFile,
    getTokenFileLocation,
)


class Params:
    """This class describes the input parameters"""

    def __init__(self):
        """C`r"""
        self.group = None
        self.scopes = []
        self.outputFile = None
        self.lifetime = None
        self.issuer = None
        self.certLoc = None
        self.keyLoc = None
        self.result = "proxy"
        self.authWith = "certificate"
        self.enableCS = True

    def disableCS(self, _arg) -> dict:
        """Set issuer

        :param arg: issuer
        """
        self.enableCS = False
        return S_OK()

    def setIssuer(self, arg: str) -> dict:
        """Set issuer

        :param arg: issuer
        """
        self.useDIRACAS(None)
        self.issuer = arg
        return S_OK()

    def useDIRACAS(self, _arg) -> dict:
        """Use DIRAC AS

        :param _arg: unuse
        """
        self.authWith = "diracas"
        return S_OK()

    def useCertificate(self, _arg) -> dict:
        """Use certificate

        :param _arg: unuse
        """
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "false"
        self.authWith = "certificate"
        self.result = "proxy"
        return S_OK()

    def setCertificate(self, arg: str) -> dict:
        """Set certificate file path

        :param arg: path
        """
        if not os.path.exists(arg):
            DIRAC.gLogger.error(f"{arg} does not exist.")
            DIRAC.exit(1)
        self.useCertificate(None)
        self.certLoc = arg
        return S_OK()

    def setPrivateKey(self, arg: str) -> dict:
        """Set private key file path

        :param arg: path
        """
        if not os.path.exists(arg):
            DIRAC.gLogger.error(f"{arg} is not exist.")
            DIRAC.exit(1)
        self.useCertificate(None)
        self.keyLoc = arg
        return S_OK()

    def setOutputFile(self, arg: str) -> dict:
        """Set output file location

        :param arg: output file location
        """
        self.outputFile = arg
        return S_OK()

    def setLifetime(self, arg: str) -> dict:
        """Set proxy lifetime

        :param arg: lifetime
        """
        self.lifetime = arg
        return S_OK()

    def setProxy(self, _arg) -> dict:
        """Return proxy

        :param _arg: unuse
        """
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "false"
        self.result = "proxy"
        return S_OK()

    def setToken(self, _arg) -> dict:
        """Return tokens

        :param _arg: unuse
        """
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "true"
        self.useDIRACAS(None)
        self.result = "token"
        return S_OK()

    def authStatus(self, _arg) -> dict:
        """Get authorization status

        :param _arg: unuse
        """
        result = self.getAuthStatus()
        if result["OK"]:
            self.howToSwitch()
            DIRAC.exit(0)
        gLogger.fatal(result["Message"])
        DIRAC.exit(1)

    def registerCLISwitches(self):
        """Register CLI switches"""
        Script.registerArgument(
            "group: select a DIRAC group for authorization, can be determined later.", mandatory=False
        )
        Script.registerArgument(["scope: scope to add to authorization request."], mandatory=False)
        Script.registerSwitch("T:", "lifetime=", "set access lifetime in hours", self.setLifetime)
        Script.registerSwitch(
            "O:",
            "save-output=",
            "where to save the authorization result(e.g: proxy or tokens). By default we will try to find a standard place.",
            self.setOutputFile,
        )
        Script.registerSwitch("I:", "issuer=", "set issuer.", self.setIssuer)
        Script.registerSwitch(
            "",
            "use-certificate",
            "in case you want to generate a proxy using a certificate. By default.",
            self.useCertificate,
        )
        Script.registerSwitch(
            "", "use-diracas", "in case you want to authorize with DIRAC Authorization Server.", self.useDIRACAS
        )
        Script.registerSwitch("C:", "certificate=", "user certificate location", self.setCertificate)
        Script.registerSwitch("K:", "key=", "user key location", self.setPrivateKey)
        Script.registerSwitch("", "proxy", "return proxy in case of successful authorization", self.setProxy)
        Script.registerSwitch("", "token", "return tokens in case of successful authorization", self.setToken)
        Script.registerSwitch("", "status", "print user authorization status", self.authStatus)
        Script.registerSwitch("", "nocs", "disable CS.", self.disableCS)

    def doOAuthMagic(self):
        """Magic method with tokens

        :return: S_OK()/S_ERROR()
        """
        params = {}
        if self.issuer:
            params["issuer"] = self.issuer
        result = IdProviderFactory().getIdProvider("DIRACCLI", **params)
        if not result["OK"]:
            return result
        idpObj = result["Value"]
        if self.group and self.group not in self.scopes:
            self.scopes.append(f"g:{self.group}")
        if self.result == "proxy" and self.result not in self.scopes:
            self.scopes.append(self.result)
        if self.lifetime:
            self.scopes.append("lifetime:%s" % (int(self.lifetime or 12) * 3600))
        idpObj.scope = "+".join(self.scopes) if self.scopes else ""

        # Submit Device authorisation flow
        result = idpObj.deviceAuthorization()
        if not result["OK"]:
            return result

        if self.result == "proxy":
            self.outputFile = self.outputFile or Locations.getDefaultProxyLocation()
            # Save new proxy certificate
            result = writeToProxyFile(idpObj.token["proxy"].encode("UTF-8"), self.outputFile)
            if not result["OK"]:
                return result
            gLogger.notice(f"Proxy is saved to {self.outputFile}.")
        else:
            # Revoke old tokens from token file
            self.outputFile = getTokenFileLocation(self.outputFile)
            if os.path.isfile(self.outputFile):
                result = readTokenFromFile(self.outputFile)
                if not result["OK"]:
                    gLogger.error(result["Message"])
                elif result["Value"]:
                    oldToken = result["Value"]
                    for tokenType in ["access_token", "refresh_token"]:
                        result = idpObj.revokeToken(oldToken[tokenType], tokenType)
                        if result["OK"]:
                            gLogger.notice(f"{tokenType} is revoked from", self.outputFile)
                        else:
                            gLogger.error(result["Message"])

            # Save new tokens to token file
            result = writeTokenDictToTokenFile(idpObj.token, self.outputFile)
            if not result["OK"]:
                return result
            self.outputFile = result["Value"]
            gLogger.notice(f"New token is saved to {self.outputFile}.")

            if not DIRAC.gConfig.getValue("/DIRAC/Security/Authorization/issuer"):
                gLogger.notice("To continue use token you need to add /DIRAC/Security/Authorization/issuer option.")
                if not self.issuer:
                    DIRAC.exit(1)
                DIRAC.gConfig.setOptionValue("/DIRAC/Security/Authorization/issuer", self.issuer)

            # Try to get user authorization information from token
            result = readTokenFromFile(self.outputFile)
            if not result["OK"]:
                return result
            gLogger.notice(result["Value"].getInfoAsString())

        return S_OK()

    def loginWithCertificate(self):
        """Login with certificate"""
        # Search certificate and key
        if not self.certLoc or not self.keyLoc:
            cakLoc = Locations.getCertificateAndKeyLocation()
            if not cakLoc:
                return S_ERROR("Can't find user certificate and key")
            self.certLoc = self.certLoc or cakLoc[0]
            self.keyLoc = self.keyLoc or cakLoc[1]

        chain = X509Chain()
        # Load user cert and key
        result = chain.loadChainFromFile(self.certLoc)
        if result["OK"]:
            result = chain.loadKeyFromFile(self.keyLoc, password=getpass.getpass("Enter Certificate password:"))
        if not result["OK"]:
            return result

        # Read user credentials
        result = chain.getCredentials(withRegistryInfo=False)
        if not result["OK"]:
            return result
        credentials = result["Value"]

        # Remember a clean proxy to then upload it in step 2
        proxy = copy.copy(chain)

        # Create local proxy with group
        self.outputFile = self.outputFile or Locations.getDefaultProxyLocation()
        result = chain.generateProxyToFile(self.outputFile, int(self.lifetime or 12) * 3600, self.group)
        if not result["OK"]:
            return S_ERROR(f"Couldn't generate proxy: {result['Message']}")

        if self.enableCS:
            # After creating the proxy, we can try to connect to the server
            result = Script.enableCS()
            if not result["OK"]:
                return S_ERROR(f"Cannot contact CS: {result['Message']}")
            gConfig.forceRefresh()

            # Step 2: Upload proxy to DIRAC server
            result = gProxyManager.getUploadedProxyLifeTime(credentials["subject"])
            if not result["OK"]:
                return result
            uploadedProxyLifetime = result["Value"]

            # Upload proxy to the server if it longer that uploaded one
            if credentials["secondsLeft"] > uploadedProxyLifetime:
                gLogger.notice("Upload proxy to server.")
                return gProxyManager.uploadProxy(proxy)
        return S_OK()

    def howToSwitch(self) -> bool:
        """Helper message, how to switch access type(proxy or access token)"""
        if "DIRAC_USE_ACCESS_TOKEN" in os.environ:
            src, useTokens = ("env", os.environ.get("DIRAC_USE_ACCESS_TOKEN", "false").lower() in ("y", "yes", "true"))
        else:
            src, useTokens = (
                "conf",
                gConfig.getValue("/DIRAC/Security/UseTokens", "false").lower() in ("y", "yes", "true"),
            )
        msg = f"\nYou are currently using {'access token' if useTokens else 'proxy'} to access new HTTP DIRAC services."
        msg += f" To use a {'proxy' if useTokens else 'access token'} instead, do the following:\n"
        if src == "conf":
            msg += f"  set /DIRAC/Security/UseTokens={not useTokens} in dirac.cfg\nor\n"
        msg += f"  export DIRAC_USE_ACCESS_TOKEN={not useTokens}\n"
        gLogger.notice(msg)

        return useTokens

    def getAuthStatus(self):
        """Try to get user authorization status.

        :return: S_OK()/S_ERROR()
        """
        result = Script.enableCS()
        if not result["OK"]:
            return S_ERROR("Cannot contact CS.")
        gConfig.forceRefresh()

        if self.result == "proxy":
            result = getProxyInfo(self.outputFile)
            if result["OK"]:
                gLogger.notice(formatProxyInfoAsString(result["Value"]))
        else:
            result = readTokenFromFile(self.outputFile)
            if result["OK"]:
                gLogger.notice(result["Value"].getInfoAsString())

        return result


@Script()
def main():
    p = Params()
    p.registerCLISwitches()

    # Check time
    deviation = getClockDeviation()
    if not deviation["OK"]:
        gLogger.warn(deviation["Message"])
    elif deviation["Value"] > 60:
        gLogger.fatal(f"Your host's clock seems to deviate by {(int(deviation['Value']) / 60):.0f} minutes!")
        sys.exit(1)

    Script.disableCS()
    Script.parseCommandLine(ignoreErrors=True)
    # It's server installation?
    if gConfig.useServerCertificate():
        # In this case you do not need to login.
        gLogger.notice(
            "You have run the command in a DIRAC server installation environment, which eliminates the need for login."
        )
        DIRAC.exit(1)

    p.group, p.scopes = Script.getPositionalArgs(group=True)
    # If you have chosen to use a certificate then a proxy will be generated locally using the specified certificate
    if p.authWith == "certificate":
        result = p.loginWithCertificate()

    # Otherwise, you must log in to the authorization server to gain access
    else:
        result = p.doOAuthMagic()

    # Print authorization status
    if result["OK"] and p.enableCS:
        result = p.getAuthStatus()

    if not result["OK"]:
        gLogger.fatal(result["Message"])
        sys.exit(1)

    p.howToSwitch()
    sys.exit(0)


if __name__ == "__main__":
    main()
