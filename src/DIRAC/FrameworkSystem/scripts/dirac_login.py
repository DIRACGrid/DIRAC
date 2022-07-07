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
from prompt_toolkit import prompt, print_formatted_text as print, HTML

import DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.Locations import getDefaultProxyLocation, getCertificateAndKeyLocation
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.NTP import getClockDeviation
from DIRAC.Core.Base.Script import Script

# At this point, we disable CS synchronization so that an error related
# to the lack of a proxy certificate does not occur when trying to synchronize.
# Synchronization will take place after passing the authorization algorithm (creating a proxy).
Script.disableCS()

from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import (
    writeTokenDictToTokenFile,
    readTokenFromFile,
    getTokenFileLocation,
)
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (
    getGroupOption,
    getVOMSAttributeForGroup,
    getVOMSVOForGroup,
    findDefaultGroupForDN,
)

# This value shows what authorization way will be by default
DEFAULT_AUTH_WAY = "certificate"  # possible values are "certificate", "diracas"
# This value shows what response will be by default
DEFAULT_RESPONSE = "proxy"  # possible values are "proxy", "token"


class Params:
    """This class describes the input parameters"""

    # A copy of the state of the environment variables will help
    # at the end of the script when creating a message to the user
    ENV = os.environ.copy()

    def __init__(self):
        """C`r"""
        self.group = None
        self.scopes = []
        self.outputFile = None
        self.lifetime = None
        self.issuer = None
        self.certLoc = None
        self.keyLoc = None
        self.response = DEFAULT_RESPONSE
        self.authWith = DEFAULT_AUTH_WAY
        self.enableCS = True

    def disableCS(self, _) -> dict:
        """Disable CS"""
        self.enableCS = False
        return S_OK()

    def setIssuer(self, issuer: str) -> dict:
        """Set DIRAC Authorization Server issuer"""
        self.useDIRACAS(None)
        self.issuer = issuer
        return S_OK()

    def useDIRACAS(self, _) -> dict:
        """Use DIRAC AS"""
        self.authWith = "diracas"
        return S_OK()

    def useCertificate(self, _) -> dict:
        """Use certificate"""
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "false"
        self.authWith = "certificate"
        self.response = "proxy"
        return S_OK()

    def setCertificate(self, filePath: str) -> dict:
        """Set certificate file path"""
        if not os.path.exists(filePath):
            DIRAC.gLogger.error(f"{filePath} does not exist.")
            DIRAC.exit(1)
        self.useCertificate(None)
        self.certLoc = filePath
        return S_OK()

    def setPrivateKey(self, filePath: str) -> dict:
        """Set private key file path"""
        if not os.path.exists(filePath):
            DIRAC.gLogger.error(f"{filePath} is not exist.")
            DIRAC.exit(1)
        self.useCertificate(None)
        self.keyLoc = filePath
        return S_OK()

    def setOutputFile(self, filePath: str) -> dict:
        """Set output file location"""
        self.outputFile = filePath
        return S_OK()

    def setLifetime(self, lifetime: str) -> dict:
        """Set proxy lifetime"""
        self.lifetime = lifetime
        return S_OK()

    def setProxy(self, _) -> dict:
        """Return proxy"""
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "false"
        self.response = "proxy"
        return S_OK()

    def setToken(self, _) -> dict:
        """Return tokens"""
        os.environ["DIRAC_USE_ACCESS_TOKEN"] = "true"
        self.useDIRACAS(None)
        self.response = "token"
        return S_OK()

    def authStatus(self, _) -> dict:
        """Get authorization status"""
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
            "out=",
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
        if self.response == "proxy" and self.response not in self.scopes:
            self.scopes.append(self.response)
        if self.lifetime:
            self.scopes.append("lifetime:%s" % (int(self.lifetime or 12) * 3600))
        idpObj.scope = "+".join(self.scopes) if self.scopes else ""

        # Submit Device authorisation flow
        result = idpObj.deviceAuthorization()
        if not result["OK"]:
            return result

        if self.response == "proxy":
            self.outputFile = self.outputFile or getDefaultProxyLocation()
            # Save new proxy certificate
            result = writeToProxyFile(idpObj.token["proxy"], self.outputFile)
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

        return S_OK()

    def loginWithCertificate(self):
        """Login with certificate"""
        # Search certificate and key
        if not self.certLoc or not self.keyLoc:
            if not (cakLoc := getCertificateAndKeyLocation()):
                if not self.authWith:  # if user do not choose this way
                    print(HTML("<yellow>Can't find user certificate and key</yellow>, trying to connect to DIRAC AS.."))
                    return self.doOAuthMagic()  # Then try to use DIRAC AS
                return S_ERROR("Can't find user certificate and key")
            self.certLoc = self.certLoc or cakLoc[0]
            self.keyLoc = self.keyLoc or cakLoc[1]

        chain = X509Chain()
        # Load user cert and key
        if (result := chain.loadChainFromFile(self.certLoc))["OK"]:
            # We try to download the key first without a password
            if not (result := chain.loadKeyFromFile(self.keyLoc))["OK"]:
                password = prompt("Enter Certificate password: ", is_password=True)
                result = chain.loadKeyFromFile(self.keyLoc, password=password)
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
        self.outputFile = self.outputFile or getDefaultProxyLocation()
        parameters = (self.outputFile, int(self.lifetime or 12) * 3600, self.group)

        # Add a VOMS extension if the group requires it
        if (result := chain.generateProxyToFile(*parameters))["OK"] and (result := self.__enableCS())["OK"]:
            if not self.group and (result := findDefaultGroupForDN(credentials["DN"]))["OK"]:
                self.group = result["Value"]  # Use default group if user don't set it
            # based on the configuration we decide whether to add VOMS extensions
            if getGroupOption(self.group, "AutoAddVOMS", False):
                if not (vomsAttr := getVOMSAttributeForGroup(self.group)):
                    print(HTML(f"<yellow>No VOMS attribute foud for {self.group}</yellow>"))
                else:
                    vo = getVOMSVOForGroup(self.group)
                    if not (result := VOMS().setVOMSAttributes(self.outputFile, attribute=vomsAttr, vo=vo))["OK"]:
                        return S_ERROR(f"Failed adding VOMS attribute: {result['Message']}")
                    chain = result["Value"]
                    result = chain.generateProxyToFile(*parameters)
        if not result["OK"]:
            return S_ERROR(f"Couldn't generate proxy: {result['Message']}")

        if self.enableCS:
            # After creating the proxy, we can try to connect to the server
            if not (result := self.__enableCS())["OK"]:
                return result

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

    def __enableCS(self):
        if not (result := Script.enableCS())["OK"] or not (result := gConfig.forceRefresh())["OK"]:
            return S_ERROR(f"Cannot contact CS: {result['Message']}")
        return result

    def howToSwitch(self) -> bool:
        """Helper message, how to switch access type(proxy or access token)"""
        if "DIRAC_USE_ACCESS_TOKEN" in self.ENV:
            src, useTokens = ("env", self.ENV.get("DIRAC_USE_ACCESS_TOKEN", "false").lower() in ("y", "yes", "true"))
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

        # Show infomation message only if the current state of the user environment does not match the authorization result
        if (useTokens and (self.response == "proxy")) or (not useTokens and (self.response == "token")):
            gLogger.notice(msg)

    def getAuthStatus(self):
        """Try to get user authorization status.
        :return: S_OK()/S_ERROR()
        """
        if not (result := self.__enableCS())["OK"]:
            return result

        if self.response == "proxy":
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
    userParams = Params()
    userParams.registerCLISwitches()

    # Check time
    deviation = getClockDeviation()
    if not deviation["OK"]:
        gLogger.warn(deviation["Message"])
    elif deviation["Value"] > 60:
        gLogger.fatal(f"Your host's clock seems to deviate by {(int(deviation['Value']) / 60):.0f} minutes!")
        sys.exit(1)

    Script.parseCommandLine(ignoreErrors=True)
    # It's server installation?
    if gConfig.useServerCertificate():
        # In this case you do not need to login.
        gLogger.notice("You should not need to run this command in a DIRAC server. Exiting.")
        DIRAC.exit(1)

    userParams.group, userParams.scopes = Script.getPositionalArgs(group=True)
    # If you have chosen to use a certificate then a proxy will be generated locally using the specified certificate
    if userParams.authWith == "certificate":
        result = userParams.loginWithCertificate()

    # Otherwise, you must log in to the authorization server to gain access
    else:
        result = userParams.doOAuthMagic()

    # Print authorization status
    if result["OK"] and userParams.enableCS:
        result = userParams.getAuthStatus()

    if not result["OK"]:
        # Format the message as it can contain special characters that would be interpreted as HTML
        print(HTML("<red>{error}</red>").format(error=result["Message"]))
        sys.exit(1)

    userParams.howToSwitch()
    sys.exit(0)


if __name__ == "__main__":
    main()
