#!/usr/bin/env python
"""
Creating a proxy.

Example:
  $ dirac-proxy-init -g dirac_user
  Enter Certificate password: **************
"""
import datetime
import glob
import os
import sys
import time

import DIRAC
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security import VOMS, ProxyInfo, X509Chain
from DIRAC.Core.Security.DiracX import addTokenToPEM
from DIRAC.Core.Security.Locations import getCAsLocation, getDefaultProxyLocation
from DIRAC.FrameworkSystem.Client import ProxyGeneration, ProxyUpload
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient


class Params(ProxyGeneration.CLIParams):
    addVOMSExt = False
    uploadProxy = True
    uploadPilot = False

    def setVOMSExt(self, _arg):
        self.addVOMSExt = True
        return S_OK()

    def disableProxyUpload(self, _arg):
        self.uploadProxy = False
        return S_OK()

    def registerCLISwitches(self):
        ProxyGeneration.CLIParams.registerCLISwitches(self)
        Script.registerSwitch(
            "N", "no-upload", "Do not upload a long lived proxy to the ProxyManager", self.disableProxyUpload
        )
        Script.registerSwitch("M", "VOMS", "Add voms extension", self.setVOMSExt)


class ProxyInit:
    def __init__(self, piParams):
        self.__piParams = piParams
        self.__issuerCert = False
        self.__proxyGenerated = False
        self.__uploadedInfo = {}

    def getIssuerCert(self):
        if self.__issuerCert:
            return self.__issuerCert
        proxyChain = X509Chain.X509Chain()
        resultProxyChainFromFile = proxyChain.loadChainFromFile(self.__piParams.certLoc)
        if not resultProxyChainFromFile["OK"]:
            gLogger.error(f"Could not load the proxy: {resultProxyChainFromFile['Message']}")
            sys.exit(1)
        resultIssuerCert = proxyChain.getIssuerCert()
        if not resultIssuerCert["OK"]:
            gLogger.error(f"Could not load the proxy: {resultIssuerCert['Message']}")
            sys.exit(1)
        self.__issuerCert = resultIssuerCert["Value"]
        return self.__issuerCert

    def certLifeTimeCheck(self):
        minLife = Registry.getGroupOption(self.__piParams.diracGroup, "SafeCertificateLifeTime", 2592000)
        resultIssuerCert = self.getIssuerCert()
        resultRemainingSecs = resultIssuerCert.getRemainingSecs()  # pylint: disable=no-member
        if not resultRemainingSecs["OK"]:
            gLogger.error("Could not retrieve certificate expiration time", resultRemainingSecs["Message"])
            return
        lifeLeft = resultRemainingSecs["Value"]
        if minLife > lifeLeft:
            daysLeft = int(lifeLeft / 86400)
            msg = "Your certificate will expire in less than %d days. Please renew it!" % daysLeft
            sep = "=" * (len(msg) + 4)
            msg = f"{sep}\n  {msg}  \n{sep}"
            gLogger.notice(msg)

    def addVOMSExtIfNeeded(self):
        addVOMS = self.__piParams.addVOMSExt or Registry.getGroupOption(
            self.__piParams.diracGroup, "AutoAddVOMS", False
        )
        if not addVOMS:
            return S_OK()

        vomsAttr = Registry.getVOMSAttributeForGroup(self.__piParams.diracGroup)
        if not vomsAttr:
            return S_ERROR(
                "Requested adding a VOMS extension but no VOMS attribute defined for group %s"
                % self.__piParams.diracGroup
            )

        resultVomsAttributes = VOMS.VOMS().setVOMSAttributes(
            self.__proxyGenerated, attribute=vomsAttr, vo=Registry.getVOForGroup(self.__piParams.diracGroup)
        )
        if not resultVomsAttributes["OK"]:
            return S_ERROR(
                "Could not add VOMS extensions to the proxy\nFailed adding VOMS attribute: %s"
                % resultVomsAttributes["Message"]
            )

        gLogger.notice(f"Added VOMS attribute {vomsAttr}")
        chain = resultVomsAttributes["Value"]
        result = chain.dumpAllToFile(self.__proxyGenerated)
        if not result["OK"]:
            return result
        return S_OK()

    def createProxy(self):
        """Creates the proxy on disk"""
        gLogger.notice("Generating proxy...")
        resultProxyGenerated = ProxyGeneration.generateProxy(piParams)
        if not resultProxyGenerated["OK"]:
            gLogger.error(resultProxyGenerated["Message"])
            sys.exit(1)
        self.__proxyGenerated = resultProxyGenerated["Value"]
        return resultProxyGenerated

    def uploadProxy(self):
        """Upload the proxy to the proxyManager service"""
        issuerCert = self.getIssuerCert()
        resultUserDN = issuerCert.getSubjectDN()  # pylint: disable=no-member
        if not resultUserDN["OK"]:
            return resultUserDN
        userDN = resultUserDN["Value"]

        gLogger.notice("Uploading proxy..")
        if userDN in self.__uploadedInfo:
            expiry = self.__uploadedInfo[userDN].get("")
            if expiry:
                if (
                    issuerCert.getNotAfterDate()["Value"] - datetime.timedelta(minutes=10) < expiry
                ):  # pylint: disable=no-member
                    gLogger.info(f'Proxy with DN "{userDN}" already uploaded')
                    return S_OK()
        gLogger.info(f"Uploading {userDN} proxy to ProxyManager...")
        upParams = ProxyUpload.CLIParams()
        upParams.onTheFly = True
        upParams.proxyLifeTime = issuerCert.getRemainingSecs()["Value"] - 300  # pylint: disable=no-member
        for k in ("certLoc", "keyLoc", "userPasswd"):
            setattr(upParams, k, getattr(self.__piParams, k))
        resultProxyUpload = ProxyUpload.uploadProxy(upParams)
        if not resultProxyUpload["OK"]:
            gLogger.error(resultProxyUpload["Message"])
            return resultProxyUpload
        self.__uploadedInfo = resultProxyUpload["Value"]
        gLogger.info("Proxy uploaded")
        return S_OK()

    def printInfo(self):
        """Printing utilities"""
        resultProxyInfoAsAString = ProxyInfo.getProxyInfoAsString(self.__proxyGenerated)
        if not resultProxyInfoAsAString["OK"]:
            gLogger.error(f"Failed to get the new proxy info: {resultProxyInfoAsAString['Message']}")
        else:
            gLogger.notice("Proxy generated:")
            gLogger.notice(resultProxyInfoAsAString["Value"])
        if self.__uploadedInfo:
            gLogger.notice("\nProxies uploaded:")
            maxDNLen = 0
            for userDN in self.__uploadedInfo:
                maxDNLen = max(maxDNLen, len(userDN))
            gLogger.notice(f" {'DN'.ljust(maxDNLen)} | Until (GMT)")
            for userDN in self.__uploadedInfo:
                gLogger.notice(f" {userDN.ljust(maxDNLen)} | {self.__uploadedInfo[userDN].strftime('%Y/%m/%d %H:%M')}")

    def checkCAs(self):
        caDir = getCAsLocation()
        if not caDir:
            gLogger.warn("No valid CA dir found.")
            return
        # In globus standards .r0 files are CRLs. They have the same names of the CAs but diffent file extension
        searchExp = os.path.join(caDir, "*.r0")
        crlList = glob.glob(searchExp)
        if not crlList:
            gLogger.warn(f"No CRL files found for {searchExp}. Abort check of CAs")
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
        if not res["OK"]:
            gLogger.error("Failed to update CAs", res["Message"])
        res = bdc.syncCRLs()
        if not res["OK"]:
            gLogger.error("Failed to update CRLs", res["Message"])
        # Continue even if the update failed...
        return S_OK()

    def doTheMagic(self):
        proxy = self.createProxy()
        if not proxy["OK"]:
            return proxy

        self.checkCAs()
        pI.certLifeTimeCheck()
        resultProxyWithVOMS = pI.addVOMSExtIfNeeded()

        proxyLoc = self.__piParams.proxyLoc or getDefaultProxyLocation()
        if not (result := addTokenToPEM(proxyLoc, self.__piParams.diracGroup))["OK"]:
            return result

        if not resultProxyWithVOMS["OK"]:
            if "returning a valid AC for the user" in resultProxyWithVOMS["Message"]:
                gLogger.error(resultProxyWithVOMS["Message"])
                gLogger.error("\n Are you sure you are properly registered in the VO?")
            elif "Missing voms-proxy" in resultProxyWithVOMS["Message"]:
                gLogger.notice("Failed to add VOMS extension: no standard grid interface available")
            else:
                gLogger.error(resultProxyWithVOMS["Message"])
            if self.__piParams.strict:
                return resultProxyWithVOMS

        if self.__piParams.uploadProxy:
            resultProxyUpload = pI.uploadProxy()
            if not resultProxyUpload["OK"]:
                if self.__piParams.strict:
                    return resultProxyUpload

        return S_OK()


@Script()
def main():
    global piParams, pI
    piParams = Params()
    piParams.registerCLISwitches()
    # Take off tokens
    os.environ["DIRAC_USE_ACCESS_TOKEN"] = "False"

    Script.disableCS()
    Script.parseCommandLine(ignoreErrors=True)
    DIRAC.gConfig.setOptionValue("/DIRAC/Security/UseServerCertificate", "False")

    pI = ProxyInit(piParams)
    resultDoTheMagic = pI.doTheMagic()
    if not resultDoTheMagic["OK"]:
        gLogger.fatal(resultDoTheMagic["Message"])
        sys.exit(1)

    pI.printInfo()

    sys.exit(0)


if __name__ == "__main__":
    main()
