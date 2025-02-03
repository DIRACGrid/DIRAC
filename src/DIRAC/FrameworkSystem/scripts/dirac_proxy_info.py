#!/usr/bin/env python
########################################################################
# File :    dirac-proxy-info.py
# Author :  Adrian Casajus
########################################################################
"""
Print information about the current proxy.

Example:
  $ dirac-proxy-info
  subject      : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy/CN=proxy
  issuer       : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy
  identity     : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  timeleft     : 23:53:55
  DIRAC group  : dirac_user
  path         : /tmp/x509up_u40885
  username     : vhamar
  VOMS         : True
  VOMS fqan    : ['/formation']
"""
import sys

from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.ReturnValues import S_OK


class Params:
    proxyLoc = False
    vomsEnabled = True
    csEnabled = True
    steps = False
    checkValid = False
    uploadedInfo = False

    def setProxyLocation(self, arg):
        self.proxyLoc = arg
        return S_OK()

    def disableVOMS(self, arg):
        self.vomsEnabled = False
        return S_OK()

    def disableCS(self, arg):
        self.csEnabled = False
        return S_OK()

    def showSteps(self, arg):
        self.steps = True
        return S_OK()

    def validityCheck(self, arg):
        self.checkValid = True
        return S_OK()

    def setManagerInfo(self, arg):
        self.uploadedInfo = True
        return S_OK()


@Script()
def main():
    params = Params()

    Script.registerSwitch("f:", "file=", "File to use as user key", params.setProxyLocation)
    Script.registerSwitch("n", "novoms", "Disable VOMS", params.disableVOMS)
    Script.registerSwitch("v", "checkvalid", "Return error if the proxy is invalid", params.validityCheck)
    Script.registerSwitch("x", "nocs", "Disable CS", params.disableCS)
    Script.registerSwitch("e", "steps", "Show steps info", params.showSteps)
    Script.registerSwitch("m", "uploadedinfo", "Show uploaded proxies info", params.setManagerInfo)

    Script.disableCS()
    Script.parseCommandLine()

    from DIRAC import gLogger, exit as dExit
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo, getProxyStepsInfo
    from DIRAC.Core.Security.ProxyInfo import formatProxyInfoAsString, formatProxyStepsInfoAsString
    from DIRAC.Core.Security import VOMS
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from DIRAC.ConfigurationSystem.Client.Helpers import Registry
    from DIRAC.Core.Security.DiracX import DiracXClient

    if params.csEnabled:
        retVal = Script.enableCS()
        if not retVal["OK"]:
            print("Cannot contact CS to get user list")

    result = getProxyInfo(params.proxyLoc, not params.vomsEnabled)
    if not result["OK"]:
        gLogger.error(result["Message"])
        dExit(1)
    infoDict = result["Value"]
    gLogger.notice(formatProxyInfoAsString(infoDict))
    if not infoDict["isProxy"]:
        gLogger.error("==============================\n!!! The proxy is not valid !!!")

    if params.steps:
        gLogger.notice("== Steps extended info ==")
        chain = infoDict["chain"]
        stepInfo = getProxyStepsInfo(chain)["Value"]
        gLogger.notice(formatProxyStepsInfoAsString(stepInfo))

    def invalidProxy(msg):
        gLogger.error("Invalid proxy:", msg)
        dExit(1)

    if params.uploadedInfo:
        result = gProxyManager.getUserProxiesInfo()
        if not result["OK"]:
            gLogger.error("Could not retrieve the uploaded proxies info", result["Message"])
            dExit(1)
        else:
            uploadedInfo = result["Value"]
            if not uploadedInfo:
                gLogger.notice("== No proxies uploaded ==")
            if uploadedInfo:
                gLogger.notice("== Proxies uploaded ==")
                maxDNLen = 0

                for userDN in uploadedInfo:
                    maxDNLen = max(maxDNLen, len(userDN))
                    # for group in uploadedInfo[userDN]:
                    #     maxGroupLen = max(maxGroupLen, len(group))
                gLogger.notice(f" {'DN'.ljust(maxDNLen)} | Until (GMT)")
                for userDN in uploadedInfo:
                    # in v8.0, expirationTime is accessed from uploadedInfo[userDN][""]
                    if isinstance(uploadedInfo[userDN], dict):
                        expirationTime = uploadedInfo[userDN][""]
                    # whereas in v9.0, expirationTime is accessed from uploadedInfo[userDN]
                    else:
                        expirationTime = uploadedInfo[userDN]

                    gLogger.notice(f" {userDN.ljust(maxDNLen)} | {expirationTime.strftime('%Y/%m/%d %H:%M')}")
    if params.checkValid:
        if infoDict["secondsLeft"] == 0:
            invalidProxy("Proxy is expired")
        if params.csEnabled and not infoDict["validGroup"]:
            invalidProxy(f"Group {infoDict['group']} is not valid")
        if "hasVOMS" in infoDict and infoDict["hasVOMS"]:
            requiredVOMS = Registry.getVOMSAttributeForGroup(infoDict["group"])
            if "VOMS" not in infoDict or not infoDict["VOMS"]:
                invalidProxy("Unable to retrieve VOMS extension")
            if len(infoDict["VOMS"]) > 1:
                invalidProxy("More than one voms attribute found")
            if requiredVOMS not in infoDict["VOMS"]:
                invalidProxy(
                    "Unexpected VOMS extension %s. Extension expected for DIRAC group is %s"
                    % (infoDict["VOMS"][0], requiredVOMS)
                )
            result = VOMS.VOMS().getVOMSProxyInfo(infoDict["chain"], "actimeleft")
            if not result["OK"]:
                invalidProxy(f"Cannot determine life time of VOMS attributes: {result['Message']}")
            if int(result["Value"].strip()) == 0:
                invalidProxy("VOMS attributes are expired")
        # Ensure the proxy is working with DiracX
        try:
            with DiracXClient() as api:
                api.auth.userinfo()
        except Exception as e:
            invalidProxy(f"Failed to access DiracX: {e}")


if __name__ == "__main__":
    main()
