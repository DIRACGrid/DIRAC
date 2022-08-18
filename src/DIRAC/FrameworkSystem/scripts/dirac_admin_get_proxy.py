#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-proxy
# Author :  Stuart Paterson
########################################################################
"""
Retrieve a delegated proxy for the given user and group

Example:
  $ dirac-admin-get-proxy vhamar dirac_user
  Proxy downloaded to /afs/in2p3.fr/home/h/hamar/proxy.vhamar.dirac_user
"""
import os

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Script import Script
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class Params:

    limited = False
    proxyPath = False
    proxyLifeTime = 86400
    enableVOMS = False
    vomsAttr = None

    def setLimited(self, args):
        """Set limited

        :param bool args: is limited

        :return: S_OK()/S_ERROR()
        """
        self.limited = True
        return S_OK()

    def setProxyLocation(self, args):
        """Set proxy location

        :param str args: proxy path

        :return: S_OK()/S_ERROR()
        """
        self.proxyPath = args
        return S_OK()

    def setProxyLifeTime(self, arg):
        """Set proxy lifetime

        :param int arg: lifetime in a seconds

        :return: S_OK()/S_ERROR()
        """
        try:
            fields = [f.strip() for f in arg.split(":")]
            self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
        except Exception:
            gLogger.notice("Can't parse %s time! Is it a HH:MM?" % arg)
            return S_ERROR("Can't parse time argument")
        return S_OK()

    def automaticVOMS(self, arg):
        """Enable VOMS

        :param bool arg: enable VOMS

        :return: S_OK()/S_ERROR()
        """
        self.enableVOMS = True
        return S_OK()

    def setVOMSAttr(self, arg):
        """Register CLI switches

        :param str arg: VOMS attribute
        """
        self.enableVOMS = True
        self.vomsAttr = arg
        return S_OK()

    def registerCLISwitches(self):
        """Register CLI switches"""
        Script.registerSwitch(
            "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime
        )
        Script.registerSwitch("l", "limited", "Get a limited proxy", self.setLimited)
        Script.registerSwitch("u:", "out=", "File to write as proxy", self.setProxyLocation)
        Script.registerSwitch(
            "a", "voms", "Get proxy with VOMS extension mapped to the DIRAC group", self.automaticVOMS
        )
        Script.registerSwitch("m:", "vomsAttr=", "VOMS attribute to require", self.setVOMSAttr)


@Script()
def main():
    params = Params()
    params.registerCLISwitches()
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        ("DN:       DN of the user", "user:     DIRAC user name (will fail if there is more than 1 DN registered)")
    )
    Script.registerArgument(" group:    DIRAC group name")

    Script.parseCommandLine(ignoreErrors=True)
    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    userDN, userGroup = Script.getPositionalArgs(group=True)

    userName = False
    if userDN.find("/") != 0:
        userName = userDN
        retVal = Registry.getDNForUsername(userName)
        if not retVal["OK"]:
            gLogger.notice("Cannot discover DN for username {}\n\t{}".format(userName, retVal["Message"]))
            DIRAC.exit(2)
        DNList = retVal["Value"]
        if len(DNList) > 1:
            gLogger.notice("Username %s has more than one DN registered" % userName)
            ind = 0
            for dn in DNList:
                gLogger.notice("%d %s" % (ind, dn))
                ind += 1
            inp = input("Which DN do you want to download? [default 0] ")
            if not inp:
                inp = 0
            else:
                inp = int(inp)
            userDN = DNList[inp]
        else:
            userDN = DNList[0]

    if not params.proxyPath:
        if not userName:
            result = Registry.getUsernameForDN(userDN)
            if not result["OK"]:
                gLogger.notice("DN '%s' is not registered in DIRAC" % userDN)
                DIRAC.exit(2)
            userName = result["Value"]
        params.proxyPath = f"{os.getcwd()}/proxy.{userName}.{userGroup}"

    if params.enableVOMS:
        result = gProxyManager.downloadVOMSProxy(
            userDN,
            userGroup,
            limited=params.limited,
            requiredTimeLeft=params.proxyLifeTime,
            requiredVOMSAttribute=params.vomsAttr,
        )
    else:
        result = gProxyManager.downloadProxy(
            userDN, userGroup, limited=params.limited, requiredTimeLeft=params.proxyLifeTime
        )
    if not result["OK"]:
        gLogger.notice("Proxy file cannot be retrieved: %s" % result["Message"])
        DIRAC.exit(2)
    chain = result["Value"]
    result = chain.dumpAllToFile(params.proxyPath)
    if not result["OK"]:
        gLogger.notice("Proxy file cannot be written to {}: {}".format(params.proxyPath, result["Message"]))
        DIRAC.exit(2)
    gLogger.notice("Proxy downloaded to %s" % params.proxyPath)
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
