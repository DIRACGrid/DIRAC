########################################################################
# File :   ProxyGeneration.py
# Author : Adrian Casajus
########################################################################
import sys
from prompt_toolkit import prompt
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.m2crypto import DEFAULT_PROXY_STRENGTH


class CLIParams:
    proxyLifeTime = 86400
    diracGroup = False
    proxyStrength = DEFAULT_PROXY_STRENGTH
    limitedProxy = False
    strict = False
    summary = False
    certLoc = False
    keyLoc = False
    proxyLoc = False
    checkWithCS = True
    stdinPasswd = False
    userPasswd = ""
    embedDefaultGroup = True

    def setProxyLifeTime(self, arg):
        """Set proxy lifetime

        :param str arg: arguments

        :return: S_OK()/S_ERROR()
        """
        try:
            fields = [f.strip() for f in arg.split(":")]
            self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
        except Exception:
            gLogger.error("Can't parse time! Is it a HH:MM?", arg)
            return S_ERROR("Can't parse time argument")
        return S_OK()

    def setProxyRemainingSecs(self, arg):
        """Set proxy lifetime

        :param int arg: lifetime in seconds

        :return: S_OK()
        """
        self.proxyLifeTime = int(arg)
        return S_OK()

    def getProxyLifeTime(self):
        """Get proxy lifetime

        :return: str
        """
        hours = int(self.proxyLifeTime / 3600)
        mins = int(self.proxyLifeTime / 60 - hours * 60)
        return f"{hours}:{mins}"

    def getProxyRemainingSecs(self):
        """Get proxy livetime

        :return: int
        """
        return self.proxyLifeTime

    def setDIRACGroup(self, arg):
        """Set DIRAC group

        :param str arg: arguments

        :return: S_OK()
        """
        self.diracGroup = arg
        return S_OK()

    def getDIRACGroup(self):
        """Get DIRAC group

        :return: str
        """
        return self.diracGroup

    def setProxyStrength(self, arg):
        """Get proxy strength

        :return: S_OK()
        """
        try:
            self.proxyStrength = int(arg)
        except Exception:
            gLogger.error("Can't parse bits! Is it a number?", f"{arg}")
            return S_ERROR("Can't parse strength argument")
        return S_OK()

    def setProxyLimited(self, _arg):
        """Set proxy limited

        :param _arg: unuse

        :return: str
        """
        self.limitedProxy = True
        return S_OK()

    def setSummary(self, _arg):
        """Set proxy limited

        :param _arg: unuse

        :return: str
        """
        gLogger.info("Enabling summary output")
        self.summary = True
        return S_OK()

    def setCertLocation(self, arg):
        """Set certificate path

        :param str arg: certificate path

        :return: S_OK()
        """
        self.certLoc = arg
        return S_OK()

    def setKeyLocation(self, arg):
        """Set key path

        :param str arg: key path

        :return: S_OK()
        """
        self.keyLoc = arg
        return S_OK()

    def setProxyLocation(self, arg):
        """Set proxy path

        :param str arg: proxy path

        :return: S_OK()
        """
        self.proxyLoc = arg
        return S_OK()

    def setDisableCSCheck(self, _arg):
        """Disable CS check

        :param _arg: unuse

        :return: S_OK()
        """
        self.checkWithCS = False
        return S_OK()

    def setStdinPasswd(self, _arg):
        """Set stdin passwd

        :param _arg: unuse

        :return: S_OK()
        """
        self.stdinPasswd = True
        return S_OK()

    def setStrict(self, _arg):
        """Set strict

        :param _arg: unuse

        :return: S_OK()
        """
        self.strict = True
        return S_OK()

    def registerCLISwitches(self):
        """Register CLI switches"""
        Script.registerSwitch(
            "v:", "valid=", "Valid HH:MM for the proxy. By default is 24 hours", self.setProxyLifeTime
        )
        Script.registerSwitch("g:", "group=", "DIRAC Group to embed in the proxy", self.setDIRACGroup)
        Script.registerSwitch("b:", "strength=", "Set the proxy strength in bytes", self.setProxyStrength)
        Script.registerSwitch("l", "limited", "Generate a limited proxy", self.setProxyLimited)
        Script.registerSwitch("t", "strict", "Fail on each error. Treat warnings as errors.", self.setStrict)
        Script.registerSwitch("S", "summary", "Enable summary output when generating proxy", self.setSummary)
        Script.registerSwitch("C:", "Cert=", "File to use as user certificate", self.setCertLocation)
        Script.registerSwitch("K:", "Key=", "File to use as user key", self.setKeyLocation)
        Script.registerSwitch("u:", "out=", "File to write as proxy", self.setProxyLocation)
        Script.registerSwitch("x", "nocs", "Disable CS check", self.setDisableCSCheck)
        Script.registerSwitch("p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd)


from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security import Locations


def generateProxy(params):
    """Generate proxy

    :param params: parameters

    :return: S_OK()/S_ERROR()
    """
    certLoc = params.certLoc
    keyLoc = params.keyLoc
    if not certLoc or not keyLoc:
        cakLoc = Locations.getCertificateAndKeyLocation()
        if not cakLoc:
            return S_ERROR("Can't find user certificate and key")
        if not certLoc:
            certLoc = cakLoc[0]
        if not keyLoc:
            keyLoc = cakLoc[1]
    params.certLoc = certLoc
    params.keyLoc = keyLoc

    # Load password
    testChain = X509Chain()
    retVal = testChain.loadChainFromFile(params.certLoc)
    if not retVal["OK"]:
        return S_ERROR(f"Cannot load certificate {params.certLoc}: {retVal['Message']}")
    timeLeft = int(testChain.getRemainingSecs()["Value"] / 86400)
    if timeLeft < 30:
        gLogger.notice("\nYour certificate will expire in %d days. Please renew it!\n" % timeLeft)

    # First try reading the key from the file
    retVal = testChain.loadKeyFromFile(params.keyLoc, password=params.userPasswd)  # XXX why so commented?
    if not retVal["OK"]:
        if params.stdinPasswd:
            userPasswd = sys.stdin.readline().strip("\n")
        else:
            try:
                userPasswd = prompt("Enter Certificate password: ", is_password=True)
            except KeyboardInterrupt:
                return S_ERROR("Caught KeyboardInterrupt, exiting...")
        params.userPasswd = userPasswd

    # Find location
    proxyLoc = params.proxyLoc
    if not proxyLoc:
        proxyLoc = Locations.getDefaultProxyLocation()

    chain = X509Chain()
    # Load user cert and key
    retVal = chain.loadChainFromFile(certLoc)
    if not retVal["OK"]:
        gLogger.warn(retVal["Message"])
        return S_ERROR(f"Can't load {certLoc}")
    retVal = chain.loadKeyFromFile(keyLoc, password=params.userPasswd)
    if not retVal["OK"]:
        gLogger.warn(retVal["Message"])
        if "bad decrypt" in retVal["Message"] or "bad pass phrase" in retVal["Message"]:
            return S_ERROR("Bad passphrase")
        return S_ERROR(f"Can't load {keyLoc}")

    if params.checkWithCS:
        retVal = chain.generateProxyToFile(
            proxyLoc, params.proxyLifeTime, strength=params.proxyStrength, limited=params.limitedProxy
        )

        gLogger.info("Contacting CS...")
        retVal = Script.enableCS()
        if not retVal["OK"]:
            gLogger.warn(retVal["Message"])
            if "Unauthorized query" in retVal["Message"]:
                # add hint for users
                return S_ERROR(
                    f"Can't contact DIRAC CS: {retVal['Message']} (User possibly not registered with dirac server) "
                )
            return S_ERROR(f"Can't contact DIRAC CS: {retVal['Message']}")
        userDN = chain.getCertInChain(-1)["Value"].getSubjectDN()["Value"]

        if not params.diracGroup:
            result = Registry.findDefaultGroupForDN(userDN)
            if not result["OK"]:
                gLogger.warn(f"Could not get a default group for DN {userDN}: {result['Message']}")
            else:
                params.diracGroup = result["Value"]
                gLogger.info(f"Default discovered group is {params.diracGroup}")
        gLogger.info(f"Checking DN {userDN}")
        retVal = Registry.getUsernameForDN(userDN)
        if not retVal["OK"]:
            gLogger.warn(retVal["Message"])
            return S_ERROR(f"DN {userDN} is not registered")
        username = retVal["Value"]
        gLogger.info(f"Username is {username}")
        retVal = Registry.getGroupsForUser(username)
        if not retVal["OK"]:
            gLogger.warn(retVal["Message"])
            return S_ERROR(f"User {username} has no groups defined")
        groups = retVal["Value"]
        if params.diracGroup not in groups:
            return S_ERROR(f"Requested group {params.diracGroup} is not valid for DN {userDN}")
        gLogger.info(f"Creating proxy for {username}@{params.diracGroup} ({userDN})")
    if params.summary:
        h = int(params.proxyLifeTime / 3600)
        m = int(params.proxyLifeTime / 60) - h * 60
        gLogger.notice("Proxy lifetime will be %02d:%02d" % (h, m))
        gLogger.notice(f"User cert is {certLoc}")
        gLogger.notice(f"User key  is {keyLoc}")
        gLogger.notice(f"Proxy will be written to {proxyLoc}")
        if params.diracGroup:
            gLogger.notice(f"DIRAC Group will be set to {params.diracGroup}")
        else:
            gLogger.notice("No DIRAC Group will be set")
        gLogger.notice(f"Proxy strength will be {params.proxyStrength}")
        if params.limitedProxy:
            gLogger.notice("Proxy will be limited")
    retVal = chain.generateProxyToFile(
        proxyLoc,
        params.proxyLifeTime,
        params.diracGroup,
        strength=params.proxyStrength,
        limited=params.limitedProxy,
    )
    if not retVal["OK"]:
        gLogger.warn(retVal["Message"])
        return S_ERROR(f"Couldn't generate proxy: {retVal['Message']}")
    return S_OK(proxyLoc)
