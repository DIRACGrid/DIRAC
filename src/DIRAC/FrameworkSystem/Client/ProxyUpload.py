import sys
from prompt_toolkit import prompt
import DIRAC

from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script


class CLIParams:
    proxyLifeTime = 2592000
    certLoc = False
    keyLoc = False
    proxyLoc = False
    onTheFly = False
    stdinPasswd = False
    userPasswd = ""

    def __str__(self):
        data = []
        for k in ("proxyLifeTime", "certLoc", "keyLoc", "proxyLoc", "onTheFly", "stdinPasswd", "userPasswd"):
            if k == "userPasswd":
                data.append("userPasswd = *****")
            else:
                data.append(f"{k}={getattr(self, k)}")
        msg = f"<UploadCLIParams {' '.join(data)}>"
        return msg

    def setProxyLifeTime(self, arg):
        try:
            fields = [f.strip() for f in arg.split(":")]
            self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
        except ValueError:
            gLogger.notice(f"Can't parse {arg} time! Is it a HH:MM?")
            return DIRAC.S_ERROR("Can't parse time argument")
        return DIRAC.S_OK()

    def setProxyRemainingSecs(self, arg):
        self.proxyLifeTime = int(arg)
        return DIRAC.S_OK()

    def getProxyLifeTime(self):
        hours = int(self.proxyLifeTime / 3600)
        mins = int(self.proxyLifeTime / 60 - hours * 60)
        return f"{hours}:{mins}"

    def getProxyRemainingSecs(self):
        return self.proxyLifeTime

    def setCertLocation(self, arg):
        self.certLoc = arg
        return DIRAC.S_OK()

    def setKeyLocation(self, arg):
        self.keyLoc = arg
        return DIRAC.S_OK()

    def setProxyLocation(self, arg):
        self.proxyLoc = arg
        return DIRAC.S_OK()

    def setOnTheFly(self, arg):
        self.onTheFly = True
        return DIRAC.S_OK()

    def setStdinPasswd(self, arg):
        self.stdinPasswd = True
        return DIRAC.S_OK()

    def registerCLISwitches(self):
        Script.registerSwitch(
            "v:", "valid=", "Valid HH:MM for the proxy. By default is one month", self.setProxyLifeTime
        )
        Script.registerSwitch("C:", "Cert=", "File to use as user certificate", self.setCertLocation)
        Script.registerSwitch("K:", "Key=", "File to use as user key", self.setKeyLocation)
        Script.registerSwitch("P:", "Proxy=", "File to use as proxy", self.setProxyLocation)
        Script.registerSwitch("f", "onthefly", "Generate a proxy on the fly", self.setOnTheFly)
        Script.registerSwitch("p", "pwstdin", "Get passwd from stdin", self.setStdinPasswd)
        Script.addDefaultOptionValue("LogLevel", "always")


from DIRAC import S_ERROR
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


def uploadProxy(params):
    DIRAC.gLogger.info("Loading user proxy")
    proxyLoc = params.proxyLoc
    if not proxyLoc:
        proxyLoc = Locations.getDefaultProxyLocation()
    if not proxyLoc:
        return S_ERROR("Can't find any proxy")

    if params.onTheFly:
        DIRAC.gLogger.info("Uploading proxy on-the-fly")
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

        DIRAC.gLogger.info(f"Cert file {certLoc}")
        DIRAC.gLogger.info(f"Key file  {keyLoc}")

        testChain = X509Chain()
        retVal = testChain.loadKeyFromFile(keyLoc, password=params.userPasswd)
        if not retVal["OK"]:
            if params.stdinPasswd:
                userPasswd = sys.stdin.readline().strip("\n")
            else:
                try:
                    userPasswd = prompt("Enter Certificate password: ", is_password=True)
                except KeyboardInterrupt:
                    return S_ERROR("Caught KeyboardInterrupt, exiting...")
            params.userPasswd = userPasswd

        DIRAC.gLogger.info("Loading cert and key")
        chain = X509Chain()
        # Load user cert and key
        retVal = chain.loadChainFromFile(certLoc)
        if not retVal["OK"]:
            return S_ERROR(f"Can't load {certLoc}")
        retVal = chain.loadKeyFromFile(keyLoc, password=params.userPasswd)
        if not retVal["OK"]:
            return S_ERROR(f"Can't load {keyLoc}")
        DIRAC.gLogger.info("User credentials loaded")
        restrictLifeTime = params.proxyLifeTime

    else:
        proxyChain = X509Chain()
        retVal = proxyChain.loadProxyFromFile(proxyLoc)
        if not retVal["OK"]:
            return S_ERROR(f"Can't load proxy file {params.proxyLoc}: {retVal['Message']}")

        chain = proxyChain
        restrictLifeTime = 0

    DIRAC.gLogger.info(" Uploading...")
    return gProxyManager.uploadProxy(proxy=chain, restrictLifeTime=restrictLifeTime)
