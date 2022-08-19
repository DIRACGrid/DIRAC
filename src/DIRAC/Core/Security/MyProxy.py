""" Utility class for dealing with MyProxy
"""

import re
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities import List
from DIRAC.Core.Security.ProxyFile import multiProxyArgument, deleteMultiProxy
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.BaseSecurity import BaseSecurity


class MyProxy(BaseSecurity):
    def uploadProxy(self, proxy=False, useDNAsUserName=False):
        """
        Upload a proxy to myproxy service.
          proxy param can be:
            : Default -> use current proxy
            : string -> upload file specified as proxy
            : X509Chain -> use chain
        """
        retVal = multiProxyArgument(proxy)
        if not retVal["OK"]:
            return retVal
        proxyDict = retVal["Value"]
        chain = proxyDict["chain"]
        proxyLocation = proxyDict["file"]

        timeLeft = int(chain.getRemainingSecs()["Value"] / 3600)

        cmdArgs = ["-n"]
        cmdArgs.append('-s "%s"' % self._secServer)
        cmdArgs.append('-c "%s"' % (timeLeft - 1))
        cmdArgs.append('-t "%s"' % self._secMaxProxyHours)
        cmdArgs.append('-C "%s"' % proxyLocation)
        cmdArgs.append('-y "%s"' % proxyLocation)
        if useDNAsUserName:
            cmdArgs.append("-d")
        else:
            retVal = self._getUsername(chain)
            if not retVal["OK"]:
                deleteMultiProxy(proxyDict)
                return retVal
            mpUsername = retVal["Value"]
            cmdArgs.append('-l "%s"' % mpUsername)

        mpEnv = self._getExternalCmdEnvironment()
        # Hack to upload properly
        mpEnv["GT_PROXY_MODE"] = "old"

        cmd = "myproxy-init %s" % " ".join(cmdArgs)
        result = shellCall(self._secCmdTimeout, cmd, env=mpEnv)

        deleteMultiProxy(proxyDict)

        if not result["OK"]:
            errMsg = "Call to myproxy-init failed: %s" % retVal["Message"]
            return S_ERROR(errMsg)

        status, _, error = result["Value"]

        # Clean-up files
        if status:
            errMsg = "Call to myproxy-init failed"
            extErrMsg = f"Command: {cmd}; StdOut: {result}; StdErr: {error}"
            return S_ERROR(f"{errMsg} {extErrMsg}")

        return S_OK()

    def getDelegatedProxy(self, proxyChain, lifeTime=604800, useDNAsUserName=False):
        """
        Get delegated proxy from MyProxy server
        return S_OK( X509Chain ) / S_ERROR
        """
        # TODO: Set the proxy coming in proxyString to be the proxy to use

        # Get myproxy username diracgroup:diracuser
        retVal = multiProxyArgument(proxyChain)
        if not retVal["OK"]:
            return retVal
        proxyDict = retVal["Value"]
        chain = proxyDict["chain"]
        proxyLocation = proxyDict["file"]

        retVal = self._generateTemporalFile()
        if not retVal["OK"]:
            deleteMultiProxy(proxyDict)
            return retVal
        newProxyLocation = retVal["Value"]

        # myproxy-get-delegation works only with environment variables
        cmdEnv = self._getExternalCmdEnvironment()
        if self._secRunningFromTrustedHost:
            cmdEnv["X509_USER_CERT"] = self._secCertLoc
            cmdEnv["X509_USER_KEY"] = self._secKeyLoc
            if "X509_USER_PROXY" in cmdEnv:
                del cmdEnv["X509_USER_PROXY"]
        else:
            cmdEnv["X509_USER_PROXY"] = proxyLocation

        cmdArgs = []
        cmdArgs.append("-s '%s'" % self._secServer)
        cmdArgs.append("-t '%s'" % (int(lifeTime / 3600)))
        cmdArgs.append("-a '%s'" % proxyLocation)
        cmdArgs.append("-o '%s'" % newProxyLocation)
        if useDNAsUserName:
            cmdArgs.append("-d")
        else:
            retVal = self._getUsername(chain)
            if not retVal["OK"]:
                deleteMultiProxy(proxyDict)
                return retVal
            mpUsername = retVal["Value"]
            cmdArgs.append('-l "%s"' % mpUsername)

        cmd = "myproxy-logon %s" % " ".join(cmdArgs)
        gLogger.verbose("myproxy-logon command:\n%s" % cmd)

        result = shellCall(self._secCmdTimeout, cmd, env=cmdEnv)

        deleteMultiProxy(proxyDict)

        if not result["OK"]:
            errMsg = "Call to myproxy-logon failed: %s" % result["Message"]
            deleteMultiProxy(proxyDict)
            return S_ERROR(errMsg)

        status, _, error = result["Value"]

        # Clean-up files
        if status:
            errMsg = "Call to myproxy-logon failed"
            extErrMsg = f"Command: {cmd}; StdOut: {result}; StdErr: {error}"
            deleteMultiProxy(proxyDict)
            return S_ERROR(f"{errMsg} {extErrMsg}")

        chain = X509Chain()
        retVal = chain.loadProxyFromFile(newProxyLocation)
        if not retVal["OK"]:
            deleteMultiProxy(proxyDict)
            return S_ERROR("myproxy-logon failed when reading delegated file: %s" % retVal["Message"])

        deleteMultiProxy(proxyDict)
        return S_OK(chain)

    def getInfo(self, proxyChain, useDNAsUserName=False):
        """
        Get info from myproxy server

        :return: S_OK( { 'username' : myproxyusername,
                       'owner' : owner DN,
                       'timeLeft' : secs left } ) / S_ERROR
        """
        # TODO: Set the proxy coming in proxyString to be the proxy to use

        # Get myproxy username diracgroup:diracuser
        retVal = multiProxyArgument(proxyChain)
        if not retVal["OK"]:
            return retVal
        proxyDict = retVal["Value"]
        chain = proxyDict["chain"]
        proxyLocation = proxyDict["file"]

        # myproxy-get-delegation works only with environment variables
        cmdEnv = self._getExternalCmdEnvironment()
        if self._secRunningFromTrustedHost:
            cmdEnv["X509_USER_CERT"] = self._secCertLoc
            cmdEnv["X509_USER_KEY"] = self._secKeyLoc
            if "X509_USER_PROXY" in cmdEnv:
                del cmdEnv["X509_USER_PROXY"]
        else:
            cmdEnv["X509_USER_PROXY"] = proxyLocation

        cmdArgs = []
        cmdArgs.append("-s '%s'" % self._secServer)
        if useDNAsUserName:
            cmdArgs.append("-d")
        else:
            retVal = self._getUsername(chain)
            if not retVal["OK"]:
                deleteMultiProxy(proxyDict)
                return retVal
            mpUsername = retVal["Value"]
            cmdArgs.append('-l "%s"' % mpUsername)

        cmd = "myproxy-info %s" % " ".join(cmdArgs)
        gLogger.verbose("myproxy-info command:\n%s" % cmd)

        result = shellCall(self._secCmdTimeout, cmd, env=cmdEnv)

        deleteMultiProxy(proxyDict)

        if not result["OK"]:
            errMsg = "Call to myproxy-info failed: %s" % result["Message"]
            deleteMultiProxy(proxyDict)
            return S_ERROR(errMsg)

        status, output, error = result["Value"]

        # Clean-up files
        if status:
            errMsg = "Call to myproxy-info failed"
            extErrMsg = f"Command: {cmd}; StdOut: {result}; StdErr: {error}"
            return S_ERROR(f"{errMsg} {extErrMsg}")

        infoDict = {}
        usernameRE = re.compile(r"username\s*:\s*(\S*)")
        ownerRE = re.compile(r"owner\s*:\s*(\S*)")
        timeLeftRE = re.compile(r"timeleft\s*:\s*(\S*)")
        for line in List.fromChar(output, "\n"):
            match = usernameRE.search(line)
            if match:
                infoDict["username"] = match.group(1)
            match = ownerRE.search(line)
            if match:
                infoDict["owner"] = match.group(1)
            match = timeLeftRE.search(line)
            if match:
                try:
                    fields = List.fromChar(match.group(1), ":")
                    fields.reverse()
                    secsLeft = 0
                    for iP in range(len(fields)):
                        if iP == 0:
                            secsLeft += int(fields[iP])
                        elif iP == 1:
                            secsLeft += int(fields[iP]) * 60
                        elif iP == 2:
                            secsLeft += int(fields[iP]) * 3600
                    infoDict["timeLeft"] = secsLeft
                except Exception as x:
                    print(x)

        return S_OK(infoDict)
