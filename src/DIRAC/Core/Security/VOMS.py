""" Module for dealing with VOMS (Virtual Organization Membership Service)
"""
from datetime import datetime
import os
import tempfile
import shlex
import shutil

from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.ProxyFile import multiProxyArgument, deleteMultiProxy
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities import List


class VOMS:
    def __init__(self, *args, **kwargs):
        """Create VOMS class, setting specific timeout for VOMS shell commands."""
        # Per-server timeout for voms-proxy-init, should be at maximum timeout/2*n
        # where n as the number of voms servers to try.
        # voms-proxy-init will try each server *twice* before moving to the next one
        # once for new interface mode, once for legacy.
        self._secCmdTimeout = 80

    def getVOMSAttributes(self, proxy, switch="all"):
        """
        Return VOMS proxy attributes as list elements if switch="all" (default) OR
        return the string prepared to be stored in DB if switch="db" OR
        return the string of elements to be used as the option string in voms-proxy-init
        if switch="option".
        If a given proxy is a grid proxy, then function will return an empty list.
        """

        # Get all possible info from voms proxy
        result = self.getVOMSProxyInfo(proxy, "all")
        if not result["OK"]:
            return S_ERROR(DErrno.EVOMS, "Failed to extract info from proxy: %s" % result["Message"])

        vomsInfoOutput = List.fromChar(result["Value"], "\n")

        # Get a list of known VOMS attributes
        validVOMSAttrs = []
        result = gConfig.getSections("/Registry/Groups")
        if result["OK"]:
            for group in result["Value"]:
                vA = gConfig.getValue("/Registry/Groups/%s/VOMSRole" % group, "")
                if vA and vA not in validVOMSAttrs:
                    validVOMSAttrs.append(vA)

        # Parse output of voms-proxy-info command
        attributes = []
        voName = ""
        nickName = ""
        for line in vomsInfoOutput:
            fields = List.fromChar(line, ":")
            key = fields[0].strip()
            value = " ".join(fields[1:])
            if key == "VO":
                voName = value
            elif key == "attribute":
                # Cut off unsupported Capability selection part
                if value.find("nickname") == 0:
                    nickName = "=".join(List.fromChar(value, "=")[1:])
                else:
                    value = value.replace("/Capability=NULL", "")
                    value = value.replace("/Role=NULL", "")
                    if value and value not in attributes and value in validVOMSAttrs:
                        attributes.append(value)

        # Sorting and joining attributes
        if switch == "db":
            returnValue = ":".join(attributes)
        elif switch == "option":
            if len(attributes) > 1:
                returnValue = voName + " -order " + " -order ".join(attributes)
            elif attributes:
                returnValue = voName + ":" + attributes[0]
            else:
                returnValue = voName
        elif switch == "nickname":
            returnValue = nickName
        elif switch == "all":
            returnValue = attributes

        return S_OK(returnValue)

    def getVOMSProxyFQAN(self, proxy):
        """Get the VOMS proxy fqan attributes"""
        return self.getVOMSProxyInfo(proxy, "fqan")

    def getVOMSProxyInfo(self, proxy, option=False):
        """
        Returns information about a proxy certificate (both grid and voms).
            Available information is:

              1. Full (grid)voms-proxy-info output
              2. Proxy Certificate Timeleft in seconds (the output is an int)
              3. DN
              4. voms group (if any)

        :type proxy: str
        :param proxy: the proxy certificate location.
        :type  option: str
        :param option: None is the default value. Other option available are:

           * timeleft
           * actimeleft
           * identity
           * fqan
           * all

        :rtype:   tuple
        :return:  status, output, error, pyerror.
        """
        validOptions = ["actimeleft", "timeleft", "identity", "fqan", "all"]
        if option and option not in validOptions:
            return S_ERROR(DErrno.EVOMS, "invalid option %s" % option)

        retVal = multiProxyArgument(proxy)
        if not retVal["OK"]:
            return retVal
        proxyDict = retVal["Value"]

        try:
            res = proxyDict["chain"].getVOMSData()
            if not res["OK"]:
                return res

            data = res["Value"]

            if option == "actimeleft":
                now = datetime.utcnow()
                left = data["notAfter"] - now
                return S_OK("%d\n" % left.total_seconds())
            if option == "timeleft":
                now = datetime.utcnow()
                left = proxyDict["chain"].getNotAfterDate()["Value"] - now
                return S_OK("%d\n" % left.total_seconds())
            if option == "identity":
                return S_OK("%s\n" % data["subject"])
            if option == "fqan":
                return S_OK(
                    "\n".join([f.replace("/Role=NULL", "").replace("/Capability=NULL", "") for f in data["fqan"]])
                )
            if option == "all":
                lines = []
                creds = proxyDict["chain"].getCredentials()["Value"]
                lines.append("subject : %s" % creds["subject"])
                lines.append("issuer : %s" % creds["issuer"])
                lines.append("identity : %s" % creds["identity"])
                if proxyDict["chain"].isRFC().get("Value"):
                    lines.append("type : RFC compliant proxy")
                else:
                    lines.append("type : proxy")
                left = creds["secondsLeft"]
                h = int(left / 3600)
                m = int(left / 60) - h * 60
                s = int(left) - m * 60 - h * 3600
                lines.append(
                    "timeleft  : %s:%s:%s\nkey usage : Digital Signature, Key Encipherment, Data Encipherment"
                    % (h, m, s)
                )
                lines.append("== VO %s extension information ==" % data["vo"])
                lines.append("VO: %s" % data["vo"])
                lines.append("subject : %s" % data["subject"])
                lines.append("issuer : %s" % data["issuer"])
                for fqan in data["fqan"]:
                    lines.append("attribute : %s" % fqan)
                if "attribute" in data:
                    lines.append("attribute : %s" % data["attribute"])
                now = datetime.utcnow()
                left = (data["notAfter"] - now).total_seconds()
                h = int(left / 3600)
                m = int(left / 60) - h * 60
                s = int(left) - m * 60 - h * 3600
                lines.append(f"timeleft : {h}:{m}:{s}")

                return S_OK("\n".join(lines))
            else:
                return S_ERROR(DErrno.EVOMS, "NOT IMP")

        finally:
            if proxyDict["tempFile"]:
                self._unlinkFiles(proxyDict["file"])

    def getVOMSESLocation(self):
        # Transition code to new behaviour
        if "DIRAC_VOMSES" not in os.environ and "X509_VOMSES" not in os.environ:
            os.environ["X509_VOMSES"] = os.path.join(rootPath, "etc", "grid-security", "vomses")
            gLogger.notice(
                "You did not set X509_VOMSES in your bashrc. DIRAC searches $DIRAC/etc/grid-security/vomses . "
                "Please use X509_VOMSES, this auto discovery will be dropped."
            )
        elif "DIRAC_VOMSES" in os.environ and "X509_VOMSES" in os.environ:
            os.environ["X509_VOMSES"] = "{}:{}".format(os.environ["DIRAC_VOMSES"], os.environ["X509_VOMSES"])
            gLogger.notice(
                "You set both variables DIRAC_VOMSES and X509_VOMSES in your bashrc. "
                "DIRAC_VOMSES will be dropped in a future version, please use only X509_VOMSES"
            )
        elif "DIRAC_VOMSES" in os.environ and "X509_VOMSES" not in os.environ:
            os.environ["X509_VOMSES"] = os.environ["DIRAC_VOMSES"]
            gLogger.notice(
                "You set the variables DIRAC_VOMSES in your bashrc. "
                "DIRAC_VOMSES will be dropped in a future version, please use X509_VOMSES"
            )
        # End of transition code
        if "X509_VOMSES" not in os.environ:
            raise Exception(
                "The env variable X509_VOMSES is not set. "
                "DIRAC does not know where to look for etc/grid-security/vomses. "
                "Please set X509_VOMSES in your bashrc."
            )
        vomsesPaths = os.environ["X509_VOMSES"].split(":")
        for vomsesPath in vomsesPaths:
            if not os.path.exists(vomsesPath):
                continue
            if os.path.isfile(vomsesPath):
                fd, tmpPath = tempfile.mkstemp("vomses")
                os.close(fd)
                try:
                    shutil.copy(vomsesPath, tmpPath)
                except OSError:
                    # file is unreadable or disk is full
                    continue
                os.environ["X509_VOMSES"] = tmpPath
                return tmpPath
            elif os.path.isdir(vomsesPath):
                # check if directory is readable and not empty
                try:
                    if not os.listdir(vomsesPath):
                        # directory is empty
                        continue
                except OSError:
                    # directory is unreadable
                    continue
                return vomsesPath

    def setVOMSAttributes(self, proxy, attribute=None, vo=None):
        """Sets voms attributes to a proxy"""
        if not vo:
            return S_ERROR(DErrno.EVOMS, "No vo specified, and can't get default in the configuration")

        retVal = multiProxyArgument(proxy)
        if not retVal["OK"]:
            return retVal
        proxyDict = retVal["Value"]
        chain = proxyDict["chain"]
        proxyLocation = proxyDict["file"]

        secs = chain.getRemainingSecs()["Value"] - 300
        if secs < 0:
            return S_ERROR(DErrno.EVOMS, "Proxy length is less that 300 secs")
        hours = int(secs / 3600)
        mins = int((secs - hours * 3600) / 60)

        # Ask VOMS a proxy the same strength as the one we already have
        bitStrength = chain.getStrength()["Value"]

        retVal = self._generateTemporalFile()
        if not retVal["OK"]:
            deleteMultiProxy(proxyDict)
            return retVal
        newProxyLocation = retVal["Value"]

        cmd = ["voms-proxy-init"]
        if chain.isLimitedProxy()["Value"]:
            cmd.append("-limited")
        cmd += ["-cert", proxyLocation]
        cmd += ["-key", proxyLocation]
        cmd += ["-out", newProxyLocation]
        cmd += ["-voms"]
        cmd += [f"{vo}:{attribute}" if attribute and attribute != "NoRole" else vo]
        cmd += ["-valid", f"{hours}:{mins}"]
        cmd += ["-bits", str(bitStrength)]
        vomsesPath = self.getVOMSESLocation()
        if vomsesPath:
            cmd += ["-vomses", vomsesPath]

        if chain.isRFC().get("Value"):
            cmd += ["-r"]
        cmd += ["-timeout", "12"]

        result = shellCall(self._secCmdTimeout, shlex.join(cmd))

        deleteMultiProxy(proxyDict)

        if not result["OK"]:
            self._unlinkFiles(newProxyLocation)
            return S_ERROR(DErrno.EVOMS, "Failed to call voms-proxy-init: %s" % result["Message"])

        status, output, error = result["Value"]

        if status:
            self._unlinkFiles(newProxyLocation)
            return S_ERROR(
                DErrno.EVOMS,
                f"Failed to set VOMS attributes. Command: {cmd}; StdOut: {output}; StdErr: {error}",
            )

        newChain = X509Chain()
        retVal = newChain.loadProxyFromFile(newProxyLocation)
        self._unlinkFiles(newProxyLocation)
        if not retVal["OK"]:
            return S_ERROR(DErrno.EVOMS, "Can't load new proxy: %s" % retVal["Message"])

        return S_OK(newChain)

    def vomsInfoAvailable(self):
        """
        Is voms info available?
        """

        vpInfoCmd = ""
        for vpInfo in ("voms-proxy-info", "voms-proxy-info2"):
            if shutil.which(vpInfo):
                vpInfoCmd = vpInfo

        if not vpInfoCmd:
            return S_ERROR(DErrno.EVOMS, "Missing voms-proxy-info")
        cmd = "%s -h" % vpInfoCmd
        result = shellCall(self._secCmdTimeout, cmd)
        if not result["OK"]:
            return False
        status, _output, _error = result["Value"]
        if status:
            return False
        return True

    def _unlinkFiles(self, files):
        if isinstance(files, (list, tuple)):
            for fileName in files:
                self._unlinkFiles(fileName)
        else:
            try:
                os.unlink(files)
            except Exception:
                pass

    def _generateTemporalFile(self):
        try:
            fd, filename = tempfile.mkstemp()
            os.close(fd)
        except OSError:
            return S_ERROR(DErrno.ECTMPF)
        return S_OK(filename)
