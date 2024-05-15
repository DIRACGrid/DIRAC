""" Module for dealing with VOMS (Virtual Organization Membership Service)
"""

from datetime import datetime
import os
import tempfile
import shlex
import shutil

from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.ProxyFile import multiProxyArgument, deleteMultiProxy
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities import List

# This is a variable so it can be monkeypatched in tests
VOMS_PROXY_INIT_CMD = "voms-proxy-init"


def voms_init_cmd(
    vo: str, attribute: str | None, chain: X509Chain, in_fn: str, out_fn: str, vomsesPath: str | None
) -> list[str]:
    secs = chain.getRemainingSecs()["Value"] - 300
    if secs < 0:
        return S_ERROR(DErrno.EVOMS, "Proxy length is less that 300 secs")
    hours = int(secs / 3600)
    mins = int((secs - hours * 3600) / 60)

    bitStrength = chain.getStrength()["Value"]

    cmd = [VOMS_PROXY_INIT_CMD]
    if chain.isLimitedProxy()["Value"]:
        cmd.append("-limited")
    cmd += ["-cert", in_fn]
    cmd += ["-key", in_fn]
    cmd += ["-out", out_fn]
    cmd += ["-voms"]
    cmd += [f"{vo}:{attribute}" if attribute and attribute != "NoRole" else vo]
    cmd += ["-valid", f"{hours}:{mins}"]
    cmd += ["-bits", str(bitStrength)]
    if vomsesPath:
        cmd += ["-vomses", vomsesPath]

    if chain.isRFC().get("Value"):
        cmd += ["-r"]
    cmd += ["-timeout", "12"]

    return cmd


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
            return S_ERROR(DErrno.EVOMS, f"Failed to extract info from proxy: {result['Message']}")

        vomsInfoOutput = List.fromChar(result["Value"], "\n")

        # Get a list of known VOMS attributes
        validVOMSAttrs = []
        result = gConfig.getSections("/Registry/Groups")
        if result["OK"]:
            for group in result["Value"]:
                vA = gConfig.getValue(f"/Registry/Groups/{group}/VOMSRole", "")
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
        else:
            raise NotImplementedError(switch)

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
            return S_ERROR(DErrno.EVOMS, f"invalid option {option}")

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
                return S_OK(f"{data['subject']}\n")
            if option == "fqan":
                return S_OK(
                    "\n".join([f.replace("/Role=NULL", "").replace("/Capability=NULL", "") for f in data["fqan"]])
                )
            if option == "all":
                lines = []
                creds = proxyDict["chain"].getCredentials()["Value"]
                lines.append(f"subject : {creds['subject']}")
                lines.append(f"issuer : {creds['issuer']}")
                lines.append(f"identity : {creds['identity']}")
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
                lines.append(f"== VO {data['vo']} extension information ==")
                lines.append(f"VO: {data['vo']}")
                lines.append(f"subject : {data['subject']}")
                lines.append(f"issuer : {data['issuer']}")
                for fqan in data["fqan"]:
                    lines.append(f"attribute : {fqan}")
                if "attribute" in data:
                    lines.append(f"attribute : {data['attribute']}")
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
        for vomsesPath in Locations.getVomsesLocation().split(":"):
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

        retVal = self._generateTemporalFile()
        if not retVal["OK"]:
            deleteMultiProxy(proxyDict)
            return retVal
        newProxyLocation = retVal["Value"]

        cmd = voms_init_cmd(vo, attribute, chain, proxyLocation, newProxyLocation, self.getVOMSESLocation())

        result = shellCall(
            self._secCmdTimeout,
            shlex.join(cmd),
            env=os.environ
            | {
                "X509_CERT_DIR": Locations.getCAsLocation(),
                "X509_VOMS_DIR": Locations.getVomsdirLocation(),
            },
        )

        deleteMultiProxy(proxyDict)

        if not result["OK"]:
            self._unlinkFiles(newProxyLocation)
            return S_ERROR(DErrno.EVOMS, f"Failed to call voms-proxy-init: {result['Message']}")

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
            return S_ERROR(DErrno.EVOMS, f"Can't load new proxy: {retVal['Message']}")

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
        cmd = f"{vpInfoCmd} -h"
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
