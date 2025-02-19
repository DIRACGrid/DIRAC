""" Collection of utilities for locating certs, proxy, CAs
"""
import os

import DIRAC
from DIRAC import gConfig

g_SecurityConfPath = "/DIRAC/Security"
DEFAULT_VOMSES_LOCATION = f"{DIRAC.rootPath}/etc/grid-security/vomses"
SYSTEM_VOMSES_LOCATION = "/etc/vomses"
DEFAULT_VOMSDIR_LOCATION = f"{DIRAC.rootPath}/etc/grid-security/vomsdir"
SYSTEM_VOMSDIR_LOCATION = "/etc/grid-security/vomsdir"


def getProxyLocation():
    """Get the path of the currently active grid proxy file"""

    for envVar in ["GRID_PROXY_FILE", "X509_USER_PROXY"]:
        if envVar in os.environ:
            proxyPath = os.path.realpath(os.environ[envVar])
            if os.path.isfile(proxyPath):
                return proxyPath
    # /tmp/x509up_u<uid>
    proxyName = "x509up_u%d" % os.getuid()
    if os.path.isfile(f"/tmp/{proxyName}"):
        return f"/tmp/{proxyName}"


def getCAsLocation():
    """Retrieve the CA's files location"""
    # Grid-Security
    retVal = gConfig.getOption(f"{g_SecurityConfPath}/Grid-Security")
    if retVal["OK"]:
        casPath = f"{retVal['Value']}/certificates"
        if os.path.isdir(casPath):
            return casPath
    # CAPath
    retVal = gConfig.getOption(f"{g_SecurityConfPath}/CALocation")
    if retVal["OK"]:
        casPath = retVal["Value"]
        if os.path.isdir(casPath):
            return casPath
    # Other locations
    return getCAsLocationNoConfig()


def getCAsLocationNoConfig():
    """Retrieve the CA's files location"""
    # Look up the X509_CERT_DIR environment variable
    if "X509_CERT_DIR" in os.environ:
        casPath = os.environ["X509_CERT_DIR"]
        return casPath
    # rootPath./etc/grid-security/certificates
    casPath = f"{DIRAC.rootPath}/etc/grid-security/certificates"
    if os.path.isdir(casPath):
        return casPath
    # /etc/grid-security/certificates
    casPath = "/etc/grid-security/certificates"
    if os.path.isdir(casPath):
        return casPath
    # rootPath./etc/grid-security/certificates
    casPath = f"{DIRAC.rootPath}/etc/grid-security/certificates"
    if os.path.isdir(casPath):
        return casPath


def getVOMSLocation():
    """Retrieve the CA's files location"""
    # Grid-Security
    retVal = gConfig.getOption(f"{g_SecurityConfPath}/Grid-Security")
    if retVal["OK"]:
        vomsPath = f"{retVal['Value']}/vomsdir"
        if os.path.isdir(vomsPath):
            return vomsPath
    # Look up the X509_VOMS_DIR environment variable
    if "X509_VOMS_DIR" in os.environ:
        vomsPath = os.environ["X509_VOMS_DIR"]
        return vomsPath
    # rootPath./etc/grid-security/vomsdir
    vomsPath = f"{DIRAC.rootPath}/etc/grid-security/vomsdir"
    if os.path.isdir(vomsPath):
        return vomsPath
    # /etc/grid-security/vomsdir
    vomsPath = "/etc/grid-security/vomsdir"
    if os.path.isdir(vomsPath):
        return vomsPath
    # rootPath./etc/grid-security/vomsdir
    vomsPath = f"{DIRAC.rootPath}/etc/grid-security/vomsdir"
    if os.path.isdir(vomsPath):
        return vomsPath


def getHostCertificateAndKeyLocation(specificLocation=None):
    """Retrieve the host certificate files location.

    Lookup order:

    * ``specificLocation`` (probably broken, don't use it)
    * Environment variables (``DIRAC_X509_HOST_CERT`` and ``DIRAC_X509_HOST_KEY``)
    * CS (``/DIRAC/Security/CertFile`` and ``/DIRAC/Security/CertKey``)
    * Alternative exotic options, with ``prefix`` in  ``server``, ``host``, ``dirac``, ``service``:
      * in `<DIRAC rootpath>/etc/grid-security/` for ``<prefix>cert.pem`` and ``<prefix>key.pem``
      * in the path defined in the CS in ``/DIRAC/Security/Grid-Security``

    :param specificLocation: CS path to look for a the path to cert and key, which then should be the same.
                             Probably does not work, don't use it

    :returns: tuple ``(<cert location>, <key location>)`` or ``False``

    """

    fileDict = {}

    # First, check the environment variables
    for fileType, envVar in (("cert", "DIRAC_X509_HOST_CERT"), ("key", "DIRAC_X509_HOST_KEY")):
        if envVar in os.environ and os.path.exists(os.environ[envVar]):
            fileDict[fileType] = os.environ[envVar]

    for fileType in ("cert", "key"):
        # Check if we already have the info
        if fileType in fileDict:
            continue

        # Direct file in config
        retVal = gConfig.getOption(f"{g_SecurityConfPath}/{fileType.capitalize()}File")
        if retVal["OK"]:
            fileDict[fileType] = retVal["Value"]
            continue
        fileFound = False
        for filePrefix in ("server", "host", "dirac", "service"):
            # Possible grid-security's
            paths = []
            retVal = gConfig.getOption(f"{g_SecurityConfPath}/Grid-Security")
            if retVal["OK"]:
                paths.append(retVal["Value"])
            paths.append(f"{DIRAC.rootPath}/etc/grid-security/")
            for path in paths:
                filePath = os.path.realpath(f"{path}/{filePrefix}{fileType}.pem")
                if os.path.isfile(filePath):
                    fileDict[fileType] = filePath
                    fileFound = True
                    break
            if fileFound:
                break
    if "cert" not in fileDict or "key" not in fileDict:
        return False
    # we can specify a location outside /opt/dirac/etc/grid-security directory
    if specificLocation:
        fileDict["cert"] = gConfig.getValue(specificLocation, fileDict["cert"])
        fileDict["key"] = gConfig.getValue(specificLocation, fileDict["key"])

    return (fileDict["cert"], fileDict["key"])


def getCertificateAndKeyLocation():
    """Get the locations of the user X509 certificate and key pem files"""

    certfile = ""
    if "X509_USER_CERT" in os.environ:
        if os.path.exists(os.environ["X509_USER_CERT"]):
            certfile = os.environ["X509_USER_CERT"]
    if not certfile and (home := os.environ.get("HOME")):
        if os.path.exists(home + "/.globus/usercert.pem"):
            certfile = home + "/.globus/usercert.pem"

    if not certfile:
        return False

    keyfile = ""
    if "X509_USER_KEY" in os.environ:
        if os.path.exists(os.environ["X509_USER_KEY"]):
            keyfile = os.environ["X509_USER_KEY"]
    if not keyfile and (home := os.environ.get("HOME")):
        if os.path.exists(home + "/.globus/userkey.pem"):
            keyfile = home + "/.globus/userkey.pem"

    if not keyfile:
        return False

    return (certfile, keyfile)


def getDefaultProxyLocation():
    """Get the location of a possible new grid proxy file"""

    for envVar in ["GRID_PROXY_FILE", "X509_USER_PROXY"]:
        if envVar in os.environ:
            proxyPath = os.path.realpath(os.environ[envVar])
            return proxyPath

    # /tmp/x509up_u<uid>
    proxyName = "x509up_u%d" % os.getuid()
    return f"/tmp/{proxyName}"


def getVomsesLocation():
    """Get the location of the directory containing the vomses files"""
    if "X509_VOMSES" in os.environ:
        return os.environ["X509_VOMSES"]
    elif os.path.isdir(DEFAULT_VOMSES_LOCATION):
        return DEFAULT_VOMSES_LOCATION
    elif os.path.isdir(SYSTEM_VOMSES_LOCATION):
        return SYSTEM_VOMSES_LOCATION
    else:
        raise Exception(
            "The env variable X509_VOMSES is not set. "
            "DIRAC does not know where to look for etc/grid-security/vomses. "
            "Please set X509_VOMSES."
        )


def getVomsdirLocation():
    """Get the location of the directory containing the vomsdir files"""
    if "X509_VOMS_DIR" in os.environ:
        return os.environ["X509_VOMS_DIR"]
    elif os.path.isdir(DEFAULT_VOMSDIR_LOCATION):
        return DEFAULT_VOMSDIR_LOCATION
    elif os.path.isdir(SYSTEM_VOMSDIR_LOCATION):
        return SYSTEM_VOMSDIR_LOCATION
    else:
        raise Exception(
            "The env variable X509_VOMS_DIR is not set. "
            "DIRAC does not know where to look for etc/grid-security/vomsdir. "
            "Please set X509_VOMS_DIR."
        )
