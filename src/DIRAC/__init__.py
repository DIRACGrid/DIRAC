"""
   DIRAC - Distributed Infrastructure with Remote Agent Control

   The distributed data production and analysis system of LHCb and other VOs.

   DIRAC is a software framework for distributed computing which
   allows to integrate various computing resources in a single
   system. At the same time it integrates all kinds of computing
   activities like Monte Carlo simulations, data processing, or
   final user analysis.

   It is build as number of cooperating systems:
    - Accounting
    - Configuration
    - Core
      - Base
      - Security
      - Utilities
      - Workflow
    - Framework
    - RequestManagement
    - Resources
    - Transformation

    Which are used by other system providing functionality to
    the end user:
    - DataManagement
    - Interfaces
    - ResourceStatus
    - StorageManagement
    - WorkloadManagement

    It defines the following data members:
    - version:       DIRAC version string

    - errorMail:     mail address for important errors
    - alarmMail:     mail address for important alarms

    It loads Modules from :
    - DIRAC.Core.Utililies

    It loads:
    - S_OK:           OK return structure
    - S_ERROR:        ERROR return structure
    - gLogger:        global Logger object
    - gConfig:        global Config object

    It defines the following functions:
    - abort:          aborts execution
    - exit:           finish execution using callbacks
    - siteName:       returns DIRAC name for current site

    - getPlatform():      DIRAC platform string for current host
    - getPlatformTuple(): DIRAC platform tuple for current host

"""
import os
import re
import sys
import warnings
from pkgutil import extend_path
from typing import Any, Optional, Union
from pkg_resources import get_distribution, DistributionNotFound


__path__ = extend_path(__path__, __name__)

# Set the environment variable such that openssl accepts proxy cert
# Sadly, this trick was removed in openssl >= 1.1.0
# https://github.com/openssl/openssl/commit/8e21938ce3a5306df753eb40a20fe30d17cf4a68
# Lets see if they would accept to put it back
# https://github.com/openssl/openssl/issues/8177
os.environ["OPENSSL_ALLOW_PROXY_CERTS"] = "True"

# Now that's one hell of a hack :)
# _strptime is not thread safe, resulting in obscure callstack
# whenever you would have multiple threads and calling datetime.datetime.strptime
# (AttributeError: 'module' object has no attribute '_strptime')
# Importing _strptime before instantiating the threads seem to be a working workaround
import _strptime

# Define Version
try:
    __version__ = get_distribution(__name__).version
    version = __version__
except DistributionNotFound:
    # package is not installed
    version = "Unknown"

errorMail = "dirac.alarms@gmail.com"
alarmMail = "dirac.alarms@gmail.com"


def convertToPy3VersionNumber(releaseVersion):
    """Convert the releaseVersion into a PEP-440 style string

    :param str releaseVersion: The software version to use
    """
    VERSION_PATTERN = re.compile(r"^(?:v)?(\d+)[r\.](\d+)(?:[p\.](\d+))?(?:(?:-pre|a)?(\d+))?$")

    match = VERSION_PATTERN.match(releaseVersion)
    # If the regex fails just return the original version
    if not match:
        return releaseVersion
    major, minor, patch, pre = match.groups()
    version = major + "." + minor
    version += "." + (patch or "0")
    if pre:
        version += "a" + pre
    return version


def _computeRootPath(rootPath):
    """Compute the root of the DIRAC installation

    Nominally DIRACOS gives us a "sysroot" in that things like ``lib/``,
    ``bin/``, ``share/``, ``etc/`` exist under ``$DIRACOS/``.

    In the case of an uncontainerised server installation it is useful to keep
    multiple versions of DIRAC+DIRACOS available so we can switch between them
    easily if something goes wrong during an upgrade. We also want to ensure
    that fixed paths like ``etc/dirac.cfg``, ``runit/``, ``startup/`` are
    preservered between versions.

    To achieve this generically while mostly following the Python 2 style DIRAC
    installation layout we start from ``sys.base_prefix`` and look if the
    folder structure looks like a "server" layout, i.e.
    ``versions/vX.Y.Z-$TIMESTAMP/$(uname -s)_$(uname -m)/``. If it does we
    return the directory that contains ``versions/``.

    :param str rootPath:
    :return: The DIRAC rootPath, accounting for server-style installations.
    """
    import re
    from pathlib import Path  # pylint: disable=import-error

    rootPath = Path(rootPath).resolve()
    versionsPath = rootPath.parent
    if versionsPath.parent.name != "versions":
        return str(rootPath)
    # VERSION-INSTALL_TIME
    pattern1 = re.compile(r"v(\d+\.\d+\.\d+[^\-]*)\-(\d+)")
    # $(uname -s)-$(uname -m)
    pattern2 = re.compile(r"([^\-]+)-([^\-]+)")
    if pattern1.fullmatch(versionsPath.name) and pattern2.fullmatch(rootPath.name):
        # This is a versioned install
        return str(versionsPath.parent.parent)
    else:
        return str(rootPath)


# Set rootPath of DIRAC installation
rootPath = _computeRootPath(sys.base_prefix)
if "DIRAC_ROOT_PATH" in os.environ:
    rootPath = os.environ["DIRAC_ROOT_PATH"]

# Import DIRAC.Core.Utils modules

# from DIRAC.Core.Utilities import *
from DIRAC.Core.Utilities.Network import getFQDN

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


# Logger
from DIRAC.FrameworkSystem.Client.Logger import gLogger

# Configuration client
from DIRAC.ConfigurationSystem.Client.Config import gConfig


from DIRAC.Core.Security.Properties import SecurityProperty, UnevaluatedProperty
from DIRAC.Core.Utilities import exceptions
from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevel


def initialize(
    *,
    require_auth: bool = True,
    security_expression: Union[None, SecurityProperty, UnevaluatedProperty] = None,
    log_level: Optional[LogLevel] = None,
    extra_config_files: Optional[list[os.PathLike]] = None,
    extra_config: Optional[dict[str, Any]] = None,
    host_credentials: Optional[tuple[os.PathLike, os.PathLike]] = None,
) -> None:
    """Prepare the global state so that DIRAC clients can be used.

    This method needs to be called before any DIRAC client is created to ensure
    that the necessary global state is configured.

    :raises DIRAC.Core.Utilities.exceptions.DIRACInitError: If initialization fails.
        (or a subclass thereof) is raised.

    :param require_auth: Set to ``False`` to skip the authentication check.
    :param security_expression: Check that the current credentials have the
        required properties. See :py:class:`~DIRAC.Core.Security.Properties.SecurityProperty` for details.
    :param log_level: Override the default DIRAC logging level.
    :param extra_config_files: Files to merge into the local configuration.
    :param extra_config: Dictionary to merge into the local configuration.
    :param host_credentials: Tuple of (host certificate, host key) to force the
        use of server certificate credentials.
    """
    from diraccfg import CFG

    from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient
    from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
    from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.Core.Utilities import List

    if security_expression is not None and not require_auth:
        raise TypeError(f"security_expression cannot be used with {require_auth=}")

    localCfg = LocalConfiguration()

    for config_file in extra_config_files or []:
        cfg = CFG()
        cfg.loadFromFile(config_file)
        gConfigurationData.mergeWithLocal(cfg)

    if extra_config:
        cfg = CFG()
        cfg.loadFromDict(extra_config)
        gConfigurationData.mergeWithLocal(cfg)

    if host_credentials:
        gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "yes")
        gConfigurationData.setOptionInCFG("/DIRAC/Security/CertFile", str(host_credentials[0]))
        gConfigurationData.setOptionInCFG("/DIRAC/Security/KeyFile", str(host_credentials[1]))

    if log_level:
        gLogger.setLevel(log_level)
    else:
        # Memorize the current log level and then suppress all message
        log_level = getattr(LogLevel, gLogger.getLevel())
        gLogger.setLevel(LogLevel.ALWAYS)
    try:
        returnValueOrRaise(localCfg.initialize())
    finally:
        # Restore the pre-existing log level
        gLogger.setLevel(log_level)

    if not gConfigurationData.extractOptionFromCFG("/DIRAC/Setup"):
        message = '/DIRAC/Setup is not defined. Have you ran "dirac-configure"?'
        if require_auth:
            raise exceptions.NotConfiguredError(message)
        else:
            warnings.warn(message, exceptions.DiracWarning)

    if require_auth:
        retVal = S_ERROR("No configuration servers found")
        for url in List.randomize(gConfigurationData.getServers()):
            retVal = ConfigurationClient(url=url).whoami()
            if retVal["OK"]:
                break
        else:
            raise exceptions.DIRACInitError(f"Failed to contact the Configuration Server: {retVal['Message']}")

        if security_expression is not None:
            if not isinstance(security_expression, UnevaluatedProperty):
                security_expression = UnevaluatedProperty(security_expression)
            proxyInfo = retVal["Value"]
            properties = list(map(SecurityProperty, proxyInfo["properties"]))

            if not security_expression(properties):
                raise exceptions.AuthError(
                    f"Current credentials (username={proxyInfo['username']!r}, "
                    f"group={proxyInfo['group']!r}, properties={properties}) "
                    f"are invalid for the required expression: {security_expression}"
                )


__siteName = False


def siteName():
    """
    Determine and return DIRAC name for current site
    """
    global __siteName
    if not __siteName:
        __siteName = gConfig.getValue("/LocalSite/Site")
        if not __siteName:
            # Some Defaults if not present in the configuration
            FQDN = getFQDN()
            if len(FQDN.split(".")) > 2:
                # Use the last component of the FQDN as country code if there are more than 2 components
                __siteName = "DIRAC.Client.%s" % FQDN.split(".")[-1]
            else:
                # else use local as country code
                __siteName = "DIRAC.Client.local"

    return __siteName


# platform detection
from DIRAC.Core.Utilities.Platform import getPlatformString, getPlatform, getPlatformTuple


def exit(exitCode=0):
    """
    Finish execution using callbacks
    """
    sys.exit(exitCode)


def abort(exitCode, *args, **kwargs):
    """
    Abort execution
    """
    try:
        gLogger.fatal(*args, **kwargs)
        os._exit(exitCode)
    except OSError:
        gLogger.exception("Error while executing DIRAC.abort")
        os._exit(exitCode)


def extension_metadata():
    return {
        "primary_extension": True,
        "priority": 0,
        "setups": {
            "DIRAC-Certification": "https://lbcertifdirac70.cern.ch:9135/Configuration/Server",
            "DIRAC-CertifOauth": "https://lbcertifdiracoauth.cern.ch:9135/Configuration/Server",
        },
    }
