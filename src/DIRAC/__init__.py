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
      - DISET
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
    - majorVersion:  DIRAC Major version number
    - minorVersion:  DIRAC Minor version number
    - patchLevel:    DIRAC Patch level number
    - preVersion:    DIRAC Pre release number
    - version:       DIRAC version string

    - errorMail:     mail address for important errors
    - alarmMail:     mail address for important alarms

    - pythonPath:    absolute real path to the directory that contains this file
    - rootPath:      absolute real path to the parent of DIRAC.pythonPath

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

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys
import os
import platform as pyPlatform
from pkgutil import extend_path

import re
import six

__path__ = extend_path(__path__, __name__)

# Set the environment variable such that openssl accepts proxy cert
# Sadly, this trick was removed in openssl >= 1.1.0
# https://github.com/openssl/openssl/commit/8e21938ce3a5306df753eb40a20fe30d17cf4a68
# Lets see if they would accept to put it back
# https://github.com/openssl/openssl/issues/8177
os.environ["OPENSSL_ALLOW_PROXY_CERTS"] = "True"

__RCSID__ = "$Id$"

# Now that's one hell of a hack :)
# _strptime is not thread safe, resulting in obscure callstack
# whenever you would have multiple threads and calling datetime.datetime.strptime
# (AttributeError: 'module' object has no attribute '_strptime')
# Importing _strptime before instantiating the threads seem to be a working workaround
import _strptime

# Define Version
if six.PY3:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        __version__ = get_distribution(__name__).version
        version = __version__
    except DistributionNotFound:
        # package is not installed
        version = "Unknown"
else:
    majorVersion = 7
    minorVersion = 3
    patchLevel = 32
    preVersion = 0

    version = "v%sr%s" % (majorVersion, minorVersion)
    # Make it so that __version__ is always PEP-440 style
    __version__ = "%s.%s" % (majorVersion, minorVersion)
    if patchLevel:
        version = "%sp%s" % (version, patchLevel)
        __version__ += ".%s" % patchLevel
    if preVersion:
        version = "%s-pre%s" % (version, preVersion)
        __version__ += "a%s" % preVersion

errorMail = "dirac.alarms@gmail.com"
alarmMail = "dirac.alarms@gmail.com"


def isPy3VersionNumber(releaseVersion):
    """Returns True if the releaseVersion is a PEP-440 style string.
    This is the `is_canonical` function defined in PEP-440 Appendix B

    :param str releaseVersion: The software version to use
    """
    return (
        re.match(
            r"^([1-9][0-9]*!)?(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*((a|b|rc)(0|[1-9][0-9]*))?(\.post(0|[1-9][0-9]*))?(\.dev(0|[1-9][0-9]*))?$",
            releaseVersion,
        )
        is not None
    )


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
if six.PY3:
    rootPath = _computeRootPath(sys.base_prefix)  # pylint: disable=no-member
else:
    pythonPath = os.path.realpath(__path__[0])
    rootPath = os.path.dirname(pythonPath)
# Allow rootPath detection to be overridden
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
            "DIRAC-CertifOauth": "dips://lbcertifdiracoauth.cern.ch:9135/Configuration/Server",
        },
    }
