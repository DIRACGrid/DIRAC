"""
Some Helper functions to retrieve common location from the CS
"""
from DIRAC.Core.Utilities.Extensions import extensionsByPriority


def getSetup() -> str:
    """
    Return setup name
    """
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/Setup", "")


def getVO(defaultVO: str = "") -> str:
    """
    Return VO from configuration
    """
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/VirtualOrganization", defaultVO)


def getCSExtensions() -> list:
    """
    Return list of extensions registered in the CS
    They do not include DIRAC
    """
    return [ext[:-5] if ext.endswith("DIRAC") else ext for ext in extensionsByPriority() if ext != "DIRAC"]


def skipCACheck() -> bool:
    """
    Skip CA check
    """
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/Security/SkipCAChecks", False)


def useServerCertificate() -> bool:
    """
    Use server certificate
    """
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/Security/UseServerCertificate", False)
