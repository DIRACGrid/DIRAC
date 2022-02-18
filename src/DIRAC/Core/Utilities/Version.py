import importlib
from importlib.metadata import version as get_version

from DIRAC import S_OK
from DIRAC.Core.Utilities.Extensions import extensionsByPriority


def getCurrentVersion():
    """Get a string corresponding to the current version of the DIRAC package and all the installed
    extension packages
    """
    for ext in extensionsByPriority():
        try:
            return S_OK(importlib.import_module(ext).version)
        except (ImportError, AttributeError):
            pass


def getVersion():
    """Get a dictionary corresponding to the current version of the DIRAC package and all the installed
    extension packages
    """
    vDict = {"Extensions": {}}
    for ext in extensionsByPriority():
        version = get_version(ext)

        vDict["Extensions"][ext] = version
    return S_OK(vDict)
