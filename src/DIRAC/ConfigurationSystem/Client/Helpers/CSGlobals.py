"""
Some Helper functions to retrieve common location from the CS
"""
import importlib
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.Core.Utilities.Extensions import extensionsByPriority


class Extensions(metaclass=DIRACSingleton):
    def __init__(self):
        self.__modules = {}
        self.__orderedExtNames = []
        self.__csExt = []

    def __load(self):
        if self.__orderedExtNames:
            return
        for extName in extensionsByPriority():
            try:
                res = importlib.import_module(extName)
                if res[0]:
                    res[0].close()
                self.__orderedExtNames.append(extName)
                self.__modules[extName] = res
            except ImportError:
                pass

    def getCSExtensions(self):
        if not self.__csExt:
            exts = extensionsByPriority()

            self.__csExt = []
            for ext in exts:
                if ext.endswith("DIRAC"):
                    ext = ext[:-5]
                # If the extension is now "" (i.e. vanilla DIRAC), don't include it
                if ext:
                    self.__csExt.append(ext)
        return self.__csExt

    @deprecated("Use DIRAC.Core.Utilities.Extensions.extensionsByPriority instead")
    def getInstalledExtensions(self):
        return extensionsByPriority()

    def getExtensionPath(self, extName):
        self.__load()
        return self.__modules[extName][1]

    def getExtensionData(self, extName):
        self.__load()
        return self.__modules[extName]


def getSetup():
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/Setup", "")


def getVO(defaultVO=""):
    """
    Return VO from configuration
    """
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/VirtualOrganization", defaultVO)


def getCSExtensions():
    """
    Return list of extensions registered in the CS
    They do not include DIRAC
    """
    return Extensions().getCSExtensions()


@deprecated("Use DIRAC.Core.Utilities.Extensions.extensionsByPriority instead")
def getInstalledExtensions():
    """
    Return list of extensions registered in the CS and available in local installation
    """
    return extensionsByPriority()


def skipCACheck():
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/Security/SkipCAChecks", "false").lower() in ("y", "yes", "true")


def useServerCertificate():
    from DIRAC import gConfig

    return gConfig.getValue("/DIRAC/Security/UseServerCertificate", "false").lower() in ("y", "yes", "true")
