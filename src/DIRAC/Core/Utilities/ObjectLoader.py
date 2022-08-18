""" An utility to load modules and objects in DIRAC and extensions, being sure that the extensions are considered
"""
import collections
import os
import re
import pkgutil

import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.Core.Utilities.Extensions import extensionsByPriority, recurseImport


class ObjectLoader(metaclass=DIRACSingleton):
    """Class for loading objects. Example:

    from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
    ol = ObjectLoader()
    ol.loadObject('TransformationSystem.Client.TransformationClient')
    """

    def __init__(self, baseModules=False):
        """init"""
        # We save the original arguments in case
        # we need to reinitialize the rootModules
        # CAUTION: we cant do it after doing
        # baseModules = ['DIRAC']
        # because then baseModules, self.baseModules, and __rootModules
        # are the same and edited in place by __generateRootModules !!
        # (Think of it, it's a binding to a list)
        self.originalBaseModules = baseModules
        self._init(baseModules)

    def _init(self, baseModules):
        """Actually performs the initialization"""
        if not baseModules:
            baseModules = ["DIRAC"]
        self.__rootModules = baseModules
        self.__objs = {}
        self.__generateRootModules(baseModules)

    def reloadRootModules(self):
        """Retrigger the initialization of the rootModules.

        This should be used with care.
        Currently, its only use is (and should stay) to retrigger
        the initialization after the CS has been fully initialized in
        LocalConfiguration.enableCS
        """
        # Load the original baseModule argument that was given
        # to the constructor
        baseModules = self.originalBaseModules
        # and replay the init sequence
        self._init(baseModules)

    def __rootImport(self, modName, hideExceptions=False):
        """Auto search which root module has to be used"""
        for rootModule in self.__rootModules:
            impName = modName
            if rootModule:
                impName = f"{rootModule}.{impName}"
            gLogger.debug("Trying to load %s" % impName)
            result = recurseImport(impName, hideExceptions=hideExceptions)
            if not result["OK"]:
                return result
            if result["Value"]:
                return S_OK((impName, result["Value"]))
        return S_OK()

    def __generateRootModules(self, baseModules):
        """Iterate over all the possible root modules"""
        self.__rootModules = baseModules
        for rootModule in reversed(extensionsByPriority()):
            if rootModule not in self.__rootModules:
                self.__rootModules.append(rootModule)
        self.__rootModules.append("")

        # Reversing the order because we want first to look in the extension(s)
        self.__rootModules.reverse()

    def loadModule(self, importString, hideExceptions=False):
        """Load a module from an import string"""
        result = self.__rootImport(importString, hideExceptions=hideExceptions)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(DErrno.EIMPERR, "No module %s found" % importString)
        return S_OK(result["Value"][1])

    def loadObject(self, importString, objName=False, hideExceptions=False):
        """Load an object from inside a module"""
        if not objName:
            objName = importString.split(".")[-1]

        result = self.loadModule(importString, hideExceptions=hideExceptions)
        if not result["OK"]:
            return result
        modObj = result["Value"]
        try:
            result = S_OK(getattr(modObj, objName))
            result["ModuleFile"] = modObj.__file__
            return result
        except AttributeError:
            return S_ERROR(DErrno.EIMPERR, f"{importString} does not contain a {objName} object")

    def getObjects(self, modulePath, reFilter=None, parentClass=None, recurse=False, continueOnError=False):
        """Search for modules under a certain path

        modulePath is the import string needed to access the parent module.
        Root modules will be included automatically (like DIRAC). For instance "ConfigurationSystem.Service"

        reFilter is a regular expression to filter what to load. For instance ".*Handler"
        parentClass is a class object from which the loaded modules have to import from. For instance RequestHandler

        :param continueOnError: if True, continue loading further module even if one fails
        """
        modules = collections.OrderedDict()
        if isinstance(reFilter, str):
            reFilter = re.compile(reFilter)

        for rootModule in self.__rootModules:
            impPath = modulePath
            if rootModule:
                impPath = f"{rootModule}.{impPath}"
            gLogger.debug("Trying to load %s" % impPath)

            result = recurseImport(impPath)
            if not result["OK"]:
                return result
            if not result["Value"]:
                continue
            parentModule = result["Value"]
            gLogger.verbose(f"Loaded module {impPath} at {parentModule.__path__}")

            for _modLoader, modName, isPkg in pkgutil.walk_packages(parentModule.__path__):
                if reFilter and not reFilter.match(modName):
                    continue
                if isPkg:
                    if recurse:
                        result = self.getObjects(
                            f"{modulePath}.{modName}", reFilter=reFilter, parentClass=parentClass, recurse=recurse
                        )
                        if not result["OK"]:
                            return result
                        modules.update(result["Value"])
                    continue
                modKeyName = f"{modulePath}.{modName}"
                if modKeyName in modules:
                    continue
                fullName = f"{impPath}.{modName}"
                result = recurseImport(fullName)
                if not result["OK"]:
                    if continueOnError:
                        gLogger.error(
                            "Error loading module but continueOnError is true",
                            f"module {fullName} error {result}",
                        )
                        continue
                    return result
                if not result["Value"]:
                    continue

                modClass = getattr(result["Value"], modName, None)
                if not modClass:
                    gLogger.warn(f"{fullName} does not contain a {modName} object")
                    continue

                if parentClass and not issubclass(modClass, parentClass):
                    continue

                modules[modKeyName] = modClass

        return S_OK(modules)


def loadObjects(path, reFilter=None, parentClass=None):
    """
    :param str path: the path to the syetem for example: DIRAC/AccountingSystem
    :param object reFilter: regular expression used to found the class
    :param object parentClass: class instance
    :return: dictionary containing the name of the class and its instance
    """
    if not reFilter:
        reFilter = re.compile(r".*[a-z1-9]\.py$")
    pathList = List.fromChar(path, "/")

    objectsToLoad = {}
    # Find which object files match
    for parentModule in extensionsByPriority():
        objDir = os.path.join(os.path.dirname(os.path.dirname(DIRAC.__file__)), parentModule, *pathList)
        if not os.path.isdir(objDir):
            continue
        for objFile in os.listdir(objDir):
            if reFilter.match(objFile):
                pythonClassName = objFile[:-3]
                if pythonClassName not in objectsToLoad:
                    gLogger.info(f"Adding to load queue {parentModule}/{path}/{pythonClassName}")
                    objectsToLoad[pythonClassName] = parentModule

    # Load them!
    loadedObjects = {}

    for pythonClassName in objectsToLoad:
        parentModule = objectsToLoad[pythonClassName]
        try:
            # Where parentModule can be DIRAC, pathList is something like [ "AccountingSystem", "Client", "Types" ]
            # And the python class name is.. well, the python class name
            objPythonPath = "{}.{}.{}".format(parentModule, ".".join(pathList), pythonClassName)
            objModule = __import__(objPythonPath, globals(), locals(), pythonClassName)
            objClass = getattr(objModule, pythonClassName)
        except Exception as e:
            gLogger.error("Can't load type", f"{parentModule}/{pythonClassName}: {str(e)}")
            continue
        if parentClass == objClass:
            continue
        if parentClass and not issubclass(objClass, parentClass):
            gLogger.warn(f"{objClass} is not a subclass of {parentClass}. Skipping")
            continue
        gLogger.info("Loaded %s" % objPythonPath)
        loadedObjects[pythonClassName] = objClass

    return loadedObjects
