""" Module invoked for finding and loading DIRAC (and extensions) modules
"""
import os
from DIRAC.Core.Utilities import List
from DIRAC import gConfig, S_ERROR, S_OK, gLogger
from DIRAC.Core.Utilities.Extensions import extensionsByPriority, recurseImport


class ModuleLoader:
    def __init__(self, importLocation, sectionFinder, csSuffix=False, moduleSuffix=False):
        self.__modules = {}
        self.__loadedModules = {}
        # Function to find the
        self.__sectionFinder = sectionFinder
        # Import from where? <Ext>.<System>System.<importLocation>.<module>
        self.__importLocation = importLocation
        # Where to look in the CS for the module? /Systems/<System>/<Instance>/<csSuffix>
        if not csSuffix:
            csSuffix = f"{importLocation}s"
        self.__csSuffix = csSuffix
        # Module suffix (for Handlers)
        self.__modSuffix = moduleSuffix

    def getModules(self):
        data = dict(self.__modules)
        for k in data:
            data[k]["standalone"] = len(data) == 1
        return data

    def loadModules(self, modulesList, hideExceptions=False):
        """
        Load all modules required in moduleList
        """
        for modName in modulesList:
            gLogger.verbose(f"Checking {modName}")
            # if it's a executor modName name just load it and be done with it
            if "/" in modName:
                gLogger.verbose(f"Module {modName} seems to be a valid name. Try to load it!")
                result = self.loadModule(modName, hideExceptions=hideExceptions)
                if not result["OK"]:
                    return result
                continue
            # Check if it's a system name
            # Look in the CS
            system = modName
            csPath = f"/Systems/{system}/Executors"
            gLogger.verbose(f"Exploring {csPath} to discover modules")
            result = gConfig.getSections(csPath)
            if result["OK"]:
                # Add all modules in the CS :P
                for modName in result["Value"]:
                    result = self.loadModule(f"{system}/{modName}", hideExceptions=hideExceptions)
                    if not result["OK"]:
                        return result
            # Look what is installed
            parentModule = None
            for rootModule in extensionsByPriority():
                if not system.endswith("System"):
                    system += "System"
                parentImport = f"{rootModule}.{system}.{self.__csSuffix}"
                # HERE!
                result = recurseImport(parentImport)
                if not result["OK"]:
                    return result
                parentModule = result["Value"]
                if parentModule:
                    break
            if not parentModule:
                continue
            parentPath = parentModule.__path__[0]
            gLogger.notice(f"Found modules path at {parentImport}")
            for entry in os.listdir(parentPath):
                if entry == "__init__.py" or not entry.endswith(".py"):
                    continue
                if not os.path.isfile(os.path.join(parentPath, entry)):
                    continue
                modName = f"{system}/{entry[:-3]}"
                gLogger.verbose(f"Trying to import {modName}")
                result = self.loadModule(modName, hideExceptions=hideExceptions, parentModule=parentModule)

        return S_OK()

    def loadModule(self, modName, hideExceptions=False, parentModule=False):
        """
        Load module name.
        name must take the form [DIRAC System Name]/[DIRAC module]
        """
        while modName and modName[0] == "/":
            modName = modName[1:]
        if modName in self.__modules:
            return S_OK()
        modList = modName.split("/")
        if len(modList) != 2:
            return S_ERROR(f"Can't load {modName}: Invalid module name")
        csSection = self.__sectionFinder(modName)
        loadGroup = gConfig.getValue(f"{csSection}/Load", [])
        # Check if it's a load group
        if loadGroup:
            gLogger.info(f"Found load group {modName}. Will load {', '.join(loadGroup)}")
            for loadModName in loadGroup:
                if "/" not in loadModName:
                    loadModName = f"{modList[0]}/{loadModName}"
                result = self.loadModule(loadModName, hideExceptions=hideExceptions)
                if not result["OK"]:
                    return result
            return S_OK()
        # Normal load
        loadName = gConfig.getValue(f"{csSection}/Module", "")
        if not loadName:
            loadName = modName
            gLogger.info(f"Loading {modName}")
        else:
            if "/" not in loadName:
                loadName = f"{modList[0]}/{loadName}"
            gLogger.info(f"Loading {modName} ({loadName})")
        # If already loaded, skip
        loadList = loadName.split("/")
        if len(loadList) != 2:
            return S_ERROR(f"Can't load {loadName}: Invalid module name")
        system, module = loadList
        # Load
        className = module
        if self.__modSuffix:
            className = f"{className}{self.__modSuffix}"
        if loadName not in self.__loadedModules:
            # Check if handler is defined
            loadCSSection = self.__sectionFinder(loadName)
            handlerPath = gConfig.getValue(f"{loadCSSection}/HandlerPath", "")
            if handlerPath:
                gLogger.info(f"Trying to load handler for {loadName} from CS defined path {handlerPath}")
                handlerPath = handlerPath.replace("/", ".")
                if handlerPath.endswith(".py"):
                    handlerPath = handlerPath[:-3]
                className = List.fromChar(handlerPath, ".")[-1]
                result = recurseImport(handlerPath)
                if not result["OK"]:
                    return S_ERROR(f"Cannot load user defined handler {handlerPath}: {result['Message']}")
                gLogger.verbose(f"Loading {handlerPath}")
            elif parentModule:
                gLogger.info(f"Trying to autodiscover {loadName} from parent")
                # If we've got a parent module, load from there.
                modImport = module
                if self.__modSuffix:
                    modImport = f"{modImport}{self.__modSuffix}"
                result = recurseImport(modImport, parentModule, hideExceptions=hideExceptions)
            else:
                # Check to see if the module exists in any of the root modules
                gLogger.info(f"Trying to autodiscover {loadName}")
                for rootModule in extensionsByPriority():
                    importString = f"{rootModule}.{system}System.{self.__importLocation}.{module}"
                    if self.__modSuffix:
                        importString = f"{importString}{self.__modSuffix}"
                    gLogger.verbose(f"Trying to load {importString}")
                    result = recurseImport(importString, hideExceptions=hideExceptions)
                    # Error while loading
                    if not result["OK"]:
                        return result
                    # Something has been found! break :)
                    if result["Value"]:
                        gLogger.verbose(f"Found {importString}")
                        break
            # Nothing found
            if not result["Value"]:
                return S_ERROR(f"Could not find {loadName}")
            modObj = result["Value"]
            try:
                # Try to get the class from the module
                modClass = getattr(modObj, className)
            except AttributeError:
                if "__file__" in dir(modObj):
                    location = modObj.__file__
                else:
                    location = modObj.__path__
                gLogger.exception(f"{location} module does not have a {module} class!")
                return S_ERROR(f"Cannot load {module}")

            self.__loadedModules[loadName] = {"classObj": modClass, "moduleObj": modObj}
            # End of loading of 'loadName' module

        # A-OK :)
        self.__modules[modName] = self.__loadedModules[loadName].copy()
        # keep the name of the real code module
        self.__modules[modName]["modName"] = modName
        self.__modules[modName]["loadName"] = loadName
        gLogger.notice(f"Loaded module {modName}")

        return S_OK()
