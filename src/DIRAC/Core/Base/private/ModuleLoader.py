""" Module invoked for finding and loading DIRAC (and extensions) modules
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from DIRAC.Core.Utilities import List
from DIRAC import gConfig, S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import getInstalledExtensions
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities.Extensions import recurseImport


class ModuleLoader(object):

  def __init__(self, importLocation, sectionFinder, superClass, csSuffix=False, moduleSuffix=False):
    self.__modules = {}
    self.__loadedModules = {}
    self.__superClass = superClass
    # Function to find the
    self.__sectionFinder = sectionFinder
    # Import from where? <Ext>.<System>System.<importLocation>.<module>
    self.__importLocation = importLocation
    # Where to look in the CS for the module? /Systems/<System>/<Instance>/<csSuffix>
    if not csSuffix:
      csSuffix = "%ss" % importLocation
    self.__csSuffix = csSuffix
    # Module suffix (for Handlers)
    self.__modSuffix = moduleSuffix

  def getModules(self):
    data = dict(self.__modules)
    for k in data:
      data[k]['standalone'] = len(data) == 1
    return data

  def loadModules(self, modulesList, hideExceptions=False):
    """
      Load all modules required in moduleList
    """
    for modName in modulesList:
      gLogger.verbose("Checking %s" % modName)
      # if it's a executor modName name just load it and be done with it
      if modName.find("/") > -1:
        gLogger.verbose("Module %s seems to be a valid name. Try to load it!" % modName)
        result = self.loadModule(modName, hideExceptions=hideExceptions)
        if not result['OK']:
          return result
        continue
      # Check if it's a system name
      # Look in the CS
      system = modName
      # Can this be generated with sectionFinder?
      csPath = "%s/Executors" % PathFinder.getSystemSection(system, (system, ))
      gLogger.verbose("Exploring %s to discover modules" % csPath)
      result = gConfig.getSections(csPath)
      if result['OK']:
        # Add all modules in the CS :P
        for modName in result['Value']:
          result = self.loadModule("%s/%s" % (system, modName), hideExceptions=hideExceptions)
          if not result['OK']:
            return result
      # Look what is installed
      parentModule = None
      for rootModule in getInstalledExtensions():
        if not system.endswith("System"):
          system += "System"
        parentImport = "%s.%s.%s" % (rootModule, system, self.__csSuffix)
        # HERE!
        result = recurseImport(parentImport)
        if not result['OK']:
          return result
        parentModule = result['Value']
        if parentModule:
          break
      if not parentModule:
        continue
      parentPath = parentModule.__path__[0]
      gLogger.notice("Found modules path at %s" % parentImport)
      for entry in os.listdir(parentPath):
        if entry[-3:] != ".py" or entry == "__init__.py":
          continue
        if not os.path.isfile(os.path.join(parentPath, entry)):
          continue
        modName = "%s/%s" % (system, entry[:-3])
        gLogger.verbose("Trying to import %s" % modName)
        result = self.loadModule(modName,
                                 hideExceptions=hideExceptions,
                                 parentModule=parentModule)

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
      return S_ERROR("Can't load %s: Invalid module name" % (modName))
    csSection = self.__sectionFinder(modName)
    loadGroup = gConfig.getValue("%s/Load" % csSection, [])
    # Check if it's a load group
    if loadGroup:
      gLogger.info("Found load group %s. Will load %s" % (modName, ", ".join(loadGroup)))
      for loadModName in loadGroup:
        if loadModName.find("/") == -1:
          loadModName = "%s/%s" % (modList[0], loadModName)
        result = self.loadModule(loadModName, hideExceptions=hideExceptions)
        if not result['OK']:
          return result
      return S_OK()
    # Normal load
    loadName = gConfig.getValue("%s/Module" % csSection, "")
    if not loadName:
      loadName = modName
      gLogger.info("Loading %s" % (modName))
    else:
      if loadName.find("/") == -1:
        loadName = "%s/%s" % (modList[0], loadName)
      gLogger.info("Loading %s (%s)" % (modName, loadName))
    # If already loaded, skip
    loadList = loadName.split("/")
    if len(loadList) != 2:
      return S_ERROR("Can't load %s: Invalid module name" % (loadName))
    system, module = loadList
    # Load
    className = module
    if self.__modSuffix:
      className = "%s%s" % (className, self.__modSuffix)
    if loadName not in self.__loadedModules:
      # Check if handler is defined
      loadCSSection = self.__sectionFinder(loadName)
      handlerPath = gConfig.getValue("%s/HandlerPath" % loadCSSection, "")
      if handlerPath:
        gLogger.info("Trying to %s from CS defined path %s" % (loadName, handlerPath))
        gLogger.verbose("Found handler for %s: %s" % (loadName, handlerPath))
        handlerPath = handlerPath.replace("/", ".")
        if handlerPath.find(".py", len(handlerPath) - 3) > -1:
          handlerPath = handlerPath[:-3]
        className = List.fromChar(handlerPath, ".")[-1]
        result = recurseImport(handlerPath)
        if not result['OK']:
          return S_ERROR("Cannot load user defined handler %s: %s" % (handlerPath, result['Message']))
        gLogger.verbose("Loaded %s" % handlerPath)
      elif parentModule:
        gLogger.info("Trying to autodiscover %s from parent" % loadName)
        # If we've got a parent module, load from there.
        modImport = module
        if self.__modSuffix:
          modImport = "%s%s" % (modImport, self.__modSuffix)
        result = recurseImport(modImport, parentModule, hideExceptions=hideExceptions)
      else:
        # Check to see if the module exists in any of the root modules
        gLogger.info("Trying to autodiscover %s" % loadName)
        rootModulesToLook = getInstalledExtensions()
        for rootModule in rootModulesToLook:
          importString = '%s.%sSystem.%s.%s' % (rootModule, system, self.__importLocation, module)
          if self.__modSuffix:
            importString = "%s%s" % (importString, self.__modSuffix)
          gLogger.verbose("Trying to load %s" % importString)
          result = recurseImport(importString, hideExceptions=hideExceptions)
          # Error while loading
          if not result['OK']:
            return result
          # Something has been found! break :)
          if result['Value']:
            gLogger.verbose("Found %s" % importString)
            break
      # Nothing found
      if not result['Value']:
        return S_ERROR("Could not find %s" % loadName)
      modObj = result['Value']
      try:
        # Try to get the class from the module
        modClass = getattr(modObj, className)
      except AttributeError:
        location = ""
        if '__file__' in dir(modObj):
          location = modObj.__file__
        else:
          location = modObj.__path__
        gLogger.exception("%s module does not have a %s class!" % (location, module))
        return S_ERROR("Cannot load %s" % module)
      # Check if it's subclass
      if not issubclass(modClass, self.__superClass):
        return S_ERROR("%s has to inherit from %s" % (loadName, self.__superClass.__name__))
      self.__loadedModules[loadName] = {'classObj': modClass, 'moduleObj': modObj}
      # End of loading of 'loadName' module

    # A-OK :)
    self.__modules[modName] = self.__loadedModules[loadName].copy()
    # keep the name of the real code module
    self.__modules[modName]['modName'] = modName
    self.__modules[modName]['loadName'] = loadName
    gLogger.notice("Loaded module %s" % modName)

    return S_OK()
