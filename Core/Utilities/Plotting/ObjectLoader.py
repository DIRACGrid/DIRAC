"""
It is used to load classes from a specific system.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import re
import os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals


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

  parentModuleList = ["%sDIRAC" % ext for ext in CSGlobals.getCSExtensions()] + ['DIRAC']
  objectsToLoad = {}
  # Find which object files match
  for parentModule in parentModuleList:
    objDir = os.path.join(DIRAC.rootPath, parentModule, *pathList)
    if not os.path.isdir(objDir):
      continue
    for objFile in os.listdir(objDir):
      if reFilter.match(objFile):
        pythonClassName = objFile[:-3]
        if pythonClassName not in objectsToLoad:
          gLogger.info("Adding to load queue %s/%s/%s" % (parentModule, path, pythonClassName))
          objectsToLoad[pythonClassName] = parentModule

  # Load them!
  loadedObjects = {}

  for pythonClassName in objectsToLoad:
    parentModule = objectsToLoad[pythonClassName]
    try:
      # Where parentModule can be DIRAC, pathList is something like [ "AccountingSystem", "Client", "Types" ]
      # And the python class name is.. well, the python class name
      objPythonPath = "%s.%s.%s" % (parentModule, ".".join(pathList), pythonClassName)
      objModule = __import__(objPythonPath,
                             globals(),
                             locals(), pythonClassName)
      objClass = getattr(objModule, pythonClassName)
    except Exception as e:
      gLogger.error("Can't load type", "%s/%s: %s" % (parentModule, pythonClassName, str(e)))
      continue
    if parentClass == objClass:
      continue
    if parentClass and not issubclass(objClass, parentClass):
      gLogger.warn("%s is not a subclass of %s. Skipping" % (objClass, parentClass))
      continue
    gLogger.info("Loaded %s" % objPythonPath)
    loadedObjects[pythonClassName] = objClass

  return loadedObjects
