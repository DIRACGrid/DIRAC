"""Module level filter."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = '$Id$'

from DIRAC.FrameworkSystem.private.standardLogging.LogLevels import LogLevels

DOT = '.'
LEVEL = '__level__'


class ModuleFilter(object):
  """Filter module to set loglevel per module.

  ::

    Resources
    {
      LogBackends
      {
        <backend>
        {
           Filter = MyModuleFilter
        }
      }
      LogFilters
      {
         MyModuleFilter
         {
            Plugin = ModuleFilter
            dirac = ERROR
            dirac.Subprocess = DEBUG
            dirac.ILCDIRAC.Interfaces.API.NewInterface = INFO
         }
      }
    }

  This results in all debug messages from the Subprocess module to be printed, but only errors from
  the rest of dirac.  And INFO from a module in an extension. For this to work the global log level
  needs to be DEBUG (e.g., -ddd for commands)

  """

  def __init__(self, optionDict):
    """Contruct the object, set the base LogLevel to DEBUG, and parse the options."""
    self._configDict = {'dirac': {LEVEL: LogLevels.DEBUG}}
    optionDict.pop('Plugin', None)
    for module, level in optionDict.items():
      self.__fillConfig(self._configDict, module.split(DOT), LogLevels.getLevelValue(level))

  def __fillConfig(self, baseDict, modules, level):
    """Fill the config Dict with the module information.

    Recursivly fill the dictionary for each submodule with given level.
    If intermediate modules are not set, use DEBUG

    :param dict baseDict: dictionary for current submodules
    :param list modules: list of submodule paths to be set
    :parma int levelno: level to be set for given module
    """
    if len(modules) == 1:  # at the end for this setting
      if modules[0] in baseDict:
        baseDict[modules[0]][LEVEL] = level
      else:
        baseDict[modules[0]] = {LEVEL: level}
      return None
    module0 = modules[0]
    modules = modules[1:]
    if module0 not in baseDict:
      # DEBUG is the default loglevel for the root logger
      baseDict[module0] = {LEVEL: LogLevels.DEBUG}
    return self.__fillConfig(baseDict[module0], modules, level)

  def __filter(self, baseDict, hierarchy, levelno):
    """Check if sublevels are defined, or return highest set level.

    Recursively go through the configured levels, returns comparison with deepest match

    :param dict baseDict: dictionary with information starting at current level
    :param list hierarchy: list of module hierarchy
    :param int levelno: integer log level of given record
    :returns: boolean for filter value
    """
    if not hierarchy:
      return baseDict.get(LEVEL, -1) <= levelno
    if hierarchy[0] in baseDict:
      return self.__filter(baseDict[hierarchy[0]], hierarchy[1:], levelno)
    return baseDict.get(LEVEL, -1) <= levelno

  def filter(self, record):
    """Filter records based on the path of the logger."""
    return self.__filter(self._configDict, record.name.split(DOT), record.levelno)
