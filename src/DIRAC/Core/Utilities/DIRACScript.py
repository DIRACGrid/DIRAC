""" DIRAC script """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import sys
import os.path
import inspect
import functools
from collections import defaultdict

from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration

if six.PY3:
  from DIRAC.Core.Utilities.Extensions import entrypointToExtension, extensionsByPriority


class DIRACScript(object):
  """Decorator for providing command line executables

  All console-scripts entrypoints in DIRAC and downstream extensions should be
  wrapped in this decorator to allow extensions to override any entry_point.
  """
  def __init__(self):
    """ c'tor
    """
    self.alreadyInitialized = False
    self.scriptName = inspect.currentframe().f_back.f_globals['__name__'].split('.')[-1].replace('_', '-')

    # Parse script __doc__'s to create a draft help message
    self.localCfg = LocalConfiguration()
    self.localCfg.setUsageMessage(inspect.currentframe().f_back.f_globals['__doc__'])

    self.showHelp = self.localCfg.showHelp
    self.initParameters()

  def initParameters(self):
    """ Script initialization """
    pass

  def __call__(self, func=None):
    """Set the wrapped function or call the script

    This function is either called with a decorator or directly to call the
    underlying function. When running with Python 2 the raw function will always
    be called however in Python 3 the priorities will be applied from the
    dirac.extension_metadata entry_point.
    """
    # If func is provided then the decorator is being applied to a function
    if func is not None:
      self._func = func
      return functools.wraps(func)(self)

    # Setuptools based installations aren't supported with Python 2
    if six.PY2:
      return self._func(self)  # pylint: disable=not-callable

    # This is only available in Python 3.8+ so it has to be here for now
    from importlib import metadata  # pylint: disable=no-name-in-module

    # Iterate through all known entry_points looking for DIRACScripts
    matches = defaultdict(list)
    function_name = None
    for entrypoint in metadata.entry_points()['console_scripts']:
      if self.scriptName == entrypoint.name:
        entrypointFunc = entrypoint.load()
        if not isinstance(entrypointFunc, DIRACScript):
          raise ImportError(
              "Invalid dirac- console_scripts entry_point: " + repr(entrypoint) + "\n" +
              "All dirac- console_scripts should be wrapped in the DiracScript " +
              "decorator to ensure extension overlays are applied correctly."
          )
        matches[entrypoint.name].append(entrypoint)
        # If the entrypoint.name is self.scriptName then we've found the currently called function
        function_name = entrypoint.name

    if function_name is None:
      # TODO: This should an error once the integration tests modified to use pip install
      return self._func(self)  # pylint: disable=not-callable
      # raise NotImplementedError("Something is very wrong")

    # Call the entry_point from the extension with the highest priority
    rankedExtensions = extensionsByPriority()
    entrypoint = max(
        matches[function_name],
        key=lambda e: rankedExtensions.index(entrypointToExtension(e)),
    )

    return entrypoint.load()._func(self)

  def parseCommandLine(self, script=False, ignoreErrors=False, initializeMonitor=False):
    """ Parse command line

        :param str script: script name
        :param bool ignoreErrors: ignore errors when loading configuration
        :param bool initializeMonitor: to use monitoring
    """
    if not self.alreadyInitialized:
      gLogger.showHeaders(False)
      self.initialize(script, ignoreErrors, initializeMonitor, True)

    return (self.localCfg.getUnprocessedSwitches(), self.localCfg.getPositionalArguments())

  def initialize(self, script=False, ignoreErrors=False, initializeMonitor=False, enableCommandLine=False):
    """ initialization

        :param str script: script name
        :param bool ignoreErrors: ignore errors when loading configuration
        :param bool initializeMonitor: to use monitoring
        :param bool enableCommandLine: enable parse command line
    """
    # Please do not call initialize in every file
    if self.alreadyInitialized:
      return False
    userDisabled = not self.localCfg.isCSEnabled()
    self.alreadyInitialized = True
    if not userDisabled:
      self.localCfg.disableCS()

    if not enableCommandLine:
      self.localCfg.disableParsingCommandLine()

    if script:
      self.scriptName = script
    self.localCfg.setConfigurationForScript(self.scriptName)

    if not ignoreErrors:
      self.localCfg.addMandatoryEntry("/DIRAC/Setup")
    resultDict = self.localCfg.loadUserData()
    if not ignoreErrors and not resultDict['OK']:
      gLogger.error("There were errors when loading configuration", resultDict['Message'])
      sys.exit(1)
    if not userDisabled:
      self.localCfg.enableCS()
    if initializeMonitor:
      gMonitor.setComponentType(gMonitor.COMPONENT_SCRIPT)
      gMonitor.setComponentName(self.scriptName)
      gMonitor.setComponentLocation("script")
      gMonitor.initialize()
    else:
      gMonitor.disable()
    includeExtensionErrors()
    return True

  def registerSwitches(self, switches):
    """ Register switches

        :param list switches: switches
    """
    for switch in switches:
      self.registerSwitch(*switch)

  def registerSwitch(self, showKey, longKey, helpString, callback=False):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.registerCmdOpt`. """
    self.localCfg.registerCmdOpt(showKey, longKey, helpString, callback)

  def registerArgument(self, description, mandatory=True, values=None, default=None):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.registerCmdArg`. """
    self.localCfg.registerCmdArg(description, mandatory, values, default)

  def getPositionalArgs(self, group=False):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.getPositionalArguments`. """
    return self.localCfg.getPositionalArguments(group)

  def getExtraCLICFGFiles(self):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.getExtraCLICFGFiles`. """
    return self.localCfg.getExtraCLICFGFiles()

  def getUnprocessedSwitches(self):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.getUnprocessedSwitches`. """
    return self.localCfg.getUnprocessedSwitches()

  def addDefaultOptionValue(self, option, value):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.addDefaultEntry`. """
    self.localCfg.addDefaultEntry(option, value)

  def setUsageMessage(self, usageMessage):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.setUsageMessage`. """
    self.localCfg.setUsageMessage(usageMessage)

  def disableCS(self):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.disableCS`. """
    self.localCfg.disableCS()

  def enableCS(self):
    """ See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.enableCS`. """
    return self.localCfg.enableCS()
