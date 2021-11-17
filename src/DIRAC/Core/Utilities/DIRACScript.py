""" DIRAC script """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import functools
import os.path
import sys
import six

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

    scriptName = None
    alreadyInitialized = False
    localCfg = LocalConfiguration()

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
            # Find the name of the command and its documentation
            DIRACScript.localCfg.setUsageMessage(func.__globals__["__doc__"])
            DIRACScript.scriptName = os.path.basename(func.__globals__["__file__"])[:-3].replace("_", "-")
            return functools.wraps(func)(self)

        # Setuptools based installations aren't supported with Python 2
        if six.PY2:
            return self._func()  # pylint: disable=not-callable

        # This is only available in Python 3.8+ so it has to be here for now
        from importlib import metadata  # pylint: disable=no-name-in-module

        # Iterate through all known entry_points looking for self.scriptName
        matches = [ep for ep in metadata.entry_points()["console_scripts"] if ep.name == self.scriptName]
        if not matches:
            # TODO: This should an error once the integration tests modified to use pip install
            return self._func()  # pylint: disable=not-callable
            # raise NotImplementedError("Something is very wrong")

        # Call the entry_point from the extension with the highest priority
        rankedExtensions = extensionsByPriority()
        entrypoint = min(
            matches,
            key=lambda e: rankedExtensions.index(entrypointToExtension(e)),
        )
        entrypointFunc = entrypoint.load()

        # Check if entrypoint is DIRACScript
        if not isinstance(entrypointFunc, DIRACScript):
            raise ImportError(
                "Invalid dirac- console_scripts entry_point: "
                + repr(entrypoint)
                + "\n"
                + "All dirac- console_scripts should be wrapped in the DiracScript "
                + "decorator to ensure extension overlays are applied correctly."
            )
        return entrypointFunc._func()

    @classmethod
    def parseCommandLine(cls, script=False, ignoreErrors=False, initializeMonitor=False):
        """Parse command line

        :param str script: script name
        :param bool ignoreErrors: ignore errors when loading configuration
        :param bool initializeMonitor: to use monitoring
        """
        if not cls.alreadyInitialized:
            gLogger.showHeaders(False)
            cls.initialize(script, ignoreErrors, initializeMonitor, True)

        return (cls.localCfg.getUnprocessedSwitches(), cls.localCfg.getPositionalArguments())

    @classmethod
    def initialize(cls, script=False, ignoreErrors=False, initializeMonitor=False, enableCommandLine=False):
        """initialization

        :param str script: script name
        :param bool ignoreErrors: ignore errors when loading configuration
        :param bool initializeMonitor: to use monitoring
        :param bool enableCommandLine: enable parse command line
        """
        # Please do not call initialize in every file
        if cls.alreadyInitialized:
            return False
        userDisabled = not cls.localCfg.isCSEnabled()
        cls.alreadyInitialized = True
        if not userDisabled:
            cls.localCfg.disableCS()

        if not enableCommandLine:
            cls.localCfg.disableParsingCommandLine()

        if script:
            cls.scriptName = script
        cls.localCfg.setConfigurationForScript(cls.scriptName)

        if not ignoreErrors:
            cls.localCfg.addMandatoryEntry("/DIRAC/Setup")
        resultDict = cls.localCfg.loadUserData()
        if not ignoreErrors and not resultDict["OK"]:
            gLogger.error("There were errors when loading configuration", resultDict["Message"])
            sys.exit(1)
        if not userDisabled:
            cls.localCfg.enableCS()
        if initializeMonitor:
            gMonitor.setComponentType(gMonitor.COMPONENT_SCRIPT)
            gMonitor.setComponentName(cls.scriptName)
            gMonitor.setComponentLocation("script")
            gMonitor.initialize()
        else:
            gMonitor.disable()
        includeExtensionErrors()
        return True

    @classmethod
    def showHelp(cls, dummy=False, exitCode=0):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.showHelp`."""
        return cls.localCfg.showHelp(dummy=dummy, exitCode=exitCode)

    @classmethod
    def registerSwitches(cls, switches):
        """Register switches

        :param list switches: switches
        """
        for switch in switches:
            cls.registerSwitch(*switch)

    @classmethod
    def registerSwitch(cls, showKey, longKey, helpString, callback=False):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.registerCmdOpt`."""
        cls.localCfg.registerCmdOpt(showKey, longKey, helpString, callback)

    @classmethod
    def registerArgument(cls, description, mandatory=True, values=None, default=None):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.registerCmdArg`."""
        cls.localCfg.registerCmdArg(description, mandatory, values, default)

    @classmethod
    def getPositionalArgs(cls, group=False):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.getPositionalArguments`."""
        return cls.localCfg.getPositionalArguments(group)

    @classmethod
    def getExtraCLICFGFiles(cls):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.getExtraCLICFGFiles`."""
        return cls.localCfg.getExtraCLICFGFiles()

    @classmethod
    def getUnprocessedSwitches(cls):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.getUnprocessedSwitches`."""
        return cls.localCfg.getUnprocessedSwitches()

    @classmethod
    def addDefaultOptionValue(cls, option, value):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.addDefaultEntry`."""
        cls.localCfg.addDefaultEntry(option, value)

    @classmethod
    def setUsageMessage(cls, usageMessage):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.setUsageMessage`."""
        cls.localCfg.setUsageMessage(usageMessage)

    @classmethod
    def disableCS(cls):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.disableCS`."""
        cls.localCfg.disableCS()

    @classmethod
    def enableCS(cls):
        """See :func:`~DIRAC.ConfigurationSystem.Client.LocalConfiguration.LocalConfiguration.enableCS`."""
        return cls.localCfg.enableCS()
