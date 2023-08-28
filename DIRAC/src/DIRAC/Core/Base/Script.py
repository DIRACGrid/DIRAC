""" DIRAC script.

Usage::

    from DIRAC.Core.Base.Script import Script

    @Script()
    def main():
        Script.registerArgument(("Name:  user name", "DN: user DN"))
        Script.parseCommandLine()
        ...
"""
import sys
import os.path
import functools

import importlib_metadata as metadata

from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration

from DIRAC.Core.Utilities.Extensions import entrypointToExtension, extensionsByPriority


class Script:
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
        underlying function. The priorities will be applied from the
        dirac.extension_metadata entry_point.
        """
        # If func is provided then the decorator is being applied to a function
        if func is not None:
            self._func = func
            # Find the name of the command and its documentation
            Script.localCfg.setUsageMessage(func.__globals__["__doc__"])
            Script.scriptName = os.path.basename(func.__globals__["__file__"])[:-3].replace("_", "-")
            return functools.wraps(func)(self)

        # Iterate through all known entry_points looking for self.scriptName
        matches = [ep for ep in metadata.entry_points(group="console_scripts") if ep.name == self.scriptName]
        if not matches:
            raise NotImplementedError("Something is very wrong")

        # Call the entry_point from the extension with the highest priority
        rankedExtensions = extensionsByPriority()
        entrypoint = min(
            matches,
            key=lambda e: rankedExtensions.index(entrypointToExtension(e)),
        )
        entrypointFunc = entrypoint.load()

        # Check if entrypoint is Script
        if not isinstance(entrypointFunc, Script):
            raise ImportError(
                "Invalid dirac- console_scripts entry_point: "
                + repr(entrypoint)
                + "\n"
                + "All dirac- console_scripts should be wrapped in the Script "
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


def parseCommandLine(*args, **kwargs):
    return Script.parseCommandLine(*args, **kwargs)


def initialize(*args, **kwargs):
    return Script.initialize(*args, **kwargs)


def registerSwitch(*args, **kwargs):
    return Script.registerSwitch(*args, **kwargs)


def registerArgument(*args, **kwargs):
    return Script.registerArgument(*args, **kwargs)


def getPositionalArgs(*args, **kwargs):
    return Script.getPositionalArgs(*args, **kwargs)


def getExtraCLICFGFiles(*args, **kwargs):
    return Script.getExtraCLICFGFiles(*args, **kwargs)


def getUnprocessedSwitches(*args, **kwargs):
    return Script.getUnprocessedSwitches(*args, **kwargs)


def addDefaultOptionValue(*args, **kwargs):
    return Script.addDefaultOptionValue(*args, **kwargs)


def setUsageMessage(*args, **kwargs):
    return Script.setUsageMessage(*args, **kwargs)


def disableCS(*args, **kwargs):
    return Script.disableCS(*args, **kwargs)


def enableCS(*args, **kwargs):
    return Script.enableCS(*args, **kwargs)
