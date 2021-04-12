""" This is the guy that you should use when you develop a script that interacts with DIRAC

    And don't forget to call parseCommandLine()
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys
import os.path
import inspect

from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors

localCfg = LocalConfiguration()

caller = inspect.currentframe().f_back.f_globals['__name__']


# There are several ways to call DIRAC scripts:
# * dirac-do-something
# * dirac-do-something.py
# * python dirac-do-something.py
# * pytest test-dirac-do-something.py
# * pytest --option test-dirac-do-something.py
# * pytest test-dirac-do-something.py::class::method
# The following lines attempt to keep only what is necessary to DIRAC, leaving the rest to pytest or whatever is before

i = 0
if caller != '__main__':
  # This is for the case when, for example, you run a DIRAC script from within pytest
  gLogger.debug("Called from module", caller)
  # Loop over until one of the argument is the caller, that is the DIRAC script
  for arg in sys.argv:
    # if the form is "pytest test-dirac-do-something.py::class::method", we
    # need to isolate the test-dirac-do-something.py
    arg = arg.split('::')[0]
    if os.path.basename(arg).replace('.py', '') == caller.split('.')[-1]:
      break
    i += 1

# If we reached the end, assume the caller is the first argument
i = 0 if i == len(sys.argv) else i
# Same thing here, get rid of the pytest specific class:meth options
scriptName = os.path.basename(sys.argv[i].split('::')[0]).replace('.py', '')
# The first argument DIRAC should parse is the next one
localCfg.firstOptionIndex = i + 1

gIsAlreadySetUsageMsg = False
gIsAlreadyInitialized = False


def parseCommandLine(script=False, ignoreErrors=False, initializeMonitor=False):
  global gIsAlreadySetUsageMsg, gIsAlreadyInitialized

  # Read and parse the script __doc__ to create a draft help message
  if not gIsAlreadySetUsageMsg:
    try:
      localCfg.setUsageMessage(inspect.currentframe().f_back.f_globals['__doc__'])
    except KeyError:
      pass
    gIsAlreadySetUsageMsg = True

  if gIsAlreadyInitialized:
    return False
  gLogger.showHeaders(False)

  return initialize(script, ignoreErrors, initializeMonitor, True)


def initialize(script=False, ignoreErrors=False, initializeMonitor=False, enableCommandLine=False):
  global scriptName, gIsAlreadyInitialized

  # Please do not call initialize in every file
  if gIsAlreadyInitialized:
    return False
  gIsAlreadyInitialized = True

  userDisabled = not localCfg.isCSEnabled()
  if not userDisabled:
    localCfg.disableCS()

  if not enableCommandLine:
    localCfg.disableParsingCommandLine()

  if script:
    scriptName = script
  localCfg.setConfigurationForScript(scriptName)

  if not ignoreErrors:
    localCfg.addMandatoryEntry("/DIRAC/Setup")
  resultDict = localCfg.loadUserData()
  if not ignoreErrors and not resultDict['OK']:
    gLogger.error("There were errors when loading configuration", resultDict['Message'])
    sys.exit(1)

  if not userDisabled:
    localCfg.enableCS()

  if initializeMonitor:
    gMonitor.setComponentType(gMonitor.COMPONENT_SCRIPT)
    gMonitor.setComponentName(scriptName)
    gMonitor.setComponentLocation("script")
    gMonitor.initialize()
  else:
    gMonitor.disable()
  includeExtensionErrors()

  return True


def registerSwitch(showKey, longKey, helpString, callback=False):
  localCfg.registerCmdOpt(showKey, longKey, helpString, callback)


def getPositionalArgs():
  return localCfg.getPositionalArguments()


def getExtraCLICFGFiles():
  return localCfg.getExtraCLICFGFiles()


def getUnprocessedSwitches():
  return localCfg.getUnprocessedSwitches()


def addDefaultOptionValue(option, value):
  localCfg.addDefaultEntry(option, value)


def setUsageMessage(usageMessage):
  global gIsAlreadySetUsageMsg
  gIsAlreadySetUsageMsg = True
  try:
    localCfg.setUsageMessage(inspect.currentframe().f_back.f_globals['__doc__'])
  except KeyError:
    pass
  localCfg.setUsageMessage(usageMessage)


def disableCS():
  localCfg.disableCS()


def enableCS():
  return localCfg.enableCS()


showHelp = localCfg.showHelp
