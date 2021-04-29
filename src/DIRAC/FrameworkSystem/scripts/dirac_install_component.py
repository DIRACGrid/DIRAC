#!/usr/bin/env python
"""
Do the initial installation and configuration of a DIRAC component

Usage:
  dirac-install-component [options] ... System Component|System/Component

Arguments:
  System:  Name of the DIRAC system (ie: WorkloadManagement)
  Service: Name of the DIRAC component (ie: Matcher)
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import exit as DIRACexit
from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities

__RCSID__ = "$Id$"

overwrite = False
module = ''
specialOptions = {}


def setOverwrite(opVal):
  global overwrite
  overwrite = True
  return S_OK()


def setModule(optVal):
  global specialOptions, module
  specialOptions['Module'] = optVal
  module = optVal
  return S_OK()


def setSpecialOption(optVal):
  global specialOptions
  option, value = optVal.split('=')
  specialOptions[option] = value
  return S_OK()


@DIRACScript()
def main():
  global overwrite
  global specialOptions
  global module
  global specialOptions

  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  gComponentInstaller.exitOnError = True

  Script.registerSwitch("w", "overwrite", "Overwrite the configuration in the global CS", setOverwrite)
  Script.registerSwitch("m:", "module=", "Python module name for the component code", setModule)
  Script.registerSwitch("p:", "parameter=", "Special component option ", setSpecialOption)
  Script.parseCommandLine()
  args = Script.getPositionalArgs()

  if len(args) == 1:
    args = args[0].split('/')

  if len(args) != 2:
    Script.showHelp(exitCode=1)

  system = args[0]
  component = args[1]
  compOrMod = module or component

  result = gComponentInstaller.getSoftwareComponents(extensionsByPriority())
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  availableComponents = result['Value']

  for compType in availableComponents:
    if system in availableComponents[compType] and compOrMod in availableComponents[compType][system]:
      cType = compType[:-1].lower()
      break
  else:
    gLogger.error('Component %s/%s is not available for installation' % (system, component))
    DIRACexit(1)

  if module:
    result = gComponentInstaller.addDefaultOptionsToCS(gConfig, cType, system, module,
                                                       extensionsByPriority(),
                                                       overwrite=overwrite)
    result = gComponentInstaller.addDefaultOptionsToCS(gConfig, cType, system, component,
                                                       extensionsByPriority(),
                                                       specialOptions=specialOptions,
                                                       overwrite=overwrite,
                                                       addDefaultOptions=False)
  else:
    result = gComponentInstaller.addDefaultOptionsToCS(gConfig, cType, system, component,
                                                       extensionsByPriority(),
                                                       specialOptions=specialOptions,
                                                       overwrite=overwrite)

  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  result = gComponentInstaller.installComponent(cType, system, component, extensionsByPriority(), module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  gLogger.notice('Successfully installed component %s in %s system, now setting it up' % (component, system))
  result = gComponentInstaller.setupComponent(cType, system, component, extensionsByPriority(), module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  if component == 'ComponentMonitoring':
    result = MonitoringUtilities.monitorInstallation('DB', system, 'InstalledComponentsDB')
    if not result['OK']:
      gLogger.error(result['Message'])
      DIRACexit(1)
  result = MonitoringUtilities.monitorInstallation(cType, system, component, module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  gLogger.notice('Successfully completed the installation of %s/%s' % (system, component))
  DIRACexit()


if __name__ == "__main__":
  main()
