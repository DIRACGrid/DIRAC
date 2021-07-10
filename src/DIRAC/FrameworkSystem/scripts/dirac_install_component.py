#!/usr/bin/env python
"""
Do the initial installation and configuration of a DIRAC component
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import exit as DIRACexit
from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities

__RCSID__ = "$Id$"


class Params(object):
  overwrite = False
  module = ''
  specialOptions = {}

  def setOverwrite(self, opVal):
    self.overwrite = True
    return S_OK()

  def setModule(self, optVal):
    self.specialOptions['Module'] = optVal
    self.module = optVal
    return S_OK()

  def setSpecialOption(self, optVal):
    option, value = optVal.split('=')
    self.specialOptions[option] = value
    return S_OK()


@DIRACScript()
def main(self):
  params = Params()

  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  gComponentInstaller.exitOnError = True

  self.registerSwitch("w", "overwrite", "Overwrite the configuration in the global CS", params.setOverwrite)
  self.registerSwitch("m:", "module=", "Python module name for the component code", params.setModule)
  self.registerSwitch("p:", "parameter=", "Special component option ", params.setSpecialOption)
  # Registering arguments will automatically add their description to the help menu
  self.registerArgument(("System/Component: Full component name (ie: WorkloadManagement/Matcher)",
                         "System:           Name of the DIRAC system (ie: WorkloadManagement)"))
  self.registerArgument(" Component:        Name of the DIRAC service (ie: Matcher)", mandatory=False)

  self.parseCommandLine()
  args = self.getPositionalArgs()

  if len(args) == 1:
    args = args[0].split('/')

  if len(args) != 2:
    self.showHelp(exitCode=1)

  system = args[0]
  component = args[1]
  compOrMod = params.module or component

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

  if params.module:
    result = gComponentInstaller.addDefaultOptionsToCS(gConfig, cType, system, params.module,
                                                       extensionsByPriority(),
                                                       overwrite=params.overwrite)
    result = gComponentInstaller.addDefaultOptionsToCS(gConfig, cType, system, component,
                                                       extensionsByPriority(),
                                                       specialOptions=params.specialOptions,
                                                       overwrite=params.overwrite,
                                                       addDefaultOptions=False)
  else:
    result = gComponentInstaller.addDefaultOptionsToCS(gConfig, cType, system, component,
                                                       extensionsByPriority(),
                                                       specialOptions=params.specialOptions,
                                                       overwrite=params.overwrite)

  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  result = gComponentInstaller.installComponent(cType, system, component, extensionsByPriority(), params.module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  gLogger.notice('Successfully installed component %s in %s system, now setting it up' % (component, system))
  result = gComponentInstaller.setupComponent(cType, system, component, extensionsByPriority(), params.module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  if component == 'ComponentMonitoring':
    result = MonitoringUtilities.monitorInstallation('DB', system, 'InstalledComponentsDB')
    if not result['OK']:
      gLogger.error(result['Message'])
      DIRACexit(1)
  result = MonitoringUtilities.monitorInstallation(cType, system, component, params.module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  gLogger.notice('Successfully completed the installation of %s/%s' % (system, component))
  DIRACexit()


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
