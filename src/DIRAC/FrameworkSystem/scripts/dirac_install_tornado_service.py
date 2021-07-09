#!/usr/bin/env python
"""
Do the initial installation and configuration of a DIRAC service based on tornado
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK, exit as DIRACexit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities


class InstallTornadoService(DIRACScript):

  def initParameters(self):
    self.overwrite = False
    self.module = ''
    self.specialOptions = {}

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


@InstallTornadoService()
def main(self):
  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  gComponentInstaller.exitOnError = True

  self.registerSwitch("w", "overwrite", "Overwrite the configuration in the global CS", self.setOverwrite)
  self.registerSwitch("m:", "module=", "Python module name for the component code", self.setModule)
  self.registerSwitch("p:", "parameter=", "Special component option ", self.setSpecialOption)
  self.registerArgument(("System/Component: Full component name (ie: WorkloadManagement/Matcher)",
                         "System:           Name of the DIRAC system (ie: WorkloadManagement)"))
  self.registerArgument(" Component:        Name of the DIRAC service (ie: Matcher)", mandatory=False)
  self.parseCommandLine()
  args = self.getPositionalArgs()

  if len(args) == 1:
    args = args[0].split('/')

  if len(args) != 2:
    self.showHelp()
    DIRACexit(1)

  system = args[0]
  component = args[1]
  compOrMod = self.module if self.module else component

  result = gComponentInstaller.addDefaultOptionsToCS(gConfig, 'service', system, component,
                                                     extensionsByPriority(),
                                                     specialOptions=self.specialOptions,
                                                     overwrite=self.overwrite)

  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)

  result = gComponentInstaller.addTornadoOptionsToCS(gConfig)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)

  result = gComponentInstaller.installTornado()
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)

  gLogger.notice('Successfully installed component %s in %s system, now setting it up' % (component, system))
  result = gComponentInstaller.setupTornadoService(system, component, extensionsByPriority(), self.module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)

  result = MonitoringUtilities.monitorInstallation('service', system, component, self.module)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  gLogger.notice('Successfully completed the installation of %s/%s' % (system, component))
  DIRACexit()


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
