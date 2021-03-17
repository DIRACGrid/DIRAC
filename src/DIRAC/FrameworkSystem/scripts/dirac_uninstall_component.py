#!/usr/bin/env python
"""
Uninstallation of a DIRAC component
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import socket

from DIRAC import gLogger, S_OK, exit as DIRACexit
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient


class UninstallComponent(DIRACScript):

  def initParameters(self):
    self.force = False

  def setForce(self, opVal):
    self.force = True
    return S_OK()


@UninstallComponent()
def main(self):
  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  gComponentInstaller.exitOnError = True

  self.registerSwitch("f", "force", "Forces the removal of the logs", self.setForce)
  self.registerArgument(("System/Component: Full component name (ie: WorkloadManagement/Matcher)",
                         "System:           Name of the DIRAC system (ie: WorkloadManagement)"))
  self.registerArgument(" Component:        Name of the DIRAC service (ie: Matcher)", mandatory=False)
  _, args = self.parseCommandLine()

  if len(args) == 1:
    args = args[0].split('/')

  if len(args) < 2:
    self.showHelp(exitCode=1)

  system = args[0]
  component = args[1]

  monitoringClient = ComponentMonitoringClient()
  result = monitoringClient.getInstallations({'Instance': component, 'UnInstallationTime': None},
                                             {'System': system},
                                             {'HostName': socket.getfqdn()}, True)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  if len(result['Value']) < 1:
    gLogger.warn('Given component does not exist')
    DIRACexit(1)
  if len(result['Value']) > 1:
    gLogger.error('Too many components match')
    DIRACexit(1)

  removeLogs = False
  if force:
    removeLogs = True
  else:
    if result['Value'][0]['Component']['Type'] in gComponentInstaller.componentTypes:
      result = promptUser('Remove logs?', ['y', 'n'], 'n')
      if result['OK']:
        removeLogs = result['Value'] == 'y'
      else:
        gLogger.error(result['Message'])
        DIRACexit(1)

  result = gComponentInstaller.uninstallComponent(system, component, removeLogs)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)

  result = MonitoringUtilities.monitorUninstallation(system, component)
  if not result['OK']:
    gLogger.error(result['Message'])
    DIRACexit(1)
  gLogger.notice('Successfully uninstalled component %s/%s' % (system, component))
  DIRACexit()


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
