#!/usr/bin/env python
"""
Uninstallation of a DIRAC component

Usage:
  dirac-uninstall-component [options] ... System Component|System/Component

Arguments:
  System:  Name of the DIRAC system (ie: WorkloadManagement)
  Component: Name of the DIRAC component (ie: Matcher)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import socket

from DIRAC import exit as DIRACexit
from DIRAC import gLogger, S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient

__RCSID__ = "$Id$"

force = False


def setForce(opVal):
  global force
  force = True
  return S_OK()


@DIRACScript()
def main():
  global force

  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  gComponentInstaller.exitOnError = True

  Script.registerSwitch("f", "force", "Forces the removal of the logs", setForce)
  Script.parseCommandLine()
  args = Script.getPositionalArgs()

  if len(args) == 1:
    args = args[0].split('/')

  if len(args) < 2:
    Script.showHelp(exitCode=1)

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
  main()
