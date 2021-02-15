#!/usr/bin/env python
"""
Status of DIRAC components using runsvstat utility

Usage:
  dirac-status-component [options] ... [system [service|agent]]

Arguments:
  system:        Name of the system for the component (default *: all)
  service|agent: Name of the particular component (default *: all)

Example:
  $ dirac-status-component
  DIRAC Root Path = /vo/dirac/versions/Lyon-HEAD-1296215324
                                           Name : Runit    Uptime    PID
            WorkloadManagement_PilotStatusAgent : Run        4029     1697
             WorkloadManagement_JobHistoryAgent : Run        4029     167
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.disableCS()
  Script.parseCommandLine()
  args = Script.getPositionalArgs()

  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

  __RCSID__ = "$Id$"

  if len(args) > 2:
    Script.showHelp(exitCode=1)

  system = '*'
  component = '*'
  if len(args) > 0:
    system = args[0]
  if system != '*':
    if len(args) > 1:
      component = args[1]
  #
  gComponentInstaller.exitOnError = True
  #
  result = gComponentInstaller.getStartupComponentStatus([system, component])
  if not result['OK']:
    print('ERROR:', result['Message'])
    exit(-1)

  gComponentInstaller.printStartupStatus(result['Value'])


if __name__ == "__main__":
  main()
