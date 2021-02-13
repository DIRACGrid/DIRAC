#!/usr/bin/env python
"""
Start DIRAC component using runsvctrl utility

Usage:
  dirac-start-component [options] ... [system [service|agent]]

Arguments:
  system:        Name of the system for the component (default *: all)
  service|agent: Name of the particular component (default *: all)
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.disableCS()
  Script.parseCommandLine()
  args = Script.getPositionalArgs()
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
  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
  #
  gComponentInstaller.exitOnError = True
  #
  result = gComponentInstaller.runsvctrlComponent(system, component, 'u')
  if not result['OK']:
    print('ERROR:', result['Message'])
    exit(-1)

  gComponentInstaller.printStartupStatus(result['Value'])


if __name__ == "__main__":
  main()
