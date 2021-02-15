#! /usr/bin/env python
########################################################################
# File :    dirac-admin-ce-info
# Author :  Vladimir Romanovsky
########################################################################
"""
Retrieve Site Associated to a given CE

Usage:
  dirac-admin-ce-info [options] ... CE ...

Arguments:
  CE:       Name of the CE (mandatory)

Example:
  $ dirac-admin-ce-info LCG.IN2P3.fr
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, exit as Dexit
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping

  if len(args) < 1:
    Script.showHelp(exitCode=1)

  res = getCESiteMapping(args[0])
  if not res['OK']:
    gLogger.error(res['Message'])
    Dexit(1)
  site = res['Value'][args[0]]

  res = gConfig.getOptionsDict(cfgPath('Resources', 'Sites', site.split('.')[0], site, 'CEs', args[0]))
  if not res['OK']:
    gLogger.error(res['Message'])
    Dexit(1)
  gLogger.notice(res['Value'])


if __name__ == "__main__":
  main()
