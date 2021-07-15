#! /usr/bin/env python
########################################################################
# File :    dirac-admin-ce-info
# Author :  Vladimir Romanovsky
########################################################################
"""
Retrieve Site Associated to a given CE

Example:
  $ dirac-admin-ce-info LCG.IN2P3.fr
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, exit as Dexit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
  # Registering arguments will automatically add their description to the help menu
  Script.registerArgument("CE:  Name of the CE")
  Script.parseCommandLine(ignoreErrors=True)
  # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
  ce = Script.getPositionalArgs(group=True)

  from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping

  res = getCESiteMapping(ce)
  if not res['OK']:
    gLogger.error(res['Message'])
    Dexit(1)
  site = res['Value'][ce]

  res = gConfig.getOptionsDict(cfgPath('Resources', 'Sites', site.split('.')[0], site, 'CEs', ce))
  if not res['OK']:
    gLogger.error(res['Message'])
    Dexit(1)
  gLogger.notice(res['Value'])


if __name__ == "__main__":
  main()
