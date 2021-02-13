#!/usr/bin/env python
########################################################################
# File :    dirac-admin-service-ports
# Author :  Stuart Paterson
########################################################################
"""
Print the service ports for the specified setup

Usage:
  dirac-admin-service-ports [options] ... [Setup]

Arguments:
  Setup:    Name of the setup

Example:
  $ dirac-admin-service-ports
  {'Framework/ProxyManager': 9152,
   'Framework/SystemAdministrator': 9162,
   'Framework/UserProfileManager': 9155,
   'WorkloadManagement/JobManager': 9132,
   'WorkloadManagement/PilotManager': 9171,
   'WorkloadManagement/Matcher': 9170,
   'WorkloadManagement/SandboxStore': 9196,
   'WorkloadManagement/WMSAdministrator': 9145}
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  setup = ''
  if args:
    setup = args[0]

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  result = diracAdmin.getServicePorts(setup, printOutput=True)
  if result['OK']:
    DIRAC.exit(0)
  else:
    print(result['Message'])
    DIRAC.exit(2)


if __name__ == "__main__":
  main()
