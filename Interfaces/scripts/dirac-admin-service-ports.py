#!/usr/bin/env python
########################################################################
# File :    dirac-admin-service-ports
# Author :  Stuart Paterson
########################################################################
"""
Print the service ports for the specified setup

Usage:

  dirac-admin-service-ports [option|cfgfile] ... [Setup]

Arguments:

  Setup:    Name of the setup

Example:

  $ dirac-admin-service-ports
  {'Accounting/DataStore': 9133,
   'Accounting/ReportGenerator': 9134,
   'DataManagement/FileCatalog': 9197,
   'DataManagement/StorageElement': 9148,
   'DataManagement/StorageElementProxy': 9149,
   'Framework/BundleDelivery': 9158,
   'Framework/Monitoring': 9142,
   'Framework/Notification': 9154,
   'Framework/Plotting': 9157,
   'Framework/ProxyManager': 9152,
   'Framework/SecurityLogging': 9153,
   'Framework/SystemAdministrator': 9162,
   'Framework/SystemLogging': 9141,
   'Framework/SystemLoggingReport': 9144,
   'Framework/UserProfileManager': 9155,
   'RequestManagement/RequestManager': 9143,
   'WorkloadManagement/JobManager': 9132,
   'WorkloadManagement/JobMonitoring': 9130,
   'WorkloadManagement/JobStateUpdate': 9136,
   'WorkloadManagement/PilotManager': 9171,
   'WorkloadManagement/Matcher': 9170,
   'WorkloadManagement/SandboxStore': 9196,
   'WorkloadManagement/WMSAdministrator': 9145}
"""
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
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
