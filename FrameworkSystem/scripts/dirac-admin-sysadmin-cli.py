#!/usr/bin/env python
"""
Usage:

  dirac-admin-sysadmin-cli.py (<options>|<cfgFile>)*

Example:

  $ dirac-admin-sysadmin-cli --host dirac.in2p3.fr
  DIRAC Root Path = /afs/in2p3.fr/home/h/hamar/DIRAC-v5r12
  dirac.in2p3.fr >
"""
from DIRAC.Core.Base import Script

__RCSID__ = "$Id$"
Script.setUsageMessage(__doc__)

host = None
Script.registerSwitch("H:", "host=", "   Target host")
Script.parseCommandLine(ignoreErrors=False)
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "h" or switch[0].lower() == "host":
    host = switch[1]

from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import SystemAdministratorClientCLI

cli = SystemAdministratorClientCLI(host)
cli.cmdloop()
