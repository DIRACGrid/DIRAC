#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = "$Id$"


@DIRACScript()
def main():
  host = None
  Script.registerSwitch("H:", "host=", "   Target host")
  Script.parseCommandLine(ignoreErrors=False)
  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() == "h" or switch[0].lower() == "host":
      host = switch[1]

  from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import SystemAdministratorClientCLI

  cli = SystemAdministratorClientCLI(host)
  cli.cmdloop()


if __name__ == "__main__":
  main()
