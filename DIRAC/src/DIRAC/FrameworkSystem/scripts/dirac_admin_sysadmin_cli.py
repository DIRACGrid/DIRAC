#!/usr/bin/env python
"""
System administrator client.

Example:
  $ dirac-admin-sysadmin-cli --host dirac.in2p3.fr
  DIRAC Root Path = /afs/in2p3.fr/home/h/hamar/DIRAC-v5r12
  dirac.in2p3.fr >
"""
from DIRAC.Core.Base.Script import Script


@Script()
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
