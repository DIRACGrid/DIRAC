#!/usr/bin/env python
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
    Script.parseCommandLine(ignoreErrors=False)
    from DIRAC.WorkloadManagementSystem.Client.VirtualMachineCLI import VirtualMachineCLI

    cli = VirtualMachineCLI(vo="enmr.eu")
    cli.cmdloop()


if __name__ == "__main__":
    main()
