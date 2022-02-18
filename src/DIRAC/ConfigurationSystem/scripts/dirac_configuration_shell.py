#!/usr/bin/env python
"""
Script that emulates the behaviour of a shell to edit the CS config.
"""
from DIRAC.Core.Base.Script import Script

# Invariants:
# * root does not end with "/" or root is "/"
# * root starts with "/"


@Script()
def main():
    Script.parseCommandLine()
    from DIRAC.ConfigurationSystem.Client.CSShellCLI import CSShellCLI

    shell = CSShellCLI()
    shell.cmdloop()


if __name__ == "__main__":
    main()
