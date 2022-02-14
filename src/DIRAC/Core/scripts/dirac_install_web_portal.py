#!/usr/bin/env python
########################################################################
# File :    dirac-install-web-portal
# Author :  Ricardo Graciani
########################################################################
"""
Do the initial installation of a DIRAC Web portal
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.disableCS()
    Script.parseCommandLine()

    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

    gComponentInstaller.exitOnError = True
    gComponentInstaller.setupPortal()


if __name__ == "__main__":
    main()
