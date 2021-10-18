#!/usr/bin/env python
########################################################################
# File :   dirac_admin_accounting_cli
# Author : Adria Casajus
########################################################################
"""
Command line administrative interface to DIRAC Accounting DataStore Service
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    Script.localCfg.addDefaultEntry("LogLevel", "info")
    Script.parseCommandLine()

    from DIRAC.AccountingSystem.Client.AccountingCLI import AccountingCLI

    acli = AccountingCLI()
    acli.start()


if __name__ == "__main__":
    main()
