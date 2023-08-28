#!/usr/bin/env python
########################################################################
# File :   dirac_admin_accounting_cli
# Author : Adria Casajus
########################################################################
"""
Command line administrative interface to DIRAC Accounting DataStore Service
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.localCfg.addDefaultEntry("LogLevel", "info")
    Script.parseCommandLine()

    from DIRAC.AccountingSystem.Client.AccountingCLI import AccountingCLI

    acli = AccountingCLI()
    acli.start()


if __name__ == "__main__":
    main()
