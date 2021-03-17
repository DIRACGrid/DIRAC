#!/usr/bin/env python
########################################################################
# File :   dirac_admin_accounting_cli
# Author : Adria Casajus
########################################################################
"""
Command line administrative interface to DIRAC Accounting DataStore Service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main(self):
  self.localCfg.addDefaultEntry("LogLevel", "info")
  self.parseCommandLine()

  from DIRAC.AccountingSystem.Client.AccountingCLI import AccountingCLI

  acli = AccountingCLI()
  acli.start()


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
