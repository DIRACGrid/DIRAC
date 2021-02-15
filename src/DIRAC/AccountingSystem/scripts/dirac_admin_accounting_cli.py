#!/usr/bin/env python
########################################################################
# File :   dirac-admin-accounting-cli
# Author : Adria Casajus
########################################################################
"""
Command line administrative interface to DIRAC Accounting DataStore Service

Usage:
  dirac-admin-accounting-cli [options] ...
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  Script.localCfg.addDefaultEntry("LogLevel", "info")
  Script.parseCommandLine()

  from DIRAC.AccountingSystem.Client.AccountingCLI import AccountingCLI

  acli = AccountingCLI()
  acli.start()


if __name__ == "__main__":
  main()
