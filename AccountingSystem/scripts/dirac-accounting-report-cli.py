#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-accounting-report-cli
# Author : Adria Casajus
########################################################################
"""
  Command line interface to DIRAC Accounting ReportGenerator Service.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.localCfg.addDefaultEntry( "LogLevel", "info" )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

from DIRAC.AccountingSystem.Client.ReportCLI import ReportCLI

if __name__=="__main__":
    reli = ReportCLI()
    reli.start()
