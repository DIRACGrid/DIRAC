#!/bin/env python

""" monitor FTSDB content """

__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile]' % Script.scriptName ] ) )

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger, gConfig

  from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
  ftsClient = FTSClient()

  ret = ftsClient.getDBSummary()
  gLogger.always( ret )


