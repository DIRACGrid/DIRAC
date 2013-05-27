#!/bin/env python  
""" show ReqDB summary """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile]' % Script.scriptName ] ) )

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC

  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  reqClient = ReqClient()
  
  dbSummary = reqClient.getDBSummary() 
  if not dbSummary["OK"]:
    DIRAC.gLogger.error( dbSumamry["Message"] )
    DIRAC.exit(-1)

  dbSumamry = dbSummary["Value"]
  if not dbSumamry:
    DIRAC.gLogger.info("ReqDB is empty!")
    DIRAC.exit(0)

  print dbSummary


