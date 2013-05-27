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
    DIRAC.exit( -1 )

  dbSummary = dbSummary["Value"]
  if not dbSummary:
    DIRAC.gLogger.info( "ReqDB is empty!" )
    DIRAC.exit( 0 )

  reqs = dbSummary.get( "Request", {} )
  ops = dbSummary.get( "Operation", {} )
  fs = dbSummary.get( "File", {} )

  DIRAC.gLogger.always( "Requests:" )
  for reqState, reqCount in sorted( reqs.items() ):
    DIRAC.gLogger.always( "- %s Requests with '%s' status" % ( reqCount, reqState ) )

  DIRAC.gLogger.always( "Operations:" )
  for opType, opDict in sorted( ops.items() ):
    for opState, opCount in sorted( opDict.items() ):
      DIRAC.gLogger.always( "  %s '%s' Operations with '%s' status" % ( opCount, opType, opState ) )
  DIRAC.gLogger.always( "Files:" )
  for fState, fCount in sorted( fs.items() ):
    DIRAC.gLogger.always( "    %s Files with '%s' status" % ( fCount, fState ) )

  DIRAC.exit( 0 )


