#!/bin/env python
""" monitor FTSDB content """

__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile]' % Script.scriptName ] ) )
from operator import itemgetter

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger, gConfig

  from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
  ftsClient = FTSClient()

  ret = ftsClient.getDBSummary()
  if not ret["OK"]:
    gLogger.error( ret["Message"] )
    DIRAC.exit( -1 )
  ret = ret["Value"]

  ic = 1

  ftsSites = ret.get( "FTSSite", None )
  if ftsSites:
    gLogger.always( "[%d] FTSSites:" % ic )
    ic += 1
    for ftsSite in ftsSites:
      gLogger.always( "- %-20s (%s)" % ( ftsSite["Name"], ftsSite["FTSServer"] ) )

  ftsJobs = ret.get( "FTSJob", None )
  if ftsJobs:
    gLogger.always( "[%d] FTSJobs:" % ic )
    ic += 1
    for status, count in sorted( ftsJobs.items() ):
       gLogger.always( "- '%s' %s" % ( status, count ) )

  ftsFiles = ret.get( "FTSFile", None )
  if ftsFiles:
    gLogger.always( "[%d] FTSFiles:" % ic )
    ic += 1
    for status, count in sorted( ftsFiles.items() ):
      gLogger.always( "- '%s' %s" % ( status, count ) )

  ftsHistory = ret.get( "FTSHistory", None )
  if ftsHistory:
    gLogger.info( "[%d] Last hour transfer history" % ic )
    ic += 1
    keys = ( "SourceSE", "TargetSE", "Status", "FTSJobs", "Files", "Size" )
    gLogger.always( "%-20s %-20s %20s %20s %20s %20s" % keys )
    for view in sorted( ftsHistory, key = itemgetter( "SourceSE" ) ):
      vals = tuple( [ view.get( key, "" ) for key in keys ] )
      gLogger.always( "%-20s %-20s %20s %20s %20s %20s" % vals )

  DIRAC.exit( -1 )
