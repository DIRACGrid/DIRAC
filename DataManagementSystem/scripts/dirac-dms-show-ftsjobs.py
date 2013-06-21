#!/bin/env python
""" monitor FTSDB content """

__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] requestID' % Script.scriptName ,
                                     'Argument:',
                                     '  requestID: RequestDB.Request.RequestID' ] ) )
Script.parseCommandLine( ignoreErrors = True )
import DIRAC

if __name__ == "__main__":
  
  args = Script.getPositionalArgs()
  if len( args ) != 1:
    Script.showHelp()
  try:
    requestID = long( args[0] )
  except ValueError:
    DIRAC.gLogger.error( "requestID should be an integer" )
    DIRAC.exit( -1 )


  from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
  from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob

  ftsClient = FTSClient()

  ftsJobs = ftsClient.getFTSJobsForRequest( requestID, list( FTSJob.INITSTATES + FTSJob.TRANSSTATES + FTSJob.FINALSTATES ) )
  if not ftsJobs["OK"]:
    DIRAC.gLogger.error( ftsJobs["Message"] )
    DIRAC.exit(-1)
  ftsJobs = ftsJobs["Value"]
  
  DIRAC.always( "Found %s FTSJobs" % len( ftsJobs ) )


  jobKeys = ( "SourceSE", "TargetSE", "Status", "Files", "Size", "Completness", "CreationTime", "SubmitTime", "LastUpdate", "Error" )

  fileKeys = ( "SourceSURL", "TargetSURL", "Attempt", "Status", "Error" )
  
  for i, ftsJob in enumerate( ftsJobs ) :
    DIRAC.gLogger.always( "[%d] FTSGUID %s" % ftsJob.FTSGUID )
    for key in jobKeys:
      DIRAC.gLogger.always( "\t%-20s: %s" % ( key, str( getattr( ftsJob, key ) ) ) )
    DIRAC.gLogger.info( "\tFiles:" )
    for j, ftsFile in enumerate( ftsJob ):
      DIRAC.gLogger.info( "[%02d] %s" % ( j, ftsFile.LFN ) )
      for key in fileKeys:
        DIRAC.gLogger.info( "\t%-20s: %s" % ( key, str( getattr( ftsJob, key ) ) ) )


      

