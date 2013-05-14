#! /usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Monitor the status of the given FTS request

Usage:
   %s <lfn|fileOfLFN> sourceSE targetSE server GUID
""" % Script.scriptName )

Script.parseCommandLine()


from DIRAC.DataManagementSystem.Client.FTSRequest     import FTSRequest
import DIRAC
import os

args = Script.getPositionalArgs()

if not len( args ) == 5:
  Script.showHelp()
else:
  inputFileName = args[0]
  sourceSE = args[1]
  targetSE = args[2]
  server = args[3]
  guid = args[4]

if not os.path.exists( inputFileName ):
  lfns = [inputFileName]
else:
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  inputFile.close()
  lfns = string.splitlines()

oFTSRequest = FTSRequest()
for lfn in lfns:
  oFTSRequest.setLFN( lfn )
oFTSRequest.setFTSGUID( guid )
oFTSRequest.setSourceSE( sourceSE )
oFTSRequest.setTargetSE( targetSE )
oFTSRequest.setFTSServer( server )
result = oFTSRequest.monitor( untilTerminal = True, printOutput = True )
if not result['OK']:
  DIRAC.gLogger.error( 'Failed to issue FTS Request', result['Message'] )
  DIRAC.exit( -1 )
