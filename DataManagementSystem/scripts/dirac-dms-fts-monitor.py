#! /usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Monitor the status of the given FTS request

Usage:
   %s {server GUID | GIUD@server} [sourceSE targetSE lfn1 [lfn2]]
""" % Script.scriptName )

Script.parseCommandLine()


from DIRAC.DataManagementSystem.Client.FTSRequest     import FTSRequest
import DIRAC
import os

args = Script.getPositionalArgs()

sourceSE = ''
targetSE = ''
lfns = []
if not args:
  Script.showHelp()
  DIRAC.exit( 1 )
if '@' in args[0]:
  guid, server = args.pop( 0 ).replace( "'", "" ).split( '@' )
elif len( args ) > 1:
  server = args.pop( 0 )
  guid = args.pop( 0 )
else:
  Script.showHelp()
  DIRAC.exit( 1 )
if args:
  sourceSE = args.pop( 0 )
if args:
  targetSE = args.pop( 0 )
for arg in args:
  for lfn in arg.split( ',' ):
    if not os.path.exists( lfn ):
      lfns.append( lfn )
    else:
      inputFile = open( lfn, 'r' )
      string = inputFile.read()
      inputFile.close()
      lfns += string.splitlines()

oFTSRequest = FTSRequest()
oFTSRequest.setSourceSE( sourceSE )
oFTSRequest.setTargetSE( targetSE )
for lfn in lfns:
  oFTSRequest.setLFN( lfn )
oFTSRequest.setFTSGUID( guid )
oFTSRequest.setFTSServer( server )
result = oFTSRequest.monitor( untilTerminal = False, printOutput = True )
if not result['OK']:
  DIRAC.gLogger.error( 'Failed to issue FTS Request', result['Message'] )
  DIRAC.exit( -1 )
