#! /usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
"""
  Submit an FTS request, monitor the execution until it completes
"""
__RCSID__ = "$Id:  $"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... sourceSE targetSE LFN1[,LFN2...]' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name or file containing LFNs',
                                     '  sourceSE: Valid DIRAC SE',
                                     '  targetSE: Valid DIRAC SE'] ) )

Script.parseCommandLine()
from DIRAC.DataManagementSystem.Client.FTSRequest import FTSRequest
import DIRAC
import os

args = Script.getPositionalArgs()

if not len( args ) == 3:
  Script.showHelp()
else:
  sourceSE = args.pop( 0 )
  targetSE = args.pop( 0 )

lfns = []
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
result = oFTSRequest.submit( monitor = True, printOutput = False )
if not result['OK']:
  DIRAC.gLogger.error( 'Failed to issue FTS Request', result['Message'] )
  DIRAC.exit( -1 )
