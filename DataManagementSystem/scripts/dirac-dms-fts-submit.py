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
                                     '  %s [option|cfgfile] ... LFN sourceSE targetSE' % Script.scriptName,
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
  inputFileName = args[0]
  sourceSE = args[1]
  targetSE = args[2]

if not os.path.exists( inputFileName ):
  lfns = [inputFileName]
else:
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  inputFile.close()
  lfns = string.splitlines()

oFTSRequest = FTSRequest()
oFTSRequest.setSourceSE( sourceSE )
oFTSRequest.setTargetSE( targetSE )

for lfn in lfns:
  oFTSRequest.setLFN( lfn )
result = oFTSRequest.submit( monitor = True, printOutput = False )
if not result['OK']:
  DIRAC.gLogger.error( 'Failed to issue FTS Request', result['Message'] )
  DIRAC.exit( -1 )
