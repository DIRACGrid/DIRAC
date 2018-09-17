#!/usr/bin/env python

"""
  Add files to an existing transformation
"""

__RCSID__ = "$Id$"

import os
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s TransID <LFN | fileContainingLFNs>' % Script.scriptName
                                   ] ) )

Script.parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()
if len( args ) != 2:
  Script.showHelp()
  DIRAC.exit( 1 )

# get arguments
inputFileName = args[1]

lfns = []
if os.path.exists( inputFileName ):
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  inputFile.close()
  lfns.extend( [ lfn.strip() for lfn in string.splitlines() ] )
else:
  lfns.append( inputFileName )

tc = TransformationClient()
res = tc.addFilesToTransformation( args[0] , lfns )  # Files added here

if not res['OK']:
  DIRAC.gLogger.error ( res['Message'] )
  DIRAC.exit( 2 )

successfullyAdded = 0
alreadyPresent = 0
for lfn, message in res['Value']['Successful'].items():
  if message == 'Added':
    successfullyAdded += 1
  elif message == 'Present':
    alreadyPresent += 1

if successfullyAdded > 0:
  DIRAC.gLogger.notice( "Successfully added %d files" % successfullyAdded )
if alreadyPresent > 0:
  DIRAC.gLogger.notice( "Already present %d files" % alreadyPresent )
DIRAC.exit( 0 )
