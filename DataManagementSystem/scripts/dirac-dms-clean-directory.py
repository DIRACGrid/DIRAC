#! /usr/bin/env python
########################################################################
# $HeadURL: $
########################################################################
__RCSID__ = "$Id:  $"

from DIRAC           import exit as DIRACExit, gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Clean the given directory or a list of directories by removing it and all the
contained files and subdirectories from the physical storage and from the
file catalogs.

Usage:
   %s <lfn | fileContainingLfns> <SE> <status>
""" % Script.scriptName )

Script.parseCommandLine()
import os, sys

args = Script.getPositionalArgs()
if len( args ) < 1:
  Script.showHelp()
  DIRACExit( -1 )
else:
  inputFileName = args[0]

if os.path.exists( inputFileName ):
  lfns = [lfn.strip().split()[0] for lfn in sorted( open( inputFileName, 'r' ).read().splitlines() )]
else:
  lfns = [inputFileName]

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()
for lfn in [lfn for lfn in lfns if lfn]:
  print "Cleaning directory %s ... " % lfn
  result = dm.cleanLogicalDirectory( lfn )
  if result['OK']:
    print 'OK'
  else:
    print "ERROR: %s" % result['Message']
