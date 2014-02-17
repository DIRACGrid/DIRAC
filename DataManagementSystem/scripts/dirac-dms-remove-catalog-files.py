#!/usr/bin/env python
########################################################################
# $Header:  $
########################################################################
__RCSID__ = "$Id:  $"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Remove the given file or a list of files from the File Catalog

Usage:
   %s <LFN | fileContainingLFNs>
""" % Script.scriptName )

Script.parseCommandLine()

from DIRAC.Core.Utilities.List import sortList
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()
import os, sys

if len( sys.argv ) < 2:
  Script.showHelp()
  DIRAC.exit( -1 )
else:
  inputFileName = sys.argv[1]

if os.path.exists( inputFileName ):
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  lfns = [ lfn.strip() for lfn in string.splitlines() ]
  inputFile.close()
else:
  lfns = [inputFileName]

res = dm.removeCatalogFile( lfns )
if not res['OK']:
  print "Error:", res['Message']
  sys.exit()
for lfn in sortList( res['Value']['Failed'].keys() ):
  message = res['Value']['Failed'][lfn]
  print 'Error: failed to remove %s: %s' % ( lfn, message )
print 'Successfully removed %d catalog files.' % ( len( res['Value']['Successful'] ) )

