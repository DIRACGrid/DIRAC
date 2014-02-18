#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
from DIRAC           import exit as DIRACExit
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Remove the given file replica or a list of file replicas from the File Catalog 
and from the storage.

Usage:
   %s <LFN | fileContainingLFNs> SE [SE]
""" % Script.scriptName )

Script.parseCommandLine()

from DIRAC.Core.Utilities.List                        import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()
import os, sys

if len( sys.argv ) < 3:
  Script.showHelp()
  DIRACExit( -1 )
else:
  inputFileName = sys.argv[1]
  storageElementNames = sys.argv[2:]

if os.path.exists( inputFileName ):
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  lfns = [ lfn.strip() for lfn in string.splitlines() ]
  inputFile.close()
else:
  lfns = [inputFileName]

for lfnList in breakListIntoChunks( sortList( lfns, True ), 500 ):
  for storageElementName in storageElementNames:
    res = dm.removeReplica( storageElementName, lfnList )
    if not res['OK']:
      print 'Error:', res['Message']
    for lfn in sortList( res['Value']['Successful'].keys() ):
      print 'Successfully removed %s replica of %s' % ( storageElementName, lfn )
    for lfn in sortList( res['Value']['Failed'].keys() ):
      message = res['Value']['Failed'][lfn]
      print 'Error: failed to remove %s replica of %s: %s' % ( storageElementName, lfn, message )

